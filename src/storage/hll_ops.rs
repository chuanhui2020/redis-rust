//! HyperLogLog 数据类型操作（PFADD/PFCOUNT/PFMERGE）

use super::*;

/// HyperLogLog 数据结构，使用 16384 个 6 位寄存器
/// 标准 Redis HLL 实现，每个寄存器存储 hash 值的前导零个数+1
#[derive(Debug, Clone)]
pub struct HyperLogLog {
    /// 16384 个寄存器，每个寄存器 6 位（值域 0-63），用 u8 存储
    pub registers: [u8; 16384],
}

impl HyperLogLog {
    /// 创建空的 HyperLogLog
    pub fn new() -> Self {
        Self {
            registers: [0; 16384],
        }
    }

    /// 添加一个元素，返回是否更新了任何寄存器
    pub fn add(&mut self, element: &str) -> bool {
        let hash = Self::hash64(element);
        let index = (hash & 0x3FFF) as usize; // 低 14 位
        let remaining = hash >> 14; // 高 50 位
        let rank = Self::rho(remaining);

        let old_val = self.registers[index];
        if rank > old_val {
            self.registers[index] = rank;
            true
        } else {
            false
        }
    }

    /// 64 位 hash 函数，使用简单混合
    fn hash64(s: &str) -> u64 {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};
        let mut hasher = DefaultHasher::new();
        s.hash(&mut hasher);
        hasher.finish()
    }

    /// 计算 50 位值的前导零个数 + 1
    fn rho(w: u64) -> u8 {
        // 高 50 位，最高位是第 49 位（从 0 开始）
        // 如果 w == 0，返回 50（所有位都是 0）
        if w == 0 {
            return 50;
        }
        // 计算前导零个数
        let leading_zeros = w.leading_zeros() as u8;
        // 高 50 位左移了 14 位，所以 leading_zeros 已经是在 64 位中的位置
        // 但我们只关心高 50 位，所以需要减去 14
        let rank = leading_zeros.saturating_sub(14) + 1;
        rank.min(50)
    }

    /// 估算基数
    pub fn count(&self) -> u64 {
        let m = 16384u64;
        let mut sum = 0.0f64;
        let mut zero_count = 0u64;

        for &reg in &self.registers {
            sum += 2.0f64.powi(-(reg as i32));
            if reg == 0 {
                zero_count += 1;
            }
        }

        // 调和平均数
        let raw_estimate = Self::alpha(m) * m as f64 * m as f64 / sum;

        // 小范围修正
        if raw_estimate <= 2.5 * m as f64 && zero_count != 0 {
            let small_estimate = m as f64 * (m as f64 / zero_count as f64).ln();
            return small_estimate.round() as u64;
        }

        // 大范围修正（当估算值非常大时）
        // 这里简化处理，不实现 64 位溢出修正
        raw_estimate.round() as u64
    }

    /// 获取 alpha 常数
    fn alpha(m: u64) -> f64 {
        // m = 16384
        0.7213 / (1.0 + 1.079 / m as f64)
    }

    /// 合并多个 HLL，每个寄存器取最大值
    pub fn merge(&mut self, others: &[&HyperLogLog]) {
        for other in others {
            for i in 0..16384 {
                if other.registers[i] > self.registers[i] {
                    self.registers[i] = other.registers[i];
                }
            }
        }
    }

    /// 将寄存器序列化为字节数组
    pub fn dump(&self) -> Vec<u8> {
        self.registers.to_vec()
    }

    /// 从字节数组恢复寄存器
    pub fn load(data: &[u8]) -> Result<Self> {
        if data.len() != 16384 {
            return Err(AppError::Storage(
                "HyperLogLog DUMP 数据长度错误".to_string(),
            ));
        }
        let mut registers = [0u8; 16384];
        registers.copy_from_slice(data);
        Ok(Self { registers })
    }
}

impl Default for HyperLogLog {
    fn default() -> Self {
        Self::new()
    }
}

impl StorageEngine {
    /// 添加元素到 HyperLogLog
    /// 返回 1 表示有寄存器被更新，0 表示没有更新
    pub fn pfadd(&self, key: &str, elements: &[String]) -> Result<i64> {
        self.evict_if_needed()?;
        let db = self.db();
        let mut map = db
            .inner
            .get_shard(key)
            .write()
            .map_err(|e| AppError::Storage(format!("锁中毒: {}", e)))?;

        let mut updated = false;
        match map.get_mut(key) {
            Some(v) => {
                if v.is_expired() {
                    map.remove(key);
                    let mut hll = HyperLogLog::new();
                    for element in elements {
                        if hll.add(element) {
                            updated = true;
                        }
                    }
                    map.insert(key.to_string(), Entry::new(StorageValue::HyperLogLog(hll)));
                } else {
                    Self::check_hll_type(&v.value)?;
                    match &mut v.value {
                        StorageValue::HyperLogLog(hll) => {
                            for element in elements {
                                if hll.add(element) {
                                    updated = true;
                                }
                            }
                        }
                        _ => unreachable!(),
                    }
                }
            }
            None => {
                let mut hll = HyperLogLog::new();
                for element in elements {
                    if hll.add(element) {
                        updated = true;
                    }
                }
                map.insert(key.to_string(), Entry::new(StorageValue::HyperLogLog(hll)));
            }
        }

        self.bump_version(key);
        drop(map);
        self.evict_if_needed()?;
        Ok(if updated { 1 } else { 0 })
    }

    /// 估算 HyperLogLog 的基数
    /// 支持单个 key 或多个 key 联合估算
    pub fn pfcount(&self, keys: &[String]) -> Result<u64> {
        let db = self.db();

        let mut merged = HyperLogLog::new();
        let mut has_data = false;

        for key in keys {
            let map = db
                .inner
                .get_shard(key)
                .read()
                .map_err(|e| AppError::Storage(format!("锁中毒: {}", e)))?;
            if let Some(v) = map.get(key)
                && !v.is_expired()
            {
                match &v.value {
                    StorageValue::HyperLogLog(hll) => {
                        merged.merge(&[hll]);
                        has_data = true;
                    }
                    _ => {
                        return Err(AppError::Storage(
                            "WRONGTYPE 操作对象持有的是错误类型的值".to_string(),
                        ));
                    }
                }
            }
        }

        if !has_data {
            return Ok(0);
        }

        Ok(merged.count())
    }

    /// 合并多个 HyperLogLog 到 destkey
    /// 每个寄存器取最大值
    pub fn pfmerge(&self, destkey: &str, sourcekeys: &[String]) -> Result<()> {
        self.evict_if_needed()?;
        let db = self.db();

        let mut merged = HyperLogLog::new();
        let mut has_data = false;

        for key in sourcekeys {
            let map = db
                .inner
                .get_shard(key)
                .write()
                .map_err(|e| AppError::Storage(format!("锁中毒: {}", e)))?;
            if let Some(v) = map.get(key)
                && !v.is_expired()
            {
                match &v.value {
                    StorageValue::HyperLogLog(hll) => {
                        merged.merge(&[hll]);
                        has_data = true;
                    }
                    _ => {
                        return Err(AppError::Storage(
                            "WRONGTYPE 操作对象持有的是错误类型的值".to_string(),
                        ));
                    }
                }
            }
        }

        if has_data {
            let mut dst_map = db
                .inner
                .get_shard(destkey)
                .write()
                .map_err(|e| AppError::Storage(format!("锁中毒: {}", e)))?;
            dst_map.insert(destkey.to_string(), Entry::new(StorageValue::HyperLogLog(merged)));
            self.bump_version(destkey);
            drop(dst_map);
        }

        self.evict_if_needed()?;
        Ok(())
    }

    // ---------- Geo 操作 ----------
}
