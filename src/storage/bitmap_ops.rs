//! Bitmap 数据类型操作（SETBIT/GETBIT/BITCOUNT/BITOP/BITFIELD）

use super::*;

/// 从字节数组中按大端序读取指定位域（返回无符号值）
fn read_bitfield_unsigned(bytes: &[u8], offset: usize, bits: usize) -> u64 {
    let mut value: u64 = 0;
    for i in 0..bits {
        let byte_idx = (offset + i) / 8;
        let bit_idx = 7 - ((offset + i) % 8); // 大端序：MSB 在前
        let bit = if byte_idx < bytes.len() {
            (bytes[byte_idx] >> bit_idx) & 1
        } else {
            0
        };
        value = (value << 1) | (bit as u64);
    }
    value
}

/// 将有符号位域值从无符号表示转换为 i64
fn unsigned_to_signed(value: u64, bits: usize) -> i64 {
    let sign_bit = 1u64 << (bits - 1);
    if value & sign_bit != 0 {
        // 负数：补码转换
        (value as i64) - (1i64 << bits)
    } else {
        value as i64
    }
}

/// 将 i64 转换为指定宽度的无符号表示
fn signed_to_unsigned(value: i64, bits: usize) -> u64 {
    let mask = if bits == 64 {
        !0u64
    } else {
        (1u64 << bits) - 1
    };
    (value as u64) & mask
}

/// 向字节数组中按大端序写入指定位域
fn write_bitfield(bytes: &mut Vec<u8>, offset: usize, bits: usize, value: u64) {
    let needed = (offset + bits).div_ceil(8);
    if bytes.len() < needed {
        bytes.resize(needed, 0);
    }
    for i in 0..bits {
        let byte_idx = (offset + i) / 8;
        let bit_idx = 7 - ((offset + i) % 8);
        let bit_pos = bits - 1 - i; // 从高位到低位
        let bit = ((value >> bit_pos) & 1) as u8;
        if bit == 1 {
            bytes[byte_idx] |= 1 << bit_idx;
        } else {
            bytes[byte_idx] &= !(1 << bit_idx);
        }
    }
}

/// 读取指定位域的值
fn read_field(bytes: &[u8], encoding: &BitFieldEncoding, offset: usize) -> i64 {
    let raw = read_bitfield_unsigned(bytes, offset, encoding.bits);
    if encoding.signed {
        unsigned_to_signed(raw, encoding.bits)
    } else {
        raw as i64
    }
}

/// 写入指定位域的值
fn write_field(bytes: &mut Vec<u8>, encoding: &BitFieldEncoding, offset: usize, value: i64) {
    let raw = if encoding.signed {
        signed_to_unsigned(value, encoding.bits)
    } else {
        (value as u64) & ((1u64 << encoding.bits) - 1)
    };
    write_bitfield(bytes, offset, encoding.bits, raw);
}

/// 执行 INCRBY 并处理溢出
fn incr_with_overflow(
    old_value: i64,
    increment: i64,
    encoding: &BitFieldEncoding,
    overflow: BitFieldOverflow,
) -> BitFieldResult {
    let new_value = old_value.saturating_add(increment);
    let max_val = encoding.max_value();
    let min_val = encoding.min_value();

    match overflow {
        BitFieldOverflow::Wrap => {
            // 回绕：直接截断
            let wrapped = if encoding.signed {
                let range = 1i64 << encoding.bits;
                let mut v = new_value % range;
                if v < min_val {
                    v += range;
                } else if v > max_val {
                    v -= range;
                }
                v
            } else {
                let mask = (1u64 << encoding.bits) - 1;
                ((new_value as u64) & mask) as i64
            };
            BitFieldResult::Value(wrapped)
        }
        BitFieldOverflow::Sat => {
            // 饱和
            if new_value > max_val {
                BitFieldResult::Value(max_val)
            } else if new_value < min_val {
                BitFieldResult::Value(min_val)
            } else {
                BitFieldResult::Value(new_value)
            }
        }
        BitFieldOverflow::Fail => {
            // 失败：溢出时返回 nil
            if new_value > max_val || new_value < min_val {
                BitFieldResult::Nil
            } else {
                BitFieldResult::Value(new_value)
            }
        }
    }
}

impl StorageEngine {
    pub fn setbit(&self, key: &str, offset: usize, value: bool) -> Result<i64> {
        self.evict_if_needed()?;
        let db = self.db();
        let mut map = db
            .inner
            .get_shard(key)
            .write()
            .map_err(|e| AppError::Storage(format!("锁中毒: {}", e)))?;

        let mut bytes = match map.get(key) {
            Some(v) => {
                if Self::is_key_expired(&db, key) {
                    map.remove(key);
                    let mut expires = db.expires.get_shard(key).write().unwrap();
                    expires.remove(key);
                    Vec::new()
                } else {
                    match v {
                        StorageValue::String(b) => b.to_vec(),
                        StorageValue::List(_)
                        | StorageValue::Hash(_)
                        | StorageValue::Set(_)
                        | StorageValue::ZSet(_)
                        | StorageValue::HyperLogLog(_)
                        | StorageValue::Stream(_) => {
                            return Err(AppError::Storage(
                                "WRONGTYPE 操作对象持有的是错误类型的值".to_string(),
                            ));
                        }
                    }
                }
            }
            None => Vec::new(),
        };

        let byte_index = offset / 8;
        let bit_index = 7 - (offset % 8);

        if byte_index >= bytes.len() {
            bytes.resize(byte_index + 1, 0);
        }

        let old_bit = (bytes[byte_index] >> bit_index) & 1;
        if value {
            bytes[byte_index] |= 1 << bit_index;
        } else {
            bytes[byte_index] &= !(1 << bit_index);
        }

        map.insert(key.to_string(), StorageValue::String(Bytes::from(bytes)));
        let mut expires = db.expires.get_shard(key).write().unwrap();
        expires.remove(key);
        self.bump_version(key);
        self.touch(key);
        drop(map);
        self.evict_if_needed()?;
        Ok(old_bit as i64)
    }

    /// 获取指定偏移量的位值
    /// 键不存在或偏移量超出范围返回 0
    pub fn getbit(&self, key: &str, offset: usize) -> Result<i64> {
        let db = self.db();
        let map = db
            .inner
            .get_shard(key)
            .read()
            .map_err(|e| AppError::Storage(format!("锁中毒: {}", e)))?;

        match map.get(key) {
            Some(v) => {
                if Self::is_key_expired(&db, key) {
                    Ok(0)
                } else {
                    match v {
                        StorageValue::String(b) => {
                            let byte_index = offset / 8;
                            if byte_index >= b.len() {
                                Ok(0)
                            } else {
                                let bit_index = 7 - (offset % 8);
                                Ok(((b[byte_index] >> bit_index) & 1) as i64)
                            }
                        }
                        StorageValue::List(_)
                        | StorageValue::Hash(_)
                        | StorageValue::Set(_)
                        | StorageValue::ZSet(_)
                        | StorageValue::HyperLogLog(_)
                        | StorageValue::Stream(_) => Err(AppError::Storage(
                            "WRONGTYPE 操作对象持有的是错误类型的值".to_string(),
                        )),
                    }
                }
            }
            None => Ok(0),
        }
    }

    /// 统计值为 1 的位数
    /// is_byte 为 true 时 start/end 是字节索引，否则是位索引
    /// start/end 支持负数索引（从末尾开始）
    pub fn bitcount(&self, key: &str, start: isize, end: isize, is_byte: bool) -> Result<usize> {
        let db = self.db();
        let mut map = db
            .inner
            .get_shard(key)
            .write()
            .map_err(|e| AppError::Storage(format!("锁中毒: {}", e)))?;

        let bytes = match map.get(key) {
            Some(v) => {
                if Self::is_key_expired(&db, key) {
                    map.remove(key);
                    let mut expires = db.expires.get_shard(key).write().unwrap();
                    expires.remove(key);
                    return Ok(0);
                } else {
                    match v {
                        StorageValue::String(b) => b.clone(),
                        StorageValue::List(_)
                        | StorageValue::Hash(_)
                        | StorageValue::Set(_)
                        | StorageValue::ZSet(_)
                        | StorageValue::HyperLogLog(_)
                        | StorageValue::Stream(_) => {
                            return Err(AppError::Storage(
                                "WRONGTYPE 操作对象持有的是错误类型的值".to_string(),
                            ));
                        }
                    }
                }
            }
            None => return Ok(0),
        };

        let len = bytes.len();
        if len == 0 {
            return Ok(0);
        }

        if is_byte {
            let mut s = start;
            let mut e = end;
            if s < 0 {
                s += len as isize;
            }
            if e < 0 {
                e += len as isize;
            }
            s = s.max(0);
            e = e.min(len as isize - 1);
            if s > e {
                return Ok(0);
            }
            let mut count = 0usize;
            for i in s..=e {
                count += bytes[i as usize].count_ones() as usize;
            }
            Ok(count)
        } else {
            let total_bits = len * 8;
            let mut s = start;
            let mut e = end;
            if s < 0 {
                s += total_bits as isize;
            }
            if e < 0 {
                e += total_bits as isize;
            }
            s = s.max(0);
            e = e.min(total_bits as isize - 1);
            if s > e {
                return Ok(0);
            }
            let mut count = 0usize;
            for bit_pos in s..=e {
                let byte_index = (bit_pos / 8) as usize;
                let bit_index = 7 - (bit_pos % 8);
                count += ((bytes[byte_index] >> bit_index) & 1) as usize;
            }
            Ok(count)
        }
    }

    /// 对多个 key 做位运算，结果存入 destkey
    /// 返回结果字节长度
    pub fn bitop(&self, operation: &str, destkey: &str, keys: &[String]) -> Result<usize> {
        self.evict_if_needed()?;
        let db = self.db();

        // 收集所有有效字符串的字节数组
        let mut byte_arrays: Vec<Vec<u8>> = Vec::new();
        let mut max_len = 0usize;

        for key in keys {
            let map = db
                .inner
                .get_shard(key)
                .write()
                .map_err(|e| AppError::Storage(format!("锁中毒: {}", e)))?;
            if let Some(v) = map.get(key)
                && !Self::is_key_expired(&db, key)
            {
                match v {
                    StorageValue::String(b) => {
                        let vec = b.to_vec();
                        max_len = max_len.max(vec.len());
                        byte_arrays.push(vec);
                    }
                    _ => {}
                }
            }
        }

        if byte_arrays.is_empty() {
            let mut dst_map = db
                .inner
                .get_shard(destkey)
                .write()
                .map_err(|e| AppError::Storage(format!("锁中毒: {}", e)))?;
            dst_map.remove(destkey);
            self.bump_version(destkey);
            return Ok(0);
        }

        let op = operation.to_ascii_uppercase();
        let mut result = vec![0u8; max_len];

        if op == "NOT" {
            // NOT 只接受一个 key
            let src = &byte_arrays[0];
            for i in 0..max_len {
                result[i] = !src[i];
            }
        } else {
            // AND / OR / XOR
            for i in 0..max_len {
                let mut val = if op == "AND" { 0xFFu8 } else { 0u8 };
                for arr in &byte_arrays {
                    let byte = if i < arr.len() { arr[i] } else { 0 };
                    match op.as_str() {
                        "AND" => val &= byte,
                        "OR" => val |= byte,
                        "XOR" => val ^= byte,
                        _ => {}
                    }
                }
                result[i] = val;
            }
        }

        let mut dst_map = db
            .inner
            .get_shard(destkey)
            .write()
            .map_err(|e| AppError::Storage(format!("锁中毒: {}", e)))?;
        dst_map.insert(
            destkey.to_string(),
            StorageValue::String(Bytes::from(result)),
        );
        let mut expires = db.expires.get_shard(destkey).write().unwrap();
        expires.remove(destkey);
        self.bump_version(destkey);
        self.touch(destkey);
        drop(dst_map);
        self.evict_if_needed()?;
        Ok(max_len)
    }

    /// 查找第一个值为 bit（0 或 1）的位的位置
    /// is_byte 为 true 时 start/end 是字节索引，否则是位索引
    pub fn bitpos(
        &self,
        key: &str,
        bit: u8,
        start: isize,
        end: isize,
        is_byte: bool,
    ) -> Result<i64> {
        let db = self.db();
        let mut map = db
            .inner
            .get_shard(key)
            .write()
            .map_err(|e| AppError::Storage(format!("锁中毒: {}", e)))?;

        let bytes = match map.get(key) {
            Some(v) => {
                if Self::is_key_expired(&db, key) {
                    map.remove(key);
                    let mut expires = db.expires.get_shard(key).write().unwrap();
                    expires.remove(key);
                    return Ok(-1);
                } else {
                    match v {
                        StorageValue::String(b) => b.clone(),
                        StorageValue::List(_)
                        | StorageValue::Hash(_)
                        | StorageValue::Set(_)
                        | StorageValue::ZSet(_)
                        | StorageValue::HyperLogLog(_)
                        | StorageValue::Stream(_) => {
                            return Err(AppError::Storage(
                                "WRONGTYPE 操作对象持有的是错误类型的值".to_string(),
                            ));
                        }
                    }
                }
            }
            None => return Ok(if bit == 1 { -1 } else { 0 }),
        };

        let len = bytes.len();
        if len == 0 {
            return Ok(if bit == 1 { -1 } else { 0 });
        }

        let total_bits = len * 8;
        let (mut s, mut e) = if is_byte {
            let mut s = start;
            let mut e_byte = end;
            if s < 0 {
                s += len as isize;
            }
            if e_byte < 0 {
                e_byte += len as isize;
            }
            s = s.max(0);
            e_byte = e_byte.min(len as isize - 1);
            if s > e_byte {
                return Ok(-1);
            }
            (s * 8, e_byte * 8 + 7)
        } else {
            let mut s = start;
            let mut e_bit = end;
            if s < 0 {
                s += total_bits as isize;
            }
            if e_bit < 0 {
                e_bit += total_bits as isize;
            }
            s = s.max(0);
            e_bit = e_bit.min(total_bits as isize - 1);
            if s > e_bit {
                return Ok(-1);
            }
            (s, e_bit)
        };

        s = s.max(0).min(total_bits as isize - 1);
        e = e.max(0).min(total_bits as isize - 1);

        for bit_pos in s..=e {
            let byte_index = (bit_pos / 8) as usize;
            let bit_index = 7 - (bit_pos % 8);
            let current_bit = (bytes[byte_index] >> bit_index) & 1;
            if i64::from(current_bit) == bit as i64 {
                return Ok(bit_pos as i64);
            }
        }

        Ok(-1)
    }

    /// 执行 BITFIELD 操作
    /// 返回每个操作的结果数组
    pub fn bitfield(&self, key: &str, ops: &[BitFieldOp]) -> Result<Vec<BitFieldResult>> {
        self.evict_if_needed()?;
        let db = self.db();
        let mut map = db
            .inner
            .get_shard(key)
            .write()
            .map_err(|e| AppError::Storage(format!("锁中毒: {}", e)))?;

        let mut bytes = match map.get(key) {
            Some(v) => {
                if Self::is_key_expired(&db, key) {
                    map.remove(key);
                    let mut expires = db.expires.get_shard(key).write().unwrap();
                    expires.remove(key);
                    Vec::new()
                } else {
                    match v {
                        StorageValue::String(b) => b.to_vec(),
                        _ => {
                            return Err(AppError::Storage(
                                "WRONGTYPE 操作对象持有的是错误类型的值".to_string(),
                            ));
                        }
                    }
                }
            }
            None => Vec::new(),
        };

        let mut results = Vec::new();
        let mut overflow = BitFieldOverflow::Wrap;
        let mut modified = false;

        for op in ops {
            match op {
                BitFieldOp::Overflow(o) => {
                    overflow = *o;
                }
                BitFieldOp::Get(enc, off) => {
                    let offset = off.resolve(enc);
                    let value = read_field(&bytes, enc, offset);
                    results.push(BitFieldResult::Value(value));
                }
                BitFieldOp::Set(enc, off, value) => {
                    let offset = off.resolve(enc);
                    let old_value = read_field(&bytes, enc, offset);
                    write_field(&mut bytes, enc, offset, *value);
                    results.push(BitFieldResult::Value(old_value));
                    modified = true;
                }
                BitFieldOp::IncrBy(enc, off, increment) => {
                    let offset = off.resolve(enc);
                    let old_value = read_field(&bytes, enc, offset);
                    let result = incr_with_overflow(old_value, *increment, enc, overflow);
                    match result {
                        BitFieldResult::Value(new_value) => {
                            write_field(&mut bytes, enc, offset, new_value);
                            results.push(BitFieldResult::Value(new_value));
                        }
                        BitFieldResult::Nil => {
                            results.push(BitFieldResult::Nil);
                        }
                    }
                    modified = true;
                }
            }
        }

        if modified {
            map.insert(key.to_string(), StorageValue::String(Bytes::from(bytes)));
            let mut expires = db.expires.get_shard(key).write().unwrap();
            expires.remove(key);
            self.bump_version(key);
            self.touch(key);
        }

        Ok(results)
    }

    /// 执行 BITFIELD_RO 操作（只读版本，只支持 GET）
    pub fn bitfield_ro(&self, key: &str, ops: &[BitFieldOp]) -> Result<Vec<BitFieldResult>> {
        let db = self.db();
        let map = db
            .inner
            .get_shard(key)
            .read()
            .map_err(|e| AppError::Storage(format!("锁中毒: {}", e)))?;

        let bytes = match map.get(key) {
            Some(v) => {
                if Self::is_key_expired(&db, key) {
                    return Ok(vec![BitFieldResult::Value(0); ops.len()]);
                }
                match v {
                    StorageValue::String(b) => b.to_vec(),
                    _ => {
                        return Err(AppError::Storage(
                            "WRONGTYPE 操作对象持有的是错误类型的值".to_string(),
                        ));
                    }
                }
            }
            None => Vec::new(),
        };

        let mut results = Vec::new();
        for op in ops {
            match op {
                BitFieldOp::Get(enc, off) => {
                    let offset = off.resolve(enc);
                    let value = read_field(&bytes, enc, offset);
                    results.push(BitFieldResult::Value(value));
                }
                _ => {
                    return Err(AppError::Command("BITFIELD_RO 只支持 GET 操作".to_string()));
                }
            }
        }
        Ok(results)
    }

    // ---------- HyperLogLog 操作 ----------
}
