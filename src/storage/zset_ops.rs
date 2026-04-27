//! Sorted Set 数据类型操作（对标 Redis ZSet 命令族）

use super::*;

#[derive(Debug, Clone)]
pub struct ZSetData {
    /// member -> score
    pub member_to_score: HashMap<String, f64>,
    /// (score, member) -> ()，按分数排序
    pub score_to_member: BTreeMap<(OrderedFloat<f64>, String), ()>,
}

impl ZSetData {
    pub fn new() -> Self {
        Self {
            member_to_score: HashMap::new(),
            score_to_member: BTreeMap::new(),
        }
    }

    /// 添加或更新成员的分数
    /// 返回 true 表示新成员，false 表示更新已有成员
    pub fn add(&mut self, member: String, score: f64) -> bool {
        let is_new = if let Some(old_score) = self.member_to_score.get(&member) {
            let old_key = (OrderedFloat(*old_score), member.clone());
            self.score_to_member.remove(&old_key);
            false
        } else {
            true
        };
        self.member_to_score.insert(member.clone(), score);
        let new_key = (OrderedFloat(score), member);
        self.score_to_member.insert(new_key, ());
        is_new
    }

    /// 删除成员，返回是否删除成功
    pub fn remove(&mut self, member: &str) -> bool {
        if let Some(score) = self.member_to_score.remove(member) {
            let key = (OrderedFloat(score), member.to_string());
            self.score_to_member.remove(&key);
            true
        } else {
            false
        }
    }

    /// 获取成员的分数
    pub fn score(&self, member: &str) -> Option<f64> {
        self.member_to_score.get(member).copied()
    }

    /// 获取成员的排名（从 0 开始，按分数升序）
    pub fn rank(&self, member: &str) -> Option<usize> {
        let score = self.member_to_score.get(member)?;
        let key = (OrderedFloat(*score), member.to_string());
        self.score_to_member.keys().position(|k| *k == key)
    }

    /// 按排名范围获取成员
    pub fn range_by_rank(&self, start: isize, stop: isize) -> Vec<(String, f64)> {
        let len = self.score_to_member.len() as isize;
        let mut s = start;
        let mut e = stop;
        if s < 0 {
            s += len;
        }
        if e < 0 {
            e += len;
        }
        s = s.max(0);
        e = e.min(len - 1);
        if s > e {
            return vec![];
        }
        self.score_to_member
            .keys()
            .skip(s as usize)
            .take((e - s + 1) as usize)
            .map(|(score, member)| (member.clone(), score.into_inner()))
            .collect()
    }

    /// 按分数范围获取成员
    pub fn range_by_score(&self, min: f64, max: f64) -> Vec<(String, f64)> {
        let min_key = (OrderedFloat(min), String::new());
        let max_key = (OrderedFloat(max), String::from("\u{10FFFF}"));
        self.score_to_member
            .range(min_key..=max_key)
            .map(|((score, member), _)| (member.clone(), score.into_inner()))
            .collect()
    }

    /// 获取成员的降序排名（从 0 开始，按分数降序）
    pub fn rev_rank(&self, member: &str) -> Option<usize> {
        let rank = self.rank(member)?;
        Some(self.member_to_score.len().saturating_sub(1) - rank)
    }

    /// 按排名范围获取成员（降序）
    pub fn rev_range_by_rank(&self, start: isize, stop: isize) -> Vec<(String, f64)> {
        let len = self.score_to_member.len() as isize;
        let mut s = start;
        let mut e = stop;
        if s < 0 {
            s += len;
        }
        if e < 0 {
            e += len;
        }
        s = s.max(0);
        e = e.min(len - 1);
        if s > e {
            return vec![];
        }
        self.score_to_member
            .keys()
            .rev()
            .skip(s as usize)
            .take((e - s + 1) as usize)
            .map(|(score, member)| (member.clone(), score.into_inner()))
            .collect()
    }

    /// 按分数范围计数
    pub fn count_by_score(&self, min: f64, max: f64) -> usize {
        self.range_by_score(min, max).len()
    }

    /// 弹出分数最小的成员
    pub fn pop_min(&mut self, count: usize) -> Vec<(String, f64)> {
        let to_pop: Vec<(String, f64)> = self
            .score_to_member
            .keys()
            .take(count)
            .map(|(score, member)| (member.clone(), score.into_inner()))
            .collect();
        for (member, _) in &to_pop {
            self.remove(member);
        }
        to_pop
    }

    /// 弹出分数最大的成员
    pub fn pop_max(&mut self, count: usize) -> Vec<(String, f64)> {
        let to_pop: Vec<(String, f64)> = self
            .score_to_member
            .keys()
            .rev()
            .take(count)
            .map(|(score, member)| (member.clone(), score.into_inner()))
            .collect();
        for (member, _) in &to_pop {
            self.remove(member);
        }
        to_pop
    }

    /// 检查所有成员是否具有相同分数
    pub fn all_same_score(&self) -> bool {
        if self.member_to_score.len() <= 1 {
            return true;
        }
        let first_score = self.score_to_member.keys().next().unwrap().0;
        self.score_to_member.keys().all(|(s, _)| *s == first_score)
    }

    /// 按字典序范围获取成员（要求所有成员具有相同分数）
    /// min/max 格式: "[member" 或 "(member" 或 "-" 或 "+"
    pub fn range_by_lex(&self, min: &str, max: &str) -> Result<Vec<String>> {
        if !self.all_same_score() {
            return Err(AppError::Storage(
                "ZRANGEBYLEX 要求所有成员具有相同分数".to_string(),
            ));
        }
        let (min_inclusive, min_val) = parse_lex_bound(min);
        let (max_inclusive, max_val) = parse_lex_bound(max);

        let mut result = Vec::new();
        for (_, member) in self.score_to_member.keys() {
            let pass_min = match &min_val {
                None => true, // "-"
                Some(v) => {
                    if min_inclusive {
                        member >= v
                    } else {
                        member > v
                    }
                }
            };
            let pass_max = match &max_val {
                None => true, // "+"
                Some(v) => {
                    if max_inclusive {
                        member <= v
                    } else {
                        member < v
                    }
                }
            };
            if pass_min && pass_max {
                result.push(member.clone());
            }
        }
        Ok(result)
    }

    /// 按字典序范围获取成员（降序，要求所有成员具有相同分数）
    pub fn rev_range_by_lex(&self, min: &str, max: &str) -> Result<Vec<String>> {
        if !self.all_same_score() {
            return Err(AppError::Storage(
                "ZREVRANGEBYLEX 要求所有成员具有相同分数".to_string(),
            ));
        }
        let (min_inclusive, min_val) = parse_lex_bound(min);
        let (max_inclusive, max_val) = parse_lex_bound(max);

        let mut result = Vec::new();
        for (_, member) in self.score_to_member.keys().rev() {
            let pass_min = match &min_val {
                None => true, // "-"
                Some(v) => {
                    if min_inclusive {
                        member >= v
                    } else {
                        member > v
                    }
                }
            };
            let pass_max = match &max_val {
                None => true, // "+"
                Some(v) => {
                    if max_inclusive {
                        member <= v
                    } else {
                        member < v
                    }
                }
            };
            if pass_min && pass_max {
                result.push(member.clone());
            }
        }
        Ok(result)
    }

    /// 按字典序范围计数（要求所有成员具有相同分数）
    pub fn lex_count(&self, min: &str, max: &str) -> Result<usize> {
        Ok(self.range_by_lex(min, max)?.len())
    }

    /// 随机返回成员
    /// count > 0: 不重复返回最多 count 个
    /// count < 0: 允许重复返回 |count| 个
    pub fn rand_member(&self, count: i64, _with_scores: bool) -> Vec<(String, f64)> {
        use rand::seq::SliceRandom;
        let members: Vec<(String, f64)> = self
            .score_to_member
            .keys()
            .map(|(score, member)| (member.clone(), score.into_inner()))
            .collect();
        if members.is_empty() {
            return vec![];
        }
        if count > 0 {
            let n = count as usize;
            let mut rng = rand::thread_rng();
            let mut selected = members
                .choose_multiple(&mut rng, n)
                .cloned()
                .collect::<Vec<_>>();
            selected.shuffle(&mut rng);
            selected
        } else {
            let n = count.unsigned_abs() as usize;
            let mut rng = rand::thread_rng();
            (0..n)
                .map(|_| members.choose(&mut rng).unwrap().clone())
                .collect()
        }
    }
}

// 解析 ZRANGEBYLEX 的范围边界
// 返回 (是否包含, 边界值)
// ---------- BITFIELD 类型定义 ----------

/// BITFIELD 编码类型
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct BitFieldEncoding {
    /// 是否有符号
    pub signed: bool,
    /// 位宽（1-64）
    pub bits: usize,
}

/// BITFIELD 偏移量
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum BitFieldOffset {
    /// 绝对位偏移（如 0, 8, 16）
    Num(usize),
    /// 类型宽度的倍数偏移（如 #0, #1, #2）
    Hash(usize),
}

/// BITFIELD 溢出策略
#[derive(Debug, Clone, Copy, PartialEq, Default)]
pub enum BitFieldOverflow {
    /// 回绕（默认）
    #[default]
    Wrap,
    /// 饱和
    Sat,
    /// 失败
    Fail,
}

/// BITFIELD 子操作
#[derive(Debug, Clone, PartialEq)]
pub enum BitFieldOp {
    /// GET type offset
    Get(BitFieldEncoding, BitFieldOffset),
    /// SET type offset value
    Set(BitFieldEncoding, BitFieldOffset, i64),
    /// INCRBY type offset increment
    IncrBy(BitFieldEncoding, BitFieldOffset, i64),
    /// OVERFLOW WRAP|SAT|FAIL
    Overflow(BitFieldOverflow),
}

/// BITFIELD 执行结果
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum BitFieldResult {
    /// 成功返回整数值
    Value(i64),
    /// FAIL 溢出时返回 nil
    Nil,
}

impl BitFieldEncoding {
    /// 解析编码字符串，如 "i8", "u16", "u63"
    pub fn parse(s: &str) -> Result<Self> {
        let s = s.to_ascii_lowercase();
        if s.len() < 2 {
            return Err(AppError::Command("BITFIELD 编码格式错误".to_string()));
        }
        let signed = match s.chars().next() {
            Some('i') => true,
            Some('u') => false,
            _ => {
                return Err(AppError::Command(
                    "BITFIELD 编码必须以 i 或 u 开头".to_string(),
                ));
            }
        };
        let bits: usize = s[1..]
            .parse()
            .map_err(|_| AppError::Command("BITFIELD 编码位宽必须是整数".to_string()))?;
        if bits == 0 || bits > 64 {
            return Err(AppError::Command(
                "BITFIELD 编码位宽必须在 1-64 之间".to_string(),
            ));
        }
        if !signed && bits == 64 {
            return Err(AppError::Command(
                "BITFIELD 无符号编码最大支持 u63".to_string(),
            ));
        }
        Ok(BitFieldEncoding { signed, bits })
    }

    /// 计算最大值
    pub fn max_value(&self) -> i64 {
        if self.signed {
            (1i64 << (self.bits - 1)) - 1
        } else {
            ((1u64 << self.bits) - 1) as i64
        }
    }

    /// 计算最小值
    pub fn min_value(&self) -> i64 {
        if self.signed {
            -(1i64 << (self.bits - 1))
        } else {
            0
        }
    }
}

impl BitFieldOffset {
    /// 解析偏移字符串，如 "0", "#1", "#2"
    pub fn parse(s: &str) -> Result<Self> {
        if let Some(num_str) = s.strip_prefix('#') {
            let num: usize = num_str
                .parse()
                .map_err(|_| AppError::Command("BITFIELD #偏移必须是整数".to_string()))?;
            Ok(BitFieldOffset::Hash(num))
        } else {
            let num: usize = s
                .parse()
                .map_err(|_| AppError::Command("BITFIELD 偏移必须是整数".to_string()))?;
            Ok(BitFieldOffset::Num(num))
        }
    }

    /// 计算实际位偏移
    pub fn resolve(&self, encoding: &BitFieldEncoding) -> usize {
        match self {
            BitFieldOffset::Num(n) => *n,
            BitFieldOffset::Hash(n) => n * encoding.bits,
        }
    }
}

/// 从字节数组中按大端序读取指定位域（返回无符号值）
#[allow(dead_code)]
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
#[allow(dead_code)]
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
#[allow(dead_code)]
fn signed_to_unsigned(value: i64, bits: usize) -> u64 {
    let mask = if bits == 64 {
        !0u64
    } else {
        (1u64 << bits) - 1
    };
    (value as u64) & mask
}

/// 向字节数组中按大端序写入指定位域
#[allow(dead_code)]
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
#[allow(dead_code)]
fn read_field(bytes: &[u8], encoding: &BitFieldEncoding, offset: usize) -> i64 {
    let raw = read_bitfield_unsigned(bytes, offset, encoding.bits);
    if encoding.signed {
        unsigned_to_signed(raw, encoding.bits)
    } else {
        raw as i64
    }
}

/// 写入指定位域的值
#[allow(dead_code)]
fn write_field(bytes: &mut Vec<u8>, encoding: &BitFieldEncoding, offset: usize, value: i64) {
    let raw = if encoding.signed {
        signed_to_unsigned(value, encoding.bits)
    } else {
        (value as u64) & ((1u64 << encoding.bits) - 1)
    };
    write_bitfield(bytes, offset, encoding.bits, raw);
}

/// 执行 INCRBY 并处理溢出
#[allow(dead_code)]
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

impl Default for ZSetData {
    fn default() -> Self {
        Self::new()
    }
}

fn parse_lex_bound(bound: &str) -> (bool, Option<String>) {
    if bound == "-" {
        return (true, None);
    }
    if bound == "+" {
        return (true, None);
    }
    if let Some(rest) = bound.strip_prefix('[') {
        return (true, Some(rest.to_string()));
    }
    if let Some(rest) = bound.strip_prefix('(') {
        return (false, Some(rest.to_string()));
    }
    // 默认按包含处理
    (true, Some(bound.to_string()))
}

impl StorageEngine {
    pub fn zadd(&self, key: &str, pairs: Vec<(f64, String)>) -> Result<i64> {
        self.evict_if_needed()?;
        let db = self.db();
        let mut map = db
            .inner
            .get_shard(key)
            .write()
            .map_err(|e| AppError::Storage(format!("锁中毒: {}", e)))?;

        match map.get_mut(key) {
            Some(v) => {
                if v.is_expired() {
                    map.remove(key);
                    let mut zset = ZSetData::new();
                    let mut count = 0i64;
                    for (score, member) in pairs {
                        if zset.add(member, score) {
                            count += 1;
                        }
                    }
                    map.insert(key.to_string(), Entry::new(StorageValue::ZSet(zset)));
                    self.notify_blocking_waiters(key);
                    Ok(count)
                } else {
                    Self::check_zset_type(&v.value)?;
                    let zset = Self::as_zset_mut(&mut v.value).unwrap();
                    let mut count = 0i64;
                    for (score, member) in pairs {
                        if zset.add(member, score) {
                            count += 1;
                        }
                    }
                    self.notify_blocking_waiters(key);
                    Ok(count)
                }
            }
            None => {
                let mut zset = ZSetData::new();
                let mut count = 0i64;
                for (score, member) in pairs {
                    if zset.add(member, score) {
                        count += 1;
                    }
                }
                map.insert(key.to_string(), Entry::new(StorageValue::ZSet(zset)));
                self.notify_blocking_waiters(key);
                Ok(count)
            }
        }
    }

    /// 从有序集合中删除一个或多个成员
    /// 返回实际删除的成员数量
    pub fn zrem(&self, key: &str, members: &[String]) -> Result<i64> {
        self.evict_if_needed()?;
        let db = self.db();
        let mut map = db
            .inner
            .get_shard(key)
            .write()
            .map_err(|e| AppError::Storage(format!("锁中毒: {}", e)))?;

        match map.get_mut(key) {
            Some(v) => {
                if v.is_expired() {
                    map.remove(key);
                    Ok(0)
                } else {
                    Self::check_zset_type(&v.value)?;
                    let zset = Self::as_zset_mut(&mut v.value).unwrap();
                    let mut count = 0i64;
                    for member in members {
                        if zset.remove(member) {
                            count += 1;
                        }
                    }
                    if zset.member_to_score.is_empty() {
                        map.remove(key);
                    }
                    Ok(count)
                }
            }
            None => Ok(0),
        }
    }

    /// 返回有序集合中成员的分数
    pub fn zscore(&self, key: &str, member: &str) -> Result<Option<f64>> {
        let db = self.db();
        let mut map = db
            .inner
            .get_shard(key)
            .write()
            .map_err(|e| AppError::Storage(format!("锁中毒: {}", e)))?;

        match map.get(key) {
            Some(v) => {
                if v.is_expired() {
                    map.remove(key);
                    Ok(None)
                } else {
                    Self::check_zset_type(&v.value)?;
                    match &v.value {
                        StorageValue::ZSet(z) => Ok(z.score(member)),
                        _ => unreachable!(),
                    }
                }
            }
            None => Ok(None),
        }
    }

    /// 返回有序集合中成员的排名（从 0 开始，按分数升序）
    pub fn zrank(&self, key: &str, member: &str) -> Result<Option<usize>> {
        let db = self.db();
        let mut map = db
            .inner
            .get_shard(key)
            .write()
            .map_err(|e| AppError::Storage(format!("锁中毒: {}", e)))?;

        match map.get(key) {
            Some(v) => {
                if v.is_expired() {
                    map.remove(key);
                    Ok(None)
                } else {
                    Self::check_zset_type(&v.value)?;
                    match &v.value {
                        StorageValue::ZSet(z) => Ok(z.rank(member)),
                        _ => unreachable!(),
                    }
                }
            }
            None => Ok(None),
        }
    }

    /// 按排名范围返回有序集合中的成员
    /// with_scores 为 true 时同时返回分数
    pub fn zrange(
        &self,
        key: &str,
        start: isize,
        stop: isize,
        _with_scores: bool,
    ) -> Result<Vec<(String, f64)>> {
        let db = self.db();
        let mut map = db
            .inner
            .get_shard(key)
            .write()
            .map_err(|e| AppError::Storage(format!("锁中毒: {}", e)))?;

        match map.get(key) {
            Some(v) => {
                if v.is_expired() {
                    map.remove(key);
                    Ok(vec![])
                } else {
                    Self::check_zset_type(&v.value)?;
                    match &v.value {
                        StorageValue::ZSet(z) => Ok(z.range_by_rank(start, stop)),
                        _ => unreachable!(),
                    }
                }
            }
            None => Ok(vec![]),
        }
    }

    /// 按分数范围返回有序集合中的成员
    /// with_scores 为 true 时同时返回分数
    pub fn zrangebyscore(
        &self,
        key: &str,
        min: f64,
        max: f64,
        _with_scores: bool,
    ) -> Result<Vec<(String, f64)>> {
        let db = self.db();
        let mut map = db
            .inner
            .get_shard(key)
            .write()
            .map_err(|e| AppError::Storage(format!("锁中毒: {}", e)))?;

        match map.get(key) {
            Some(v) => {
                if v.is_expired() {
                    map.remove(key);
                    Ok(vec![])
                } else {
                    Self::check_zset_type(&v.value)?;
                    match &v.value {
                        StorageValue::ZSet(z) => Ok(z.range_by_score(min, max)),
                        _ => unreachable!(),
                    }
                }
            }
            None => Ok(vec![]),
        }
    }

    /// 返回有序集合的成员数量
    pub fn zcard(&self, key: &str) -> Result<usize> {
        let db = self.db();
        let mut map = db
            .inner
            .get_shard(key)
            .write()
            .map_err(|e| AppError::Storage(format!("锁中毒: {}", e)))?;

        match map.get(key) {
            Some(v) => {
                if v.is_expired() {
                    map.remove(key);
                    Ok(0)
                } else {
                    Self::check_zset_type(&v.value)?;
                    match &v.value {
                        StorageValue::ZSet(z) => Ok(z.member_to_score.len()),
                        _ => unreachable!(),
                    }
                }
            }
            None => Ok(0),
        }
    }

    /// 按排名范围返回有序集合中的成员（降序）
    pub fn zrevrange(
        &self,
        key: &str,
        start: isize,
        stop: isize,
        _with_scores: bool,
    ) -> Result<Vec<(String, f64)>> {
        let db = self.db();
        let mut map = db
            .inner
            .get_shard(key)
            .write()
            .map_err(|e| AppError::Storage(format!("锁中毒: {}", e)))?;

        match map.get(key) {
            Some(v) => {
                if v.is_expired() {
                    map.remove(key);
                    Ok(vec![])
                } else {
                    Self::check_zset_type(&v.value)?;
                    match &v.value {
                        StorageValue::ZSet(z) => Ok(z.rev_range_by_rank(start, stop)),
                        _ => unreachable!(),
                    }
                }
            }
            None => Ok(vec![]),
        }
    }

    /// 获取成员的降序排名
    pub fn zrevrank(&self, key: &str, member: &str) -> Result<Option<usize>> {
        let db = self.db();
        let mut map = db
            .inner
            .get_shard(key)
            .write()
            .map_err(|e| AppError::Storage(format!("锁中毒: {}", e)))?;

        match map.get(key) {
            Some(v) => {
                if v.is_expired() {
                    map.remove(key);
                    Ok(None)
                } else {
                    Self::check_zset_type(&v.value)?;
                    match &v.value {
                        StorageValue::ZSet(z) => Ok(z.rev_rank(member)),
                        _ => unreachable!(),
                    }
                }
            }
            None => Ok(None),
        }
    }

    /// 增加成员的分数
    /// 返回新的分数字符串
    pub fn zincrby(&self, key: &str, increment: f64, member: String) -> Result<String> {
        let db = self.db();
        let mut map = db
            .inner
            .get_shard(key)
            .write()
            .map_err(|e| AppError::Storage(format!("锁中毒: {}", e)))?;

        match map.get_mut(key) {
            Some(v) => {
                if v.is_expired() {
                    map.remove(key);
                    let mut zset = ZSetData::new();
                    zset.add(member.clone(), increment);
                    map.insert(key.to_string(), Entry::new(StorageValue::ZSet(zset)));
                    self.bump_version(key);
                    self.notify_blocking_waiters(key);
                    Ok(format!("{}", increment))
                } else {
                    Self::check_zset_type(&v.value)?;
                    match &mut v.value {
                        StorageValue::ZSet(z) => {
                            let new_score = z.score(&member).unwrap_or(0.0) + increment;
                            z.add(member, new_score);
                            self.bump_version(key);
                                    self.notify_blocking_waiters(key);
                            Ok(format!("{}", new_score))
                        }
                        _ => unreachable!(),
                    }
                }
            }
            None => {
                let mut zset = ZSetData::new();
                zset.add(member.clone(), increment);
                map.insert(key.to_string(), Entry::new(StorageValue::ZSet(zset)));
                self.bump_version(key);
                self.notify_blocking_waiters(key);
                Ok(format!("{}", increment))
            }
        }
    }

    /// 返回分数范围内的成员数量
    pub fn zcount(&self, key: &str, min: f64, max: f64) -> Result<usize> {
        let db = self.db();
        let mut map = db
            .inner
            .get_shard(key)
            .write()
            .map_err(|e| AppError::Storage(format!("锁中毒: {}", e)))?;

        match map.get(key) {
            Some(v) => {
                if v.is_expired() {
                    map.remove(key);
                    Ok(0)
                } else {
                    Self::check_zset_type(&v.value)?;
                    match &v.value {
                        StorageValue::ZSet(z) => Ok(z.count_by_score(min, max)),
                        _ => unreachable!(),
                    }
                }
            }
            None => Ok(0),
        }
    }

    /// 弹出分数最小的成员
    pub fn zpopmin(&self, key: &str, count: usize) -> Result<Vec<(String, f64)>> {
        let db = self.db();
        let mut map = db
            .inner
            .get_shard(key)
            .write()
            .map_err(|e| AppError::Storage(format!("锁中毒: {}", e)))?;

        match map.get_mut(key) {
            Some(v) => {
                if v.is_expired() {
                    map.remove(key);
                    Ok(vec![])
                } else {
                    Self::check_zset_type(&v.value)?;
                    match &mut v.value {
                        StorageValue::ZSet(z) => {
                            let result = z.pop_min(count);
                            if z.member_to_score.is_empty() {
                                map.remove(key);
                            }
                            self.bump_version(key);
                                    Ok(result)
                        }
                        _ => unreachable!(),
                    }
                }
            }
            None => Ok(vec![]),
        }
    }

    /// 弹出分数最大的成员
    pub fn zpopmax(&self, key: &str, count: usize) -> Result<Vec<(String, f64)>> {
        let db = self.db();
        let mut map = db
            .inner
            .get_shard(key)
            .write()
            .map_err(|e| AppError::Storage(format!("锁中毒: {}", e)))?;

        match map.get_mut(key) {
            Some(v) => {
                if v.is_expired() {
                    map.remove(key);
                    Ok(vec![])
                } else {
                    Self::check_zset_type(&v.value)?;
                    match &mut v.value {
                        StorageValue::ZSet(z) => {
                            let result = z.pop_max(count);
                            if z.member_to_score.is_empty() {
                                map.remove(key);
                            }
                            self.bump_version(key);
                                    Ok(result)
                        }
                        _ => unreachable!(),
                    }
                }
            }
            None => Ok(vec![]),
        }
    }

    /// 有序集合并集存储
    /// 有序集合交集存储
    /// 增量迭代有序集合
    pub fn zscan(
        &self,
        key: &str,
        cursor: usize,
        pattern: &str,
        count: usize,
    ) -> Result<(usize, Vec<(String, f64)>)> {
        let db = self.db();
        let mut map = db
            .inner
            .get_shard(key)
            .write()
            .map_err(|e| AppError::Storage(format!("锁中毒: {}", e)))?;

        match map.get(key) {
            Some(v) => {
                if v.is_expired() {
                    map.remove(key);
                    return Ok((0, vec![]));
                }
                Self::check_zset_type(&v.value)?;
                match &v.value {
                    StorageValue::ZSet(z) => {
                        let items: Vec<(String, f64)> = z
                            .score_to_member
                            .keys()
                            .map(|(score, member)| (member.clone(), score.into_inner()))
                            .collect();
                        let mut filtered = Vec::new();
                        for (member, score) in items {
                            if Self::glob_match(&member, pattern) {
                                filtered.push((member, score));
                            }
                        }
                        if cursor >= filtered.len() {
                            return Ok((0, vec![]));
                        }
                        let count = if count == 0 { 10 } else { count };
                        let end = (cursor + count).min(filtered.len());
                        let result = filtered[cursor..end].to_vec();
                        let new_cursor = if end >= filtered.len() { 0 } else { end };
                        Ok((new_cursor, result))
                    }
                    _ => unreachable!(),
                }
            }
            None => Ok((0, vec![])),
        }
    }

    /// 按字典序范围返回有序集合成员
    pub fn zrangebylex(&self, key: &str, min: &str, max: &str) -> Result<Vec<String>> {
        let db = self.db();
        let mut map = db
            .inner
            .get_shard(key)
            .write()
            .map_err(|e| AppError::Storage(format!("锁中毒: {}", e)))?;

        match map.get(key) {
            Some(v) => {
                if v.is_expired() {
                    map.remove(key);
                    Ok(vec![])
                } else {
                    Self::check_zset_type(&v.value)?;
                    match &v.value {
                        StorageValue::ZSet(z) => z.range_by_lex(min, max),
                        _ => unreachable!(),
                    }
                }
            }
            None => Ok(vec![]),
        }
    }

    /// 随机返回有序集合中的成员
    pub fn zrandmember(
        &self,
        key: &str,
        count: i64,
        with_scores: bool,
    ) -> Result<Vec<(String, f64)>> {
        let db = self.db();
        let mut map = db
            .inner
            .get_shard(key)
            .write()
            .map_err(|e| AppError::Storage(format!("锁中毒: {}", e)))?;

        match map.get(key) {
            Some(v) => {
                if v.is_expired() {
                    map.remove(key);
                    Ok(vec![])
                } else {
                    Self::check_zset_type(&v.value)?;
                    match &v.value {
                        StorageValue::ZSet(z) => Ok(z.rand_member(count, with_scores)),
                        _ => unreachable!(),
                    }
                }
            }
            None => Ok(vec![]),
        }
    }

    /// 返回有序集合差集（不存储）
    /// 有序集合差集存储
    /// 返回有序集合交集（不存储）
    /// 返回有序集合并集（不存储）
    /// 将范围查询结果存储到目标键
    /// 从多个有序集合中弹出成员
    /// 返回 (键名, Vec<(成员, 分数)>)
    /// 阻塞版 ZMPOP
    /// 阻塞弹出最小分数成员
    /// 阻塞弹出最大分数成员
    /// 按降序分数范围返回成员
    pub fn zrevrangebyscore(
        &self,
        key: &str,
        max: f64,
        min: f64,
        _with_scores: bool,
        limit_offset: usize,
        limit_count: usize,
    ) -> Result<Vec<(String, f64)>> {
        let db = self.db();
        let mut map = db
            .inner
            .get_shard(key)
            .write()
            .map_err(|e| AppError::Storage(format!("锁中毒: {}", e)))?;

        match map.get(key) {
            Some(v) => {
                if v.is_expired() {
                    map.remove(key);
                    Ok(vec![])
                } else {
                    Self::check_zset_type(&v.value)?;
                    match &v.value {
                        StorageValue::ZSet(z) => {
                            let max_key = (OrderedFloat(max), String::from("\u{10FFFF}"));
                            let min_key = (OrderedFloat(min), String::new());
                            let mut result: Vec<(String, f64)> = z
                                .score_to_member
                                .range(min_key..=max_key)
                                .rev()
                                .map(|((score, member), _)| (member.clone(), score.into_inner()))
                                .collect();
                            if limit_count > 0 {
                                result = result
                                    .into_iter()
                                    .skip(limit_offset)
                                    .take(limit_count)
                                    .collect();
                            }
                            Ok(result)
                        }
                        _ => unreachable!(),
                    }
                }
            }
            None => Ok(vec![]),
        }
    }

    /// 按降序字典序范围返回成员
    pub fn zrevrangebylex(
        &self,
        key: &str,
        max: &str,
        min: &str,
        limit_offset: usize,
        limit_count: usize,
    ) -> Result<Vec<String>> {
        let db = self.db();
        let mut map = db
            .inner
            .get_shard(key)
            .write()
            .map_err(|e| AppError::Storage(format!("锁中毒: {}", e)))?;

        match map.get(key) {
            Some(v) => {
                if v.is_expired() {
                    map.remove(key);
                    Ok(vec![])
                } else {
                    Self::check_zset_type(&v.value)?;
                    match &v.value {
                        StorageValue::ZSet(z) => {
                            let mut result = z.rev_range_by_lex(min, max)?;
                            if limit_count > 0 {
                                result = result
                                    .into_iter()
                                    .skip(limit_offset)
                                    .take(limit_count)
                                    .collect();
                            }
                            Ok(result)
                        }
                        _ => unreachable!(),
                    }
                }
            }
            None => Ok(vec![]),
        }
    }

    /// 批量获取成员分数
    pub fn zmscore(&self, key: &str, members: &[String]) -> Result<Vec<Option<f64>>> {
        let db = self.db();
        let mut map = db
            .inner
            .get_shard(key)
            .write()
            .map_err(|e| AppError::Storage(format!("锁中毒: {}", e)))?;

        match map.get(key) {
            Some(v) => {
                if v.is_expired() {
                    map.remove(key);
                    Ok(vec![None; members.len()])
                } else {
                    Self::check_zset_type(&v.value)?;
                    match &v.value {
                        StorageValue::ZSet(z) => Ok(members.iter().map(|m| z.score(m)).collect()),
                        _ => unreachable!(),
                    }
                }
            }
            None => Ok(vec![None; members.len()]),
        }
    }

    /// 字典序范围计数
    pub fn zlexcount(&self, key: &str, min: &str, max: &str) -> Result<usize> {
        let db = self.db();
        let mut map = db
            .inner
            .get_shard(key)
            .write()
            .map_err(|e| AppError::Storage(format!("锁中毒: {}", e)))?;

        match map.get(key) {
            Some(v) => {
                if v.is_expired() {
                    map.remove(key);
                    Ok(0)
                } else {
                    Self::check_zset_type(&v.value)?;
                    match &v.value {
                        StorageValue::ZSet(z) => z.lex_count(min, max),
                        _ => unreachable!(),
                    }
                }
            }
            None => Ok(0),
        }
    }

    /// 按字典序范围删除有序集合成员，返回删除数量
    pub fn zremrangebylex(&self, key: &str, min: &str, max: &str) -> Result<usize> {
        self.evict_if_needed()?;
        let db = self.db();
        let mut map = db
            .inner
            .get_shard(key)
            .write()
            .map_err(|e| AppError::Storage(format!("锁中毒: {}", e)))?;

        match map.get_mut(key) {
            Some(v) => {
                if v.is_expired() {
                    map.remove(key);
                    Ok(0)
                } else {
                    Self::check_zset_type(&v.value)?;
                    match &mut v.value {
                        StorageValue::ZSet(z) => {
                            let to_remove = z.range_by_lex(min, max)?;
                            let count = to_remove.len();
                            for member in to_remove {
                                z.remove(&member);
                            }
                            if z.member_to_score.is_empty() {
                                map.remove(key);
                            }
                            self.bump_version(key);
                                    Ok(count)
                        }
                        _ => unreachable!(),
                    }
                }
            }
            None => Ok(0),
        }
    }

    /// 按排名范围删除有序集合成员，返回删除数量
    pub fn zremrangebyrank(&self, key: &str, start: isize, stop: isize) -> Result<usize> {
        self.evict_if_needed()?;
        let db = self.db();
        let mut map = db
            .inner
            .get_shard(key)
            .write()
            .map_err(|e| AppError::Storage(format!("锁中毒: {}", e)))?;

        match map.get_mut(key) {
            Some(v) => {
                if v.is_expired() {
                    map.remove(key);
                    Ok(0)
                } else {
                    Self::check_zset_type(&v.value)?;
                    match &mut v.value {
                        StorageValue::ZSet(z) => {
                            let to_remove: Vec<String> = z
                                .range_by_rank(start, stop)
                                .into_iter()
                                .map(|(member, _)| member)
                                .collect();
                            let count = to_remove.len();
                            for member in to_remove {
                                z.remove(&member);
                            }
                            if z.member_to_score.is_empty() {
                                map.remove(key);
                            }
                            self.bump_version(key);
                                    Ok(count)
                        }
                        _ => unreachable!(),
                    }
                }
            }
            None => Ok(0),
        }
    }

    /// 按分数范围删除有序集合成员，返回删除数量
    pub fn zremrangebyscore(&self, key: &str, min: f64, max: f64) -> Result<usize> {
        self.evict_if_needed()?;
        let db = self.db();
        let mut map = db
            .inner
            .get_shard(key)
            .write()
            .map_err(|e| AppError::Storage(format!("锁中毒: {}", e)))?;

        match map.get_mut(key) {
            Some(v) => {
                if v.is_expired() {
                    map.remove(key);
                    Ok(0)
                } else {
                    Self::check_zset_type(&v.value)?;
                    match &mut v.value {
                        StorageValue::ZSet(z) => {
                            let to_remove: Vec<String> = z
                                .range_by_score(min, max)
                                .into_iter()
                                .map(|(member, _)| member)
                                .collect();
                            let count = to_remove.len();
                            for member in to_remove {
                                z.remove(&member);
                            }
                            if z.member_to_score.is_empty() {
                                map.remove(key);
                            }
                            self.bump_version(key);
                                    Ok(count)
                        }
                        _ => unreachable!(),
                    }
                }
            }
            None => Ok(0),
        }
    }

    /// 修改后的 zrange 支持统一语法：
    /// ZRANGE key min max [BYSCORE|BYLEX] [REV] [LIMIT offset count] [WITHSCORES]
    pub fn zrange_unified(
        &self,
        key: &str,
        min: &str,
        max: &str,
        by_score: bool,
        by_lex: bool,
        rev: bool,
        _with_scores: bool,
        limit_offset: usize,
        limit_count: usize,
    ) -> Result<Vec<(String, f64)>> {
        let db = self.db();
        let mut map = db
            .inner
            .get_shard(key)
            .write()
            .map_err(|e| AppError::Storage(format!("锁中毒: {}", e)))?;

        match map.get(key) {
            Some(v) => {
                if v.is_expired() {
                    map.remove(key);
                    Ok(vec![])
                } else {
                    Self::check_zset_type(&v.value)?;
                    match &v.value {
                        StorageValue::ZSet(z) => {
                            let mut result: Vec<(String, f64)> = if by_score {
                                let min_score: f64 = min.parse().map_err(|_| {
                                    AppError::Command("ZRANGE BYSCORE min 必须是数字".to_string())
                                })?;
                                let max_score: f64 = max.parse().map_err(|_| {
                                    AppError::Command("ZRANGE BYSCORE max 必须是数字".to_string())
                                })?;
                                if rev {
                                    z.score_to_member
                                        .range(
                                            ..=(
                                                OrderedFloat(max_score),
                                                String::from("\u{10FFFF}"),
                                            ),
                                        )
                                        .rev()
                                        .filter(|((s, _), _)| s.into_inner() >= min_score)
                                        .map(|((s, m), _)| (m.clone(), s.into_inner()))
                                        .collect()
                                } else {
                                    z.range_by_score(min_score, max_score)
                                }
                            } else if by_lex {
                                let lex_result = if rev {
                                    z.rev_range_by_lex(min, max)
                                } else {
                                    z.range_by_lex(min, max)
                                };
                                lex_result?
                                    .into_iter()
                                    .map(|m| {
                                        let score =
                                            z.member_to_score.get(&m).copied().unwrap_or(0.0);
                                        (m, score)
                                    })
                                    .collect()
                            } else {
                                let start: isize = min.parse().map_err(|_| {
                                    AppError::Command("ZRANGE start 必须是整数".to_string())
                                })?;
                                let stop: isize = max.parse().map_err(|_| {
                                    AppError::Command("ZRANGE stop 必须是整数".to_string())
                                })?;
                                if rev {
                                    z.rev_range_by_rank(start, stop)
                                } else {
                                    z.range_by_rank(start, stop)
                                }
                            };
                            if limit_count > 0 {
                                result = result
                                    .into_iter()
                                    .skip(limit_offset)
                                    .take(limit_count)
                                    .collect();
                            }
                            Ok(result)
                        }
                        _ => unreachable!(),
                    }
                }
            }
            None => Ok(vec![]),
        }
    }
}
