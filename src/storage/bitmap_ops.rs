//! Bitmap 数据类型操作（SETBIT/GETBIT/BITCOUNT/BITOP/BITFIELD）

use super::*;

fn read_bitfield_unsigned(bytes: &[u8], offset: usize, bits: usize) -> u64 {
    let mut value: u64 = 0;
    for i in 0..bits {
        let byte_idx = (offset + i) / 8;
        let bit_idx = 7 - ((offset + i) % 8);
        let bit = if byte_idx < bytes.len() {
            (bytes[byte_idx] >> bit_idx) & 1
        } else {
            0
        };
        value = (value << 1) | (bit as u64);
    }
    value
}

fn unsigned_to_signed(value: u64, bits: usize) -> i64 {
    let sign_bit = 1u64 << (bits - 1);
    if value & sign_bit != 0 {
        (value as i64) - (1i64 << bits)
    } else {
        value as i64
    }
}

fn signed_to_unsigned(value: i64, bits: usize) -> u64 {
    let mask = if bits == 64 {
        !0u64
    } else {
        (1u64 << bits) - 1
    };
    (value as u64) & mask
}

fn write_bitfield(bytes: &mut Vec<u8>, offset: usize, bits: usize, value: u64) {
    let needed = (offset + bits).div_ceil(8);
    if bytes.len() < needed {
        bytes.resize(needed, 0);
    }
    for i in 0..bits {
        let byte_idx = (offset + i) / 8;
        let bit_idx = 7 - ((offset + i) % 8);
        let bit_pos = bits - 1 - i;
        let bit = ((value >> bit_pos) & 1) as u8;
        if bit == 1 {
            bytes[byte_idx] |= 1 << bit_idx;
        } else {
            bytes[byte_idx] &= !(1 << bit_idx);
        }
    }
}

fn read_field(bytes: &[u8], encoding: &BitFieldEncoding, offset: usize) -> i64 {
    let raw = read_bitfield_unsigned(bytes, offset, encoding.bits);
    if encoding.signed {
        unsigned_to_signed(raw, encoding.bits)
    } else {
        raw as i64
    }
}

fn write_field(bytes: &mut Vec<u8>, encoding: &BitFieldEncoding, offset: usize, value: i64) {
    let raw = if encoding.signed {
        signed_to_unsigned(value, encoding.bits)
    } else {
        (value as u64) & ((1u64 << encoding.bits) - 1)
    };
    write_bitfield(bytes, offset, encoding.bits, raw);
}

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
            if new_value > max_val {
                BitFieldResult::Value(max_val)
            } else if new_value < min_val {
                BitFieldResult::Value(min_val)
            } else {
                BitFieldResult::Value(new_value)
            }
        }
        BitFieldOverflow::Fail => {
            if new_value > max_val || new_value < min_val {
                BitFieldResult::Nil
            } else {
                BitFieldResult::Value(new_value)
            }
        }
    }
}

fn get_string_bytes(map: &mut HashMap<String, Entry>, key: &str) -> Result<Vec<u8>> {
    match map.get(key) {
        Some(entry) => {
            if entry.is_expired() {
                map.remove(key);
                Ok(Vec::new())
            } else {
                match &entry.value {
                    StorageValue::String(b) => Ok(b.to_vec()),
                    _ => Err(AppError::Storage(
                        "WRONGTYPE 操作对象持有的是错误类型的值".to_string(),
                    )),
                }
            }
        }
        None => Ok(Vec::new()),
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

        let mut bytes = get_string_bytes(&mut map, key)?;

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

        map.insert(key.to_string(), Entry::new(StorageValue::String(Bytes::from(bytes))));
self.bump_version(&mut map, key);
        drop(map);
        self.evict_if_needed()?;
        Ok(old_bit as i64)
    }

    pub fn getbit(&self, key: &str, offset: usize) -> Result<i64> {
        let db = self.db();
        let map = db
            .inner
            .get_shard(key)
            .read()
            .map_err(|e| AppError::Storage(format!("锁中毒: {}", e)))?;

        match map.get(key) {
            Some(entry) => {
                if entry.is_expired() {
                    Ok(0)
                } else {
                    match &entry.value {
                        StorageValue::String(b) => {
                            let byte_index = offset / 8;
                            if byte_index >= b.len() {
                                Ok(0)
                            } else {
                                let bit_index = 7 - (offset % 8);
                                Ok(((b[byte_index] >> bit_index) & 1) as i64)
                            }
                        }
                        _ => Err(AppError::Storage(
                            "WRONGTYPE 操作对象持有的是错误类型的值".to_string(),
                        )),
                    }
                }
            }
            None => Ok(0),
        }
    }

    pub fn bitcount(&self, key: &str, start: isize, end: isize, is_byte: bool) -> Result<usize> {
        let db = self.db();
        let mut map = db
            .inner
            .get_shard(key)
            .write()
            .map_err(|e| AppError::Storage(format!("锁中毒: {}", e)))?;

        let bytes = match map.get(key) {
            Some(entry) => {
                if entry.is_expired() {
                    map.remove(key);
                    return Ok(0);
                } else {
                    match &entry.value {
                        StorageValue::String(b) => b.clone(),
                        _ => {
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

    pub fn bitop(&self, operation: &str, destkey: &str, keys: &[String]) -> Result<usize> {
        self.evict_if_needed()?;
        let db = self.db();

        let mut byte_arrays: Vec<Vec<u8>> = Vec::new();
        let mut max_len = 0usize;

        for key in keys {
            let map = db
                .inner
                .get_shard(key)
                .read()
                .map_err(|e| AppError::Storage(format!("锁中毒: {}", e)))?;
            if let Some(entry) = map.get(key) {
                if !entry.is_expired() {
                    if let StorageValue::String(b) = &entry.value {
                        let vec = b.to_vec();
                        max_len = max_len.max(vec.len());
                        byte_arrays.push(vec);
                    }
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
self.bump_version(&mut dst_map, destkey);
            return Ok(0);
        }

        let op = operation.to_ascii_uppercase();
        let mut result = vec![0u8; max_len];

        if op == "NOT" {
            let src = &byte_arrays[0];
            for i in 0..max_len {
                result[i] = !src[i];
            }
        } else {
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
            Entry::new(StorageValue::String(Bytes::from(result))),
        );
self.bump_version(&mut dst_map, destkey);
        drop(dst_map);
        self.evict_if_needed()?;
        Ok(max_len)
    }

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
            Some(entry) => {
                if entry.is_expired() {
                    map.remove(key);
                    return Ok(-1);
                } else {
                    match &entry.value {
                        StorageValue::String(b) => b.clone(),
                        _ => {
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

    pub fn bitfield(&self, key: &str, ops: &[BitFieldOp]) -> Result<Vec<BitFieldResult>> {
        self.evict_if_needed()?;
        let db = self.db();
        let mut map = db
            .inner
            .get_shard(key)
            .write()
            .map_err(|e| AppError::Storage(format!("锁中毒: {}", e)))?;

        let mut bytes = get_string_bytes(&mut map, key)?;

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
            map.insert(key.to_string(), Entry::new(StorageValue::String(Bytes::from(bytes))));
self.bump_version(&mut map, key);
        }

        Ok(results)
    }

    pub fn bitfield_ro(&self, key: &str, ops: &[BitFieldOp]) -> Result<Vec<BitFieldResult>> {
        let db = self.db();
        let map = db
            .inner
            .get_shard(key)
            .read()
            .map_err(|e| AppError::Storage(format!("锁中毒: {}", e)))?;

        let bytes = match map.get(key) {
            Some(entry) => {
                if entry.is_expired() {
                    return Ok(vec![BitFieldResult::Value(0); ops.len()]);
                }
                match &entry.value {
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
}
