// RDB 快照持久化模块

use std::collections::{HashMap, HashSet, VecDeque};
use std::fs::File;
use std::io::{BufReader, BufWriter, Read, Write};

use bytes::Bytes;

use crate::error::{AppError, Result};
use crate::storage::{StorageEngine, StorageValue, ZSetData};

/// RDB 文件魔数
const RDB_MAGIC: &[u8] = b"REDIS-RUST";
/// RDB 文件版本号
const RDB_VERSION: u32 = 1;
/// EOF 标记
const RDB_EOF: u8 = 0xFF;
/// AUX 标记
const RDB_AUX: u8 = 0xFE;

/// 值类型标记
const TYPE_STRING: u8 = 0;
const TYPE_LIST: u8 = 1;
const TYPE_HASH: u8 = 2;
const TYPE_SET: u8 = 3;
const TYPE_ZSET: u8 = 4;
const TYPE_HLL: u8 = 5;

/// 将 u16 写入为大端序字节
#[allow(dead_code)]
fn write_u16(buf: &mut Vec<u8>, v: u16) {
    buf.extend_from_slice(&v.to_be_bytes());
}

/// 将 u32 写入为大端序字节
#[allow(dead_code)]
fn write_u32(buf: &mut Vec<u8>, v: u32) {
    buf.extend_from_slice(&v.to_be_bytes());
}

/// 将 u64 写入为大端序字节
#[allow(dead_code)]
fn write_u64(buf: &mut Vec<u8>, v: u64) {
    buf.extend_from_slice(&v.to_be_bytes());
}

/// 将 f64 写入为大端序字节
#[allow(dead_code)]
fn write_f64(buf: &mut Vec<u8>, v: f64) {
    buf.extend_from_slice(&v.to_be_bytes());
}

/// 从字节流读取 u16（大端序）
#[allow(dead_code)]
fn read_u16(reader: &mut impl Read) -> Result<u16> {
    let mut buf = [0u8; 2];
    reader.read_exact(&mut buf)?;
    Ok(u16::from_be_bytes(buf))
}

/// 从字节流读取 u32（大端序）
fn read_u32(reader: &mut impl Read) -> Result<u32> {
    let mut buf = [0u8; 4];
    reader.read_exact(&mut buf)?;
    Ok(u32::from_be_bytes(buf))
}

/// 从字节流读取 u64（大端序）
fn read_u64(reader: &mut impl Read) -> Result<u64> {
    let mut buf = [0u8; 8];
    reader.read_exact(&mut buf)?;
    Ok(u64::from_be_bytes(buf))
}

/// 从字节流读取 f64（大端序）
fn read_f64(reader: &mut impl Read) -> Result<f64> {
    let mut buf = [0u8; 8];
    reader.read_exact(&mut buf)?;
    Ok(f64::from_be_bytes(buf))
}

/// 从字节流读取指定长度的字节
fn read_bytes(reader: &mut impl Read, len: usize) -> Result<Vec<u8>> {
    let mut buf = vec![0u8; len];
    reader.read_exact(&mut buf)?;
    Ok(buf)
}

/// 保存 RDB 快照到任意写入器（不包含文件头魔数/版本号以外的封装）
/// 此方法会写入完整的 RDB 格式数据（魔数 + 版本 + 数据库段 + AUX + EOF + CRC）
/// repl_id: 可选的复制 ID
/// repl_offset: 可选的复制偏移量
pub fn save_to_writer_with_repl(
    storage: &StorageEngine,
    writer: &mut impl Write,
    repl_id: Option<&str>,
    repl_offset: Option<i64>,
) -> Result<()> {
    let mut crc_hasher = crc32fast::Hasher::new();

    // 写入文件头：魔数 + 版本号
    writer.write_all(RDB_MAGIC)?;
    crc_hasher.update(RDB_MAGIC);
    let version_bytes = RDB_VERSION.to_be_bytes();
    writer.write_all(&version_bytes)?;
    crc_hasher.update(&version_bytes);

    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64;

    // 遍历所有 16 个数据库
    for (db_index, db) in storage.all_dbs().iter().enumerate() {
        // 过滤掉已过期和空的 key
        let mut valid_entries: Vec<(String, StorageValue, Option<u64>)> = Vec::new();
        for shard in db.inner.all_shards() {
            let map = shard.read().map_err(|e| {
                AppError::Storage(format!("RDB 保存时锁中毒: {}", e))
            })?;
            for (key, value) in map.iter() {
                let (ttl, is_expired) = match value {
                    StorageValue::ExpiringString(_, expire_at) => {
                        let expired = now >= *expire_at;
                        (Some(*expire_at), expired)
                    }
                    _ => (None, false),
                };
                if !is_expired {
                    valid_entries.push((key.clone(), value.clone(), ttl));
                }
            }
        }

        if valid_entries.is_empty() {
            continue;
        }

        // 写入数据库段头部
        let db_index_u16 = db_index as u16;
        let db_index_bytes = db_index_u16.to_be_bytes();
        writer.write_all(&db_index_bytes)?;
        crc_hasher.update(&db_index_bytes);

        let key_count = valid_entries.len() as u32;
        let key_count_bytes = key_count.to_be_bytes();
        writer.write_all(&key_count_bytes)?;
        crc_hasher.update(&key_count_bytes);

        // 写入每个 key
        for (key, value, ttl) in valid_entries {
            write_key_value(writer, &mut crc_hasher, &key, &value, ttl)?;
        }
    }

    // 写入 AUX 段（复制信息）
    if let (Some(replid), Some(offset)) = (repl_id, repl_offset) {
        writer.write_all(&[RDB_AUX])?;
        crc_hasher.update(&[RDB_AUX]);
        write_aux_string(writer, &mut crc_hasher, "repl-id", replid)?;
        write_aux_string(writer, &mut crc_hasher, "repl-offset", &offset.to_string())?;
    }

    // 写入 EOF 标记
    writer.write_all(&[RDB_EOF])?;
    crc_hasher.update(&[RDB_EOF]);

    // 写入 CRC32
    let crc = crc_hasher.finalize();
    writer.write_all(&crc.to_be_bytes())?;

    writer.flush()?;
    Ok(())
}

/// 保存 RDB 快照到任意写入器（不包含复制信息）
pub fn save_to_writer(storage: &StorageEngine, writer: &mut impl Write) -> Result<()> {
    save_to_writer_with_repl(storage, writer, None, None)
}

/// 保存 RDB 快照到文件
pub fn save(storage: &StorageEngine, path: &str, repl_info: Option<(String, i64)>) -> Result<()> {
    let file = File::create(path)?;
    let mut writer = BufWriter::new(file);
    if let Some((ref id, offset)) = repl_info {
        save_to_writer_with_repl(storage, &mut writer, Some(id.as_str()), Some(offset))
    } else {
        save_to_writer_with_repl(storage, &mut writer, None, None)
    }
}

/// 写入单个 key-value 到 RDB
fn write_key_value(
    writer: &mut impl Write,
    crc: &mut crc32fast::Hasher,
    key: &str,
    value: &StorageValue,
    ttl: Option<u64>,
) -> Result<()> {
    // 类型标记
    let type_byte = match value {
        StorageValue::String(_) | StorageValue::ExpiringString(_, _) => TYPE_STRING,
        StorageValue::List(_) => TYPE_LIST,
        StorageValue::Hash(_) => TYPE_HASH,
        StorageValue::Set(_) => TYPE_SET,
        StorageValue::ZSet(_) => TYPE_ZSET,
        StorageValue::HyperLogLog(_) => TYPE_HLL,
        StorageValue::Stream(_) => {
            return Err(AppError::Storage("Stream 类型暂不支持 RDB 持久化".to_string()));
        }
    };
    writer.write_all(&[type_byte])?;
    crc.update(&[type_byte]);

    // key
    let key_bytes = key.as_bytes();
    let key_len = key_bytes.len() as u32;
    let key_len_bytes = key_len.to_be_bytes();
    writer.write_all(&key_len_bytes)?;
    crc.update(&key_len_bytes);
    writer.write_all(key_bytes)?;
    crc.update(key_bytes);

    // TTL 标记和时间
    if let Some(expire_at) = ttl {
        writer.write_all(&[1u8])?;
        crc.update(&[1u8]);
        let ttl_bytes = expire_at.to_be_bytes();
        writer.write_all(&ttl_bytes)?;
        crc.update(&ttl_bytes);
    } else {
        writer.write_all(&[0u8])?;
        crc.update(&[0u8]);
    }

    // value 数据
    match value {
        StorageValue::String(bytes) | StorageValue::ExpiringString(bytes, _) => {
            let len = bytes.len() as u32;
            let len_bytes = len.to_be_bytes();
            writer.write_all(&len_bytes)?;
            crc.update(&len_bytes);
            writer.write_all(bytes)?;
            crc.update(bytes);
        }
        StorageValue::List(list) => {
            let count = list.len() as u32;
            let count_bytes = count.to_be_bytes();
            writer.write_all(&count_bytes)?;
            crc.update(&count_bytes);
            for item in list {
                let len = item.len() as u32;
                let len_bytes = len.to_be_bytes();
                writer.write_all(&len_bytes)?;
                crc.update(&len_bytes);
                writer.write_all(item)?;
                crc.update(item);
            }
        }
        StorageValue::Hash(hash) => {
            let count = hash.len() as u32;
            let count_bytes = count.to_be_bytes();
            writer.write_all(&count_bytes)?;
            crc.update(&count_bytes);
            for (field, val) in hash {
                let field_bytes = field.as_bytes();
                let field_len = field_bytes.len() as u32;
                let field_len_bytes = field_len.to_be_bytes();
                writer.write_all(&field_len_bytes)?;
                crc.update(&field_len_bytes);
                writer.write_all(field_bytes)?;
                crc.update(field_bytes);

                let val_len = val.len() as u32;
                let val_len_bytes = val_len.to_be_bytes();
                writer.write_all(&val_len_bytes)?;
                crc.update(&val_len_bytes);
                writer.write_all(val)?;
                crc.update(val);
            }
        }
        StorageValue::Set(set) => {
            let count = set.len() as u32;
            let count_bytes = count.to_be_bytes();
            writer.write_all(&count_bytes)?;
            crc.update(&count_bytes);
            for item in set {
                let len = item.len() as u32;
                let len_bytes = len.to_be_bytes();
                writer.write_all(&len_bytes)?;
                crc.update(&len_bytes);
                writer.write_all(item)?;
                crc.update(item);
            }
        }
        StorageValue::ZSet(zset) => {
            let count = zset.member_to_score.len() as u32;
            let count_bytes = count.to_be_bytes();
            writer.write_all(&count_bytes)?;
            crc.update(&count_bytes);
            for (member, score) in &zset.member_to_score {
                let member_bytes = member.as_bytes();
                let member_len = member_bytes.len() as u32;
                let member_len_bytes = member_len.to_be_bytes();
                writer.write_all(&member_len_bytes)?;
                crc.update(&member_len_bytes);
                writer.write_all(member_bytes)?;
                crc.update(member_bytes);

                let score_bytes = score.to_be_bytes();
                writer.write_all(&score_bytes)?;
                crc.update(&score_bytes);
            }
        }
        StorageValue::HyperLogLog(hll) => {
            let data = hll.dump();
            writer.write_all(&data)?;
            crc.update(&data);
        }
        StorageValue::Stream(_) => {
            return Err(AppError::Storage("Stream 类型暂不支持 RDB 持久化".to_string()));
        }
    }

    Ok(())
}

/// 写入 AUX 字符串键值对
fn write_aux_string(
    writer: &mut impl Write,
    crc: &mut crc32fast::Hasher,
    key: &str,
    value: &str,
) -> Result<()> {
    let key_bytes = key.as_bytes();
    let key_len = key_bytes.len() as u32;
    let key_len_bytes = key_len.to_be_bytes();
    writer.write_all(&key_len_bytes)?;
    crc.update(&key_len_bytes);
    writer.write_all(key_bytes)?;
    crc.update(key_bytes);

    let val_bytes = value.as_bytes();
    let val_len = val_bytes.len() as u32;
    let val_len_bytes = val_len.to_be_bytes();
    writer.write_all(&val_len_bytes)?;
    crc.update(&val_len_bytes);
    writer.write_all(val_bytes)?;
    crc.update(val_bytes);
    Ok(())
}

/// 读取 AUX 字符串
fn read_aux_string(
    reader: &mut impl Read,
    crc: &mut crc32fast::Hasher,
) -> Result<String> {
    let len = read_u32(reader)?;
    crc.update(&len.to_be_bytes());
    let bytes = read_bytes(reader, len as usize)?;
    crc.update(&bytes);
    Ok(String::from_utf8_lossy(&bytes).to_string())
}

/// 从 RDB 字节流加载数据到 storage
/// check_trailing: 是否校验 CRC 后文件必须结束（混合格式时应设为 false）
/// 返回加载的复制信息 (replid, offset)
pub fn load_from_reader(
    storage: &StorageEngine,
    reader: &mut impl Read,
    check_trailing: bool,
) -> Result<(Option<String>, Option<i64>)> {
    // 读取文件头：魔数
    let mut magic_buf = vec![0u8; RDB_MAGIC.len()];
    reader.read_exact(&mut magic_buf)?;
    if magic_buf != RDB_MAGIC {
        return Err(AppError::Storage("RDB 文件魔数不匹配".to_string()));
    }

    // 读取版本号
    let version = read_u32(reader)?;
    if version != RDB_VERSION {
        return Err(AppError::Storage(format!(
            "RDB 文件版本不兼容: 期望 {}，实际 {}",
            RDB_VERSION, version
        )));
    }

    // 重置 CRC
    let mut crc_hasher = crc32fast::Hasher::new();
    crc_hasher.update(&magic_buf);
    crc_hasher.update(&version.to_be_bytes());

    let mut loaded_replid: Option<String> = None;
    let mut loaded_offset: Option<i64> = None;

    // 读取数据库段，直到遇到 EOF
    loop {
        // 预读一个字节判断是否为 EOF
        let mut peek_buf = [0u8; 1];
        match reader.read_exact(&mut peek_buf) {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                return Err(AppError::Storage("RDB 文件截断：缺少 EOF 标记".to_string()));
            }
            Err(e) => return Err(AppError::Io(e)),
        }

        if peek_buf[0] == RDB_EOF {
            crc_hasher.update(&peek_buf);
            break;
        }

        // 遇到 AUX 标记，解析辅助字段（读取两对 key-value）
        if peek_buf[0] == RDB_AUX {
            crc_hasher.update(&peek_buf);
            for _ in 0..2 {
                let key = read_aux_string(reader, &mut crc_hasher)?;
                let value = read_aux_string(reader, &mut crc_hasher)?;
                if key == "repl-id" {
                    loaded_replid = Some(value);
                } else if key == "repl-offset" {
                    loaded_offset = value.parse::<i64>().ok();
                }
            }
            continue;
        }

        // 不是 EOF 或 AUX，说明是 db_index 的高字节
        let db_index = {
            let low_byte = read_u8(reader)?;
            u16::from_be_bytes([peek_buf[0], low_byte])
        };
        crc_hasher.update(&db_index.to_be_bytes());

        let key_count = read_u32(reader)?;
        crc_hasher.update(&key_count.to_be_bytes());

        let db = storage.db_at(db_index as usize).ok_or_else(|| {
            AppError::Storage(format!("RDB 文件包含无效的数据库索引: {}", db_index))
        })?;

        for _ in 0..key_count {
            read_key_value(reader, &mut crc_hasher, &db)?;
        }
    }

    // 验证 CRC32
    let stored_crc = read_u32(reader)?;
    let computed_crc = crc_hasher.finalize();
    if stored_crc != computed_crc {
        return Err(AppError::Storage(format!(
            "RDB 文件 CRC32 校验失败: 期望 {:08x}，实际 {:08x}",
            stored_crc, computed_crc
        )));
    }

    if check_trailing {
        // 确保文件已读完
        let mut trailing = [0u8; 1];
        match reader.read(&mut trailing) {
            Ok(0) => Ok((loaded_replid, loaded_offset)),
            Ok(_) => Err(AppError::Storage("RDB 文件尾部有多余数据".to_string())),
            Err(e) => Err(AppError::Io(e)),
        }
    } else {
        Ok((loaded_replid, loaded_offset))
    }
}

/// 从 RDB 文件加载数据到 storage
/// 返回加载的复制信息 (replid, offset)
pub fn load(storage: &StorageEngine, path: &str) -> Result<(Option<String>, Option<i64>)> {
    let file = File::open(path).map_err(|e| {
        if e.kind() == std::io::ErrorKind::NotFound {
            AppError::Io(e)
        } else {
            AppError::Io(e)
        }
    })?;
    let mut reader = BufReader::new(file);
    load_from_reader(storage, &mut reader, true)
}

/// 读取单个 u8
fn read_u8(reader: &mut impl Read) -> Result<u8> {
    let mut buf = [0u8; 1];
    reader.read_exact(&mut buf)?;
    Ok(buf[0])
}

/// 读取单个 key-value 对到分片存储
fn read_key_value(
    reader: &mut impl Read,
    crc: &mut crc32fast::Hasher,
    db: &crate::storage::Db,
) -> Result<()> {
    // 类型标记
    let type_byte = read_u8(reader)?;
    crc.update(&[type_byte]);

    // key
    let key_len = read_u32(reader)?;
    crc.update(&key_len.to_be_bytes());
    let key_bytes = read_bytes(reader, key_len as usize)?;
    crc.update(&key_bytes);
    let key = String::from_utf8_lossy(&key_bytes).to_string();

    // TTL
    let has_ttl = read_u8(reader)?;
    crc.update(&[has_ttl]);
    let ttl = if has_ttl == 1 {
        let ttl_ms = read_u64(reader)?;
        crc.update(&ttl_ms.to_be_bytes());
        Some(ttl_ms)
    } else {
        None
    };

    // value
    let value = match type_byte {
        TYPE_STRING => {
            let len = read_u32(reader)?;
            crc.update(&len.to_be_bytes());
            let bytes = read_bytes(reader, len as usize)?;
            crc.update(&bytes);
            if let Some(expire_at) = ttl {
                StorageValue::ExpiringString(Bytes::from(bytes), expire_at)
            } else {
                StorageValue::String(Bytes::from(bytes))
            }
        }
        TYPE_LIST => {
            let count = read_u32(reader)?;
            crc.update(&count.to_be_bytes());
            let mut list = VecDeque::new();
            for _ in 0..count {
                let len = read_u32(reader)?;
                crc.update(&len.to_be_bytes());
                let bytes = read_bytes(reader, len as usize)?;
                crc.update(&bytes);
                list.push_back(Bytes::from(bytes));
            }
            StorageValue::List(list)
        }
        TYPE_HASH => {
            let count = read_u32(reader)?;
            crc.update(&count.to_be_bytes());
            let mut hash = HashMap::new();
            for _ in 0..count {
                let field_len = read_u32(reader)?;
                crc.update(&field_len.to_be_bytes());
                let field_bytes = read_bytes(reader, field_len as usize)?;
                crc.update(&field_bytes);
                let field = String::from_utf8_lossy(&field_bytes).to_string();

                let val_len = read_u32(reader)?;
                crc.update(&val_len.to_be_bytes());
                let val_bytes = read_bytes(reader, val_len as usize)?;
                crc.update(&val_bytes);
                hash.insert(field, Bytes::from(val_bytes));
            }
            StorageValue::Hash(hash)
        }
        TYPE_SET => {
            let count = read_u32(reader)?;
            crc.update(&count.to_be_bytes());
            let mut set = HashSet::new();
            for _ in 0..count {
                let len = read_u32(reader)?;
                crc.update(&len.to_be_bytes());
                let bytes = read_bytes(reader, len as usize)?;
                crc.update(&bytes);
                set.insert(Bytes::from(bytes));
            }
            StorageValue::Set(set)
        }
        TYPE_ZSET => {
            let count = read_u32(reader)?;
            crc.update(&count.to_be_bytes());
            let mut zset = ZSetData::new();
            for _ in 0..count {
                let member_len = read_u32(reader)?;
                crc.update(&member_len.to_be_bytes());
                let member_bytes = read_bytes(reader, member_len as usize)?;
                crc.update(&member_bytes);
                let member = String::from_utf8_lossy(&member_bytes).to_string();

                let score = read_f64(reader)?;
                crc.update(&score.to_be_bytes());
                zset.add(member, score);
            }
            StorageValue::ZSet(zset)
        }
        TYPE_HLL => {
            let data = read_bytes(reader, 16384)?;
            crc.update(&data);
            let hll = crate::storage::HyperLogLog::load(&data)?;
            StorageValue::HyperLogLog(hll)
        }
        _ => {
            return Err(AppError::Storage(format!(
                "RDB 文件包含未知的类型标记: {}",
                type_byte
            )));
        }
    };

    let mut map = db.inner.get_shard(&key).write().map_err(|e| {
        AppError::Storage(format!("RDB 加载时锁中毒: {}", e))
    })?;
    map.insert(key, value);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::StorageEngine;
    use bytes::Bytes;

    fn create_test_storage() -> StorageEngine {
        let storage = StorageEngine::new();

        // db 0: String
        storage.set("str_key".to_string(), Bytes::from("hello")).unwrap();

        // db 0: ExpiringString
        storage.set_with_ttl("ttl_key".to_string(), Bytes::from("ttl_value"), 100_000).unwrap();

        // db 0: List
        storage.rpush("list_key", vec![Bytes::from("a"), Bytes::from("b"), Bytes::from("c")]).unwrap();

        // db 0: Hash
        storage.hset("hash_key", "f1".to_string(), Bytes::from("v1")).unwrap();
        storage.hset("hash_key", "f2".to_string(), Bytes::from("v2")).unwrap();

        // db 0: Set
        storage.sadd("set_key", vec![Bytes::from("x"), Bytes::from("y")]).unwrap();

        // db 0: ZSet
        storage.zadd("zset_key", vec![(1.0, "a".to_string()), (2.0, "b".to_string())]).unwrap();

        // db 0: HyperLogLog
        let mut hll = crate::storage::HyperLogLog::new();
        hll.add("elem1");
        hll.add("elem2");
        storage.pfadd("hll_key", &["elem1".to_string(), "elem2".to_string()]).unwrap();

        // db 1: 另一个 key
        storage.select(1).unwrap();
        storage.set("db1_key".to_string(), Bytes::from("db1_value")).unwrap();
        storage.select(0).unwrap();

        storage
    }

    #[test]
    fn test_save_load_roundtrip() {
        let storage = create_test_storage();
        let path = "/tmp/test_rdb_1.rdb";
        save(&storage, path, None).unwrap();

        let loaded = StorageEngine::new();
        let (replid, offset) = load(&loaded, path).unwrap();
        assert!(replid.is_none());
        assert!(offset.is_none());

        // String
        assert_eq!(loaded.get("str_key").unwrap(), Some(Bytes::from("hello")));

        // List
        let list = loaded.lrange("list_key", 0, -1).unwrap();
        let list_str: Vec<String> = list.iter().map(|b| String::from_utf8_lossy(b).to_string()).collect();
        assert_eq!(list_str, vec!["a", "b", "c"]);

        // Hash
        assert_eq!(loaded.hget("hash_key", "f1").unwrap(), Some(Bytes::from("v1")));
        assert_eq!(loaded.hget("hash_key", "f2").unwrap(), Some(Bytes::from("v2")));

        // Set
        assert!(loaded.sismember("set_key", &Bytes::from("x")).unwrap());
        assert!(loaded.sismember("set_key", &Bytes::from("y")).unwrap());

        // ZSet
        assert_eq!(loaded.zscore("zset_key", "a").unwrap(), Some(1.0));
        assert_eq!(loaded.zscore("zset_key", "b").unwrap(), Some(2.0));

        // HyperLogLog
        assert_eq!(loaded.pfcount(&["hll_key".to_string()]).unwrap(), 2);

        // db 1
        loaded.select(1).unwrap();
        assert_eq!(loaded.get("db1_key").unwrap(), Some(Bytes::from("db1_value")));

        std::fs::remove_file(path).ok();
    }

    #[test]
    fn test_save_load_ttl() {
        let storage = StorageEngine::new();
        storage.set_with_ttl("ttl_key".to_string(), Bytes::from("value"), 100_000).unwrap();

        let path = "/tmp/test_rdb_ttl.rdb";
        save(&storage, path, None).unwrap();

        let loaded = StorageEngine::new();
        load(&loaded, path).unwrap();

        // 加载后 TTL 应存在（key 未过期）
        assert!(loaded.exists("ttl_key").unwrap());
        assert_eq!(loaded.get("ttl_key").unwrap(), Some(Bytes::from("value")));

        std::fs::remove_file(path).ok();
    }

    #[test]
    fn test_save_load_empty() {
        let storage = StorageEngine::new();
        let path = "/tmp/test_rdb_empty.rdb";
        save(&storage, path, None).unwrap();

        let loaded = StorageEngine::new();
        load(&loaded, path).unwrap();
        assert_eq!(loaded.dbsize().unwrap(), 0);

        std::fs::remove_file(path).ok();
    }

    #[test]
    fn test_load_corrupted_truncated() {
        let path = "/tmp/test_rdb_truncated.rdb";
        std::fs::write(path, b"REDIS-RUST").unwrap();
        let loaded = StorageEngine::new();
        let result = load(&loaded, path);
        assert!(result.is_err());
        std::fs::remove_file(path).ok();
    }

    #[test]
    fn test_load_corrupted_wrong_magic() {
        let path = "/tmp/test_rdb_magic.rdb";
        std::fs::write(path, b"WRONG-MAGIC").unwrap();
        let loaded = StorageEngine::new();
        let result = load(&loaded, path);
        assert!(result.is_err());
        std::fs::remove_file(path).ok();
    }

    #[test]
    fn test_save_load_with_repl_info() {
        let storage = create_test_storage();
        let path = "/tmp/test_rdb_repl.rdb";
        let replid = "abc123def456abc123def456abc123def456abc1";
        let offset = 12345i64;
        save(&storage, path, Some((replid.to_string(), offset))).unwrap();

        let loaded = StorageEngine::new();
        let (loaded_replid, loaded_offset) = load(&loaded, path).unwrap();
        assert_eq!(loaded_replid, Some(replid.to_string()));
        assert_eq!(loaded_offset, Some(offset));

        // 验证数据也正确加载
        assert_eq!(loaded.get("str_key").unwrap(), Some(Bytes::from("hello")));

        std::fs::remove_file(path).ok();
    }

    #[test]
    fn test_load_old_rdb_without_aux() {
        // 测试旧格式 RDB（没有 AUX 段）仍然可以正常加载
        let storage = create_test_storage();
        let path = "/tmp/test_rdb_old.rdb";
        save(&storage, path, None).unwrap();

        let loaded = StorageEngine::new();
        let (replid, offset) = load(&loaded, path).unwrap();
        assert!(replid.is_none());
        assert!(offset.is_none());
        assert_eq!(loaded.get("str_key").unwrap(), Some(Bytes::from("hello")));

        std::fs::remove_file(path).ok();
    }
}
