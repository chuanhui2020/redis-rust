// AOF (Append Only File) 持久化模块

use std::fs::{File, OpenOptions};
use std::io::{BufWriter, Write};

use bytes::Bytes;
use bytes::BytesMut;

use crate::command::{Command, CommandExecutor, CommandParser};
use crate::error::{AppError, Result};
use crate::protocol::RespParser;
use crate::storage::StorageEngine;

/// AOF 混合格式文件头标记
const AOF_RDB_PREAMBLE: &[u8] = b"REDIS-RUST-AOF-PREAMBLE\n";

/// AOF 写入器，将写操作以 RESP 格式追加到文件
pub struct AofWriter {
    /// 带缓冲的文件写入器
    writer: BufWriter<File>,
    /// AOF 文件路径
    path: String,
}

impl AofWriter {
    /// 创建或打开 AOF 文件，以追加模式写入
    pub fn new(path: &str) -> Result<Self> {
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)
            .map_err(AppError::Io)?;
        Ok(Self {
            writer: BufWriter::new(file),
            path: path.to_string(),
        })
    }

    /// 获取 AOF 文件路径
    pub fn path(&self) -> &str {
        &self.path
    }

    /// 重新打开 AOF 文件（用于重写后切换）
    pub fn reopen(&mut self) -> Result<()> {
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.path)
            .map_err(AppError::Io)?;
        self.writer = BufWriter::new(file);
        Ok(())
    }

    /// 将命令追加到 AOF 文件
    /// 只记录写操作命令，读取命令会被忽略
    pub fn append(&mut self, cmd: &Command) -> Result<()> {
        let resp = cmd.to_resp_value();
        let parser = RespParser::new();
        let encoded = parser.encode(&resp);
        self.writer
            .write_all(&encoded)
            .map_err(AppError::Io)?;
        Ok(())
    }

    /// 强制将缓冲区数据刷写到磁盘
    pub fn flush(&mut self) -> Result<()> {
        self.writer.flush().map_err(AppError::Io)?;
        Ok(())
    }
}

impl std::fmt::Debug for AofWriter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AofWriter").finish()
    }
}

/// AOF 重放器，启动时从 AOF 文件恢复数据
pub struct AofReplayer;

impl AofReplayer {
    /// 从 AOF 文件重放命令到存储引擎
    /// 如果文件不存在或为空则跳过
    /// 自动检测混合格式（RDB preamble + AOF 命令）
    pub fn replay(path: &str, storage: StorageEngine) -> Result<()> {
        let content = match std::fs::read(path) {
            Ok(data) => data,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                log::info!("AOF 文件不存在，跳过重放: {}", path);
                return Ok(());
            }
            Err(e) => return Err(AppError::Io(e)),
        };

        if content.is_empty() {
            log::info!("AOF 文件为空，跳过重放");
            return Ok(());
        }

        // 检测是否为混合格式
        if content.starts_with(AOF_RDB_PREAMBLE) {
            log::info!("检测到混合格式 AOF 文件，先加载 RDB 快照");
            let rdb_start = AOF_RDB_PREAMBLE.len();
            let mut cursor = std::io::Cursor::new(&content[rdb_start..]);
            let _ = crate::rdb::load_from_reader(&storage, &mut cursor, false)?;

            // 剩余字节作为 AOF 命令重放
            let rdb_consumed = cursor.position() as usize;
            let aof_data = &content[rdb_start + rdb_consumed..];
            log::info!("RDB 快照加载完成，剩余 AOF 数据: {} 字节", aof_data.len());
            Self::replay_raw_aof(aof_data, storage)?;
            return Ok(());
        }

        log::info!("开始 AOF 重放，文件大小: {} 字节", content.len());
        Self::replay_raw_aof(&content, storage)
    }

    /// 重放纯 AOF 字节数据
    fn replay_raw_aof(data: &[u8], storage: StorageEngine) -> Result<()> {
        let parser = RespParser::new();
        let cmd_parser = CommandParser::new();
        // 重放时不使用 AOF writer，避免循环写入
        let executor = CommandExecutor::new(storage);

        let mut buf = BytesMut::from(data);
        let mut count = 0usize;
        let mut errors = 0usize;

        while !buf.is_empty() {
            match parser.parse(&mut buf) {
                Ok(Some(resp)) => {
                    match cmd_parser.parse(resp) {
                        Ok(cmd) => {
                            match executor.execute(cmd) {
                                Ok(_) => count += 1,
                                Err(e) => {
                                    log::warn!("AOF 重放命令执行失败: {}", e);
                                    errors += 1;
                                }
                            }
                        }
                        Err(e) => {
                            log::warn!("AOF 重放命令解析失败: {}", e);
                            errors += 1;
                        }
                    }
                }
                Ok(None) => {
                    log::warn!(
                        "AOF 文件末尾数据不完整，忽略剩余 {} 字节",
                        buf.len()
                    );
                    break;
                }
                Err(e) => {
                    log::error!("AOF 重放 RESP 解析失败: {}", e);
                    errors += 1;
                    break;
                }
            }
        }

        log::info!(
            "AOF 重放完成，成功: {} 条，失败: {} 条",
            count,
            errors
        );
        Ok(())
    }
}

/// AOF 重写器，遍历当前存储生成最小化的 AOF 文件
pub struct AofRewriter;

impl AofRewriter {
    /// 执行 AOF 重写
    /// 将当前 storage 中所有存活的 key 写入临时文件，然后原子替换目标文件
    /// use_rdb_preamble: 是否先写入 RDB 快照作为 preamble
    pub fn rewrite(storage: &StorageEngine, temp_path: &str, target_path: &str, use_rdb_preamble: bool) -> Result<()> {
        let parser = RespParser::new();
        let mut writer = BufWriter::new(
            File::create(temp_path).map_err(AppError::Io)?
        );

        // 如果启用混合格式，先写入 RDB 快照
        if use_rdb_preamble {
            writer.write_all(AOF_RDB_PREAMBLE).map_err(AppError::Io)?;
            crate::rdb::save_to_writer(storage, &mut writer)?;
        } else {
            // 纯 AOF 格式：遍历所有存活的 key 生成重建命令
            let keys = storage.keys("*")?;

            for key in keys {
                let key_type = storage.key_type(&key)?;
                match key_type.as_str() {
                    "string" => {
                        if let Some(value) = storage.get(&key)? {
                            let cmd = Command::Set(key.clone(), value, crate::storage::SetOptions::default());
                            Self::write_cmd(&parser, &mut writer, &cmd)?;

                            // 如果有 TTL，追加 PEXPIRE
                            let ttl_ms = storage.pttl(&key)?;
                            if ttl_ms > 0 {
                                let expire_cmd = Command::PExpire(key.clone(), ttl_ms as u64);
                                Self::write_cmd(&parser, &mut writer, &expire_cmd)?;
                            }
                        }
                    }
                    "list" => {
                        let items = storage.lrange(&key, 0, -1)?;
                        if !items.is_empty() {
                            let cmd = Command::RPush(key.clone(), items);
                            Self::write_cmd(&parser, &mut writer, &cmd)?;
                        }
                    }
                    "hash" => {
                        let fields = storage.hgetall(&key)?;
                        if !fields.is_empty() {
                            let pairs: Vec<(String, Bytes)> = fields
                                .into_iter()
                                .map(|(f, v)| (f, v))
                                .collect();
                            let cmd = Command::HMSet(key.clone(), pairs);
                            Self::write_cmd(&parser, &mut writer, &cmd)?;
                        }
                    }
                    "set" => {
                        let members = storage.smembers(&key)?;
                        if !members.is_empty() {
                            let cmd = Command::SAdd(key.clone(), members);
                            Self::write_cmd(&parser, &mut writer, &cmd)?;
                        }
                    }
                    "zset" => {
                        let members = storage.zrange(&key, 0, -1, true)?;
                        if !members.is_empty() {
                            let pairs: Vec<(f64, String)> = members
                                .into_iter()
                                .map(|(m, s)| (s, m))
                                .collect();
                            let cmd = Command::ZAdd(key.clone(), pairs);
                            Self::write_cmd(&parser, &mut writer, &cmd)?;
                        }
                    }
                    _ => {}
                }
            }
        }

        // 刷盘并关闭临时文件
        writer.flush().map_err(AppError::Io)?;
        drop(writer);

        // 原子替换目标文件
        std::fs::rename(temp_path, target_path).map_err(AppError::Io)?;

        log::info!("AOF 重写完成，已替换: {}", target_path);
        Ok(())
    }

    /// 将单个命令编码并写入
    fn write_cmd(
        parser: &RespParser,
        writer: &mut BufWriter<File>,
        cmd: &Command,
    ) -> Result<()> {
        let resp = cmd.to_resp_value();
        let encoded = parser.encode(&resp);
        writer.write_all(&encoded).map_err(AppError::Io)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use super::*;
    use bytes::Bytes;

    fn temp_aof_path(name: &str) -> String {
        let mut path = std::env::temp_dir();
        path.push(format!("redis_rust_test_{}.aof", name));
        let path_str = path.to_string_lossy().to_string();
        // 清理旧文件
        let _ = std::fs::remove_file(&path_str);
        path_str
    }

    #[test]
    fn test_aof_write_and_replay() {
        let path = temp_aof_path("write_and_replay");

        let storage1 = StorageEngine::new();
        let aof = Arc::new(Mutex::new(AofWriter::new(&path).unwrap()));
        let executor = CommandExecutor::new_with_aof(storage1.clone(), aof.clone());

        executor
            .execute(Command::Set("name".to_string(), Bytes::from("redis"), crate::storage::SetOptions::default()))
            .unwrap();
        executor
            .execute(Command::Set("age".to_string(), Bytes::from("10"), crate::storage::SetOptions::default()))
            .unwrap();
        aof.lock().unwrap().flush().unwrap();

        // 用新 storage 重放
        let storage2 = StorageEngine::new();
        AofReplayer::replay(&path, storage2.clone()).unwrap();

        assert_eq!(
            storage2.get("name").unwrap(),
            Some(Bytes::from("redis"))
        );
        assert_eq!(storage2.get("age").unwrap(), Some(Bytes::from("10")));

        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn test_aof_del_replay() {
        let path = temp_aof_path("del_replay");

        let storage1 = StorageEngine::new();
        let aof = Arc::new(Mutex::new(AofWriter::new(&path).unwrap()));
        let executor = CommandExecutor::new_with_aof(storage1.clone(), aof.clone());

        executor
            .execute(Command::Set("key".to_string(), Bytes::from("val"), crate::storage::SetOptions::default()))
            .unwrap();
        executor
            .execute(Command::Del(vec!["key".to_string()]))
            .unwrap();
        aof.lock().unwrap().flush().unwrap();

        let storage2 = StorageEngine::new();
        AofReplayer::replay(&path, storage2.clone()).unwrap();

        assert_eq!(storage2.get("key").unwrap(), None);

        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn test_aof_setex_replay() {
        let path = temp_aof_path("setex_replay");

        let storage1 = StorageEngine::new();
        let aof = Arc::new(Mutex::new(AofWriter::new(&path).unwrap()));
        let executor = CommandExecutor::new_with_aof(storage1.clone(), aof.clone());

        // 设置 1 小时的 TTL
        executor
            .execute(Command::SetEx(
                "key".to_string(),
                Bytes::from("val"),
                3_600_000,
            ))
            .unwrap();
        aof.lock().unwrap().flush().unwrap();

        let storage2 = StorageEngine::new();
        AofReplayer::replay(&path, storage2.clone()).unwrap();

        assert_eq!(storage2.get("key").unwrap(), Some(Bytes::from("val")));
        let ttl = storage2.ttl("key").unwrap();
        assert!(ttl > 3_500_000); // 大于 3500 秒

        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn test_aof_flushall_replay() {
        let path = temp_aof_path("flushall_replay");

        let storage1 = StorageEngine::new();
        let aof = Arc::new(Mutex::new(AofWriter::new(&path).unwrap()));
        let executor = CommandExecutor::new_with_aof(storage1.clone(), aof.clone());

        executor
            .execute(Command::Set("a".to_string(), Bytes::from("1"), crate::storage::SetOptions::default()))
            .unwrap();
        executor.execute(Command::FlushAll).unwrap();
        executor
            .execute(Command::Set("b".to_string(), Bytes::from("2"), crate::storage::SetOptions::default()))
            .unwrap();
        aof.lock().unwrap().flush().unwrap();

        let storage2 = StorageEngine::new();
        AofReplayer::replay(&path, storage2.clone()).unwrap();

        assert_eq!(storage2.get("a").unwrap(), None);
        assert_eq!(storage2.get("b").unwrap(), Some(Bytes::from("2")));

        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn test_aof_replay_missing_file() {
        let path = temp_aof_path("missing");
        // 确保文件不存在
        let _ = std::fs::remove_file(&path);

        let storage = StorageEngine::new();
        // 不应报错
        AofReplayer::replay(&path, storage).unwrap();
    }

    #[test]
    fn test_aof_rewrite() {
        let path = temp_aof_path("rewrite");
        let temp_path = format!("{}.tmp", path);

        let storage = StorageEngine::new();
        // 写入一些数据
        storage.set("a".to_string(), Bytes::from("1")).unwrap();
        storage.set("b".to_string(), Bytes::from("2")).unwrap();
        storage.lpush("list", vec![Bytes::from("x"), Bytes::from("y")]).unwrap();

        // 执行重写
        AofRewriter::rewrite(&storage, &temp_path, &path, false).unwrap();

        // 验证重写后的文件能正确重放
        let storage2 = StorageEngine::new();
        AofReplayer::replay(&path, storage2.clone()).unwrap();

        assert_eq!(storage2.get("a").unwrap(), Some(Bytes::from("1")));
        assert_eq!(storage2.get("b").unwrap(), Some(Bytes::from("2")));
        let list = storage2.lrange("list", 0, -1).unwrap();
        assert_eq!(list.len(), 2);

        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn test_aof_rewrite_with_ttl() {
        let path = temp_aof_path("rewrite_ttl");
        let temp_path = format!("{}.tmp", path);

        let storage = StorageEngine::new();
        storage.set_with_ttl("k".to_string(), Bytes::from("v"), 3_600_000).unwrap();

        AofRewriter::rewrite(&storage, &temp_path, &path, false).unwrap();

        let storage2 = StorageEngine::new();
        AofReplayer::replay(&path, storage2.clone()).unwrap();

        assert_eq!(storage2.get("k").unwrap(), Some(Bytes::from("v")));
        let ttl = storage2.ttl("k").unwrap();
        assert!(ttl > 3_500_000);

        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn test_aof_rewrite_reduces_size() {
        let path = temp_aof_path("rewrite_size");
        let temp_path = format!("{}.tmp", path);

        let storage = StorageEngine::new();
        let aof = Arc::new(Mutex::new(AofWriter::new(&path).unwrap()));
        let executor = CommandExecutor::new_with_aof(storage.clone(), aof.clone());

        // 写入然后删除再写入，产生冗余的 AOF
        executor.execute(Command::Set("a".to_string(), Bytes::from("old"), crate::storage::SetOptions::default())).unwrap();
        executor.execute(Command::Set("a".to_string(), Bytes::from("mid"), crate::storage::SetOptions::default())).unwrap();
        executor.execute(Command::Set("a".to_string(), Bytes::from("new"), crate::storage::SetOptions::default())).unwrap();
        executor.execute(Command::Del(vec!["b".to_string()])).unwrap(); // 删除不存在的 key
        aof.lock().unwrap().flush().unwrap();

        let old_size = std::fs::metadata(&path).unwrap().len();

        // 重写后只保留最终状态
        AofRewriter::rewrite(&storage, &temp_path, &path, false).unwrap();
        let new_size = std::fs::metadata(&path).unwrap().len();

        assert!(new_size < old_size, "重写后文件应变小: {} -> {}", old_size, new_size);

        // 验证数据正确
        let storage2 = StorageEngine::new();
        AofReplayer::replay(&path, storage2.clone()).unwrap();
        assert_eq!(storage2.get("a").unwrap(), Some(Bytes::from("new")));

        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn test_aof_rdb_preamble_rewrite() {
        let path = temp_aof_path("rdb_preamble");
        let temp_path = format!("{}.tmp", path);

        let storage = StorageEngine::new();
        storage.set("a".to_string(), Bytes::from("1")).unwrap();
        storage.set("b".to_string(), Bytes::from("2")).unwrap();
        storage.lpush("list", vec![Bytes::from("x"), Bytes::from("y")]).unwrap();

        // 使用混合格式重写
        AofRewriter::rewrite(&storage, &temp_path, &path, true).unwrap();

        // 验证文件头
        let content = std::fs::read(&path).unwrap();
        assert!(content.starts_with(b"REDIS-RUST-AOF-PREAMBLE\n"));

        // 验证能正确重放
        let storage2 = StorageEngine::new();
        AofReplayer::replay(&path, storage2.clone()).unwrap();

        assert_eq!(storage2.get("a").unwrap(), Some(Bytes::from("1")));
        assert_eq!(storage2.get("b").unwrap(), Some(Bytes::from("2")));
        let list = storage2.lrange("list", 0, -1).unwrap();
        assert_eq!(list.len(), 2);

        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn test_aof_rdb_preamble_with_commands() {
        let path = temp_aof_path("rdb_preamble_cmds");
        let temp_path = format!("{}.tmp", path);

        let storage = StorageEngine::new();
        let aof = Arc::new(Mutex::new(AofWriter::new(&path).unwrap()));
        let executor = CommandExecutor::new_with_aof(storage.clone(), aof.clone());

        // 写入初始数据（会被记录到 AOF）
        executor.execute(Command::Set("a".to_string(), Bytes::from("1"), crate::storage::SetOptions::default())).unwrap();
        executor.execute(Command::Set("b".to_string(), Bytes::from("2"), crate::storage::SetOptions::default())).unwrap();
        aof.lock().unwrap().flush().unwrap();

        // 混合格式重写：RDB 快照 + 当前 AOF 中的命令被重写为新的 AOF 命令
        AofRewriter::rewrite(&storage, &temp_path, &path, true).unwrap();

        // 重放混合格式文件
        let storage2 = StorageEngine::new();
        AofReplayer::replay(&path, storage2.clone()).unwrap();

        assert_eq!(storage2.get("a").unwrap(), Some(Bytes::from("1")));
        assert_eq!(storage2.get("b").unwrap(), Some(Bytes::from("2")));

        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn test_aof_rdb_preamble_pure_aof_compat() {
        let path = temp_aof_path("pure_aof_compat");

        let storage1 = StorageEngine::new();
        let aof = Arc::new(Mutex::new(AofWriter::new(&path).unwrap()));
        let executor = CommandExecutor::new_with_aof(storage1.clone(), aof.clone());

        executor.execute(Command::Set("key".to_string(), Bytes::from("val"), crate::storage::SetOptions::default())).unwrap();
        aof.lock().unwrap().flush().unwrap();

        // 纯 AOF 格式（无 RDB preamble）应能正常重放
        let storage2 = StorageEngine::new();
        AofReplayer::replay(&path, storage2.clone()).unwrap();

        assert_eq!(storage2.get("key").unwrap(), Some(Bytes::from("val")));

        let _ = std::fs::remove_file(&path);
    }
}
