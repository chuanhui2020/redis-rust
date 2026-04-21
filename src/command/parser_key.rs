use crate::error::{AppError, Result};
use crate::protocol::RespValue;
use super::{Command, CommandParser};

impl CommandParser {
    pub(crate) fn parse_del(&self, arr: &[RespValue]) -> Result<Command> {
        if arr.len() < 2 {
            return Err(AppError::Command(
                "DEL 命令需要至少 1 个参数".to_string(),
            ));
        }

        let keys = arr[1..]
            .iter()
            .map(|v| self.extract_string(v))
            .collect::<Result<Vec<String>>>()?;

        Ok(Command::Del(keys))
    }
    pub(crate) fn parse_exists(&self, arr: &[RespValue]) -> Result<Command> {
        if arr.len() < 2 {
            return Err(AppError::Command(
                "EXISTS 命令需要至少 1 个参数".to_string(),
            ));
        }

        let keys = arr[1..]
            .iter()
            .map(|v| self.extract_string(v))
            .collect::<Result<Vec<String>>>()?;

        Ok(Command::Exists(keys))
    }
    pub(crate) fn parse_expire(&self, arr: &[RespValue]) -> Result<Command> {
        if arr.len() != 3 {
            return Err(AppError::Command(
                "EXPIRE 命令需要 2 个参数".to_string(),
            ));
        }

        let key = self.extract_string(&arr[1])?;
        let seconds: u64 = self.extract_string(&arr[2])?.parse().map_err(|_| {
            AppError::Command("EXPIRE 的秒数必须是正整数".to_string())
        })?;

        Ok(Command::Expire(key, seconds))
    }
    pub(crate) fn parse_ttl(&self, arr: &[RespValue]) -> Result<Command> {
        if arr.len() != 2 {
            return Err(AppError::Command(
                "TTL 命令需要 1 个参数".to_string(),
            ));
        }

        let key = self.extract_string(&arr[1])?;
        Ok(Command::Ttl(key))
    }
    pub(crate) fn parse_keys(&self, arr: &[RespValue]) -> Result<Command> {
        if arr.len() != 2 {
            return Err(AppError::Command(
                "KEYS 命令需要 1 个参数".to_string(),
            ));
        }
        let pattern = self.extract_string(&arr[1])?;
        Ok(Command::Keys(pattern))
    }
    pub(crate) fn parse_scan(&self, arr: &[RespValue]) -> Result<Command> {
        if arr.len() < 2 {
            return Err(AppError::Command(
                "SCAN 命令需要至少 1 个参数".to_string(),
            ));
        }
        let cursor = self.extract_string(&arr[1])?.parse::<usize>()
            .map_err(|_| AppError::Command("SCAN cursor 必须是数字".to_string()))?;
        let mut pattern = String::new();
        let mut count = 0usize;
        let mut i = 2;
        while i < arr.len() {
            let flag = self.extract_string(&arr[i])?.to_ascii_uppercase();
            match flag.as_str() {
                "MATCH" => {
                    if i + 1 >= arr.len() {
                        return Err(AppError::Command("SCAN MATCH 需要参数".to_string()));
                    }
                    pattern = self.extract_string(&arr[i + 1])?;
                    i += 2;
                }
                "COUNT" => {
                    if i + 1 >= arr.len() {
                        return Err(AppError::Command("SCAN COUNT 需要参数".to_string()));
                    }
                    count = self.extract_string(&arr[i + 1])?.parse::<usize>()
                        .map_err(|_| AppError::Command("SCAN COUNT 必须是数字".to_string()))?;
                    i += 2;
                }
                _ => {
                    return Err(AppError::Command(
                        format!("SCAN 不支持参数 {}", flag),
                    ));
                }
            }
        }
        Ok(Command::Scan(cursor, pattern, count))
    }
    pub(crate) fn parse_rename(&self, arr: &[RespValue]) -> Result<Command> {
        if arr.len() != 3 {
            return Err(AppError::Command(
                "RENAME 命令需要 2 个参数".to_string(),
            ));
        }
        let key = self.extract_string(&arr[1])?;
        let newkey = self.extract_string(&arr[2])?;
        Ok(Command::Rename(key, newkey))
    }
    pub(crate) fn parse_type(&self, arr: &[RespValue]) -> Result<Command> {
        if arr.len() != 2 {
            return Err(AppError::Command(
                "TYPE 命令需要 1 个参数".to_string(),
            ));
        }
        let key = self.extract_string(&arr[1])?;
        Ok(Command::Type(key))
    }
    pub(crate) fn parse_persist(&self, arr: &[RespValue]) -> Result<Command> {
        if arr.len() != 2 {
            return Err(AppError::Command(
                "PERSIST 命令需要 1 个参数".to_string(),
            ));
        }
        let key = self.extract_string(&arr[1])?;
        Ok(Command::Persist(key))
    }
    pub(crate) fn parse_pexpire(&self, arr: &[RespValue]) -> Result<Command> {
        if arr.len() != 3 {
            return Err(AppError::Command(
                "PEXPIRE 命令需要 2 个参数".to_string(),
            ));
        }
        let key = self.extract_string(&arr[1])?;
        let ms = self.extract_string(&arr[2])?.parse::<u64>()
            .map_err(|_| AppError::Command("PEXPIRE 时间必须是数字".to_string()))?;
        Ok(Command::PExpire(key, ms))
    }
    pub(crate) fn parse_pttl(&self, arr: &[RespValue]) -> Result<Command> {
        if arr.len() != 2 {
            return Err(AppError::Command(
                "PTTL 命令需要 1 个参数".to_string(),
            ));
        }
        let key = self.extract_string(&arr[1])?;
        Ok(Command::PTtl(key))
    }
    pub(crate) fn parse_unlink(&self, arr: &[RespValue]) -> Result<Command> {
        if arr.len() < 2 {
            return Err(AppError::Command(
                "UNLINK 命令需要至少 1 个参数".to_string(),
            ));
        }
        let mut keys = Vec::new();
        for i in 1..arr.len() {
            keys.push(self.extract_string(&arr[i])?);
        }
        Ok(Command::Unlink(keys))
    }
    pub(crate) fn parse_copy(&self, arr: &[RespValue]) -> Result<Command> {
        if arr.len() < 3 {
            return Err(AppError::Command(
                "COPY 命令需要 2 个参数".to_string(),
            ));
        }
        let source = self.extract_string(&arr[1])?;
        let destination = self.extract_string(&arr[2])?;
        let mut replace = false;
        if arr.len() == 4 {
            let arg = self.extract_string(&arr[3])?.to_ascii_uppercase();
            if arg == "REPLACE" {
                replace = true;
            } else {
                return Err(AppError::Command(format!("COPY 未知参数: {}", arg)));
            }
        } else if arr.len() > 4 {
            return Err(AppError::Command("COPY 参数过多".to_string()));
        }
        Ok(Command::Copy(source, destination, replace))
    }
    pub(crate) fn parse_dump(&self, arr: &[RespValue]) -> Result<Command> {
        if arr.len() != 2 {
            return Err(AppError::Command(
                "DUMP 命令需要 1 个参数".to_string(),
            ));
        }
        let key = self.extract_string(&arr[1])?;
        Ok(Command::Dump(key))
    }
    pub(crate) fn parse_restore(&self, arr: &[RespValue]) -> Result<Command> {
        if arr.len() < 4 {
            return Err(AppError::Command(
                "RESTORE 命令需要至少 3 个参数".to_string(),
            ));
        }
        let key = self.extract_string(&arr[1])?;
        let ttl_ms: u64 = self.extract_string(&arr[2])?.parse().map_err(|_| {
            AppError::Command("RESTORE ttl 必须是整数".to_string())
        })?;
        let serialized = self.extract_bytes(&arr[3])?;
        let mut replace = false;
        if arr.len() == 5 {
            let arg = self.extract_string(&arr[4])?.to_ascii_uppercase();
            if arg == "REPLACE" {
                replace = true;
            } else {
                return Err(AppError::Command(format!("RESTORE 未知参数: {}", arg)));
            }
        } else if arr.len() > 5 {
            return Err(AppError::Command("RESTORE 参数过多".to_string()));
        }
        Ok(Command::Restore(key, ttl_ms, serialized.to_vec(), replace))
    }
    pub(crate) fn parse_expireat(&self, arr: &[RespValue]) -> Result<Command> {
        if arr.len() != 3 {
            return Err(AppError::Command(
                "EXPIREAT 命令需要 2 个参数".to_string(),
            ));
        }
        let key = self.extract_string(&arr[1])?;
        let timestamp: u64 = self.extract_string(&arr[2])?.parse().map_err(|_| {
            AppError::Command("EXPIREAT 时间戳必须是整数".to_string())
        })?;
        Ok(Command::ExpireAt(key, timestamp))
    }
    pub(crate) fn parse_pexpireat(&self, arr: &[RespValue]) -> Result<Command> {
        if arr.len() != 3 {
            return Err(AppError::Command(
                "PEXPIREAT 命令需要 2 个参数".to_string(),
            ));
        }
        let key = self.extract_string(&arr[1])?;
        let timestamp: u64 = self.extract_string(&arr[2])?.parse().map_err(|_| {
            AppError::Command("PEXPIREAT 时间戳必须是整数".to_string())
        })?;
        Ok(Command::PExpireAt(key, timestamp))
    }
    pub(crate) fn parse_expiretime(&self, arr: &[RespValue]) -> Result<Command> {
        if arr.len() != 2 {
            return Err(AppError::Command(
                "EXPIRETIME 命令需要 1 个参数".to_string(),
            ));
        }
        Ok(Command::ExpireTime(self.extract_string(&arr[1])?))
    }
    pub(crate) fn parse_pexpiretime(&self, arr: &[RespValue]) -> Result<Command> {
        if arr.len() != 2 {
            return Err(AppError::Command(
                "PEXPIRETIME 命令需要 1 个参数".to_string(),
            ));
        }
        Ok(Command::PExpireTime(self.extract_string(&arr[1])?))
    }
    pub(crate) fn parse_renamenx(&self, arr: &[RespValue]) -> Result<Command> {
        if arr.len() != 3 {
            return Err(AppError::Command(
                "RENAMENX 命令需要 2 个参数".to_string(),
            ));
        }
        Ok(Command::RenameNx(
            self.extract_string(&arr[1])?,
            self.extract_string(&arr[2])?,
        ))
    }
    pub(crate) fn parse_touch(&self, arr: &[RespValue]) -> Result<Command> {
        if arr.len() < 2 {
            return Err(AppError::Command(
                "TOUCH 命令需要至少 1 个 key".to_string(),
            ));
        }
        let keys: Vec<String> = arr[1..].iter()
            .map(|v| self.extract_string(v))
            .collect::<Result<_>>()?;
        Ok(Command::Touch(keys))
    }
}
