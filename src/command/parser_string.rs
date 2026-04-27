//! String 命令解析器
use super::*;

use super::parser::CommandParser;
use crate::error::{AppError, Result};
use crate::protocol::RespValue;

impl CommandParser {
    /// 解析 GET 命令：GET key
    pub(crate) fn parse_get(&self, arr: &[RespValue]) -> Result<Command> {
        if arr.len() != 2 {
            return Err(AppError::Command("GET 命令需要 1 个参数".to_string()));
        }

        let key = self.extract_string(&arr[1])?;
        Ok(Command::Get(key))
    }

    /// 解析 SET 命令：SET key value [EX seconds]
    pub(crate) fn parse_set(&self, arr: &[RespValue]) -> Result<Command> {
        if arr.len() < 3 {
            return Err(AppError::Command(
                "SET 命令至少需要 key 和 value".to_string(),
            ));
        }

        let key = self.extract_string(&arr[1])?;
        let value = self.extract_bytes(&arr[2])?;

        let mut options = crate::storage::SetOptions::default();
        let mut i = 3;

        while i < arr.len() {
            let opt = self.extract_string(&arr[i])?.to_ascii_uppercase();
            match opt.as_str() {
                "NX" => options.nx = true,
                "XX" => options.xx = true,
                "GET" => options.get = true,
                "KEEPTTL" => options.keepttl = true,
                "EX" => {
                    i += 1;
                    if i >= arr.len() {
                        return Err(AppError::Command("EX 需要值".to_string()));
                    }
                    let seconds: u64 = self
                        .extract_string(&arr[i])?
                        .parse()
                        .map_err(|_| AppError::Command("EX 值必须是正整数".to_string()))?;
                    options.expire = Some(crate::storage::SetExpireOption::Ex(seconds));
                }
                "PX" => {
                    i += 1;
                    if i >= arr.len() {
                        return Err(AppError::Command("PX 需要值".to_string()));
                    }
                    let millis: u64 = self
                        .extract_string(&arr[i])?
                        .parse()
                        .map_err(|_| AppError::Command("PX 值必须是正整数".to_string()))?;
                    options.expire = Some(crate::storage::SetExpireOption::Px(millis));
                }
                "EXAT" => {
                    i += 1;
                    if i >= arr.len() {
                        return Err(AppError::Command("EXAT 需要值".to_string()));
                    }
                    let ts: u64 = self
                        .extract_string(&arr[i])?
                        .parse()
                        .map_err(|_| AppError::Command("EXAT 值必须是正整数".to_string()))?;
                    options.expire = Some(crate::storage::SetExpireOption::ExAt(ts));
                }
                "PXAT" => {
                    i += 1;
                    if i >= arr.len() {
                        return Err(AppError::Command("PXAT 需要值".to_string()));
                    }
                    let ts: u64 = self
                        .extract_string(&arr[i])?
                        .parse()
                        .map_err(|_| AppError::Command("PXAT 值必须是正整数".to_string()))?;
                    options.expire = Some(crate::storage::SetExpireOption::PxAt(ts));
                }
                _ => {
                    return Err(AppError::Command(format!("SET 未知选项: {}", opt)));
                }
            }
            i += 1;
        }

        Ok(Command::Set(key, value, options))
    }

    /// 解析 MGET 命令：MGET key [key ...]
    pub(crate) fn parse_mget(&self, arr: &[RespValue]) -> Result<Command> {
        if arr.len() < 2 {
            return Err(AppError::Command("MGET 命令需要至少 1 个参数".to_string()));
        }

        let keys = arr[1..]
            .iter()
            .map(|v| self.extract_string(v))
            .collect::<Result<Vec<String>>>()?;
        Ok(Command::MGet(keys))
    }

    /// 解析 MSET 命令：MSET key value [key value ...]
    pub(crate) fn parse_mset(&self, arr: &[RespValue]) -> Result<Command> {
        if arr.len() < 3 || arr.len().is_multiple_of(2) {
            return Err(AppError::Command(
                "MSET 命令需要成对的 key value 参数".to_string(),
            ));
        }

        let mut pairs = Vec::new();
        for i in (1..arr.len()).step_by(2) {
            let key = self.extract_string(&arr[i])?;
            let value = self.extract_bytes(&arr[i + 1])?;
            pairs.push((key, value));
        }
        Ok(Command::MSet(pairs))
    }

    /// 解析 INCR 命令：INCR key
    pub(crate) fn parse_incr(&self, arr: &[RespValue]) -> Result<Command> {
        if arr.len() != 2 {
            return Err(AppError::Command("INCR 命令需要 1 个参数".to_string()));
        }
        let key = self.extract_string(&arr[1])?;
        Ok(Command::Incr(key))
    }

    /// 解析 DECR 命令：DECR key
    pub(crate) fn parse_decr(&self, arr: &[RespValue]) -> Result<Command> {
        if arr.len() != 2 {
            return Err(AppError::Command("DECR 命令需要 1 个参数".to_string()));
        }
        let key = self.extract_string(&arr[1])?;
        Ok(Command::Decr(key))
    }

    /// 解析 INCRBY 命令：INCRBY key delta
    pub(crate) fn parse_incrby(&self, arr: &[RespValue]) -> Result<Command> {
        if arr.len() != 3 {
            return Err(AppError::Command("INCRBY 命令需要 2 个参数".to_string()));
        }
        let key = self.extract_string(&arr[1])?;
        let delta: i64 = self
            .extract_string(&arr[2])?
            .parse()
            .map_err(|_| AppError::Command("INCRBY 的增量必须是整数".to_string()))?;
        Ok(Command::IncrBy(key, delta))
    }

    /// 解析 DECRBY 命令：DECRBY key delta
    pub(crate) fn parse_decrby(&self, arr: &[RespValue]) -> Result<Command> {
        if arr.len() != 3 {
            return Err(AppError::Command("DECRBY 命令需要 2 个参数".to_string()));
        }
        let key = self.extract_string(&arr[1])?;
        let delta: i64 = self
            .extract_string(&arr[2])?
            .parse()
            .map_err(|_| AppError::Command("DECRBY 的减量必须是整数".to_string()))?;
        Ok(Command::DecrBy(key, delta))
    }

    /// 解析 APPEND 命令：APPEND key value
    pub(crate) fn parse_append(&self, arr: &[RespValue]) -> Result<Command> {
        if arr.len() != 3 {
            return Err(AppError::Command("APPEND 命令需要 2 个参数".to_string()));
        }
        let key = self.extract_string(&arr[1])?;
        let value = self.extract_bytes(&arr[2])?;
        Ok(Command::Append(key, value))
    }

    /// 解析 SETNX 命令：SETNX key value
    pub(crate) fn parse_setnx(&self, arr: &[RespValue]) -> Result<Command> {
        if arr.len() != 3 {
            return Err(AppError::Command("SETNX 命令需要 2 个参数".to_string()));
        }
        let key = self.extract_string(&arr[1])?;
        let value = self.extract_bytes(&arr[2])?;
        Ok(Command::SetNx(key, value))
    }

    /// 解析 SETEX 命令：SETEX key seconds value
    pub(crate) fn parse_setex(&self, arr: &[RespValue]) -> Result<Command> {
        if arr.len() != 4 {
            return Err(AppError::Command("SETEX 命令需要 3 个参数".to_string()));
        }
        let key = self.extract_string(&arr[1])?;
        let seconds: u64 = self
            .extract_string(&arr[2])?
            .parse()
            .map_err(|_| AppError::Command("SETEX 的秒数必须是正整数".to_string()))?;
        let value = self.extract_bytes(&arr[3])?;
        Ok(Command::SetExCmd(key, value, seconds))
    }

    /// 解析 PSETEX 命令：PSETEX key milliseconds value
    pub(crate) fn parse_psetex(&self, arr: &[RespValue]) -> Result<Command> {
        if arr.len() != 4 {
            return Err(AppError::Command("PSETEX 命令需要 3 个参数".to_string()));
        }
        let key = self.extract_string(&arr[1])?;
        let ms: u64 = self
            .extract_string(&arr[2])?
            .parse()
            .map_err(|_| AppError::Command("PSETEX 的毫秒数必须是正整数".to_string()))?;
        let value = self.extract_bytes(&arr[3])?;
        Ok(Command::PSetEx(key, value, ms))
    }

    /// 解析 GETSET 命令：GETSET key value
    pub(crate) fn parse_getset(&self, arr: &[RespValue]) -> Result<Command> {
        if arr.len() != 3 {
            return Err(AppError::Command("GETSET 命令需要 2 个参数".to_string()));
        }
        let key = self.extract_string(&arr[1])?;
        let value = self.extract_bytes(&arr[2])?;
        Ok(Command::GetSet(key, value))
    }

    /// 解析 GETDEL 命令：GETDEL key
    pub(crate) fn parse_getdel(&self, arr: &[RespValue]) -> Result<Command> {
        if arr.len() != 2 {
            return Err(AppError::Command("GETDEL 命令需要 1 个参数".to_string()));
        }
        let key = self.extract_string(&arr[1])?;
        Ok(Command::GetDel(key))
    }

    /// 解析 GETEX 命令：GETEX key [EX seconds|PX milliseconds|EXAT timestamp|PXAT ms-timestamp|PERSIST]
    pub(crate) fn parse_getex(&self, arr: &[RespValue]) -> Result<Command> {
        if arr.len() < 2 {
            return Err(AppError::Command("GETEX 命令需要至少 1 个参数".to_string()));
        }
        let key = self.extract_string(&arr[1])?;
        if arr.len() == 2 {
            return Ok(Command::GetEx(key, GetExOption::Persist));
        }

        let opt = self.extract_string(&arr[2])?.to_ascii_uppercase();
        match opt.as_str() {
            "EX" => {
                if arr.len() != 4 {
                    return Err(AppError::Command("GETEX EX 需要 1 个参数".to_string()));
                }
                let seconds: u64 = self
                    .extract_string(&arr[3])?
                    .parse()
                    .map_err(|_| AppError::Command("GETEX EX 值必须是正整数".to_string()))?;
                Ok(Command::GetEx(key, GetExOption::Ex(seconds)))
            }
            "PX" => {
                if arr.len() != 4 {
                    return Err(AppError::Command("GETEX PX 需要 1 个参数".to_string()));
                }
                let ms: u64 = self
                    .extract_string(&arr[3])?
                    .parse()
                    .map_err(|_| AppError::Command("GETEX PX 值必须是正整数".to_string()))?;
                Ok(Command::GetEx(key, GetExOption::Px(ms)))
            }
            "EXAT" => {
                if arr.len() != 4 {
                    return Err(AppError::Command("GETEX EXAT 需要 1 个参数".to_string()));
                }
                let ts: u64 = self
                    .extract_string(&arr[3])?
                    .parse()
                    .map_err(|_| AppError::Command("GETEX EXAT 值必须是正整数".to_string()))?;
                Ok(Command::GetEx(key, GetExOption::ExAt(ts)))
            }
            "PXAT" => {
                if arr.len() != 4 {
                    return Err(AppError::Command("GETEX PXAT 需要 1 个参数".to_string()));
                }
                let ts: u64 = self
                    .extract_string(&arr[3])?
                    .parse()
                    .map_err(|_| AppError::Command("GETEX PXAT 值必须是正整数".to_string()))?;
                Ok(Command::GetEx(key, GetExOption::PxAt(ts)))
            }
            "PERSIST" => {
                if arr.len() != 3 {
                    return Err(AppError::Command(
                        "GETEX PERSIST 不需要额外参数".to_string(),
                    ));
                }
                Ok(Command::GetEx(key, GetExOption::Persist))
            }
            _ => Err(AppError::Command(format!("GETEX 不支持的选项: {}", opt))),
        }
    }

    /// 解析 MSETNX 命令：MSETNX key value [key value ...]
    pub(crate) fn parse_msetnx(&self, arr: &[RespValue]) -> Result<Command> {
        if arr.len() < 3 || arr.len().is_multiple_of(2) {
            return Err(AppError::Command(
                "MSETNX 命令需要成对的 key-value 参数".to_string(),
            ));
        }
        let mut pairs = Vec::new();
        for i in (1..arr.len()).step_by(2) {
            let key = self.extract_string(&arr[i])?;
            let value = self.extract_bytes(&arr[i + 1])?;
            pairs.push((key, value));
        }
        Ok(Command::MSetNx(pairs))
    }

    /// 解析 INCRBYFLOAT 命令：INCRBYFLOAT key increment
    pub(crate) fn parse_incrbyfloat(&self, arr: &[RespValue]) -> Result<Command> {
        if arr.len() != 3 {
            return Err(AppError::Command(
                "INCRBYFLOAT 命令需要 2 个参数".to_string(),
            ));
        }
        let key = self.extract_string(&arr[1])?;
        let delta: f64 = self
            .extract_string(&arr[2])?
            .parse()
            .map_err(|_| AppError::Command("INCRBYFLOAT 的增量必须是有效的浮点数".to_string()))?;
        Ok(Command::IncrByFloat(key, delta))
    }

    /// 解析 SETRANGE 命令：SETRANGE key offset value
    pub(crate) fn parse_setrange(&self, arr: &[RespValue]) -> Result<Command> {
        if arr.len() != 4 {
            return Err(AppError::Command("SETRANGE 命令需要 3 个参数".to_string()));
        }
        let key = self.extract_string(&arr[1])?;
        let offset: usize = self
            .extract_string(&arr[2])?
            .parse()
            .map_err(|_| AppError::Command("SETRANGE 的 offset 必须是非负整数".to_string()))?;
        let value = self.extract_bytes(&arr[3])?;
        Ok(Command::SetRange(key, offset, value))
    }

    /// 解析 GETRANGE 命令：GETRANGE key start end
    pub(crate) fn parse_getrange(&self, arr: &[RespValue]) -> Result<Command> {
        if arr.len() != 4 {
            return Err(AppError::Command("GETRANGE 命令需要 3 个参数".to_string()));
        }
        let key = self.extract_string(&arr[1])?;
        let start: i64 = self
            .extract_string(&arr[2])?
            .parse()
            .map_err(|_| AppError::Command("GETRANGE 的 start 必须是整数".to_string()))?;
        let end: i64 = self
            .extract_string(&arr[3])?
            .parse()
            .map_err(|_| AppError::Command("GETRANGE 的 end 必须是整数".to_string()))?;
        Ok(Command::GetRange(key, start, end))
    }

    /// 解析 STRLEN 命令：STRLEN key
    pub(crate) fn parse_strlen(&self, arr: &[RespValue]) -> Result<Command> {
        if arr.len() != 2 {
            return Err(AppError::Command("STRLEN 命令需要 1 个参数".to_string()));
        }
        let key = self.extract_string(&arr[1])?;
        Ok(Command::StrLen(key))
    }

    /// 解析 UNKNOWN 命令
    ///
    /// Redis 语法:
    ///
    /// # 参数
    /// - `arr` - RESP 数组，arr[0] 为命令名，后续为命令参数
    ///
    /// # 返回值
    /// - `Ok(Command::Unknown(...))` - 解析成功
    /// - `Err(AppError::Command)` - 参数不足或格式错误
    pub(crate) fn parse_substr(&self, arr: &[RespValue]) -> Result<Command> {
        if arr.len() != 4 {
            return Err(AppError::Command("SUBSTR 命令需要 3 个参数".to_string()));
        }
        let key = self.extract_string(&arr[1])?;
        let start: i64 = self
            .extract_string(&arr[2])?
            .parse()
            .map_err(|_| AppError::Command("SUBSTR start 必须是整数".to_string()))?;
        let end: i64 = self
            .extract_string(&arr[3])?
            .parse()
            .map_err(|_| AppError::Command("SUBSTR end 必须是整数".to_string()))?;
        Ok(Command::SubStr(key, start, end))
    }

    /// 解析 LCS 命令
    ///
    /// Redis 语法: LCS key1 key2 [LEN] [IDX] [MINMATCHLEN len] [WITHMATCHLEN]
    ///
    /// # 参数
    /// - `arr` - RESP 数组，arr[0] 为命令名，后续为命令参数
    ///
    /// # 返回值
    /// - `Ok(Command::Lcs(...))` - 解析成功
    /// - `Err(AppError::Command)` - 参数不足或格式错误
    pub(crate) fn parse_lcs(&self, arr: &[RespValue]) -> Result<Command> {
        if arr.len() < 3 {
            return Err(AppError::Command("LCS 命令需要至少 2 个 key".to_string()));
        }
        let key1 = self.extract_string(&arr[1])?;
        let key2 = self.extract_string(&arr[2])?;
        let mut len = false;
        let mut idx = false;
        let mut minmatchlen = 0usize;
        let mut withmatchlen = false;

        let mut i = 3;
        while i < arr.len() {
            let opt = self.extract_string(&arr[i])?.to_ascii_uppercase();
            match opt.as_str() {
                "LEN" => len = true,
                "IDX" => idx = true,
                "MINMATCHLEN" => {
                    i += 1;
                    if i >= arr.len() {
                        return Err(AppError::Command("MINMATCHLEN 需要值".to_string()));
                    }
                    minmatchlen = self
                        .extract_string(&arr[i])?
                        .parse()
                        .map_err(|_| AppError::Command("MINMATCHLEN 必须是整数".to_string()))?;
                }
                "WITHMATCHLEN" => withmatchlen = true,
                _ => {
                    return Err(AppError::Command(format!("LCS 未知选项: {}", opt)));
                }
            }
            i += 1;
        }

        Ok(Command::Lcs(
            key1,
            key2,
            len,
            idx,
            minmatchlen,
            withmatchlen,
        ))
    }
}
