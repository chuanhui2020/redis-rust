use super::*;

use crate::error::{AppError, Result};
use crate::protocol::RespValue;
use super::parser::CommandParser;

impl CommandParser {
    /// 解析 HSET 命令：HSET key field value [field value ...]
    pub(crate) fn parse_hset(&self, arr: &[RespValue]) -> Result<Command> {
        if arr.len() < 4 || arr.len() % 2 == 1 {
            return Err(AppError::Command(
                "HSET 命令需要成对的 field value 参数".to_string(),
            ));
        }
        let key = self.extract_string(&arr[1])?;
        let mut pairs = Vec::new();
        for i in (2..arr.len()).step_by(2) {
            let field = self.extract_string(&arr[i])?;
            let value = self.extract_bytes(&arr[i + 1])?;
            pairs.push((field, value));
        }
        Ok(Command::HSet(key, pairs))
    }


    /// 解析 HGET 命令：HGET key field
    pub(crate) fn parse_hget(&self, arr: &[RespValue]) -> Result<Command> {
        if arr.len() != 3 {
            return Err(AppError::Command(
                "HGET 命令需要 2 个参数".to_string(),
            ));
        }
        let key = self.extract_string(&arr[1])?;
        let field = self.extract_string(&arr[2])?;
        Ok(Command::HGet(key, field))
    }


    /// 解析 HDEL 命令：HDEL key field [field ...]
    pub(crate) fn parse_hdel(&self, arr: &[RespValue]) -> Result<Command> {
        if arr.len() < 3 {
            return Err(AppError::Command(
                "HDEL 命令需要至少 2 个参数".to_string(),
            ));
        }
        let key = self.extract_string(&arr[1])?;
        let fields = arr[2..]
            .iter()
            .map(|v| self.extract_string(v))
            .collect::<Result<Vec<String>>>()?;
        Ok(Command::HDel(key, fields))
    }


    /// 解析 HEXISTS 命令：HEXISTS key field
    pub(crate) fn parse_hexists(&self, arr: &[RespValue]) -> Result<Command> {
        if arr.len() != 3 {
            return Err(AppError::Command(
                "HEXISTS 命令需要 2 个参数".to_string(),
            ));
        }
        let key = self.extract_string(&arr[1])?;
        let field = self.extract_string(&arr[2])?;
        Ok(Command::HExists(key, field))
    }


    /// 解析 HGETALL 命令：HGETALL key
    pub(crate) fn parse_hgetall(&self, arr: &[RespValue]) -> Result<Command> {
        if arr.len() != 2 {
            return Err(AppError::Command(
                "HGETALL 命令需要 1 个参数".to_string(),
            ));
        }
        let key = self.extract_string(&arr[1])?;
        Ok(Command::HGetAll(key))
    }


    /// 解析 HLEN 命令：HLEN key
    pub(crate) fn parse_hlen(&self, arr: &[RespValue]) -> Result<Command> {
        if arr.len() != 2 {
            return Err(AppError::Command(
                "HLEN 命令需要 1 个参数".to_string(),
            ));
        }
        let key = self.extract_string(&arr[1])?;
        Ok(Command::HLen(key))
    }


    /// 解析 HMSET 命令：HMSET key field value [field value ...]
    pub(crate) fn parse_hmset(&self, arr: &[RespValue]) -> Result<Command> {
        if arr.len() < 4 || arr.len() % 2 == 1 {
            return Err(AppError::Command(
                "HMSET 命令需要成对的 field value 参数".to_string(),
            ));
        }
        let key = self.extract_string(&arr[1])?;
        let mut pairs = Vec::new();
        for i in (2..arr.len()).step_by(2) {
            let field = self.extract_string(&arr[i])?;
            let value = self.extract_bytes(&arr[i + 1])?;
            pairs.push((field, value));
        }
        Ok(Command::HMSet(key, pairs))
    }


    /// 解析 HMGET 命令：HMGET key field [field ...]
    pub(crate) fn parse_hmget(&self, arr: &[RespValue]) -> Result<Command> {
        if arr.len() < 3 {
            return Err(AppError::Command(
                "HMGET 命令需要至少 2 个参数".to_string(),
            ));
        }
        let key = self.extract_string(&arr[1])?;
        let fields = arr[2..]
            .iter()
            .map(|v| self.extract_string(v))
            .collect::<Result<Vec<String>>>()?;
        Ok(Command::HMGet(key, fields))
    }


    /// 解析 HINCRBY 命令：HINCRBY key field increment
    pub(crate) fn parse_hincrby(&self, arr: &[RespValue]) -> Result<Command> {
        if arr.len() != 4 {
            return Err(AppError::Command(
                "HINCRBY 命令需要 3 个参数".to_string(),
            ));
        }
        let key = self.extract_string(&arr[1])?;
        let field = self.extract_string(&arr[2])?;
        let delta: i64 = self.extract_string(&arr[3])?.parse().map_err(|_| {
            AppError::Command("HINCRBY 的增量必须是整数".to_string())
        })?;
        Ok(Command::HIncrBy(key, field, delta))
    }


    /// 解析 HINCRBYFLOAT 命令：HINCRBYFLOAT key field increment
    pub(crate) fn parse_hincrbyfloat(&self, arr: &[RespValue]) -> Result<Command> {
        if arr.len() != 4 {
            return Err(AppError::Command(
                "HINCRBYFLOAT 命令需要 3 个参数".to_string(),
            ));
        }
        let key = self.extract_string(&arr[1])?;
        let field = self.extract_string(&arr[2])?;
        let delta: f64 = self.extract_string(&arr[3])?.parse().map_err(|_| {
            AppError::Command("HINCRBYFLOAT 的增量必须是有效的浮点数".to_string())
        })?;
        Ok(Command::HIncrByFloat(key, field, delta))
    }


    /// 解析 HKEYS 命令：HKEYS key
    pub(crate) fn parse_hkeys(&self, arr: &[RespValue]) -> Result<Command> {
        if arr.len() != 2 {
            return Err(AppError::Command(
                "HKEYS 命令需要 1 个参数".to_string(),
            ));
        }
        let key = self.extract_string(&arr[1])?;
        Ok(Command::HKeys(key))
    }


    /// 解析 HVALS 命令：HVALS key
    pub(crate) fn parse_hvals(&self, arr: &[RespValue]) -> Result<Command> {
        if arr.len() != 2 {
            return Err(AppError::Command(
                "HVALS 命令需要 1 个参数".to_string(),
            ));
        }
        let key = self.extract_string(&arr[1])?;
        Ok(Command::HVals(key))
    }


    /// 解析 HSETNX 命令：HSETNX key field value
    pub(crate) fn parse_hsetnx(&self, arr: &[RespValue]) -> Result<Command> {
        if arr.len() != 4 {
            return Err(AppError::Command(
                "HSETNX 命令需要 3 个参数".to_string(),
            ));
        }
        let key = self.extract_string(&arr[1])?;
        let field = self.extract_string(&arr[2])?;
        let value = self.extract_bytes(&arr[3])?;
        Ok(Command::HSetNx(key, field, value))
    }


    /// 解析 HRANDFIELD 命令：HRANDFIELD key [count [WITHVALUES]]
    pub(crate) fn parse_hrandfield(&self, arr: &[RespValue]) -> Result<Command> {
        if arr.len() < 2 || arr.len() > 4 {
            return Err(AppError::Command(
                "HRANDFIELD 命令参数错误".to_string(),
            ));
        }
        let key = self.extract_string(&arr[1])?;
        let mut count = 1i64;
        let mut with_values = false;
        if arr.len() >= 3 {
            count = self.extract_string(&arr[2])?.parse().map_err(|_| {
                AppError::Command("HRANDFIELD 的 count 必须是整数".to_string())
            })?;
        }
        if arr.len() == 4 {
            let opt = self.extract_string(&arr[3])?.to_ascii_uppercase();
            if opt == "WITHVALUES" {
                with_values = true;
            } else {
                return Err(AppError::Command(format!(
                    "HRANDFIELD 不支持的选项: {}",
                    opt
                )));
            }
        }
        Ok(Command::HRandField(key, count, with_values))
    }


    /// 解析 HSCAN 命令：HSCAN key cursor [MATCH pattern] [COUNT count]
    pub(crate) fn parse_hscan(&self, arr: &[RespValue]) -> Result<Command> {
        if arr.len() < 3 {
            return Err(AppError::Command(
                "HSCAN 命令需要至少 2 个参数".to_string(),
            ));
        }
        let key = self.extract_string(&arr[1])?;
        let cursor: usize = self.extract_string(&arr[2])?.parse().map_err(|_| {
            AppError::Command("HSCAN 的 cursor 必须是整数".to_string())
        })?;
        let mut pattern = "*".to_string();
        let mut count = 0usize;

        let mut i = 3;
        while i < arr.len() {
            let opt = self.extract_string(&arr[i])?.to_ascii_uppercase();
            match opt.as_str() {
                "MATCH" => {
                    if i + 1 >= arr.len() {
                        return Err(AppError::Command("HSCAN MATCH 需要参数".to_string()));
                    }
                    pattern = self.extract_string(&arr[i + 1])?;
                    i += 2;
                }
                "COUNT" => {
                    if i + 1 >= arr.len() {
                        return Err(AppError::Command("HSCAN COUNT 需要参数".to_string()));
                    }
                    count = self.extract_string(&arr[i + 1])?.parse().map_err(|_| {
                        AppError::Command("HSCAN COUNT 必须是整数".to_string())
                    })?;
                    i += 2;
                }
                _ => {
                    return Err(AppError::Command(format!(
                        "HSCAN 不支持的选项: {}",
                        opt
                    )))
                }
            }
        }
        Ok(Command::HScan(key, cursor, pattern, count))
    }


    /// 解析 HEXPIRE 命令：HEXPIRE key field [field ...] seconds
    pub(crate) fn parse_hexpire(&self, arr: &[RespValue]) -> Result<Command> {
        if arr.len() < 4 {
            return Err(AppError::Command(
                "HEXPIRE 命令需要至少 3 个参数".to_string(),
            ));
        }
        let key = self.extract_string(&arr[1])?;
        let seconds: u64 = self.extract_string(&arr[arr.len() - 1])?.parse().map_err(|_| {
            AppError::Command("HEXPIRE 的秒数必须是正整数".to_string())
        })?;
        let fields: Vec<String> = arr[2..arr.len() - 1]
            .iter()
            .map(|v| self.extract_string(v))
            .collect::<Result<Vec<_>>>()?;
        Ok(Command::HExpire(key, fields, seconds))
    }


    /// 解析 HPEXPIRE 命令：HPEXPIRE key field [field ...] milliseconds
    pub(crate) fn parse_hpexpire(&self, arr: &[RespValue]) -> Result<Command> {
        if arr.len() < 4 {
            return Err(AppError::Command(
                "HPEXPIRE 命令需要至少 3 个参数".to_string(),
            ));
        }
        let key = self.extract_string(&arr[1])?;
        let ms: u64 = self.extract_string(&arr[arr.len() - 1])?.parse().map_err(|_| {
            AppError::Command("HPEXPIRE 的毫秒数必须是正整数".to_string())
        })?;
        let fields: Vec<String> = arr[2..arr.len() - 1]
            .iter()
            .map(|v| self.extract_string(v))
            .collect::<Result<Vec<_>>>()?;
        Ok(Command::HPExpire(key, fields, ms))
    }


    /// 解析 HEXPIREAT 命令：HEXPIREAT key field [field ...] timestamp-seconds
    pub(crate) fn parse_hexpireat(&self, arr: &[RespValue]) -> Result<Command> {
        if arr.len() < 4 {
            return Err(AppError::Command(
                "HEXPIREAT 命令需要至少 3 个参数".to_string(),
            ));
        }
        let key = self.extract_string(&arr[1])?;
        let ts: u64 = self.extract_string(&arr[arr.len() - 1])?.parse().map_err(|_| {
            AppError::Command("HEXPIREAT 的时间戳必须是正整数".to_string())
        })?;
        let fields: Vec<String> = arr[2..arr.len() - 1]
            .iter()
            .map(|v| self.extract_string(v))
            .collect::<Result<Vec<_>>>()?;
        Ok(Command::HExpireAt(key, fields, ts))
    }


    /// 解析 HPEXPIREAT 命令：HPEXPIREAT key field [field ...] timestamp-ms
    pub(crate) fn parse_hpexpireat(&self, arr: &[RespValue]) -> Result<Command> {
        if arr.len() < 4 {
            return Err(AppError::Command(
                "HPEXPIREAT 命令需要至少 3 个参数".to_string(),
            ));
        }
        let key = self.extract_string(&arr[1])?;
        let ts: u64 = self.extract_string(&arr[arr.len() - 1])?.parse().map_err(|_| {
            AppError::Command("HPEXPIREAT 的时间戳必须是正整数".to_string())
        })?;
        let fields: Vec<String> = arr[2..arr.len() - 1]
            .iter()
            .map(|v| self.extract_string(v))
            .collect::<Result<Vec<_>>>()?;
        Ok(Command::HPExpireAt(key, fields, ts))
    }


    /// 解析 HTTL 命令：HTTL key field [field ...]
    pub(crate) fn parse_httl(&self, arr: &[RespValue]) -> Result<Command> {
        if arr.len() < 3 {
            return Err(AppError::Command(
                "HTTL 命令需要至少 2 个参数".to_string(),
            ));
        }
        let key = self.extract_string(&arr[1])?;
        let fields: Vec<String> = arr[2..]
            .iter()
            .map(|v| self.extract_string(v))
            .collect::<Result<Vec<_>>>()?;
        Ok(Command::HTtl(key, fields))
    }


    /// 解析 HPTTL 命令：HPTTL key field [field ...]
    pub(crate) fn parse_hpttl(&self, arr: &[RespValue]) -> Result<Command> {
        if arr.len() < 3 {
            return Err(AppError::Command(
                "HPTTL 命令需要至少 2 个参数".to_string(),
            ));
        }
        let key = self.extract_string(&arr[1])?;
        let fields: Vec<String> = arr[2..]
            .iter()
            .map(|v| self.extract_string(v))
            .collect::<Result<Vec<_>>>()?;
        Ok(Command::HPTtl(key, fields))
    }


    /// 解析 HEXPIRETIME 命令：HEXPIRETIME key field [field ...]
    pub(crate) fn parse_hexpiretime(&self, arr: &[RespValue]) -> Result<Command> {
        if arr.len() < 3 {
            return Err(AppError::Command(
                "HEXPIRETIME 命令需要至少 2 个参数".to_string(),
            ));
        }
        let key = self.extract_string(&arr[1])?;
        let fields: Vec<String> = arr[2..]
            .iter()
            .map(|v| self.extract_string(v))
            .collect::<Result<Vec<_>>>()?;
        Ok(Command::HExpireTime(key, fields))
    }


    /// 解析 HPEXPIRETIME 命令：HPEXPIRETIME key field [field ...]
    pub(crate) fn parse_hpexpiretime(&self, arr: &[RespValue]) -> Result<Command> {
        if arr.len() < 3 {
            return Err(AppError::Command(
                "HPEXPIRETIME 命令需要至少 2 个参数".to_string(),
            ));
        }
        let key = self.extract_string(&arr[1])?;
        let fields: Vec<String> = arr[2..]
            .iter()
            .map(|v| self.extract_string(v))
            .collect::<Result<Vec<_>>>()?;
        Ok(Command::HPExpireTime(key, fields))
    }


    /// 解析 HPERSIST 命令：HPERSIST key field [field ...]
    pub(crate) fn parse_hpersist(&self, arr: &[RespValue]) -> Result<Command> {
        if arr.len() < 3 {
            return Err(AppError::Command(
                "HPERSIST 命令需要至少 2 个参数".to_string(),
            ));
        }
        let key = self.extract_string(&arr[1])?;
        let fields: Vec<String> = arr[2..]
            .iter()
            .map(|v| self.extract_string(v))
            .collect::<Result<Vec<_>>>()?;
        Ok(Command::HPersist(key, fields))
    }


}
