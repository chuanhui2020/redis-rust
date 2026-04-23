use super::*;

use crate::error::{AppError, Result};
use crate::protocol::RespValue;
use super::parser::CommandParser;

impl CommandParser {
    /// 解析 LPUSH 命令：LPUSH key value [value ...]
    pub(crate) fn parse_lpush(&self, arr: &[RespValue]) -> Result<Command> {
        if arr.len() < 3 {
            return Err(AppError::Command(
                "LPUSH 命令需要至少 2 个参数".to_string(),
            ));
        }
        let key = self.extract_string(&arr[1])?;
        let values = arr[2..]
            .iter()
            .map(|v| self.extract_bytes(v))
            .collect::<Result<Vec<Bytes>>>()?;
        Ok(Command::LPush(key, values))
    }


    /// 解析 RPUSH 命令：RPUSH key value [value ...]
    pub(crate) fn parse_rpush(&self, arr: &[RespValue]) -> Result<Command> {
        if arr.len() < 3 {
            return Err(AppError::Command(
                "RPUSH 命令需要至少 2 个参数".to_string(),
            ));
        }
        let key = self.extract_string(&arr[1])?;
        let values = arr[2..]
            .iter()
            .map(|v| self.extract_bytes(v))
            .collect::<Result<Vec<Bytes>>>()?;
        Ok(Command::RPush(key, values))
    }


    /// 解析 LPOP 命令：LPOP key
    pub(crate) fn parse_lpop(&self, arr: &[RespValue]) -> Result<Command> {
        if arr.len() != 2 {
            return Err(AppError::Command(
                "LPOP 命令需要 1 个参数".to_string(),
            ));
        }
        let key = self.extract_string(&arr[1])?;
        Ok(Command::LPop(key))
    }


    /// 解析 RPOP 命令：RPOP key
    pub(crate) fn parse_rpop(&self, arr: &[RespValue]) -> Result<Command> {
        if arr.len() != 2 {
            return Err(AppError::Command(
                "RPOP 命令需要 1 个参数".to_string(),
            ));
        }
        let key = self.extract_string(&arr[1])?;
        Ok(Command::RPop(key))
    }


    /// 解析 LLEN 命令：LLEN key
    pub(crate) fn parse_llen(&self, arr: &[RespValue]) -> Result<Command> {
        if arr.len() != 2 {
            return Err(AppError::Command(
                "LLEN 命令需要 1 个参数".to_string(),
            ));
        }
        let key = self.extract_string(&arr[1])?;
        Ok(Command::LLen(key))
    }


    /// 解析 LRANGE 命令：LRANGE key start stop
    pub(crate) fn parse_lrange(&self, arr: &[RespValue]) -> Result<Command> {
        if arr.len() != 4 {
            return Err(AppError::Command(
                "LRANGE 命令需要 3 个参数".to_string(),
            ));
        }
        let key = self.extract_string(&arr[1])?;
        let start: i64 = self.extract_string(&arr[2])?.parse().map_err(|_| {
            AppError::Command("LRANGE 的 start 必须是整数".to_string())
        })?;
        let stop: i64 = self.extract_string(&arr[3])?.parse().map_err(|_| {
            AppError::Command("LRANGE 的 stop 必须是整数".to_string())
        })?;
        Ok(Command::LRange(key, start, stop))
    }


    /// 解析 LINDEX 命令：LINDEX key index
    pub(crate) fn parse_lindex(&self, arr: &[RespValue]) -> Result<Command> {
        if arr.len() != 3 {
            return Err(AppError::Command(
                "LINDEX 命令需要 2 个参数".to_string(),
            ));
        }
        let key = self.extract_string(&arr[1])?;
        let index: i64 = self.extract_string(&arr[2])?.parse().map_err(|_| {
            AppError::Command("LINDEX 的 index 必须是整数".to_string())
        })?;
        Ok(Command::LIndex(key, index))
    }


    /// 解析 LSET 命令：LSET key index value
    pub(crate) fn parse_lset(&self, arr: &[RespValue]) -> Result<Command> {
        if arr.len() != 4 {
            return Err(AppError::Command(
                "LSET 命令需要 3 个参数".to_string(),
            ));
        }
        let key = self.extract_string(&arr[1])?;
        let index: i64 = self.extract_string(&arr[2])?.parse().map_err(|_| {
            AppError::Command("LSET 的 index 必须是整数".to_string())
        })?;
        let value = self.extract_bytes(&arr[3])?;
        Ok(Command::LSet(key, index, value))
    }


    /// 解析 LINSERT 命令：LINSERT key BEFORE|AFTER pivot value
    pub(crate) fn parse_linsert(&self, arr: &[RespValue]) -> Result<Command> {
        if arr.len() != 5 {
            return Err(AppError::Command(
                "LINSERT 命令需要 4 个参数".to_string(),
            ));
        }
        let key = self.extract_string(&arr[1])?;
        let pos = match self.extract_string(&arr[2])?.to_ascii_uppercase().as_str() {
            "BEFORE" => crate::storage::LInsertPosition::Before,
            "AFTER" => crate::storage::LInsertPosition::After,
            other => {
                return Err(AppError::Command(format!(
                    "LINSERT 位置必须是 BEFORE 或 AFTER，得到: {}",
                    other
                )))
            }
        };
        let pivot = self.extract_bytes(&arr[3])?;
        let value = self.extract_bytes(&arr[4])?;
        Ok(Command::LInsert(key, pos, pivot, value))
    }


    /// 解析 LREM 命令：LREM key count value
    pub(crate) fn parse_lrem(&self, arr: &[RespValue]) -> Result<Command> {
        if arr.len() != 4 {
            return Err(AppError::Command(
                "LREM 命令需要 3 个参数".to_string(),
            ));
        }
        let key = self.extract_string(&arr[1])?;
        let count: i64 = self.extract_string(&arr[2])?.parse().map_err(|_| {
            AppError::Command("LREM 的 count 必须是整数".to_string())
        })?;
        let value = self.extract_bytes(&arr[3])?;
        Ok(Command::LRem(key, count, value))
    }


    /// 解析 LTRIM 命令：LTRIM key start stop
    pub(crate) fn parse_ltrim(&self, arr: &[RespValue]) -> Result<Command> {
        if arr.len() != 4 {
            return Err(AppError::Command(
                "LTRIM 命令需要 3 个参数".to_string(),
            ));
        }
        let key = self.extract_string(&arr[1])?;
        let start: i64 = self.extract_string(&arr[2])?.parse().map_err(|_| {
            AppError::Command("LTRIM 的 start 必须是整数".to_string())
        })?;
        let stop: i64 = self.extract_string(&arr[3])?.parse().map_err(|_| {
            AppError::Command("LTRIM 的 stop 必须是整数".to_string())
        })?;
        Ok(Command::LTrim(key, start, stop))
    }


    /// 解析 LPOS 命令：LPOS key element [RANK rank] [COUNT count] [MAXLEN maxlen]
    pub(crate) fn parse_lpos(&self, arr: &[RespValue]) -> Result<Command> {
        if arr.len() < 3 {
            return Err(AppError::Command(
                "LPOS 命令需要至少 2 个参数".to_string(),
            ));
        }
        let key = self.extract_string(&arr[1])?;
        let value = self.extract_bytes(&arr[2])?;
        let mut rank = 1i64;
        let mut count = 0i64;
        let mut maxlen = 0i64;

        let mut i = 3;
        while i < arr.len() {
            let opt = self.extract_string(&arr[i])?.to_ascii_uppercase();
            match opt.as_str() {
                "RANK" => {
                    if i + 1 >= arr.len() {
                        return Err(AppError::Command("LPOS RANK 需要参数".to_string()));
                    }
                    rank = self.extract_string(&arr[i + 1])?.parse().map_err(|_| {
                        AppError::Command("LPOS RANK 必须是整数".to_string())
                    })?;
                    i += 2;
                }
                "COUNT" => {
                    if i + 1 >= arr.len() {
                        return Err(AppError::Command("LPOS COUNT 需要参数".to_string()));
                    }
                    count = self.extract_string(&arr[i + 1])?.parse().map_err(|_| {
                        AppError::Command("LPOS COUNT 必须是整数".to_string())
                    })?;
                    i += 2;
                }
                "MAXLEN" => {
                    if i + 1 >= arr.len() {
                        return Err(AppError::Command("LPOS MAXLEN 需要参数".to_string()));
                    }
                    maxlen = self.extract_string(&arr[i + 1])?.parse().map_err(|_| {
                        AppError::Command("LPOS MAXLEN 必须是整数".to_string())
                    })?;
                    i += 2;
                }
                _ => {
                    return Err(AppError::Command(format!(
                        "LPOS 不支持的选项: {}",
                        opt
                    )))
                }
            }
        }
        Ok(Command::LPos(key, value, rank, count, maxlen))
    }


    /// 解析 BLPOP 命令：BLPOP key [key ...] timeout
    pub(crate) fn parse_blpop(&self, arr: &[RespValue]) -> Result<Command> {
        if arr.len() < 3 {
            return Err(AppError::Command(
                "BLPOP 命令需要至少 2 个参数".to_string(),
            ));
        }
        let timeout: f64 = self.extract_string(&arr[arr.len() - 1])?.parse().map_err(|_| {
            AppError::Command("BLPOP 的 timeout 必须是数字".to_string())
        })?;
        let mut keys = Vec::new();
        for item in arr.iter().take(arr.len() - 1).skip(1) {
            keys.push(self.extract_string(item)?);
        }
        Ok(Command::BLPop(keys, timeout))
    }


    /// 解析 BRPOP 命令：BRPOP key [key ...] timeout
    pub(crate) fn parse_brpop(&self, arr: &[RespValue]) -> Result<Command> {
        if arr.len() < 3 {
            return Err(AppError::Command(
                "BRPOP 命令需要至少 2 个参数".to_string(),
            ));
        }
        let timeout: f64 = self.extract_string(&arr[arr.len() - 1])?.parse().map_err(|_| {
            AppError::Command("BRPOP 的 timeout 必须是数字".to_string())
        })?;
        let mut keys = Vec::new();
        for item in arr.iter().take(arr.len() - 1).skip(1) {
            keys.push(self.extract_string(item)?);
        }
        Ok(Command::BRPop(keys, timeout))
    }


    pub(crate) fn parse_lmove(&self, arr: &[RespValue]) -> Result<Command> {
        if arr.len() != 5 {
            return Err(AppError::Command("LMOVE 命令需要 4 个参数".to_string()));
        }
        let source = self.extract_string(&arr[1])?;
        let dest = self.extract_string(&arr[2])?;
        let from = self.extract_string(&arr[3])?.to_ascii_uppercase();
        let to = self.extract_string(&arr[4])?.to_ascii_uppercase();
        let left_from = match from.as_str() {
            "LEFT" => true,
            "RIGHT" => false,
            _ => return Err(AppError::Command("LMOVE wherefrom 必须是 LEFT 或 RIGHT".to_string())),
        };
        let left_to = match to.as_str() {
            "LEFT" => true,
            "RIGHT" => false,
            _ => return Err(AppError::Command("LMOVE whereto 必须是 LEFT 或 RIGHT".to_string())),
        };
        Ok(Command::Lmove(source, dest, left_from, left_to))
    }


    pub(crate) fn parse_rpoplpush(&self, arr: &[RespValue]) -> Result<Command> {
        if arr.len() != 3 {
            return Err(AppError::Command("RPOPLPUSH 命令需要 2 个参数".to_string()));
        }
        Ok(Command::Rpoplpush(
            self.extract_string(&arr[1])?,
            self.extract_string(&arr[2])?,
        ))
    }


    pub(crate) fn parse_lmpop(&self, arr: &[RespValue]) -> Result<Command> {
        if arr.len() < 4 {
            return Err(AppError::Command("LMPOP 命令需要至少 3 个参数".to_string()));
        }
        let numkeys: usize = self.extract_string(&arr[1])?.parse().map_err(|_| {
            AppError::Command("LMPOP numkeys 必须是整数".to_string())
        })?;
        if arr.len() < 2 + numkeys + 1 {
            return Err(AppError::Command("LMPOP 参数不足".to_string()));
        }
        let mut keys = Vec::new();
        for i in 0..numkeys {
            keys.push(self.extract_string(&arr[2 + i])?);
        }
        let mut pos = 2 + numkeys;
        let direction = self.extract_string(&arr[pos])?.to_ascii_uppercase();
        let left = match direction.as_str() {
            "LEFT" => true,
            "RIGHT" => false,
            _ => return Err(AppError::Command("LMPOP 方向必须是 LEFT 或 RIGHT".to_string())),
        };
        pos += 1;
        let mut count = 1usize;
        if pos < arr.len() {
            let opt = self.extract_string(&arr[pos])?.to_ascii_uppercase();
            if opt == "COUNT" {
                pos += 1;
                if pos >= arr.len() {
                    return Err(AppError::Command("LMPOP COUNT 需要值".to_string()));
                }
                count = self.extract_string(&arr[pos])?.parse().map_err(|_| {
                    AppError::Command("LMPOP COUNT 必须是整数".to_string())
                })?;
            }
        }
        Ok(Command::Lmpop(keys, left, count))
    }


    pub(crate) fn parse_blmove(&self, arr: &[RespValue]) -> Result<Command> {
        if arr.len() != 6 {
            return Err(AppError::Command("BLMOVE 命令需要 5 个参数".to_string()));
        }
        let source = self.extract_string(&arr[1])?;
        let dest = self.extract_string(&arr[2])?;
        let from = self.extract_string(&arr[3])?.to_ascii_uppercase();
        let to = self.extract_string(&arr[4])?.to_ascii_uppercase();
        let timeout: f64 = self.extract_string(&arr[5])?.parse().map_err(|_| {
            AppError::Command("BLMOVE timeout 必须是数字".to_string())
        })?;
        let left_from = match from.as_str() {
            "LEFT" => true,
            "RIGHT" => false,
            _ => return Err(AppError::Command("BLMOVE wherefrom 必须是 LEFT 或 RIGHT".to_string())),
        };
        let left_to = match to.as_str() {
            "LEFT" => true,
            "RIGHT" => false,
            _ => return Err(AppError::Command("BLMOVE whereto 必须是 LEFT 或 RIGHT".to_string())),
        };
        Ok(Command::BLmove(source, dest, left_from, left_to, timeout))
    }


    pub(crate) fn parse_blmpop(&self, arr: &[RespValue]) -> Result<Command> {
        if arr.len() < 5 {
            return Err(AppError::Command("BLMPOP 命令需要至少 4 个参数".to_string()));
        }
        let timeout: f64 = self.extract_string(&arr[1])?.parse().map_err(|_| {
            AppError::Command("BLMPOP timeout 必须是数字".to_string())
        })?;
        let numkeys: usize = self.extract_string(&arr[2])?.parse().map_err(|_| {
            AppError::Command("BLMPOP numkeys 必须是整数".to_string())
        })?;
        if arr.len() < 3 + numkeys + 1 {
            return Err(AppError::Command("BLMPOP 参数不足".to_string()));
        }
        let mut keys = Vec::new();
        for i in 0..numkeys {
            keys.push(self.extract_string(&arr[3 + i])?);
        }
        let mut pos = 3 + numkeys;
        let direction = self.extract_string(&arr[pos])?.to_ascii_uppercase();
        let left = match direction.as_str() {
            "LEFT" => true,
            "RIGHT" => false,
            _ => return Err(AppError::Command("BLMPOP 方向必须是 LEFT 或 RIGHT".to_string())),
        };
        pos += 1;
        let mut count = 1usize;
        if pos < arr.len() {
            let opt = self.extract_string(&arr[pos])?.to_ascii_uppercase();
            if opt == "COUNT" {
                pos += 1;
                if pos >= arr.len() {
                    return Err(AppError::Command("BLMPOP COUNT 需要值".to_string()));
                }
                count = self.extract_string(&arr[pos])?.parse().map_err(|_| {
                    AppError::Command("BLMPOP COUNT 必须是整数".to_string())
                })?;
            }
        }
        Ok(Command::BLmpop(keys, left, count, timeout))
    }


    pub(crate) fn parse_brpoplpush(&self, arr: &[RespValue]) -> Result<Command> {
        if arr.len() != 4 {
            return Err(AppError::Command("BRPOPLPUSH 命令需要 3 个参数".to_string()));
        }
        let source = self.extract_string(&arr[1])?;
        let dest = self.extract_string(&arr[2])?;
        let timeout: f64 = self.extract_string(&arr[3])?.parse().map_err(|_| {
            AppError::Command("BRPOPLPUSH timeout 必须是数字".to_string())
        })?;
        Ok(Command::BRpoplpush(source, dest, timeout))
    }

}
