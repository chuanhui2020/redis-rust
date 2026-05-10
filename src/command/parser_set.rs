//! Set 命令解析器

use std::borrow::Cow;
use super::*;

use super::parser::CommandParser;
use crate::error::{AppError, Result};
use crate::protocol::RespValue;

impl CommandParser {
    /// 解析 SADD 命令：SADD key member [member ...]
    pub(crate) fn parse_sadd(&self, arr: &[RespValue]) -> Result<Command> {
        if arr.len() < 3 {
            return Err(AppError::Command(Cow::Borrowed("SADD 命令需要至少 2 个参数")));
        }
        let key = self.extract_string(&arr[1])?;
        let members = arr[2..]
            .iter()
            .map(|v| self.extract_bytes(v))
            .collect::<Result<Vec<Bytes>>>()?;
        Ok(Command::SAdd(key, members))
    }

    /// 解析 SREM 命令：SREM key member [member ...]
    pub(crate) fn parse_srem(&self, arr: &[RespValue]) -> Result<Command> {
        if arr.len() < 3 {
            return Err(AppError::Command(Cow::Borrowed("SREM 命令需要至少 2 个参数")));
        }
        let key = self.extract_string(&arr[1])?;
        let members = arr[2..]
            .iter()
            .map(|v| self.extract_bytes(v))
            .collect::<Result<Vec<Bytes>>>()?;
        Ok(Command::SRem(key, members))
    }

    /// 解析 SMEMBERS 命令：SMEMBERS key
    pub(crate) fn parse_smembers(&self, arr: &[RespValue]) -> Result<Command> {
        if arr.len() != 2 {
            return Err(AppError::Command(Cow::Borrowed("SMEMBERS 命令需要 1 个参数")));
        }
        let key = self.extract_string(&arr[1])?;
        Ok(Command::SMembers(key))
    }

    /// 解析 SISMEMBER 命令：SISMEMBER key member
    pub(crate) fn parse_sismember(&self, arr: &[RespValue]) -> Result<Command> {
        if arr.len() != 3 {
            return Err(AppError::Command(Cow::Borrowed("SISMEMBER 命令需要 2 个参数")));
        }
        let key = self.extract_string(&arr[1])?;
        let member = self.extract_bytes(&arr[2])?;
        Ok(Command::SIsMember(key, member))
    }

    /// 解析 SCARD 命令：SCARD key
    pub(crate) fn parse_scard(&self, arr: &[RespValue]) -> Result<Command> {
        if arr.len() != 2 {
            return Err(AppError::Command(Cow::Borrowed("SCARD 命令需要 1 个参数")));
        }
        let key = self.extract_string(&arr[1])?;
        Ok(Command::SCard(key))
    }

    /// 解析 SINTER 命令：SINTER key [key ...]
    pub(crate) fn parse_sinter(&self, arr: &[RespValue]) -> Result<Command> {
        if arr.len() < 2 {
            return Err(AppError::Command(Cow::Borrowed("SINTER 命令需要至少 1 个参数")));
        }
        let keys = arr[1..]
            .iter()
            .map(|v| self.extract_string(v))
            .collect::<Result<Vec<String>>>()?;
        Ok(Command::SInter(keys))
    }

    /// 解析 SUNION 命令：SUNION key [key ...]
    pub(crate) fn parse_sunion(&self, arr: &[RespValue]) -> Result<Command> {
        if arr.len() < 2 {
            return Err(AppError::Command(Cow::Borrowed("SUNION 命令需要至少 1 个参数")));
        }
        let keys = arr[1..]
            .iter()
            .map(|v| self.extract_string(v))
            .collect::<Result<Vec<String>>>()?;
        Ok(Command::SUnion(keys))
    }

    /// 解析 SDIFF 命令：SDIFF key [key ...]
    pub(crate) fn parse_sdiff(&self, arr: &[RespValue]) -> Result<Command> {
        if arr.len() < 2 {
            return Err(AppError::Command(Cow::Borrowed("SDIFF 命令需要至少 1 个参数")));
        }
        let keys = arr[1..]
            .iter()
            .map(|v| self.extract_string(v))
            .collect::<Result<Vec<String>>>()?;
        Ok(Command::SDiff(keys))
    }

    /// 解析 SPOP 命令：SPOP key [count]
    pub(crate) fn parse_spop(&self, arr: &[RespValue]) -> Result<Command> {
        if arr.len() < 2 || arr.len() > 3 {
            return Err(AppError::Command(Cow::Borrowed("SPOP 命令参数错误")));
        }
        let key = self.extract_string(&arr[1])?;
        let count = if arr.len() == 3 {
            self.extract_string(&arr[2])?
                .parse()
                .map_err(|_| AppError::Command(Cow::Borrowed("SPOP 的 count 必须是整数")))?
        } else {
            1i64
        };
        Ok(Command::SPop(key, count))
    }

    /// 解析 SRANDMEMBER 命令：SRANDMEMBER key [count]
    pub(crate) fn parse_srandmember(&self, arr: &[RespValue]) -> Result<Command> {
        if arr.len() < 2 || arr.len() > 3 {
            return Err(AppError::Command(Cow::Borrowed("SRANDMEMBER 命令参数错误")));
        }
        let key = self.extract_string(&arr[1])?;
        let count = if arr.len() == 3 {
            self.extract_string(&arr[2])?
                .parse()
                .map_err(|_| AppError::Command(Cow::Borrowed("SRANDMEMBER 的 count 必须是整数")))?
        } else {
            1i64
        };
        Ok(Command::SRandMember(key, count))
    }

    /// 解析 SMOVE 命令：SMOVE source destination member
    pub(crate) fn parse_smove(&self, arr: &[RespValue]) -> Result<Command> {
        if arr.len() != 4 {
            return Err(AppError::Command(Cow::Borrowed("SMOVE 命令需要 3 个参数")));
        }
        let source = self.extract_string(&arr[1])?;
        let destination = self.extract_string(&arr[2])?;
        let member = self.extract_bytes(&arr[3])?;
        Ok(Command::SMove(source, destination, member))
    }

    /// 解析 SINTERSTORE 命令：SINTERSTORE destination key [key ...]
    pub(crate) fn parse_sinterstore(&self, arr: &[RespValue]) -> Result<Command> {
        if arr.len() < 3 {
            return Err(AppError::Command(Cow::Borrowed("SINTERSTORE 命令需要至少 2 个参数")));
        }
        let destination = self.extract_string(&arr[1])?;
        let keys = arr[2..]
            .iter()
            .map(|v| self.extract_string(v))
            .collect::<Result<Vec<String>>>()?;
        Ok(Command::SInterStore(destination, keys))
    }

    /// 解析 SUNIONSTORE 命令：SUNIONSTORE destination key [key ...]
    pub(crate) fn parse_sunionstore(&self, arr: &[RespValue]) -> Result<Command> {
        if arr.len() < 3 {
            return Err(AppError::Command(Cow::Borrowed("SUNIONSTORE 命令需要至少 2 个参数")));
        }
        let destination = self.extract_string(&arr[1])?;
        let keys = arr[2..]
            .iter()
            .map(|v| self.extract_string(v))
            .collect::<Result<Vec<String>>>()?;
        Ok(Command::SUnionStore(destination, keys))
    }

    /// 解析 SDIFFSTORE 命令：SDIFFSTORE destination key [key ...]
    pub(crate) fn parse_sdiffstore(&self, arr: &[RespValue]) -> Result<Command> {
        if arr.len() < 3 {
            return Err(AppError::Command(Cow::Borrowed("SDIFFSTORE 命令需要至少 2 个参数")));
        }
        let destination = self.extract_string(&arr[1])?;
        let keys = arr[2..]
            .iter()
            .map(|v| self.extract_string(v))
            .collect::<Result<Vec<String>>>()?;
        Ok(Command::SDiffStore(destination, keys))
    }

    /// 解析 SSCAN 命令：SSCAN key cursor [MATCH pattern] [COUNT count]
    pub(crate) fn parse_sscan(&self, arr: &[RespValue]) -> Result<Command> {
        if arr.len() < 3 {
            return Err(AppError::Command(Cow::Borrowed("SSCAN 命令需要至少 2 个参数")));
        }
        let key = self.extract_string(&arr[1])?;
        let cursor: usize = self
            .extract_string(&arr[2])?
            .parse()
            .map_err(|_| AppError::Command(Cow::Borrowed("SSCAN 的 cursor 必须是整数")))?;
        let mut pattern = "*".to_string();
        let mut count = 0usize;

        let mut i = 3;
        while i < arr.len() {
            let opt = self.extract_string(&arr[i])?.to_ascii_uppercase();
            match opt.as_str() {
                "MATCH" => {
                    if i + 1 >= arr.len() {
                        return Err(AppError::Command(Cow::Borrowed("SSCAN MATCH 需要参数")));
                    }
                    pattern = self.extract_string(&arr[i + 1])?;
                    i += 2;
                }
                "COUNT" => {
                    if i + 1 >= arr.len() {
                        return Err(AppError::Command(Cow::Borrowed("SSCAN COUNT 需要参数")));
                    }
                    count = self
                        .extract_string(&arr[i + 1])?
                        .parse()
                        .map_err(|_| AppError::Command(Cow::Borrowed("SSCAN COUNT 必须是整数")))?;
                    i += 2;
                }
                _ => return Err(AppError::Command(Cow::Owned(format!("SSCAN 不支持的选项: {}", opt)))),
            }
        }
        Ok(Command::SScan(key, cursor, pattern, count))
    }

    /// 解析 SINTERCARD 命令：SINTERCARD numkeys key [key ...] [LIMIT limit]
    pub(crate) fn parse_sintercard(&self, arr: &[RespValue]) -> Result<Command> {
        if arr.len() < 3 {
            return Err(AppError::Command(Cow::Borrowed("SINTERCARD 命令需要至少 2 个参数")));
        }
        let numkeys: usize = self
            .extract_string(&arr[1])?
            .parse()
            .map_err(|_| AppError::Command(Cow::Borrowed("SINTERCARD numkeys 必须是整数")))?;
        if arr.len() < 2 + numkeys {
            return Err(AppError::Command(Cow::Borrowed("SINTERCARD 参数数量不足")));
        }
        let keys: Vec<String> = arr[2..2 + numkeys]
            .iter()
            .map(|v| self.extract_string(v))
            .collect::<Result<Vec<_>>>()?;
        let mut limit = 0usize;
        if arr.len() > 2 + numkeys {
            let opt = self.extract_string(&arr[2 + numkeys])?.to_ascii_uppercase();
            if opt == "LIMIT" {
                if arr.len() < 4 + numkeys {
                    return Err(AppError::Command(Cow::Borrowed("SINTERCARD LIMIT 需要参数")));
                }
                limit = self
                    .extract_string(&arr[3 + numkeys])?
                    .parse()
                    .map_err(|_| AppError::Command(Cow::Borrowed("SINTERCARD LIMIT 必须是整数")))?;
            }
        }
        Ok(Command::SInterCard(keys, limit))
    }

    /// 解析 SMISMEMBER 命令：SMISMEMBER key member [member ...]
    pub(crate) fn parse_smismember(&self, arr: &[RespValue]) -> Result<Command> {
        if arr.len() < 3 {
            return Err(AppError::Command(Cow::Borrowed("SMISMEMBER 命令需要至少 2 个参数")));
        }
        let key = self.extract_string(&arr[1])?;
        let members: Vec<Bytes> = arr[2..]
            .iter()
            .map(|v| self.extract_bytes(v))
            .collect::<Result<Vec<_>>>()?;
        Ok(Command::SMisMember(key, members))
    }
}
