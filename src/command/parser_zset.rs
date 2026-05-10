//! Sorted Set 命令解析器

use std::borrow::Cow;
use super::*;

use super::parser::CommandParser;
use crate::error::{AppError, Result};
use crate::protocol::RespValue;

impl CommandParser {
    /// 解析 ZADD 命令：ZADD key score member [score member ...]
    pub(crate) fn parse_zadd(&self, arr: &[RespValue]) -> Result<Command> {
        if arr.len() < 4 || arr.len() % 2 == 1 {
            return Err(AppError::Command(Cow::Borrowed("ZADD 命令需要成对的 score member 参数")));
        }
        let key = self.extract_string(&arr[1])?;
        let mut pairs = Vec::new();
        for i in (2..arr.len()).step_by(2) {
            let score: f64 = self
                .extract_string(&arr[i])?
                .parse()
                .map_err(|_| AppError::Command(Cow::Borrowed("ZADD 的 score 必须是数字")))?;
            let member = self.extract_string(&arr[i + 1])?;
            pairs.push((score, member));
        }
        Ok(Command::ZAdd(key, pairs))
    }

    /// 解析 ZREM 命令：ZREM key member [member ...]
    pub(crate) fn parse_zrem(&self, arr: &[RespValue]) -> Result<Command> {
        if arr.len() < 3 {
            return Err(AppError::Command(Cow::Borrowed("ZREM 命令需要至少 2 个参数")));
        }
        let key = self.extract_string(&arr[1])?;
        let members = arr[2..]
            .iter()
            .map(|v| self.extract_string(v))
            .collect::<Result<Vec<String>>>()?;
        Ok(Command::ZRem(key, members))
    }

    /// 解析 ZSCORE 命令：ZSCORE key member
    pub(crate) fn parse_zscore(&self, arr: &[RespValue]) -> Result<Command> {
        if arr.len() != 3 {
            return Err(AppError::Command(Cow::Borrowed("ZSCORE 命令需要 2 个参数")));
        }
        let key = self.extract_string(&arr[1])?;
        let member = self.extract_string(&arr[2])?;
        Ok(Command::ZScore(key, member))
    }

    /// 解析 ZRANK 命令：ZRANK key member
    pub(crate) fn parse_zrank(&self, arr: &[RespValue]) -> Result<Command> {
        if arr.len() != 3 {
            return Err(AppError::Command(Cow::Borrowed("ZRANK 命令需要 2 个参数")));
        }
        let key = self.extract_string(&arr[1])?;
        let member = self.extract_string(&arr[2])?;
        Ok(Command::ZRank(key, member))
    }

    /// 解析 ZRANGE 命令：ZRANGE key start stop [WITHSCORES]
    pub(crate) fn parse_zrange(&self, arr: &[RespValue]) -> Result<Command> {
        if arr.len() < 4 {
            return Err(AppError::Command(Cow::Borrowed("ZRANGE 命令需要至少 3 个参数")));
        }
        let key = self.extract_string(&arr[1])?;
        let min = self.extract_string(&arr[2])?;
        let max = self.extract_string(&arr[3])?;

        // 检查是否使用统一语法（包含 BYSCORE/BYLEX/REV/LIMIT）
        let mut use_unified = false;
        for item in arr.iter().skip(4) {
            let opt = self
                .extract_string(item)
                .unwrap_or_default()
                .to_ascii_uppercase();
            if matches!(opt.as_str(), "BYSCORE" | "BYLEX" | "REV" | "LIMIT") {
                use_unified = true;
                break;
            }
        }

        if use_unified {
            let mut by_score = false;
            let mut by_lex = false;
            let mut rev = false;
            let mut with_scores = false;
            let mut limit_offset = 0usize;
            let mut limit_count = 0usize;
            let mut i = 4;
            while i < arr.len() {
                let opt = self.extract_string(&arr[i])?.to_ascii_uppercase();
                match opt.as_str() {
                    "BYSCORE" => {
                        by_score = true;
                        i += 1;
                    }
                    "BYLEX" => {
                        by_lex = true;
                        i += 1;
                    }
                    "REV" => {
                        rev = true;
                        i += 1;
                    }
                    "WITHSCORES" => {
                        with_scores = true;
                        i += 1;
                    }
                    "LIMIT" => {
                        if i + 2 >= arr.len() {
                            return Err(AppError::Command(Cow::Borrowed("ZRANGE LIMIT 需要 offset count")));
                        }
                        limit_offset = self.extract_string(&arr[i + 1])?.parse().map_err(|_| {
                            AppError::Command(Cow::Borrowed("LIMIT offset 必须是整数"))
                        })?;
                        limit_count = self
                            .extract_string(&arr[i + 2])?
                            .parse()
                            .map_err(|_| AppError::Command(Cow::Borrowed("LIMIT count 必须是整数")))?;
                        i += 3;
                    }
                    _ => {
                        return Err(AppError::Command(Cow::Owned(format!("ZRANGE 不支持的选项: {}", opt))));
                    }
                }
            }
            Ok(Command::ZRangeUnified(
                key,
                min,
                max,
                by_score,
                by_lex,
                rev,
                with_scores,
                limit_offset,
                limit_count,
            ))
        } else {
            // 传统语法：ZRANGE key start stop [WITHSCORES]
            let start: isize = min
                .parse()
                .map_err(|_| AppError::Command(Cow::Borrowed("ZRANGE 的 start 必须是整数")))?;
            let stop: isize = max
                .parse()
                .map_err(|_| AppError::Command(Cow::Borrowed("ZRANGE 的 stop 必须是整数")))?;
            let with_scores = if arr.len() == 5 {
                let flag = self.extract_string(&arr[4])?.to_ascii_uppercase();
                if flag == "WITHSCORES" {
                    true
                } else {
                    return Err(AppError::Command(Cow::Borrowed("ZRANGE 可选参数只能是 WITHSCORES")));
                }
            } else {
                false
            };
            Ok(Command::ZRange(key, start, stop, with_scores))
        }
    }

    /// 解析 ZRANGEBYSCORE 命令：ZRANGEBYSCORE key min max [WITHSCORES]
    pub(crate) fn parse_zrangebyscore(&self, arr: &[RespValue]) -> Result<Command> {
        if arr.len() < 4 || arr.len() > 5 {
            return Err(AppError::Command(Cow::Borrowed("ZRANGEBYSCORE 命令参数数量错误")));
        }
        let key = self.extract_string(&arr[1])?;
        let min: f64 = self
            .extract_string(&arr[2])?
            .parse()
            .map_err(|_| AppError::Command(Cow::Borrowed("ZRANGEBYSCORE 的 min 必须是数字")))?;
        let max: f64 = self
            .extract_string(&arr[3])?
            .parse()
            .map_err(|_| AppError::Command(Cow::Borrowed("ZRANGEBYSCORE 的 max 必须是数字")))?;
        let with_scores = if arr.len() == 5 {
            let flag = self.extract_string(&arr[4])?.to_ascii_uppercase();
            if flag == "WITHSCORES" {
                true
            } else {
                return Err(AppError::Command(Cow::Borrowed("ZRANGEBYSCORE 可选参数只能是 WITHSCORES")));
            }
        } else {
            false
        };
        Ok(Command::ZRangeByScore(key, min, max, with_scores))
    }

    /// 解析 ZCARD 命令：ZCARD key
    pub(crate) fn parse_zcard(&self, arr: &[RespValue]) -> Result<Command> {
        if arr.len() != 2 {
            return Err(AppError::Command(Cow::Borrowed("ZCARD 命令需要 1 个参数")));
        }
        let key = self.extract_string(&arr[1])?;
        Ok(Command::ZCard(key))
    }

    /// 解析 ZREVRANGE 命令：ZREVRANGE key start stop [WITHSCORES]
    pub(crate) fn parse_zrevrange(&self, arr: &[RespValue]) -> Result<Command> {
        if arr.len() < 4 || arr.len() > 5 {
            return Err(AppError::Command(Cow::Borrowed("ZREVRANGE 命令参数数量错误")));
        }
        let key = self.extract_string(&arr[1])?;
        let start: isize = self
            .extract_string(&arr[2])?
            .parse()
            .map_err(|_| AppError::Command(Cow::Borrowed("ZREVRANGE 的 start 必须是整数")))?;
        let stop: isize = self
            .extract_string(&arr[3])?
            .parse()
            .map_err(|_| AppError::Command(Cow::Borrowed("ZREVRANGE 的 stop 必须是整数")))?;
        let with_scores = if arr.len() == 5 {
            let flag = self.extract_string(&arr[4])?.to_ascii_uppercase();
            if flag == "WITHSCORES" {
                true
            } else {
                return Err(AppError::Command(Cow::Borrowed("ZREVRANGE 可选参数只能是 WITHSCORES")));
            }
        } else {
            false
        };
        Ok(Command::ZRevRange(key, start, stop, with_scores))
    }

    /// 解析 ZREVRANK 命令：ZREVRANK key member
    pub(crate) fn parse_zrevrank(&self, arr: &[RespValue]) -> Result<Command> {
        if arr.len() != 3 {
            return Err(AppError::Command(Cow::Borrowed("ZREVRANK 命令需要 2 个参数")));
        }
        let key = self.extract_string(&arr[1])?;
        let member = self.extract_string(&arr[2])?;
        Ok(Command::ZRevRank(key, member))
    }

    /// 解析 ZINCRBY 命令：ZINCRBY key increment member
    pub(crate) fn parse_zincrby(&self, arr: &[RespValue]) -> Result<Command> {
        if arr.len() != 4 {
            return Err(AppError::Command(Cow::Borrowed("ZINCRBY 命令需要 3 个参数")));
        }
        let key = self.extract_string(&arr[1])?;
        let increment: f64 = self
            .extract_string(&arr[2])?
            .parse()
            .map_err(|_| AppError::Command(Cow::Borrowed("ZINCRBY 的 increment 必须是数字")))?;
        let member = self.extract_string(&arr[3])?;
        Ok(Command::ZIncrBy(key, increment, member))
    }

    /// 解析 ZCOUNT 命令：ZCOUNT key min max
    pub(crate) fn parse_zcount(&self, arr: &[RespValue]) -> Result<Command> {
        if arr.len() != 4 {
            return Err(AppError::Command(Cow::Borrowed("ZCOUNT 命令需要 3 个参数")));
        }
        let key = self.extract_string(&arr[1])?;
        let min: f64 = self
            .extract_string(&arr[2])?
            .parse()
            .map_err(|_| AppError::Command(Cow::Borrowed("ZCOUNT 的 min 必须是数字")))?;
        let max: f64 = self
            .extract_string(&arr[3])?
            .parse()
            .map_err(|_| AppError::Command(Cow::Borrowed("ZCOUNT 的 max 必须是数字")))?;
        Ok(Command::ZCount(key, min, max))
    }

    /// 解析 ZPOPMIN 命令：ZPOPMIN key [count]
    pub(crate) fn parse_zpopmin(&self, arr: &[RespValue]) -> Result<Command> {
        if arr.len() < 2 || arr.len() > 3 {
            return Err(AppError::Command(Cow::Borrowed("ZPOPMIN 命令参数数量错误")));
        }
        let key = self.extract_string(&arr[1])?;
        let count = if arr.len() == 3 {
            self.extract_string(&arr[2])?
                .parse()
                .map_err(|_| AppError::Command(Cow::Borrowed("ZPOPMIN 的 count 必须是整数")))?
        } else {
            1
        };
        Ok(Command::ZPopMin(key, count))
    }

    /// 解析 ZPOPMAX 命令：ZPOPMAX key [count]
    pub(crate) fn parse_zpopmax(&self, arr: &[RespValue]) -> Result<Command> {
        if arr.len() < 2 || arr.len() > 3 {
            return Err(AppError::Command(Cow::Borrowed("ZPOPMAX 命令参数数量错误")));
        }
        let key = self.extract_string(&arr[1])?;
        let count = if arr.len() == 3 {
            self.extract_string(&arr[2])?
                .parse()
                .map_err(|_| AppError::Command(Cow::Borrowed("ZPOPMAX 的 count 必须是整数")))?
        } else {
            1
        };
        Ok(Command::ZPopMax(key, count))
    }

    /// 解析 ZUNIONSTORE 命令：ZUNIONSTORE destination numkeys key [key ...] [WEIGHTS weight ...] [AGGREGATE SUM|MIN|MAX]
    pub(crate) fn parse_zunionstore(&self, arr: &[RespValue]) -> Result<Command> {
        if arr.len() < 4 {
            return Err(AppError::Command(Cow::Borrowed("ZUNIONSTORE 命令参数数量错误")));
        }
        let destination = self.extract_string(&arr[1])?;
        let numkeys: usize = self
            .extract_string(&arr[2])?
            .parse()
            .map_err(|_| AppError::Command(Cow::Borrowed("ZUNIONSTORE 的 numkeys 必须是整数")))?;
        if arr.len() < 3 + numkeys {
            return Err(AppError::Command(Cow::Borrowed("ZUNIONSTORE 提供的 key 数量不足")));
        }
        let keys: Vec<String> = arr[3..3 + numkeys]
            .iter()
            .map(|v| self.extract_string(v))
            .collect::<Result<Vec<String>>>()?;

        let mut weights = Vec::new();
        let mut aggregate = "SUM".to_string();
        let mut idx = 3 + numkeys;
        while idx < arr.len() {
            let flag = self.extract_string(&arr[idx])?.to_ascii_uppercase();
            if flag == "WEIGHTS" {
                idx += 1;
                for _ in 0..numkeys {
                    if idx >= arr.len() {
                        return Err(AppError::Command(Cow::Borrowed("ZUNIONSTORE WEIGHTS 参数不足")));
                    }
                    let w: f64 = self.extract_string(&arr[idx])?.parse().map_err(|_| {
                        AppError::Command(Cow::Borrowed("ZUNIONSTORE 的 weight 必须是数字"))
                    })?;
                    weights.push(w);
                    idx += 1;
                }
            } else if flag == "AGGREGATE" {
                idx += 1;
                if idx >= arr.len() {
                    return Err(AppError::Command(Cow::Borrowed("ZUNIONSTORE AGGREGATE 缺少参数")));
                }
                aggregate = self.extract_string(&arr[idx])?.to_ascii_uppercase();
                if aggregate != "SUM" && aggregate != "MIN" && aggregate != "MAX" {
                    return Err(AppError::Command(Cow::Borrowed("ZUNIONSTORE AGGREGATE 只能是 SUM|MIN|MAX")));
                }
                idx += 1;
            } else {
                return Err(AppError::Command(Cow::Owned(format!("ZUNIONSTORE 未知参数: {}", flag))));
            }
        }
        Ok(Command::ZUnionStore(destination, keys, weights, aggregate))
    }

    /// 解析 ZINTERSTORE 命令：ZINTERSTORE destination numkeys key [key ...] [WEIGHTS weight ...] [AGGREGATE SUM|MIN|MAX]
    pub(crate) fn parse_zinterstore(&self, arr: &[RespValue]) -> Result<Command> {
        if arr.len() < 4 {
            return Err(AppError::Command(Cow::Borrowed("ZINTERSTORE 命令参数数量错误")));
        }
        let destination = self.extract_string(&arr[1])?;
        let numkeys: usize = self
            .extract_string(&arr[2])?
            .parse()
            .map_err(|_| AppError::Command(Cow::Borrowed("ZINTERSTORE 的 numkeys 必须是整数")))?;
        if arr.len() < 3 + numkeys {
            return Err(AppError::Command(Cow::Borrowed("ZINTERSTORE 提供的 key 数量不足")));
        }
        let keys: Vec<String> = arr[3..3 + numkeys]
            .iter()
            .map(|v| self.extract_string(v))
            .collect::<Result<Vec<String>>>()?;

        let mut weights = Vec::new();
        let mut aggregate = "SUM".to_string();
        let mut idx = 3 + numkeys;
        while idx < arr.len() {
            let flag = self.extract_string(&arr[idx])?.to_ascii_uppercase();
            if flag == "WEIGHTS" {
                idx += 1;
                for _ in 0..numkeys {
                    if idx >= arr.len() {
                        return Err(AppError::Command(Cow::Borrowed("ZINTERSTORE WEIGHTS 参数不足")));
                    }
                    let w: f64 = self.extract_string(&arr[idx])?.parse().map_err(|_| {
                        AppError::Command(Cow::Borrowed("ZINTERSTORE 的 weight 必须是数字"))
                    })?;
                    weights.push(w);
                    idx += 1;
                }
            } else if flag == "AGGREGATE" {
                idx += 1;
                if idx >= arr.len() {
                    return Err(AppError::Command(Cow::Borrowed("ZINTERSTORE AGGREGATE 缺少参数")));
                }
                aggregate = self.extract_string(&arr[idx])?.to_ascii_uppercase();
                if aggregate != "SUM" && aggregate != "MIN" && aggregate != "MAX" {
                    return Err(AppError::Command(Cow::Borrowed("ZINTERSTORE AGGREGATE 只能是 SUM|MIN|MAX")));
                }
                idx += 1;
            } else {
                return Err(AppError::Command(Cow::Owned(format!("ZINTERSTORE 未知参数: {}", flag))));
            }
        }
        Ok(Command::ZInterStore(destination, keys, weights, aggregate))
    }

    /// 解析 ZSCAN 命令：ZSCAN key cursor [MATCH pattern] [COUNT count]
    pub(crate) fn parse_zscan(&self, arr: &[RespValue]) -> Result<Command> {
        if arr.len() < 3 {
            return Err(AppError::Command(Cow::Borrowed("ZSCAN 命令需要至少 2 个参数")));
        }
        let key = self.extract_string(&arr[1])?;
        let cursor: usize = self
            .extract_string(&arr[2])?
            .parse()
            .map_err(|_| AppError::Command(Cow::Borrowed("ZSCAN 的 cursor 必须是整数")))?;
        let mut pattern = String::new();
        let mut count = 0usize;
        let mut idx = 3;
        while idx < arr.len() {
            let flag = self.extract_string(&arr[idx])?.to_ascii_uppercase();
            if flag == "MATCH" {
                idx += 1;
                if idx >= arr.len() {
                    return Err(AppError::Command(Cow::Borrowed("ZSCAN MATCH 缺少 pattern")));
                }
                pattern = self.extract_string(&arr[idx])?;
                idx += 1;
            } else if flag == "COUNT" {
                idx += 1;
                if idx >= arr.len() {
                    return Err(AppError::Command(Cow::Borrowed("ZSCAN COUNT 缺少 count")));
                }
                count = self
                    .extract_string(&arr[idx])?
                    .parse()
                    .map_err(|_| AppError::Command(Cow::Borrowed("ZSCAN 的 count 必须是整数")))?;
                idx += 1;
            } else {
                return Err(AppError::Command(Cow::Owned(format!("ZSCAN 未知参数: {}", flag))));
            }
        }
        Ok(Command::ZScan(key, cursor, pattern, count))
    }

    /// 解析 ZRANGEBYLEX 命令：ZRANGEBYLEX key min max
    pub(crate) fn parse_zrangebylex(&self, arr: &[RespValue]) -> Result<Command> {
        if arr.len() != 4 {
            return Err(AppError::Command(Cow::Borrowed("ZRANGEBYLEX 命令需要 3 个参数")));
        }
        let key = self.extract_string(&arr[1])?;
        let min = self.extract_string(&arr[2])?;
        let max = self.extract_string(&arr[3])?;
        Ok(Command::ZRangeByLex(key, min, max))
    }

    /// 解析 ZINTERCARD 命令：ZINTERCARD numkeys key [key ...] [LIMIT limit]
    pub(crate) fn parse_zintercard(&self, arr: &[RespValue]) -> Result<Command> {
        if arr.len() < 3 {
            return Err(AppError::Command(Cow::Borrowed("ZINTERCARD 命令需要至少 2 个参数")));
        }
        let numkeys: usize = self
            .extract_string(&arr[1])?
            .parse()
            .map_err(|_| AppError::Command(Cow::Borrowed("ZINTERCARD numkeys 必须是整数")))?;
        if arr.len() < 2 + numkeys {
            return Err(AppError::Command(Cow::Borrowed("ZINTERCARD 参数数量不足")));
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
                    return Err(AppError::Command(Cow::Borrowed("ZINTERCARD LIMIT 需要参数")));
                }
                limit = self
                    .extract_string(&arr[3 + numkeys])?
                    .parse()
                    .map_err(|_| AppError::Command(Cow::Borrowed("ZINTERCARD LIMIT 必须是整数")))?;
            }
        }
        Ok(Command::ZInterCard(keys, limit))
    }

    /// 解析 ZREMRANGEBYLEX 命令：ZREMRANGEBYLEX key min max
    pub(crate) fn parse_zremrangebylex(&self, arr: &[RespValue]) -> Result<Command> {
        if arr.len() != 4 {
            return Err(AppError::Command(Cow::Borrowed("ZREMRANGEBYLEX 命令需要 3 个参数")));
        }
        let key = self.extract_string(&arr[1])?;
        let min = self.extract_string(&arr[2])?;
        let max = self.extract_string(&arr[3])?;
        Ok(Command::ZRemRangeByLex(key, min, max))
    }

    /// 解析 ZREMRANGEBYRANK 命令：ZREMRANGEBYRANK key start stop
    pub(crate) fn parse_zremrangebyrank(&self, arr: &[RespValue]) -> Result<Command> {
        if arr.len() != 4 {
            return Err(AppError::Command(Cow::Borrowed("ZREMRANGEBYRANK 命令需要 3 个参数")));
        }
        let key = self.extract_string(&arr[1])?;
        let start: isize = self
            .extract_string(&arr[2])?
            .parse()
            .map_err(|_| AppError::Command(Cow::Borrowed("ZREMRANGEBYRANK 的 start 必须是整数")))?;
        let stop: isize = self
            .extract_string(&arr[3])?
            .parse()
            .map_err(|_| AppError::Command(Cow::Borrowed("ZREMRANGEBYRANK 的 stop 必须是整数")))?;
        Ok(Command::ZRemRangeByRank(key, start, stop))
    }

    /// 解析 ZREMRANGEBYSCORE 命令：ZREMRANGEBYSCORE key min max
    pub(crate) fn parse_zremrangebyscore(&self, arr: &[RespValue]) -> Result<Command> {
        if arr.len() != 4 {
            return Err(AppError::Command(Cow::Borrowed("ZREMRANGEBYSCORE 命令需要 3 个参数")));
        }
        let key = self.extract_string(&arr[1])?;
        let min: f64 = self
            .extract_string(&arr[2])?
            .parse()
            .map_err(|_| AppError::Command(Cow::Borrowed("ZREMRANGEBYSCORE 的 min 必须是数字")))?;
        let max: f64 = self
            .extract_string(&arr[3])?
            .parse()
            .map_err(|_| AppError::Command(Cow::Borrowed("ZREMRANGEBYSCORE 的 max 必须是数字")))?;
        Ok(Command::ZRemRangeByScore(key, min, max))
    }

    /// 解析 ZRANDMEMBER 命令：ZRANDMEMBER key [count [WITHSCORES]]
    pub(crate) fn parse_zrandmember(&self, arr: &[RespValue]) -> Result<Command> {
        if arr.len() < 2 || arr.len() > 4 {
            return Err(AppError::Command(Cow::Borrowed("ZRANDMEMBER 命令参数数量错误")));
        }
        let key = self.extract_string(&arr[1])?;
        let mut count = 1i64;
        let mut with_scores = false;
        if arr.len() >= 3 {
            count = self
                .extract_string(&arr[2])?
                .parse()
                .map_err(|_| AppError::Command(Cow::Borrowed("ZRANDMEMBER count 必须是整数")))?;
        }
        if arr.len() == 4 {
            let flag = self.extract_string(&arr[3])?.to_ascii_uppercase();
            if flag == "WITHSCORES" {
                with_scores = true;
            } else {
                return Err(AppError::Command(Cow::Borrowed("ZRANDMEMBER 可选参数只能是 WITHSCORES")));
            }
        }
        Ok(Command::ZRandMember(key, count, with_scores))
    }

    /// 解析 ZDIFF 命令：ZDIFF numkeys key [key ...] [WITHSCORES]
    pub(crate) fn parse_zdiff(&self, arr: &[RespValue]) -> Result<Command> {
        if arr.len() < 3 {
            return Err(AppError::Command(Cow::Borrowed("ZDIFF 命令需要至少 2 个参数")));
        }
        let numkeys: usize = self
            .extract_string(&arr[1])?
            .parse()
            .map_err(|_| AppError::Command(Cow::Borrowed("ZDIFF numkeys 必须是整数")))?;
        if arr.len() < 2 + numkeys {
            return Err(AppError::Command(Cow::Borrowed("ZDIFF 参数数量不足")));
        }
        let keys: Vec<String> = arr[2..2 + numkeys]
            .iter()
            .map(|v| self.extract_string(v))
            .collect::<Result<Vec<_>>>()?;
        let mut with_scores = false;
        if arr.len() > 2 + numkeys {
            let flag = self.extract_string(&arr[2 + numkeys])?.to_ascii_uppercase();
            if flag == "WITHSCORES" {
                with_scores = true;
            }
        }
        Ok(Command::ZDiff(keys, with_scores))
    }

    /// 解析 ZDIFFSTORE 命令：ZDIFFSTORE destination numkeys key [key ...]
    pub(crate) fn parse_zdiffstore(&self, arr: &[RespValue]) -> Result<Command> {
        if arr.len() < 4 {
            return Err(AppError::Command(Cow::Borrowed("ZDIFFSTORE 命令需要至少 3 个参数")));
        }
        let destination = self.extract_string(&arr[1])?;
        let numkeys: usize = self
            .extract_string(&arr[2])?
            .parse()
            .map_err(|_| AppError::Command(Cow::Borrowed("ZDIFFSTORE numkeys 必须是整数")))?;
        if arr.len() < 3 + numkeys {
            return Err(AppError::Command(Cow::Borrowed("ZDIFFSTORE 参数数量不足")));
        }
        let keys: Vec<String> = arr[3..3 + numkeys]
            .iter()
            .map(|v| self.extract_string(v))
            .collect::<Result<Vec<_>>>()?;
        Ok(Command::ZDiffStore(destination, keys))
    }

    /// 解析 ZINTER 命令：ZINTER numkeys key [key ...] [WEIGHTS weight ...] [AGGREGATE SUM|MIN|MAX] [WITHSCORES]
    pub(crate) fn parse_zinter(&self, arr: &[RespValue]) -> Result<Command> {
        self.parse_zset_union_inter(arr, true)
    }

    /// 解析 ZUNION 命令：ZUNION numkeys key [key ...] [WEIGHTS weight ...] [AGGREGATE SUM|MIN|MAX] [WITHSCORES]
    pub(crate) fn parse_zunion(&self, arr: &[RespValue]) -> Result<Command> {
        self.parse_zset_union_inter(arr, false)
    }

    /// 辅助方法：解析 ZINTER/ZUNION
    pub(crate) fn parse_zset_union_inter(
        &self,
        arr: &[RespValue],
        is_inter: bool,
    ) -> Result<Command> {
        if arr.len() < 3 {
            return Err(AppError::Command(if is_inter {
                Cow::Borrowed("ZINTER 命令需要至少 2 个参数")
            } else {
                Cow::Borrowed("ZUNION 命令需要至少 2 个参数")
            }));
        }
        let numkeys: usize = self
            .extract_string(&arr[1])?
            .parse()
            .map_err(|_| AppError::Command(Cow::Borrowed("numkeys 必须是整数")))?;
        if arr.len() < 2 + numkeys {
            return Err(AppError::Command(Cow::Borrowed("参数数量不足")));
        }
        let keys: Vec<String> = arr[2..2 + numkeys]
            .iter()
            .map(|v| self.extract_string(v))
            .collect::<Result<Vec<_>>>()?;
        let mut weights = Vec::new();
        let mut aggregate = "SUM".to_string();
        let mut with_scores = false;
        let mut i = 2 + numkeys;
        while i < arr.len() {
            let opt = self.extract_string(&arr[i])?.to_ascii_uppercase();
            match opt.as_str() {
                "WEIGHTS" => {
                    i += 1;
                    while i < arr.len() {
                        let next = self.extract_string(&arr[i])?;
                        if let Ok(w) = next.parse::<f64>() {
                            weights.push(w);
                            i += 1;
                        } else {
                            break;
                        }
                    }
                }
                "AGGREGATE" => {
                    if i + 1 >= arr.len() {
                        return Err(AppError::Command(Cow::Borrowed("AGGREGATE 需要参数")));
                    }
                    aggregate = self.extract_string(&arr[i + 1])?.to_ascii_uppercase();
                    if !matches!(aggregate.as_str(), "SUM" | "MIN" | "MAX") {
                        return Err(AppError::Command(Cow::Borrowed("AGGREGATE 必须是 SUM|MIN|MAX")));
                    }
                    i += 2;
                }
                "WITHSCORES" => {
                    with_scores = true;
                    i += 1;
                }
                _ => {
                    return Err(AppError::Command(Cow::Owned(format!("不支持的选项: {}", opt))));
                }
            }
        }
        if is_inter {
            Ok(Command::ZInter(keys, weights, aggregate, with_scores))
        } else {
            Ok(Command::ZUnion(keys, weights, aggregate, with_scores))
        }
    }

    /// 解析 ZRANGESTORE 命令：ZRANGESTORE dst src min max [BYSCORE|BYLEX] [REV] [LIMIT offset count]
    pub(crate) fn parse_zrangestore(&self, arr: &[RespValue]) -> Result<Command> {
        if arr.len() < 5 {
            return Err(AppError::Command(Cow::Borrowed("ZRANGESTORE 命令需要至少 4 个参数")));
        }
        let dst = self.extract_string(&arr[1])?;
        let src = self.extract_string(&arr[2])?;
        let min = self.extract_string(&arr[3])?;
        let max = self.extract_string(&arr[4])?;
        let mut by_score = false;
        let mut by_lex = false;
        let mut rev = false;
        let mut limit_offset = 0usize;
        let mut limit_count = 0usize;
        let mut i = 5;
        while i < arr.len() {
            let opt = self.extract_string(&arr[i])?.to_ascii_uppercase();
            match opt.as_str() {
                "BYSCORE" => {
                    by_score = true;
                    i += 1;
                }
                "BYLEX" => {
                    by_lex = true;
                    i += 1;
                }
                "REV" => {
                    rev = true;
                    i += 1;
                }
                "LIMIT" => {
                    if i + 2 >= arr.len() {
                        return Err(AppError::Command(Cow::Borrowed("ZRANGESTORE LIMIT 需要 offset count")));
                    }
                    limit_offset = self
                        .extract_string(&arr[i + 1])?
                        .parse()
                        .map_err(|_| AppError::Command(Cow::Borrowed("LIMIT offset 必须是整数")))?;
                    limit_count = self
                        .extract_string(&arr[i + 2])?
                        .parse()
                        .map_err(|_| AppError::Command(Cow::Borrowed("LIMIT count 必须是整数")))?;
                    i += 3;
                }
                _ => {
                    return Err(AppError::Command(Cow::Owned(format!(
                        "ZRANGESTORE 不支持的选项: {}",
                        opt
                    ))));
                }
            }
        }
        Ok(Command::ZRangeStore(
            dst,
            src,
            min,
            max,
            by_score,
            by_lex,
            rev,
            limit_offset,
            limit_count,
        ))
    }

    /// 解析 ZMPOP 命令：ZMPOP numkeys key [key ...] MIN|MAX [COUNT count]
    pub(crate) fn parse_zmpop(&self, arr: &[RespValue]) -> Result<Command> {
        if arr.len() < 4 {
            return Err(AppError::Command(Cow::Borrowed("ZMPOP 命令需要至少 3 个参数")));
        }
        let numkeys: usize = self
            .extract_string(&arr[1])?
            .parse()
            .map_err(|_| AppError::Command(Cow::Borrowed("ZMPOP numkeys 必须是整数")))?;
        if arr.len() < 2 + numkeys + 1 {
            return Err(AppError::Command(Cow::Borrowed("ZMPOP 参数数量不足")));
        }
        let keys: Vec<String> = arr[2..2 + numkeys]
            .iter()
            .map(|v| self.extract_string(v))
            .collect::<Result<Vec<_>>>()?;
        let min_or_max_str = self.extract_string(&arr[2 + numkeys])?.to_ascii_uppercase();
        let min_or_max = match min_or_max_str.as_str() {
            "MIN" => true,
            "MAX" => false,
            _ => return Err(AppError::Command(Cow::Borrowed("ZMPOP 必须是 MIN 或 MAX"))),
        };
        let mut count = 1usize;
        let i = 3 + numkeys;
        if i < arr.len() {
            let opt = self.extract_string(&arr[i])?.to_ascii_uppercase();
            if opt == "COUNT" {
                if i + 1 >= arr.len() {
                    return Err(AppError::Command(Cow::Borrowed("ZMPOP COUNT 需要参数")));
                }
                count = self
                    .extract_string(&arr[i + 1])?
                    .parse()
                    .map_err(|_| AppError::Command(Cow::Borrowed("ZMPOP COUNT 必须是整数")))?;
            }
        }
        Ok(Command::ZMpop(keys, min_or_max, count))
    }

    /// 解析 BZMPOP 命令：BZMPOP timeout numkeys key [key ...] MIN|MAX [COUNT count]
    pub(crate) fn parse_bzmpop(&self, arr: &[RespValue]) -> Result<Command> {
        if arr.len() < 5 {
            return Err(AppError::Command(Cow::Borrowed("BZMPOP 命令需要至少 4 个参数")));
        }
        let timeout: f64 = self
            .extract_string(&arr[1])?
            .parse()
            .map_err(|_| AppError::Command(Cow::Borrowed("BZMPOP timeout 必须是数字")))?;
        let numkeys: usize = self
            .extract_string(&arr[2])?
            .parse()
            .map_err(|_| AppError::Command(Cow::Borrowed("BZMPOP numkeys 必须是整数")))?;
        if arr.len() < 3 + numkeys + 1 {
            return Err(AppError::Command(Cow::Borrowed("BZMPOP 参数数量不足")));
        }
        let keys: Vec<String> = arr[3..3 + numkeys]
            .iter()
            .map(|v| self.extract_string(v))
            .collect::<Result<Vec<_>>>()?;
        let min_or_max_str = self.extract_string(&arr[3 + numkeys])?.to_ascii_uppercase();
        let min_or_max = match min_or_max_str.as_str() {
            "MIN" => true,
            "MAX" => false,
            _ => return Err(AppError::Command(Cow::Borrowed("BZMPOP 必须是 MIN 或 MAX"))),
        };
        let mut count = 1usize;
        let i = 4 + numkeys;
        if i < arr.len() {
            let opt = self.extract_string(&arr[i])?.to_ascii_uppercase();
            if opt == "COUNT" {
                if i + 1 >= arr.len() {
                    return Err(AppError::Command(Cow::Borrowed("BZMPOP COUNT 需要参数")));
                }
                count = self
                    .extract_string(&arr[i + 1])?
                    .parse()
                    .map_err(|_| AppError::Command(Cow::Borrowed("BZMPOP COUNT 必须是整数")))?;
            }
        }
        Ok(Command::BZMpop(timeout, keys, min_or_max, count))
    }

    /// 解析 BZPOPMIN 命令：BZPOPMIN key [key ...] timeout
    pub(crate) fn parse_bzpopmin(&self, arr: &[RespValue]) -> Result<Command> {
        if arr.len() < 3 {
            return Err(AppError::Command(Cow::Borrowed("BZPOPMIN 命令需要至少 2 个参数")));
        }
        let timeout: f64 = self
            .extract_string(&arr[arr.len() - 1])?
            .parse()
            .map_err(|_| AppError::Command(Cow::Borrowed("BZPOPMIN timeout 必须是数字")))?;
        let keys: Vec<String> = arr[1..arr.len() - 1]
            .iter()
            .map(|v| self.extract_string(v))
            .collect::<Result<Vec<_>>>()?;
        Ok(Command::BZPopMin(keys, timeout))
    }

    /// 解析 BZPOPMAX 命令：BZPOPMAX key [key ...] timeout
    pub(crate) fn parse_bzpopmax(&self, arr: &[RespValue]) -> Result<Command> {
        if arr.len() < 3 {
            return Err(AppError::Command(Cow::Borrowed("BZPOPMAX 命令需要至少 2 个参数")));
        }
        let timeout: f64 = self
            .extract_string(&arr[arr.len() - 1])?
            .parse()
            .map_err(|_| AppError::Command(Cow::Borrowed("BZPOPMAX timeout 必须是数字")))?;
        let keys: Vec<String> = arr[1..arr.len() - 1]
            .iter()
            .map(|v| self.extract_string(v))
            .collect::<Result<Vec<_>>>()?;
        Ok(Command::BZPopMax(keys, timeout))
    }

    /// 解析 ZREVRANGEBYSCORE 命令：ZREVRANGEBYSCORE key max min [WITHSCORES] [LIMIT offset count]
    pub(crate) fn parse_zrevrangebyscore(&self, arr: &[RespValue]) -> Result<Command> {
        if arr.len() < 4 {
            return Err(AppError::Command(Cow::Borrowed("ZREVRANGEBYSCORE 命令需要至少 4 个参数")));
        }
        let key = self.extract_string(&arr[1])?;
        let max: f64 = self
            .extract_string(&arr[2])?
            .parse()
            .map_err(|_| AppError::Command(Cow::Borrowed("ZREVRANGEBYSCORE max 必须是数字")))?;
        let min: f64 = self
            .extract_string(&arr[3])?
            .parse()
            .map_err(|_| AppError::Command(Cow::Borrowed("ZREVRANGEBYSCORE min 必须是数字")))?;
        let mut with_scores = false;
        let mut limit_offset = 0usize;
        let mut limit_count = 0usize;
        let mut i = 4;
        while i < arr.len() {
            let opt = self.extract_string(&arr[i])?.to_ascii_uppercase();
            match opt.as_str() {
                "WITHSCORES" => {
                    with_scores = true;
                    i += 1;
                }
                "LIMIT" => {
                    if i + 2 >= arr.len() {
                        return Err(AppError::Command(Cow::Borrowed("ZREVRANGEBYSCORE LIMIT 需要 offset count")));
                    }
                    limit_offset = self
                        .extract_string(&arr[i + 1])?
                        .parse()
                        .map_err(|_| AppError::Command(Cow::Borrowed("LIMIT offset 必须是整数")))?;
                    limit_count = self
                        .extract_string(&arr[i + 2])?
                        .parse()
                        .map_err(|_| AppError::Command(Cow::Borrowed("LIMIT count 必须是整数")))?;
                    i += 3;
                }
                _ => {
                    return Err(AppError::Command(Cow::Owned(format!(
                        "ZREVRANGEBYSCORE 不支持的选项: {}",
                        opt
                    ))));
                }
            }
        }
        Ok(Command::ZRevRangeByScore(
            key,
            max,
            min,
            with_scores,
            limit_offset,
            limit_count,
        ))
    }

    /// 解析 ZREVRANGEBYLEX 命令：ZREVRANGEBYLEX key max min [LIMIT offset count]
    pub(crate) fn parse_zrevrangebylex(&self, arr: &[RespValue]) -> Result<Command> {
        if arr.len() < 4 {
            return Err(AppError::Command(Cow::Borrowed("ZREVRANGEBYLEX 命令需要至少 4 个参数")));
        }
        let key = self.extract_string(&arr[1])?;
        let max = self.extract_string(&arr[2])?;
        let min = self.extract_string(&arr[3])?;
        let mut limit_offset = 0usize;
        let mut limit_count = 0usize;
        if arr.len() >= 7 {
            let opt = self.extract_string(&arr[4])?.to_ascii_uppercase();
            if opt == "LIMIT" {
                limit_offset = self
                    .extract_string(&arr[5])?
                    .parse()
                    .map_err(|_| AppError::Command(Cow::Borrowed("LIMIT offset 必须是整数")))?;
                limit_count = self
                    .extract_string(&arr[6])?
                    .parse()
                    .map_err(|_| AppError::Command(Cow::Borrowed("LIMIT count 必须是整数")))?;
            }
        }
        Ok(Command::ZRevRangeByLex(
            key,
            max,
            min,
            limit_offset,
            limit_count,
        ))
    }

    /// 解析 ZMSCORE 命令：ZMSCORE key member [member ...]
    pub(crate) fn parse_zmscore(&self, arr: &[RespValue]) -> Result<Command> {
        if arr.len() < 3 {
            return Err(AppError::Command(Cow::Borrowed("ZMSCORE 命令需要至少 2 个参数")));
        }
        let key = self.extract_string(&arr[1])?;
        let members: Vec<String> = arr[2..]
            .iter()
            .map(|v| self.extract_string(v))
            .collect::<Result<Vec<_>>>()?;
        Ok(Command::ZMScore(key, members))
    }

    /// 解析 ZLEXCOUNT 命令：ZLEXCOUNT key min max
    pub(crate) fn parse_zlexcount(&self, arr: &[RespValue]) -> Result<Command> {
        if arr.len() != 4 {
            return Err(AppError::Command(Cow::Borrowed("ZLEXCOUNT 命令需要 3 个参数")));
        }
        let key = self.extract_string(&arr[1])?;
        let min = self.extract_string(&arr[2])?;
        let max = self.extract_string(&arr[3])?;
        Ok(Command::ZLexCount(key, min, max))
    }
}
