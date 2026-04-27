//! Sorted Set 命令解析器

use super::*;

use super::parser::CommandParser;
use crate::error::{AppError, Result};
use crate::protocol::RespValue;

impl CommandParser {
    /// 解析 ZADD 命令：ZADD key score member [score member ...]
    pub(crate) fn parse_zadd(&self, arr: &[RespValue]) -> Result<Command> {
        if arr.len() < 4 || arr.len() % 2 == 1 {
            return Err(AppError::Command(
                "ZADD 命令需要成对的 score member 参数".to_string(),
            ));
        }
        let key = self.extract_string(&arr[1])?;
        let mut pairs = Vec::new();
        for i in (2..arr.len()).step_by(2) {
            let score: f64 = self
                .extract_string(&arr[i])?
                .parse()
                .map_err(|_| AppError::Command("ZADD 的 score 必须是数字".to_string()))?;
            let member = self.extract_string(&arr[i + 1])?;
            pairs.push((score, member));
        }
        Ok(Command::ZAdd(key, pairs))
    }

    /// 解析 ZREM 命令：ZREM key member [member ...]
    pub(crate) fn parse_zrem(&self, arr: &[RespValue]) -> Result<Command> {
        if arr.len() < 3 {
            return Err(AppError::Command("ZREM 命令需要至少 2 个参数".to_string()));
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
            return Err(AppError::Command("ZSCORE 命令需要 2 个参数".to_string()));
        }
        let key = self.extract_string(&arr[1])?;
        let member = self.extract_string(&arr[2])?;
        Ok(Command::ZScore(key, member))
    }

    /// 解析 ZRANK 命令：ZRANK key member
    pub(crate) fn parse_zrank(&self, arr: &[RespValue]) -> Result<Command> {
        if arr.len() != 3 {
            return Err(AppError::Command("ZRANK 命令需要 2 个参数".to_string()));
        }
        let key = self.extract_string(&arr[1])?;
        let member = self.extract_string(&arr[2])?;
        Ok(Command::ZRank(key, member))
    }

    /// 解析 ZRANGE 命令：ZRANGE key start stop [WITHSCORES]
    pub(crate) fn parse_zrange(&self, arr: &[RespValue]) -> Result<Command> {
        if arr.len() < 4 {
            return Err(AppError::Command(
                "ZRANGE 命令需要至少 3 个参数".to_string(),
            ));
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
                            return Err(AppError::Command(
                                "ZRANGE LIMIT 需要 offset count".to_string(),
                            ));
                        }
                        limit_offset = self.extract_string(&arr[i + 1])?.parse().map_err(|_| {
                            AppError::Command("LIMIT offset 必须是整数".to_string())
                        })?;
                        limit_count = self
                            .extract_string(&arr[i + 2])?
                            .parse()
                            .map_err(|_| AppError::Command("LIMIT count 必须是整数".to_string()))?;
                        i += 3;
                    }
                    _ => {
                        return Err(AppError::Command(format!("ZRANGE 不支持的选项: {}", opt)));
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
                .map_err(|_| AppError::Command("ZRANGE 的 start 必须是整数".to_string()))?;
            let stop: isize = max
                .parse()
                .map_err(|_| AppError::Command("ZRANGE 的 stop 必须是整数".to_string()))?;
            let with_scores = if arr.len() == 5 {
                let flag = self.extract_string(&arr[4])?.to_ascii_uppercase();
                if flag == "WITHSCORES" {
                    true
                } else {
                    return Err(AppError::Command(
                        "ZRANGE 可选参数只能是 WITHSCORES".to_string(),
                    ));
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
            return Err(AppError::Command(
                "ZRANGEBYSCORE 命令参数数量错误".to_string(),
            ));
        }
        let key = self.extract_string(&arr[1])?;
        let min: f64 = self
            .extract_string(&arr[2])?
            .parse()
            .map_err(|_| AppError::Command("ZRANGEBYSCORE 的 min 必须是数字".to_string()))?;
        let max: f64 = self
            .extract_string(&arr[3])?
            .parse()
            .map_err(|_| AppError::Command("ZRANGEBYSCORE 的 max 必须是数字".to_string()))?;
        let with_scores = if arr.len() == 5 {
            let flag = self.extract_string(&arr[4])?.to_ascii_uppercase();
            if flag == "WITHSCORES" {
                true
            } else {
                return Err(AppError::Command(
                    "ZRANGEBYSCORE 可选参数只能是 WITHSCORES".to_string(),
                ));
            }
        } else {
            false
        };
        Ok(Command::ZRangeByScore(key, min, max, with_scores))
    }

    /// 解析 ZCARD 命令：ZCARD key
    pub(crate) fn parse_zcard(&self, arr: &[RespValue]) -> Result<Command> {
        if arr.len() != 2 {
            return Err(AppError::Command("ZCARD 命令需要 1 个参数".to_string()));
        }
        let key = self.extract_string(&arr[1])?;
        Ok(Command::ZCard(key))
    }

    /// 解析 ZREVRANGE 命令：ZREVRANGE key start stop [WITHSCORES]
    pub(crate) fn parse_zrevrange(&self, arr: &[RespValue]) -> Result<Command> {
        if arr.len() < 4 || arr.len() > 5 {
            return Err(AppError::Command("ZREVRANGE 命令参数数量错误".to_string()));
        }
        let key = self.extract_string(&arr[1])?;
        let start: isize = self
            .extract_string(&arr[2])?
            .parse()
            .map_err(|_| AppError::Command("ZREVRANGE 的 start 必须是整数".to_string()))?;
        let stop: isize = self
            .extract_string(&arr[3])?
            .parse()
            .map_err(|_| AppError::Command("ZREVRANGE 的 stop 必须是整数".to_string()))?;
        let with_scores = if arr.len() == 5 {
            let flag = self.extract_string(&arr[4])?.to_ascii_uppercase();
            if flag == "WITHSCORES" {
                true
            } else {
                return Err(AppError::Command(
                    "ZREVRANGE 可选参数只能是 WITHSCORES".to_string(),
                ));
            }
        } else {
            false
        };
        Ok(Command::ZRevRange(key, start, stop, with_scores))
    }

    /// 解析 ZREVRANK 命令：ZREVRANK key member
    pub(crate) fn parse_zrevrank(&self, arr: &[RespValue]) -> Result<Command> {
        if arr.len() != 3 {
            return Err(AppError::Command("ZREVRANK 命令需要 2 个参数".to_string()));
        }
        let key = self.extract_string(&arr[1])?;
        let member = self.extract_string(&arr[2])?;
        Ok(Command::ZRevRank(key, member))
    }

    /// 解析 ZINCRBY 命令：ZINCRBY key increment member
    pub(crate) fn parse_zincrby(&self, arr: &[RespValue]) -> Result<Command> {
        if arr.len() != 4 {
            return Err(AppError::Command("ZINCRBY 命令需要 3 个参数".to_string()));
        }
        let key = self.extract_string(&arr[1])?;
        let increment: f64 = self
            .extract_string(&arr[2])?
            .parse()
            .map_err(|_| AppError::Command("ZINCRBY 的 increment 必须是数字".to_string()))?;
        let member = self.extract_string(&arr[3])?;
        Ok(Command::ZIncrBy(key, increment, member))
    }

    /// 解析 ZCOUNT 命令：ZCOUNT key min max
    pub(crate) fn parse_zcount(&self, arr: &[RespValue]) -> Result<Command> {
        if arr.len() != 4 {
            return Err(AppError::Command("ZCOUNT 命令需要 3 个参数".to_string()));
        }
        let key = self.extract_string(&arr[1])?;
        let min: f64 = self
            .extract_string(&arr[2])?
            .parse()
            .map_err(|_| AppError::Command("ZCOUNT 的 min 必须是数字".to_string()))?;
        let max: f64 = self
            .extract_string(&arr[3])?
            .parse()
            .map_err(|_| AppError::Command("ZCOUNT 的 max 必须是数字".to_string()))?;
        Ok(Command::ZCount(key, min, max))
    }

    /// 解析 ZPOPMIN 命令：ZPOPMIN key [count]
    pub(crate) fn parse_zpopmin(&self, arr: &[RespValue]) -> Result<Command> {
        if arr.len() < 2 || arr.len() > 3 {
            return Err(AppError::Command("ZPOPMIN 命令参数数量错误".to_string()));
        }
        let key = self.extract_string(&arr[1])?;
        let count = if arr.len() == 3 {
            self.extract_string(&arr[2])?
                .parse()
                .map_err(|_| AppError::Command("ZPOPMIN 的 count 必须是整数".to_string()))?
        } else {
            1
        };
        Ok(Command::ZPopMin(key, count))
    }

    /// 解析 ZPOPMAX 命令：ZPOPMAX key [count]
    pub(crate) fn parse_zpopmax(&self, arr: &[RespValue]) -> Result<Command> {
        if arr.len() < 2 || arr.len() > 3 {
            return Err(AppError::Command("ZPOPMAX 命令参数数量错误".to_string()));
        }
        let key = self.extract_string(&arr[1])?;
        let count = if arr.len() == 3 {
            self.extract_string(&arr[2])?
                .parse()
                .map_err(|_| AppError::Command("ZPOPMAX 的 count 必须是整数".to_string()))?
        } else {
            1
        };
        Ok(Command::ZPopMax(key, count))
    }

    /// 解析 ZUNIONSTORE 命令：ZUNIONSTORE destination numkeys key [key ...] [WEIGHTS weight ...] [AGGREGATE SUM|MIN|MAX]
    pub(crate) fn parse_zunionstore(&self, arr: &[RespValue]) -> Result<Command> {
        if arr.len() < 4 {
            return Err(AppError::Command(
                "ZUNIONSTORE 命令参数数量错误".to_string(),
            ));
        }
        let destination = self.extract_string(&arr[1])?;
        let numkeys: usize = self
            .extract_string(&arr[2])?
            .parse()
            .map_err(|_| AppError::Command("ZUNIONSTORE 的 numkeys 必须是整数".to_string()))?;
        if arr.len() < 3 + numkeys {
            return Err(AppError::Command(
                "ZUNIONSTORE 提供的 key 数量不足".to_string(),
            ));
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
                        return Err(AppError::Command(
                            "ZUNIONSTORE WEIGHTS 参数不足".to_string(),
                        ));
                    }
                    let w: f64 = self.extract_string(&arr[idx])?.parse().map_err(|_| {
                        AppError::Command("ZUNIONSTORE 的 weight 必须是数字".to_string())
                    })?;
                    weights.push(w);
                    idx += 1;
                }
            } else if flag == "AGGREGATE" {
                idx += 1;
                if idx >= arr.len() {
                    return Err(AppError::Command(
                        "ZUNIONSTORE AGGREGATE 缺少参数".to_string(),
                    ));
                }
                aggregate = self.extract_string(&arr[idx])?.to_ascii_uppercase();
                if aggregate != "SUM" && aggregate != "MIN" && aggregate != "MAX" {
                    return Err(AppError::Command(
                        "ZUNIONSTORE AGGREGATE 只能是 SUM|MIN|MAX".to_string(),
                    ));
                }
                idx += 1;
            } else {
                return Err(AppError::Command(format!("ZUNIONSTORE 未知参数: {}", flag)));
            }
        }
        Ok(Command::ZUnionStore(destination, keys, weights, aggregate))
    }

    /// 解析 ZINTERSTORE 命令：ZINTERSTORE destination numkeys key [key ...] [WEIGHTS weight ...] [AGGREGATE SUM|MIN|MAX]
    pub(crate) fn parse_zinterstore(&self, arr: &[RespValue]) -> Result<Command> {
        if arr.len() < 4 {
            return Err(AppError::Command(
                "ZINTERSTORE 命令参数数量错误".to_string(),
            ));
        }
        let destination = self.extract_string(&arr[1])?;
        let numkeys: usize = self
            .extract_string(&arr[2])?
            .parse()
            .map_err(|_| AppError::Command("ZINTERSTORE 的 numkeys 必须是整数".to_string()))?;
        if arr.len() < 3 + numkeys {
            return Err(AppError::Command(
                "ZINTERSTORE 提供的 key 数量不足".to_string(),
            ));
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
                        return Err(AppError::Command(
                            "ZINTERSTORE WEIGHTS 参数不足".to_string(),
                        ));
                    }
                    let w: f64 = self.extract_string(&arr[idx])?.parse().map_err(|_| {
                        AppError::Command("ZINTERSTORE 的 weight 必须是数字".to_string())
                    })?;
                    weights.push(w);
                    idx += 1;
                }
            } else if flag == "AGGREGATE" {
                idx += 1;
                if idx >= arr.len() {
                    return Err(AppError::Command(
                        "ZINTERSTORE AGGREGATE 缺少参数".to_string(),
                    ));
                }
                aggregate = self.extract_string(&arr[idx])?.to_ascii_uppercase();
                if aggregate != "SUM" && aggregate != "MIN" && aggregate != "MAX" {
                    return Err(AppError::Command(
                        "ZINTERSTORE AGGREGATE 只能是 SUM|MIN|MAX".to_string(),
                    ));
                }
                idx += 1;
            } else {
                return Err(AppError::Command(format!("ZINTERSTORE 未知参数: {}", flag)));
            }
        }
        Ok(Command::ZInterStore(destination, keys, weights, aggregate))
    }

    /// 解析 ZSCAN 命令：ZSCAN key cursor [MATCH pattern] [COUNT count]
    pub(crate) fn parse_zscan(&self, arr: &[RespValue]) -> Result<Command> {
        if arr.len() < 3 {
            return Err(AppError::Command("ZSCAN 命令需要至少 2 个参数".to_string()));
        }
        let key = self.extract_string(&arr[1])?;
        let cursor: usize = self
            .extract_string(&arr[2])?
            .parse()
            .map_err(|_| AppError::Command("ZSCAN 的 cursor 必须是整数".to_string()))?;
        let mut pattern = String::new();
        let mut count = 0usize;
        let mut idx = 3;
        while idx < arr.len() {
            let flag = self.extract_string(&arr[idx])?.to_ascii_uppercase();
            if flag == "MATCH" {
                idx += 1;
                if idx >= arr.len() {
                    return Err(AppError::Command("ZSCAN MATCH 缺少 pattern".to_string()));
                }
                pattern = self.extract_string(&arr[idx])?;
                idx += 1;
            } else if flag == "COUNT" {
                idx += 1;
                if idx >= arr.len() {
                    return Err(AppError::Command("ZSCAN COUNT 缺少 count".to_string()));
                }
                count = self
                    .extract_string(&arr[idx])?
                    .parse()
                    .map_err(|_| AppError::Command("ZSCAN 的 count 必须是整数".to_string()))?;
                idx += 1;
            } else {
                return Err(AppError::Command(format!("ZSCAN 未知参数: {}", flag)));
            }
        }
        Ok(Command::ZScan(key, cursor, pattern, count))
    }

    /// 解析 ZRANGEBYLEX 命令：ZRANGEBYLEX key min max
    pub(crate) fn parse_zrangebylex(&self, arr: &[RespValue]) -> Result<Command> {
        if arr.len() != 4 {
            return Err(AppError::Command(
                "ZRANGEBYLEX 命令需要 3 个参数".to_string(),
            ));
        }
        let key = self.extract_string(&arr[1])?;
        let min = self.extract_string(&arr[2])?;
        let max = self.extract_string(&arr[3])?;
        Ok(Command::ZRangeByLex(key, min, max))
    }

    /// 解析 ZINTERCARD 命令：ZINTERCARD numkeys key [key ...] [LIMIT limit]
    pub(crate) fn parse_zintercard(&self, arr: &[RespValue]) -> Result<Command> {
        if arr.len() < 3 {
            return Err(AppError::Command(
                "ZINTERCARD 命令需要至少 2 个参数".to_string(),
            ));
        }
        let numkeys: usize = self
            .extract_string(&arr[1])?
            .parse()
            .map_err(|_| AppError::Command("ZINTERCARD numkeys 必须是整数".to_string()))?;
        if arr.len() < 2 + numkeys {
            return Err(AppError::Command("ZINTERCARD 参数数量不足".to_string()));
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
                    return Err(AppError::Command("ZINTERCARD LIMIT 需要参数".to_string()));
                }
                limit = self
                    .extract_string(&arr[3 + numkeys])?
                    .parse()
                    .map_err(|_| AppError::Command("ZINTERCARD LIMIT 必须是整数".to_string()))?;
            }
        }
        Ok(Command::ZInterCard(keys, limit))
    }

    /// 解析 ZREMRANGEBYLEX 命令：ZREMRANGEBYLEX key min max
    pub(crate) fn parse_zremrangebylex(&self, arr: &[RespValue]) -> Result<Command> {
        if arr.len() != 4 {
            return Err(AppError::Command(
                "ZREMRANGEBYLEX 命令需要 3 个参数".to_string(),
            ));
        }
        let key = self.extract_string(&arr[1])?;
        let min = self.extract_string(&arr[2])?;
        let max = self.extract_string(&arr[3])?;
        Ok(Command::ZRemRangeByLex(key, min, max))
    }

    /// 解析 ZREMRANGEBYRANK 命令：ZREMRANGEBYRANK key start stop
    pub(crate) fn parse_zremrangebyrank(&self, arr: &[RespValue]) -> Result<Command> {
        if arr.len() != 4 {
            return Err(AppError::Command(
                "ZREMRANGEBYRANK 命令需要 3 个参数".to_string(),
            ));
        }
        let key = self.extract_string(&arr[1])?;
        let start: isize = self
            .extract_string(&arr[2])?
            .parse()
            .map_err(|_| AppError::Command("ZREMRANGEBYRANK 的 start 必须是整数".to_string()))?;
        let stop: isize = self
            .extract_string(&arr[3])?
            .parse()
            .map_err(|_| AppError::Command("ZREMRANGEBYRANK 的 stop 必须是整数".to_string()))?;
        Ok(Command::ZRemRangeByRank(key, start, stop))
    }

    /// 解析 ZREMRANGEBYSCORE 命令：ZREMRANGEBYSCORE key min max
    pub(crate) fn parse_zremrangebyscore(&self, arr: &[RespValue]) -> Result<Command> {
        if arr.len() != 4 {
            return Err(AppError::Command(
                "ZREMRANGEBYSCORE 命令需要 3 个参数".to_string(),
            ));
        }
        let key = self.extract_string(&arr[1])?;
        let min: f64 = self
            .extract_string(&arr[2])?
            .parse()
            .map_err(|_| AppError::Command("ZREMRANGEBYSCORE 的 min 必须是数字".to_string()))?;
        let max: f64 = self
            .extract_string(&arr[3])?
            .parse()
            .map_err(|_| AppError::Command("ZREMRANGEBYSCORE 的 max 必须是数字".to_string()))?;
        Ok(Command::ZRemRangeByScore(key, min, max))
    }

    /// 解析 ZRANDMEMBER 命令：ZRANDMEMBER key [count [WITHSCORES]]
    pub(crate) fn parse_zrandmember(&self, arr: &[RespValue]) -> Result<Command> {
        if arr.len() < 2 || arr.len() > 4 {
            return Err(AppError::Command(
                "ZRANDMEMBER 命令参数数量错误".to_string(),
            ));
        }
        let key = self.extract_string(&arr[1])?;
        let mut count = 1i64;
        let mut with_scores = false;
        if arr.len() >= 3 {
            count = self
                .extract_string(&arr[2])?
                .parse()
                .map_err(|_| AppError::Command("ZRANDMEMBER count 必须是整数".to_string()))?;
        }
        if arr.len() == 4 {
            let flag = self.extract_string(&arr[3])?.to_ascii_uppercase();
            if flag == "WITHSCORES" {
                with_scores = true;
            } else {
                return Err(AppError::Command(
                    "ZRANDMEMBER 可选参数只能是 WITHSCORES".to_string(),
                ));
            }
        }
        Ok(Command::ZRandMember(key, count, with_scores))
    }

    /// 解析 ZDIFF 命令：ZDIFF numkeys key [key ...] [WITHSCORES]
    pub(crate) fn parse_zdiff(&self, arr: &[RespValue]) -> Result<Command> {
        if arr.len() < 3 {
            return Err(AppError::Command("ZDIFF 命令需要至少 2 个参数".to_string()));
        }
        let numkeys: usize = self
            .extract_string(&arr[1])?
            .parse()
            .map_err(|_| AppError::Command("ZDIFF numkeys 必须是整数".to_string()))?;
        if arr.len() < 2 + numkeys {
            return Err(AppError::Command("ZDIFF 参数数量不足".to_string()));
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
            return Err(AppError::Command(
                "ZDIFFSTORE 命令需要至少 3 个参数".to_string(),
            ));
        }
        let destination = self.extract_string(&arr[1])?;
        let numkeys: usize = self
            .extract_string(&arr[2])?
            .parse()
            .map_err(|_| AppError::Command("ZDIFFSTORE numkeys 必须是整数".to_string()))?;
        if arr.len() < 3 + numkeys {
            return Err(AppError::Command("ZDIFFSTORE 参数数量不足".to_string()));
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
                "ZINTER 命令需要至少 2 个参数".to_string()
            } else {
                "ZUNION 命令需要至少 2 个参数".to_string()
            }));
        }
        let numkeys: usize = self
            .extract_string(&arr[1])?
            .parse()
            .map_err(|_| AppError::Command("numkeys 必须是整数".to_string()))?;
        if arr.len() < 2 + numkeys {
            return Err(AppError::Command("参数数量不足".to_string()));
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
                        return Err(AppError::Command("AGGREGATE 需要参数".to_string()));
                    }
                    aggregate = self.extract_string(&arr[i + 1])?.to_ascii_uppercase();
                    if !matches!(aggregate.as_str(), "SUM" | "MIN" | "MAX") {
                        return Err(AppError::Command(
                            "AGGREGATE 必须是 SUM|MIN|MAX".to_string(),
                        ));
                    }
                    i += 2;
                }
                "WITHSCORES" => {
                    with_scores = true;
                    i += 1;
                }
                _ => {
                    return Err(AppError::Command(format!("不支持的选项: {}", opt)));
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
            return Err(AppError::Command(
                "ZRANGESTORE 命令需要至少 4 个参数".to_string(),
            ));
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
                        return Err(AppError::Command(
                            "ZRANGESTORE LIMIT 需要 offset count".to_string(),
                        ));
                    }
                    limit_offset = self
                        .extract_string(&arr[i + 1])?
                        .parse()
                        .map_err(|_| AppError::Command("LIMIT offset 必须是整数".to_string()))?;
                    limit_count = self
                        .extract_string(&arr[i + 2])?
                        .parse()
                        .map_err(|_| AppError::Command("LIMIT count 必须是整数".to_string()))?;
                    i += 3;
                }
                _ => {
                    return Err(AppError::Command(format!(
                        "ZRANGESTORE 不支持的选项: {}",
                        opt
                    )));
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
            return Err(AppError::Command("ZMPOP 命令需要至少 3 个参数".to_string()));
        }
        let numkeys: usize = self
            .extract_string(&arr[1])?
            .parse()
            .map_err(|_| AppError::Command("ZMPOP numkeys 必须是整数".to_string()))?;
        if arr.len() < 2 + numkeys + 1 {
            return Err(AppError::Command("ZMPOP 参数数量不足".to_string()));
        }
        let keys: Vec<String> = arr[2..2 + numkeys]
            .iter()
            .map(|v| self.extract_string(v))
            .collect::<Result<Vec<_>>>()?;
        let min_or_max_str = self.extract_string(&arr[2 + numkeys])?.to_ascii_uppercase();
        let min_or_max = match min_or_max_str.as_str() {
            "MIN" => true,
            "MAX" => false,
            _ => return Err(AppError::Command("ZMPOP 必须是 MIN 或 MAX".to_string())),
        };
        let mut count = 1usize;
        let i = 3 + numkeys;
        if i < arr.len() {
            let opt = self.extract_string(&arr[i])?.to_ascii_uppercase();
            if opt == "COUNT" {
                if i + 1 >= arr.len() {
                    return Err(AppError::Command("ZMPOP COUNT 需要参数".to_string()));
                }
                count = self
                    .extract_string(&arr[i + 1])?
                    .parse()
                    .map_err(|_| AppError::Command("ZMPOP COUNT 必须是整数".to_string()))?;
            }
        }
        Ok(Command::ZMpop(keys, min_or_max, count))
    }

    /// 解析 BZMPOP 命令：BZMPOP timeout numkeys key [key ...] MIN|MAX [COUNT count]
    pub(crate) fn parse_bzmpop(&self, arr: &[RespValue]) -> Result<Command> {
        if arr.len() < 5 {
            return Err(AppError::Command(
                "BZMPOP 命令需要至少 4 个参数".to_string(),
            ));
        }
        let timeout: f64 = self
            .extract_string(&arr[1])?
            .parse()
            .map_err(|_| AppError::Command("BZMPOP timeout 必须是数字".to_string()))?;
        let numkeys: usize = self
            .extract_string(&arr[2])?
            .parse()
            .map_err(|_| AppError::Command("BZMPOP numkeys 必须是整数".to_string()))?;
        if arr.len() < 3 + numkeys + 1 {
            return Err(AppError::Command("BZMPOP 参数数量不足".to_string()));
        }
        let keys: Vec<String> = arr[3..3 + numkeys]
            .iter()
            .map(|v| self.extract_string(v))
            .collect::<Result<Vec<_>>>()?;
        let min_or_max_str = self.extract_string(&arr[3 + numkeys])?.to_ascii_uppercase();
        let min_or_max = match min_or_max_str.as_str() {
            "MIN" => true,
            "MAX" => false,
            _ => return Err(AppError::Command("BZMPOP 必须是 MIN 或 MAX".to_string())),
        };
        let mut count = 1usize;
        let i = 4 + numkeys;
        if i < arr.len() {
            let opt = self.extract_string(&arr[i])?.to_ascii_uppercase();
            if opt == "COUNT" {
                if i + 1 >= arr.len() {
                    return Err(AppError::Command("BZMPOP COUNT 需要参数".to_string()));
                }
                count = self
                    .extract_string(&arr[i + 1])?
                    .parse()
                    .map_err(|_| AppError::Command("BZMPOP COUNT 必须是整数".to_string()))?;
            }
        }
        Ok(Command::BZMpop(timeout, keys, min_or_max, count))
    }

    /// 解析 BZPOPMIN 命令：BZPOPMIN key [key ...] timeout
    pub(crate) fn parse_bzpopmin(&self, arr: &[RespValue]) -> Result<Command> {
        if arr.len() < 3 {
            return Err(AppError::Command(
                "BZPOPMIN 命令需要至少 2 个参数".to_string(),
            ));
        }
        let timeout: f64 = self
            .extract_string(&arr[arr.len() - 1])?
            .parse()
            .map_err(|_| AppError::Command("BZPOPMIN timeout 必须是数字".to_string()))?;
        let keys: Vec<String> = arr[1..arr.len() - 1]
            .iter()
            .map(|v| self.extract_string(v))
            .collect::<Result<Vec<_>>>()?;
        Ok(Command::BZPopMin(keys, timeout))
    }

    /// 解析 BZPOPMAX 命令：BZPOPMAX key [key ...] timeout
    pub(crate) fn parse_bzpopmax(&self, arr: &[RespValue]) -> Result<Command> {
        if arr.len() < 3 {
            return Err(AppError::Command(
                "BZPOPMAX 命令需要至少 2 个参数".to_string(),
            ));
        }
        let timeout: f64 = self
            .extract_string(&arr[arr.len() - 1])?
            .parse()
            .map_err(|_| AppError::Command("BZPOPMAX timeout 必须是数字".to_string()))?;
        let keys: Vec<String> = arr[1..arr.len() - 1]
            .iter()
            .map(|v| self.extract_string(v))
            .collect::<Result<Vec<_>>>()?;
        Ok(Command::BZPopMax(keys, timeout))
    }

    /// 解析 ZREVRANGEBYSCORE 命令：ZREVRANGEBYSCORE key max min [WITHSCORES] [LIMIT offset count]
    pub(crate) fn parse_zrevrangebyscore(&self, arr: &[RespValue]) -> Result<Command> {
        if arr.len() < 4 {
            return Err(AppError::Command(
                "ZREVRANGEBYSCORE 命令需要至少 4 个参数".to_string(),
            ));
        }
        let key = self.extract_string(&arr[1])?;
        let max: f64 = self
            .extract_string(&arr[2])?
            .parse()
            .map_err(|_| AppError::Command("ZREVRANGEBYSCORE max 必须是数字".to_string()))?;
        let min: f64 = self
            .extract_string(&arr[3])?
            .parse()
            .map_err(|_| AppError::Command("ZREVRANGEBYSCORE min 必须是数字".to_string()))?;
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
                        return Err(AppError::Command(
                            "ZREVRANGEBYSCORE LIMIT 需要 offset count".to_string(),
                        ));
                    }
                    limit_offset = self
                        .extract_string(&arr[i + 1])?
                        .parse()
                        .map_err(|_| AppError::Command("LIMIT offset 必须是整数".to_string()))?;
                    limit_count = self
                        .extract_string(&arr[i + 2])?
                        .parse()
                        .map_err(|_| AppError::Command("LIMIT count 必须是整数".to_string()))?;
                    i += 3;
                }
                _ => {
                    return Err(AppError::Command(format!(
                        "ZREVRANGEBYSCORE 不支持的选项: {}",
                        opt
                    )));
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
            return Err(AppError::Command(
                "ZREVRANGEBYLEX 命令需要至少 4 个参数".to_string(),
            ));
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
                    .map_err(|_| AppError::Command("LIMIT offset 必须是整数".to_string()))?;
                limit_count = self
                    .extract_string(&arr[6])?
                    .parse()
                    .map_err(|_| AppError::Command("LIMIT count 必须是整数".to_string()))?;
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
            return Err(AppError::Command(
                "ZMSCORE 命令需要至少 2 个参数".to_string(),
            ));
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
            return Err(AppError::Command("ZLEXCOUNT 命令需要 3 个参数".to_string()));
        }
        let key = self.extract_string(&arr[1])?;
        let min = self.extract_string(&arr[2])?;
        let max = self.extract_string(&arr[3])?;
        Ok(Command::ZLexCount(key, min, max))
    }
}
