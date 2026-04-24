//! Sorted Set 命令执行器
use super::*;

use crate::error::Result;
use crate::protocol::RespValue;
use super::executor::CommandExecutor;

/// 执行 Z_ADD 命令
///
/// Redis 语法: ZADD key score member [score member ...]
///
/// # 参数
/// - `executor` - 命令执行器
///
/// # 返回值
/// - `Ok(RespValue::...)` - 执行成功
/// - `Err(AppError::...)` - 执行失败（键不存在、类型错误等）
pub(crate) fn execute_z_add(executor: &CommandExecutor, key: String, pairs: Vec<(f64, String)>) -> Result<RespValue> {
                let count = executor.storage.zadd(&key, pairs)?;
                Ok(RespValue::Integer(count))
}

/// 执行 Z_REM 命令
///
/// Redis 语法: ZREM key member [member ...]
///
/// # 参数
/// - `executor` - 命令执行器
///
/// # 返回值
/// - `Ok(RespValue::...)` - 执行成功
/// - `Err(AppError::...)` - 执行失败（键不存在、类型错误等）
pub(crate) fn execute_z_rem(executor: &CommandExecutor, key: String, members: Vec<String>) -> Result<RespValue> {
                let count = executor.storage.zrem(&key, &members)?;
                Ok(RespValue::Integer(count))
}

/// 执行 Z_SCORE 命令
///
/// Redis 语法: ZSCORE key member
///
/// # 参数
/// - `executor` - 命令执行器
///
/// # 返回值
/// - `Ok(RespValue::...)` - 执行成功
/// - `Err(AppError::...)` - 执行失败（键不存在、类型错误等）
pub(crate) fn execute_z_score(executor: &CommandExecutor, key: String, member: String) -> Result<RespValue> {
                match executor.storage.zscore(&key, &member)? {
                    Some(score) => {
                        Ok(RespValue::BulkString(Some(Bytes::from(
                            format!("{}", score),
                        ))))
                    }
                    None => Ok(RespValue::BulkString(None)),
                }
}

/// 执行 Z_RANK 命令
///
/// Redis 语法: ZRANK key member
///
/// # 参数
/// - `executor` - 命令执行器
///
/// # 返回值
/// - `Ok(RespValue::...)` - 执行成功
/// - `Err(AppError::...)` - 执行失败（键不存在、类型错误等）
pub(crate) fn execute_z_rank(executor: &CommandExecutor, key: String, member: String) -> Result<RespValue> {
                match executor.storage.zrank(&key, &member)? {
                    Some(rank) => Ok(RespValue::Integer(rank as i64)),
                    None => Ok(RespValue::BulkString(None)),
                }
}

/// 执行 Z_RANGE 命令
///
/// Redis 语法: ZRANGE key start stop [WITHSCORES]
///
/// # 参数
/// - `executor` - 命令执行器
///
/// # 返回值
/// - `Ok(RespValue::...)` - 执行成功
/// - `Err(AppError::...)` - 执行失败（键不存在、类型错误等）
pub(crate) fn execute_z_range(executor: &CommandExecutor, key: String, start: isize, stop: isize, with_scores: bool) -> Result<RespValue> {
                let pairs = executor.storage.zrange(&key, start, stop, with_scores)?;
                let mut resp_values = Vec::new();
                for (member, score) in pairs {
                    resp_values.push(RespValue::BulkString(Some(Bytes::from(member))));
                    if with_scores {
                        resp_values.push(RespValue::BulkString(Some(Bytes::from(
                            format!("{}", score),
                        ))));
                    }
                }
                Ok(RespValue::Array(resp_values))
}

/// 执行 Z_RANGE_BY_SCORE 命令
///
/// Redis 语法: ZRANGEBYSCORE key min max [WITHSCORES]
///
/// # 参数
/// - `executor` - 命令执行器
///
/// # 返回值
/// - `Ok(RespValue::...)` - 执行成功
/// - `Err(AppError::...)` - 执行失败（键不存在、类型错误等）
pub(crate) fn execute_z_range_by_score(executor: &CommandExecutor, key: String, min: f64, max: f64, with_scores: bool) -> Result<RespValue> {
                let pairs = executor.storage.zrangebyscore(&key, min, max, with_scores)?;
                let mut resp_values = Vec::new();
                for (member, score) in pairs {
                    resp_values.push(RespValue::BulkString(Some(Bytes::from(member))));
                    if with_scores {
                        resp_values.push(RespValue::BulkString(Some(Bytes::from(
                            format!("{}", score),
                        ))));
                    }
                }
                Ok(RespValue::Array(resp_values))
}

/// 执行 Z_CARD 命令
///
/// Redis 语法: ZCARD key
///
/// # 参数
/// - `executor` - 命令执行器
///
/// # 返回值
/// - `Ok(RespValue::...)` - 执行成功
/// - `Err(AppError::...)` - 执行失败（键不存在、类型错误等）
pub(crate) fn execute_z_card(executor: &CommandExecutor, key: String) -> Result<RespValue> {
                let len = executor.storage.zcard(&key)?;
                Ok(RespValue::Integer(len as i64))
}

/// 执行 Z_REV_RANGE 命令
///
/// Redis 语法: ZREVRANGE key start stop [WITHSCORES]
///
/// # 参数
/// - `executor` - 命令执行器
///
/// # 返回值
/// - `Ok(RespValue::...)` - 执行成功
/// - `Err(AppError::...)` - 执行失败（键不存在、类型错误等）
pub(crate) fn execute_z_rev_range(executor: &CommandExecutor, key: String, start: isize, stop: isize, with_scores: bool) -> Result<RespValue> {
                let pairs = executor.storage.zrevrange(&key, start, stop, with_scores)?;
                let mut resp_values = Vec::new();
                for (member, score) in pairs {
                    resp_values.push(RespValue::BulkString(Some(Bytes::from(member))));
                    if with_scores {
                        resp_values.push(RespValue::BulkString(Some(Bytes::from(
                            format!("{}", score),
                        ))));
                    }
                }
                Ok(RespValue::Array(resp_values))
}

/// 执行 Z_REV_RANK 命令
///
/// Redis 语法: ZREVRANK key member
///
/// # 参数
/// - `executor` - 命令执行器
///
/// # 返回值
/// - `Ok(RespValue::...)` - 执行成功
/// - `Err(AppError::...)` - 执行失败（键不存在、类型错误等）
pub(crate) fn execute_z_rev_rank(executor: &CommandExecutor, key: String, member: String) -> Result<RespValue> {
                match executor.storage.zrevrank(&key, &member)? {
                    Some(rank) => Ok(RespValue::Integer(rank as i64)),
                    None => Ok(RespValue::BulkString(None)),
                }
}

/// 执行 Z_INCR_BY 命令
///
/// Redis 语法: ZINCRBY key increment member
///
/// # 参数
/// - `executor` - 命令执行器
///
/// # 返回值
/// - `Ok(RespValue::...)` - 执行成功
/// - `Err(AppError::...)` - 执行失败（键不存在、类型错误等）
pub(crate) fn execute_z_incr_by(executor: &CommandExecutor, key: String, increment: f64, member: String) -> Result<RespValue> {
                let new_score = executor.storage.zincrby(&key, increment, member)?;
                Ok(RespValue::BulkString(Some(Bytes::from(new_score))))
}

/// 执行 Z_COUNT 命令
///
/// Redis 语法: ZCOUNT key min max
///
/// # 参数
/// - `executor` - 命令执行器
///
/// # 返回值
/// - `Ok(RespValue::...)` - 执行成功
/// - `Err(AppError::...)` - 执行失败（键不存在、类型错误等）
pub(crate) fn execute_z_count(executor: &CommandExecutor, key: String, min: f64, max: f64) -> Result<RespValue> {
                let count = executor.storage.zcount(&key, min, max)?;
                Ok(RespValue::Integer(count as i64))
}

/// 执行 Z_POP_MIN 命令
///
/// Redis 语法: ZPOPMIN key [count]
///
/// # 参数
/// - `executor` - 命令执行器
///
/// # 返回值
/// - `Ok(RespValue::...)` - 执行成功
/// - `Err(AppError::...)` - 执行失败（键不存在、类型错误等）
pub(crate) fn execute_z_pop_min(executor: &CommandExecutor, key: String, count: usize) -> Result<RespValue> {
                let pairs = executor.storage.zpopmin(&key, count)?;
                let mut resp_values = Vec::new();
                for (member, score) in pairs {
                    resp_values.push(RespValue::BulkString(Some(Bytes::from(member))));
                    resp_values.push(RespValue::BulkString(Some(Bytes::from(
                        format!("{}", score),
                    ))));
                }
                Ok(RespValue::Array(resp_values))
}

/// 执行 Z_POP_MAX 命令
///
/// Redis 语法: ZPOPMAX key [count]
///
/// # 参数
/// - `executor` - 命令执行器
///
/// # 返回值
/// - `Ok(RespValue::...)` - 执行成功
/// - `Err(AppError::...)` - 执行失败（键不存在、类型错误等）
pub(crate) fn execute_z_pop_max(executor: &CommandExecutor, key: String, count: usize) -> Result<RespValue> {
                let pairs = executor.storage.zpopmax(&key, count)?;
                let mut resp_values = Vec::new();
                for (member, score) in pairs {
                    resp_values.push(RespValue::BulkString(Some(Bytes::from(member))));
                    resp_values.push(RespValue::BulkString(Some(Bytes::from(
                        format!("{}", score),
                    ))));
                }
                Ok(RespValue::Array(resp_values))
}

/// 执行 Z_UNION_STORE 命令
///
/// Redis 语法: ZUNIONSTORE destination numkeys key [key ...] [WEIGHTS weight ...] [AGGREGATE SUM|MIN|MAX]
///
/// # 参数
/// - `executor` - 命令执行器
///
/// # 返回值
/// - `Ok(RespValue::...)` - 执行成功
/// - `Err(AppError::...)` - 执行失败（键不存在、类型错误等）
pub(crate) fn execute_z_union_store(executor: &CommandExecutor, destination: String, keys: Vec<String>, weights: Vec<f64>, aggregate: String) -> Result<RespValue> {
                let weights_slice = if weights.is_empty() { None } else { Some(weights.as_slice()) };
                let count = executor.storage.zunionstore(&destination, &keys, weights_slice, &aggregate)?;
                Ok(RespValue::Integer(count as i64))
}

/// 执行 Z_INTER_STORE 命令
///
/// Redis 语法: ZINTERSTORE destination numkeys key [key ...] [WEIGHTS weight ...] [AGGREGATE SUM|MIN|MAX]
///
/// # 参数
/// - `executor` - 命令执行器
///
/// # 返回值
/// - `Ok(RespValue::...)` - 执行成功
/// - `Err(AppError::...)` - 执行失败（键不存在、类型错误等）
pub(crate) fn execute_z_inter_store(executor: &CommandExecutor, destination: String, keys: Vec<String>, weights: Vec<f64>, aggregate: String) -> Result<RespValue> {
                let weights_slice = if weights.is_empty() { None } else { Some(weights.as_slice()) };
                let count = executor.storage.zinterstore(&destination, &keys, weights_slice, &aggregate)?;
                Ok(RespValue::Integer(count as i64))
}

/// 执行 Z_SCAN 命令
///
/// Redis 语法: ZSCAN key cursor [MATCH pattern] [COUNT count]
///
/// # 参数
/// - `executor` - 命令执行器
///
/// # 返回值
/// - `Ok(RespValue::...)` - 执行成功
/// - `Err(AppError::...)` - 执行失败（键不存在、类型错误等）
pub(crate) fn execute_z_scan(executor: &CommandExecutor, key: String, cursor: usize, pattern: String, count: usize) -> Result<RespValue> {
                let (next_cursor, items) = executor.storage.zscan(&key, cursor, &pattern, count)?;
                let mut resp_values = Vec::new();
                resp_values.push(RespValue::BulkString(Some(Bytes::from(
                    next_cursor.to_string(),
                ))));
                let mut item_values = Vec::new();
                for (member, score) in items {
                    item_values.push(RespValue::BulkString(Some(Bytes::from(member))));
                    item_values.push(RespValue::BulkString(Some(Bytes::from(
                        format!("{}", score),
                    ))));
                }
                resp_values.push(RespValue::Array(item_values));
                Ok(RespValue::Array(resp_values))
}

/// 执行 Z_RANGE_BY_LEX 命令
///
/// Redis 语法: ZRANGEBYLEX key min max [LIMIT offset count]
///
/// # 参数
/// - `executor` - 命令执行器
///
/// # 返回值
/// - `Ok(RespValue::...)` - 执行成功
/// - `Err(AppError::...)` - 执行失败（键不存在、类型错误等）
pub(crate) fn execute_z_range_by_lex(executor: &CommandExecutor, key: String, min: String, max: String) -> Result<RespValue> {
                let members = executor.storage.zrangebylex(&key, &min, &max)?;
                let resp_values: Vec<RespValue> = members
                    .into_iter()
                    .map(|m| RespValue::BulkString(Some(Bytes::from(m))))
                    .collect();
                Ok(RespValue::Array(resp_values))
}

/// 执行 Z_INTER_CARD 命令
///
/// Redis 语法: ZINTERCARD numkeys key [key ...] [LIMIT limit]
///
/// # 参数
/// - `executor` - 命令执行器
///
/// # 返回值
/// - `Ok(RespValue::...)` - 执行成功
/// - `Err(AppError::...)` - 执行失败（键不存在、类型错误等）
pub(crate) fn execute_z_inter_card(executor: &CommandExecutor, keys: Vec<String>, limit: usize) -> Result<RespValue> {
                let count = executor.storage.zintercard(&keys, limit)?;
                Ok(RespValue::Integer(count as i64))
}

/// 执行 Z_REM_RANGE_BY_LEX 命令
///
/// Redis 语法: ZREMRANGEBYLEX key min max
///
/// # 参数
/// - `executor` - 命令执行器
///
/// # 返回值
/// - `Ok(RespValue::...)` - 执行成功
/// - `Err(AppError::...)` - 执行失败（键不存在、类型错误等）
pub(crate) fn execute_z_rem_range_by_lex(executor: &CommandExecutor, key: String, min: String, max: String) -> Result<RespValue> {
                let count = executor.storage.zremrangebylex(&key, &min, &max)?;
                Ok(RespValue::Integer(count as i64))
}

/// 执行 Z_REM_RANGE_BY_RANK 命令
///
/// Redis 语法: ZREMRANGEBYRANK key start stop
///
/// # 参数
/// - `executor` - 命令执行器
///
/// # 返回值
/// - `Ok(RespValue::...)` - 执行成功
/// - `Err(AppError::...)` - 执行失败（键不存在、类型错误等）
pub(crate) fn execute_z_rem_range_by_rank(executor: &CommandExecutor, key: String, start: isize, stop: isize) -> Result<RespValue> {
                let count = executor.storage.zremrangebyrank(&key, start, stop)?;
                Ok(RespValue::Integer(count as i64))
}

/// 执行 Z_REM_RANGE_BY_SCORE 命令
///
/// Redis 语法: ZREMRANGEBYSCORE key min max
///
/// # 参数
/// - `executor` - 命令执行器
///
/// # 返回值
/// - `Ok(RespValue::...)` - 执行成功
/// - `Err(AppError::...)` - 执行失败（键不存在、类型错误等）
pub(crate) fn execute_z_rem_range_by_score(executor: &CommandExecutor, key: String, min: f64, max: f64) -> Result<RespValue> {
                let count = executor.storage.zremrangebyscore(&key, min, max)?;
                Ok(RespValue::Integer(count as i64))
}

/// 执行 Z_RAND_MEMBER 命令
///
/// Redis 语法: ZRANDMEMBER key [count [WITHSCORES]]
///
/// # 参数
/// - `executor` - 命令执行器
///
/// # 返回值
/// - `Ok(RespValue::...)` - 执行成功
/// - `Err(AppError::...)` - 执行失败（键不存在、类型错误等）
pub(crate) fn execute_z_rand_member(executor: &CommandExecutor, key: String, count: i64, with_scores: bool) -> Result<RespValue> {
                let pairs = executor.storage.zrandmember(&key, count, with_scores)?;
                let mut resp_values = Vec::new();
                for (member, score) in pairs {
                    resp_values.push(RespValue::BulkString(Some(Bytes::from(member))));
                    if with_scores {
                        resp_values.push(RespValue::BulkString(Some(Bytes::from(format!("{}", score)))));
                    }
                }
                Ok(RespValue::Array(resp_values))
}

/// 执行 Z_DIFF 命令
///
/// Redis 语法: ZDIFF key [key ...] [WITHSCORES]
///
/// # 参数
/// - `executor` - 命令执行器
///
/// # 返回值
/// - `Ok(RespValue::...)` - 执行成功
/// - `Err(AppError::...)` - 执行失败（键不存在、类型错误等）
pub(crate) fn execute_z_diff(executor: &CommandExecutor, keys: Vec<String>, with_scores: bool) -> Result<RespValue> {
                let pairs = executor.storage.zdiff(&keys, with_scores)?;
                let mut resp_values = Vec::new();
                for (member, score) in pairs {
                    resp_values.push(RespValue::BulkString(Some(Bytes::from(member))));
                    if with_scores {
                        resp_values.push(RespValue::BulkString(Some(Bytes::from(format!("{}", score)))));
                    }
                }
                Ok(RespValue::Array(resp_values))
}

/// 执行 Z_DIFF_STORE 命令
///
/// Redis 语法: ZDIFFSTORE destination numkeys key [key ...]
///
/// # 参数
/// - `executor` - 命令执行器
///
/// # 返回值
/// - `Ok(RespValue::...)` - 执行成功
/// - `Err(AppError::...)` - 执行失败（键不存在、类型错误等）
pub(crate) fn execute_z_diff_store(executor: &CommandExecutor, destination: String, keys: Vec<String>) -> Result<RespValue> {
                let count = executor.storage.zdiffstore(&destination, &keys)?;
                Ok(RespValue::Integer(count as i64))
}

/// 执行 Z_INTER 命令
///
/// Redis 语法: ZINTER numkeys key [key ...] [WEIGHTS weight ...] [AGGREGATE SUM|MIN|MAX] [WITHSCORES]
///
/// # 参数
/// - `executor` - 命令执行器
///
/// # 返回值
/// - `Ok(RespValue::...)` - 执行成功
/// - `Err(AppError::...)` - 执行失败（键不存在、类型错误等）
pub(crate) fn execute_z_inter(executor: &CommandExecutor, keys: Vec<String>, weights: Vec<f64>, aggregate: String, with_scores: bool) -> Result<RespValue> {
                let weights_slice = if weights.is_empty() { None } else { Some(weights.as_slice()) };
                let pairs = executor.storage.zinter(&keys, weights_slice, &aggregate, with_scores)?;
                let mut resp_values = Vec::new();
                for (member, score) in pairs {
                    resp_values.push(RespValue::BulkString(Some(Bytes::from(member))));
                    if with_scores {
                        resp_values.push(RespValue::BulkString(Some(Bytes::from(format!("{}", score)))));
                    }
                }
                Ok(RespValue::Array(resp_values))
}

/// 执行 Z_UNION 命令
///
/// Redis 语法: ZUNION numkeys key [key ...] [WEIGHTS weight ...] [AGGREGATE SUM|MIN|MAX] [WITHSCORES]
///
/// # 参数
/// - `executor` - 命令执行器
///
/// # 返回值
/// - `Ok(RespValue::...)` - 执行成功
/// - `Err(AppError::...)` - 执行失败（键不存在、类型错误等）
pub(crate) fn execute_z_union(executor: &CommandExecutor, keys: Vec<String>, weights: Vec<f64>, aggregate: String, with_scores: bool) -> Result<RespValue> {
                let weights_slice = if weights.is_empty() { None } else { Some(weights.as_slice()) };
                let pairs = executor.storage.zunion(&keys, weights_slice, &aggregate, with_scores)?;
                let mut resp_values = Vec::new();
                for (member, score) in pairs {
                    resp_values.push(RespValue::BulkString(Some(Bytes::from(member))));
                    if with_scores {
                        resp_values.push(RespValue::BulkString(Some(Bytes::from(format!("{}", score)))));
                    }
                }
                Ok(RespValue::Array(resp_values))
}

/// 执行 Z_RANGE_STORE 命令
///
/// Redis 语法: ZRANGESTORE dst src min max [BYSCORE|BYLEX] [REV] [LIMIT offset count]
///
/// # 参数
/// - `executor` - 命令执行器
///
/// # 返回值
/// - `Ok(RespValue::...)` - 执行成功
/// - `Err(AppError::...)` - 执行失败（键不存在、类型错误等）
pub(crate) fn execute_z_range_store(executor: &CommandExecutor, dst: String, src: String, min: String, max: String, by_score: bool, by_lex: bool, rev: bool, limit_offset: usize, limit_count: usize) -> Result<RespValue> {
                let count = executor.storage.zrangestore(&dst, &src, &min, &max, by_score, by_lex, rev, limit_offset, limit_count)?;
                Ok(RespValue::Integer(count as i64))
}

/// 执行 Z_MPOP 命令
///
/// Redis 语法: ZMPOP numkeys key [key ...] MIN|MAX [COUNT count]
///
/// # 参数
/// - `executor` - 命令执行器
///
/// # 返回值
/// - `Ok(RespValue::...)` - 执行成功
/// - `Err(AppError::...)` - 执行失败（键不存在、类型错误等）
pub(crate) fn execute_z_mpop(executor: &CommandExecutor, keys: Vec<String>, min_or_max: bool, count: usize) -> Result<RespValue> {
                match executor.storage.zmpop(&keys, min_or_max, count)? {
                    Some((key, pairs)) => {
                        let mut pair_values = Vec::new();
                        for (member, score) in pairs {
                            pair_values.push(RespValue::BulkString(Some(Bytes::from(member))));
                            pair_values.push(RespValue::BulkString(Some(Bytes::from(format!("{}", score)))));
                        }
                        let resp = RespValue::Array(vec![
                            RespValue::BulkString(Some(Bytes::from(key))),
                            RespValue::Array(pair_values),
                        ]);
                        Ok(resp)
                    }
                    None => Ok(RespValue::BulkString(None)),
                }
}

/// 执行 Z_REV_RANGE_BY_SCORE 命令
///
/// Redis 语法: ZREVRANGEBYSCORE key max min [WITHSCORES] [LIMIT offset count]
///
/// # 参数
/// - `executor` - 命令执行器
///
/// # 返回值
/// - `Ok(RespValue::...)` - 执行成功
/// - `Err(AppError::...)` - 执行失败（键不存在、类型错误等）
pub(crate) fn execute_z_rev_range_by_score(executor: &CommandExecutor, key: String, max: f64, min: f64, with_scores: bool, limit_offset: usize, limit_count: usize) -> Result<RespValue> {
                let pairs = executor.storage.zrevrangebyscore(&key, max, min, with_scores, limit_offset, limit_count)?;
                let mut resp_values = Vec::new();
                for (member, score) in pairs {
                    resp_values.push(RespValue::BulkString(Some(Bytes::from(member))));
                    if with_scores {
                        resp_values.push(RespValue::BulkString(Some(Bytes::from(format!("{}", score)))));
                    }
                }
                Ok(RespValue::Array(resp_values))
}

/// 执行 Z_REV_RANGE_BY_LEX 命令
///
/// Redis 语法: ZREVRANGEBYLEX key max min [LIMIT offset count]
///
/// # 参数
/// - `executor` - 命令执行器
///
/// # 返回值
/// - `Ok(RespValue::...)` - 执行成功
/// - `Err(AppError::...)` - 执行失败（键不存在、类型错误等）
pub(crate) fn execute_z_rev_range_by_lex(executor: &CommandExecutor, key: String, max: String, min: String, limit_offset: usize, limit_count: usize) -> Result<RespValue> {
                let members = executor.storage.zrevrangebylex(&key, &max, &min, limit_offset, limit_count)?;
                let resp_values: Vec<RespValue> = members
                    .into_iter()
                    .map(|m| RespValue::BulkString(Some(Bytes::from(m))))
                    .collect();
                Ok(RespValue::Array(resp_values))
}

/// 执行 Z_M_SCORE 命令
///
/// Redis 语法: ZMSCORE key member [member ...]
///
/// # 参数
/// - `executor` - 命令执行器
///
/// # 返回值
/// - `Ok(RespValue::...)` - 执行成功
/// - `Err(AppError::...)` - 执行失败（键不存在、类型错误等）
pub(crate) fn execute_z_m_score(executor: &CommandExecutor, key: String, members: Vec<String>) -> Result<RespValue> {
                let scores = executor.storage.zmscore(&key, &members)?;
                let resp_values: Vec<RespValue> = scores
                    .into_iter()
                    .map(|s| match s {
                        Some(score) => RespValue::BulkString(Some(Bytes::from(format!("{}", score)))),
                        None => RespValue::BulkString(None),
                    })
                    .collect();
                Ok(RespValue::Array(resp_values))
}

/// 执行 Z_LEX_COUNT 命令
///
/// Redis 语法: ZLEXCOUNT key min max
///
/// # 参数
/// - `executor` - 命令执行器
///
/// # 返回值
/// - `Ok(RespValue::...)` - 执行成功
/// - `Err(AppError::...)` - 执行失败（键不存在、类型错误等）
pub(crate) fn execute_z_lex_count(executor: &CommandExecutor, key: String, min: String, max: String) -> Result<RespValue> {
                let count = executor.storage.zlexcount(&key, &min, &max)?;
                Ok(RespValue::Integer(count as i64))
}

