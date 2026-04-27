//! Set 命令执行器
use super::*;

use super::executor::CommandExecutor;
use crate::error::Result;
use crate::protocol::RespValue;

/// 执行 S_ADD 命令
///
/// Redis 语法: SADD key member [member ...]
///
/// # 参数
/// - `executor` - 命令执行器
///
/// # 返回值
/// - `Ok(RespValue::...)` - 执行成功
/// - `Err(AppError::...)` - 执行失败（键不存在、类型错误等）
pub(crate) fn execute_s_add(
    executor: &CommandExecutor,
    key: String,
    members: Vec<Bytes>,
) -> Result<RespValue> {
    let count = executor.storage.sadd(&key, members)?;
    Ok(RespValue::Integer(count))
}

/// 执行 S_REM 命令
///
/// Redis 语法: SREM key member [member ...]
///
/// # 参数
/// - `executor` - 命令执行器
///
/// # 返回值
/// - `Ok(RespValue::...)` - 执行成功
/// - `Err(AppError::...)` - 执行失败（键不存在、类型错误等）
pub(crate) fn execute_s_rem(
    executor: &CommandExecutor,
    key: String,
    members: Vec<Bytes>,
) -> Result<RespValue> {
    let count = executor.storage.srem(&key, &members)?;
    Ok(RespValue::Integer(count))
}

/// 执行 S_MEMBERS 命令
///
/// Redis 语法: SMEMBERS key
///
/// # 参数
/// - `executor` - 命令执行器
///
/// # 返回值
/// - `Ok(RespValue::...)` - 执行成功
/// - `Err(AppError::...)` - 执行失败（键不存在、类型错误等）
pub(crate) fn execute_s_members(executor: &CommandExecutor, key: String) -> Result<RespValue> {
    let members = executor.storage.smembers(&key)?;
    let resp_values: Vec<RespValue> = members
        .into_iter()
        .map(|m| RespValue::BulkString(Some(m)))
        .collect();
    Ok(RespValue::Array(resp_values))
}

/// 执行 S_IS_MEMBER 命令
///
/// Redis 语法: SISMEMBER key member
///
/// # 参数
/// - `executor` - 命令执行器
///
/// # 返回值
/// - `Ok(RespValue::...)` - 执行成功
/// - `Err(AppError::...)` - 执行失败（键不存在、类型错误等）
pub(crate) fn execute_s_is_member(
    executor: &CommandExecutor,
    key: String,
    member: Bytes,
) -> Result<RespValue> {
    let result = if executor.storage.sismember(&key, &member)? {
        1i64
    } else {
        0i64
    };
    Ok(RespValue::Integer(result))
}

/// 执行 S_CARD 命令
///
/// Redis 语法: SCARD key
///
/// # 参数
/// - `executor` - 命令执行器
///
/// # 返回值
/// - `Ok(RespValue::...)` - 执行成功
/// - `Err(AppError::...)` - 执行失败（键不存在、类型错误等）
pub(crate) fn execute_s_card(executor: &CommandExecutor, key: String) -> Result<RespValue> {
    let len = executor.storage.scard(&key)?;
    Ok(RespValue::Integer(len as i64))
}

/// 执行 S_INTER 命令
///
/// Redis 语法: SINTER key [key ...]
///
/// # 参数
/// - `executor` - 命令执行器
///
/// # 返回值
/// - `Ok(RespValue::...)` - 执行成功
/// - `Err(AppError::...)` - 执行失败（键不存在、类型错误等）
pub(crate) fn execute_s_inter(executor: &CommandExecutor, keys: Vec<String>) -> Result<RespValue> {
    let members = executor.storage.sinter(&keys)?;
    let resp_values: Vec<RespValue> = members
        .into_iter()
        .map(|m| RespValue::BulkString(Some(m)))
        .collect();
    Ok(RespValue::Array(resp_values))
}

/// 执行 S_UNION 命令
///
/// Redis 语法: SUNION key [key ...]
///
/// # 参数
/// - `executor` - 命令执行器
///
/// # 返回值
/// - `Ok(RespValue::...)` - 执行成功
/// - `Err(AppError::...)` - 执行失败（键不存在、类型错误等）
pub(crate) fn execute_s_union(executor: &CommandExecutor, keys: Vec<String>) -> Result<RespValue> {
    let members = executor.storage.sunion(&keys)?;
    let resp_values: Vec<RespValue> = members
        .into_iter()
        .map(|m| RespValue::BulkString(Some(m)))
        .collect();
    Ok(RespValue::Array(resp_values))
}

/// 执行 S_DIFF 命令
///
/// Redis 语法: SDIFF key [key ...]
///
/// # 参数
/// - `executor` - 命令执行器
///
/// # 返回值
/// - `Ok(RespValue::...)` - 执行成功
/// - `Err(AppError::...)` - 执行失败（键不存在、类型错误等）
pub(crate) fn execute_s_diff(executor: &CommandExecutor, keys: Vec<String>) -> Result<RespValue> {
    let members = executor.storage.sdiff(&keys)?;
    let resp_values: Vec<RespValue> = members
        .into_iter()
        .map(|m| RespValue::BulkString(Some(m)))
        .collect();
    Ok(RespValue::Array(resp_values))
}

/// 执行 S_POP 命令
///
/// Redis 语法: SPOP key [count]
///
/// # 参数
/// - `executor` - 命令执行器
///
/// # 返回值
/// - `Ok(RespValue::...)` - 执行成功
/// - `Err(AppError::...)` - 执行失败（键不存在、类型错误等）
pub(crate) fn execute_s_pop(
    executor: &CommandExecutor,
    key: String,
    count: i64,
) -> Result<RespValue> {
    let members = executor.storage.spop(&key, count)?;
    if count == 1 {
        if let Some(m) = members.into_iter().next() {
            Ok(RespValue::BulkString(Some(m)))
        } else {
            Ok(RespValue::BulkString(None))
        }
    } else {
        let resp_values: Vec<RespValue> = members
            .into_iter()
            .map(|m| RespValue::BulkString(Some(m)))
            .collect();
        Ok(RespValue::Array(resp_values))
    }
}

/// 执行 S_RAND_MEMBER 命令
///
/// Redis 语法: SRANDMEMBER key [count]
///
/// # 参数
/// - `executor` - 命令执行器
///
/// # 返回值
/// - `Ok(RespValue::...)` - 执行成功
/// - `Err(AppError::...)` - 执行失败（键不存在、类型错误等）
pub(crate) fn execute_s_rand_member(
    executor: &CommandExecutor,
    key: String,
    count: i64,
) -> Result<RespValue> {
    let members = executor.storage.srandmember(&key, count)?;
    if count == 1 {
        if let Some(m) = members.into_iter().next() {
            Ok(RespValue::BulkString(Some(m)))
        } else {
            Ok(RespValue::BulkString(None))
        }
    } else {
        let resp_values: Vec<RespValue> = members
            .into_iter()
            .map(|m| RespValue::BulkString(Some(m)))
            .collect();
        Ok(RespValue::Array(resp_values))
    }
}

/// 执行 S_MOVE 命令
///
/// Redis 语法: SMOVE source destination member
///
/// # 参数
/// - `executor` - 命令执行器
///
/// # 返回值
/// - `Ok(RespValue::...)` - 执行成功
/// - `Err(AppError::...)` - 执行失败（键不存在、类型错误等）
pub(crate) fn execute_s_move(
    executor: &CommandExecutor,
    source: String,
    destination: String,
    member: Bytes,
) -> Result<RespValue> {
    let result = if executor.storage.smove(&source, &destination, member)? {
        1i64
    } else {
        0i64
    };
    Ok(RespValue::Integer(result))
}

/// 执行 S_INTER_STORE 命令
///
/// Redis 语法: SINTERSTORE destination key [key ...]
///
/// # 参数
/// - `executor` - 命令执行器
///
/// # 返回值
/// - `Ok(RespValue::...)` - 执行成功
/// - `Err(AppError::...)` - 执行失败（键不存在、类型错误等）
pub(crate) fn execute_s_inter_store(
    executor: &CommandExecutor,
    destination: String,
    keys: Vec<String>,
) -> Result<RespValue> {
    let count = executor.storage.sinterstore(&destination, &keys)?;
    Ok(RespValue::Integer(count as i64))
}

/// 执行 S_UNION_STORE 命令
///
/// Redis 语法: SUNIONSTORE destination key [key ...]
///
/// # 参数
/// - `executor` - 命令执行器
///
/// # 返回值
/// - `Ok(RespValue::...)` - 执行成功
/// - `Err(AppError::...)` - 执行失败（键不存在、类型错误等）
pub(crate) fn execute_s_union_store(
    executor: &CommandExecutor,
    destination: String,
    keys: Vec<String>,
) -> Result<RespValue> {
    let count = executor.storage.sunionstore(&destination, &keys)?;
    Ok(RespValue::Integer(count as i64))
}

/// 执行 S_DIFF_STORE 命令
///
/// Redis 语法: SDIFFSTORE destination key [key ...]
///
/// # 参数
/// - `executor` - 命令执行器
///
/// # 返回值
/// - `Ok(RespValue::...)` - 执行成功
/// - `Err(AppError::...)` - 执行失败（键不存在、类型错误等）
pub(crate) fn execute_s_diff_store(
    executor: &CommandExecutor,
    destination: String,
    keys: Vec<String>,
) -> Result<RespValue> {
    let count = executor.storage.sdiffstore(&destination, &keys)?;
    Ok(RespValue::Integer(count as i64))
}

/// 执行 S_SCAN 命令
///
/// Redis 语法: SSCAN key cursor [MATCH pattern] [COUNT count]
///
/// # 参数
/// - `executor` - 命令执行器
///
/// # 返回值
/// - `Ok(RespValue::...)` - 执行成功
/// - `Err(AppError::...)` - 执行失败（键不存在、类型错误等）
pub(crate) fn execute_s_scan(
    executor: &CommandExecutor,
    key: String,
    cursor: usize,
    pattern: String,
    count: usize,
) -> Result<RespValue> {
    let (new_cursor, members) = executor.storage.sscan(&key, cursor, &pattern, count)?;
    let mut parts: Vec<RespValue> = vec![RespValue::BulkString(Some(Bytes::from(
        new_cursor.to_string(),
    )))];
    let member_values: Vec<RespValue> = members
        .into_iter()
        .map(|m| RespValue::BulkString(Some(m)))
        .collect();
    parts.push(RespValue::Array(member_values));
    Ok(RespValue::Array(parts))
}
