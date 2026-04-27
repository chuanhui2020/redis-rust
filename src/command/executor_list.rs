//! List 命令执行器
use super::*;

use super::executor::CommandExecutor;
use crate::error::Result;
use crate::protocol::RespValue;

/// 执行 LMPOP 命令
///
/// Redis 语法: LMPOP numkeys key [key ...] LEFT|RIGHT [COUNT count]
///
/// # 参数
/// - `executor` - 命令执行器
///
/// # 返回值
/// - `Ok(RespValue::...)` - 执行成功
/// - `Err(AppError::...)` - 执行失败（键不存在、类型错误等）
pub(crate) fn execute_lmpop(
    executor: &CommandExecutor,
    keys: Vec<String>,
    left: bool,
    count: usize,
) -> Result<RespValue> {
    match executor.storage.lmpop(&keys, left, count)? {
        Some((key, values)) => {
            let mut arr = Vec::new();
            arr.push(RespValue::BulkString(Some(Bytes::from(key))));
            let vals: Vec<RespValue> = values
                .into_iter()
                .map(|v| RespValue::BulkString(Some(v)))
                .collect();
            arr.push(RespValue::Array(vals));
            Ok(RespValue::Array(arr))
        }
        None => Ok(RespValue::BulkString(None)),
    }
}

/// 执行 L_PUSH 命令
///
/// Redis 语法: LPUSH key value [value ...]
///
/// # 参数
/// - `executor` - 命令执行器
///
/// # 返回值
/// - `Ok(RespValue::...)` - 执行成功
/// - `Err(AppError::...)` - 执行失败（键不存在、类型错误等）
pub(crate) fn execute_l_push(
    executor: &CommandExecutor,
    key: String,
    values: Vec<Bytes>,
) -> Result<RespValue> {
    let len = executor.storage.lpush(&key, values)?;
    Ok(RespValue::Integer(len as i64))
}

/// 执行 R_PUSH 命令
///
/// Redis 语法: RPUSH key value [value ...]
///
/// # 参数
/// - `executor` - 命令执行器
///
/// # 返回值
/// - `Ok(RespValue::...)` - 执行成功
/// - `Err(AppError::...)` - 执行失败（键不存在、类型错误等）
pub(crate) fn execute_r_push(
    executor: &CommandExecutor,
    key: String,
    values: Vec<Bytes>,
) -> Result<RespValue> {
    let len = executor.storage.rpush(&key, values)?;
    Ok(RespValue::Integer(len as i64))
}

/// 执行 L_PUSH_X 命令
///
/// Redis 语法: LPUSHX key value [value ...]
///
/// # 参数
/// - `executor` - 命令执行器
///
/// # 返回值
/// - `Ok(RespValue::...)` - 执行成功
/// - `Err(AppError::...)` - 执行失败（键不存在、类型错误等）
pub(crate) fn execute_l_push_x(
    executor: &CommandExecutor,
    key: String,
    values: Vec<Bytes>,
) -> Result<RespValue> {
    if !executor.storage.exists(&key)? {
        return Ok(RespValue::Integer(0));
    }
    let len = executor.storage.lpush(&key, values)?;
    Ok(RespValue::Integer(len as i64))
}

/// 执行 R_PUSH_X 命令
///
/// Redis 语法: RPUSHX key value [value ...]
///
/// # 参数
/// - `executor` - 命令执行器
///
/// # 返回值
/// - `Ok(RespValue::...)` - 执行成功
/// - `Err(AppError::...)` - 执行失败（键不存在、类型错误等）
pub(crate) fn execute_r_push_x(
    executor: &CommandExecutor,
    key: String,
    values: Vec<Bytes>,
) -> Result<RespValue> {
    if !executor.storage.exists(&key)? {
        return Ok(RespValue::Integer(0));
    }
    let len = executor.storage.rpush(&key, values)?;
    Ok(RespValue::Integer(len as i64))
}

/// 执行 L_POP 命令
///
/// Redis 语法: LPOP key
///
/// # 参数
/// - `executor` - 命令执行器
///
/// # 返回值
/// - `Ok(RespValue::...)` - 执行成功
/// - `Err(AppError::...)` - 执行失败（键不存在、类型错误等）
pub(crate) fn execute_l_pop(executor: &CommandExecutor, key: String) -> Result<RespValue> {
    match executor.storage.lpop(&key)? {
        Some(value) => Ok(RespValue::BulkString(Some(value))),
        None => Ok(RespValue::BulkString(None)),
    }
}

/// 执行 R_POP 命令
///
/// Redis 语法: RPOP key
///
/// # 参数
/// - `executor` - 命令执行器
///
/// # 返回值
/// - `Ok(RespValue::...)` - 执行成功
/// - `Err(AppError::...)` - 执行失败（键不存在、类型错误等）
pub(crate) fn execute_r_pop(executor: &CommandExecutor, key: String) -> Result<RespValue> {
    match executor.storage.rpop(&key)? {
        Some(value) => Ok(RespValue::BulkString(Some(value))),
        None => Ok(RespValue::BulkString(None)),
    }
}

/// 执行 L_LEN 命令
///
/// Redis 语法: LLEN key
///
/// # 参数
/// - `executor` - 命令执行器
///
/// # 返回值
/// - `Ok(RespValue::...)` - 执行成功
/// - `Err(AppError::...)` - 执行失败（键不存在、类型错误等）
pub(crate) fn execute_l_len(executor: &CommandExecutor, key: String) -> Result<RespValue> {
    let len = executor.storage.llen(&key)?;
    Ok(RespValue::Integer(len as i64))
}

/// 执行 L_RANGE 命令
///
/// Redis 语法: LRANGE key start stop
///
/// # 参数
/// - `executor` - 命令执行器
///
/// # 返回值
/// - `Ok(RespValue::...)` - 执行成功
/// - `Err(AppError::...)` - 执行失败（键不存在、类型错误等）
pub(crate) fn execute_l_range(
    executor: &CommandExecutor,
    key: String,
    start: i64,
    stop: i64,
) -> Result<RespValue> {
    let values = executor.storage.lrange(&key, start, stop)?;
    let resp_values: Vec<RespValue> = values
        .into_iter()
        .map(|v| RespValue::BulkString(Some(v)))
        .collect();
    Ok(RespValue::Array(resp_values))
}

/// 执行 L_INDEX 命令
///
/// Redis 语法: LINDEX key index
///
/// # 参数
/// - `executor` - 命令执行器
///
/// # 返回值
/// - `Ok(RespValue::...)` - 执行成功
/// - `Err(AppError::...)` - 执行失败（键不存在、类型错误等）
pub(crate) fn execute_l_index(
    executor: &CommandExecutor,
    key: String,
    index: i64,
) -> Result<RespValue> {
    match executor.storage.lindex(&key, index)? {
        Some(value) => Ok(RespValue::BulkString(Some(value))),
        None => Ok(RespValue::BulkString(None)),
    }
}

/// 执行 L_SET 命令
///
/// Redis 语法: LSET key index value
///
/// # 参数
/// - `executor` - 命令执行器
///
/// # 返回值
/// - `Ok(RespValue::...)` - 执行成功
/// - `Err(AppError::...)` - 执行失败（键不存在、类型错误等）
pub(crate) fn execute_l_set(
    executor: &CommandExecutor,
    key: String,
    index: i64,
    value: Bytes,
) -> Result<RespValue> {
    executor.storage.lset(&key, index, value)?;
    Ok(RespValue::SimpleString("OK".to_string()))
}

/// 执行 L_INSERT 命令
///
/// Redis 语法: LINSERT key BEFORE|AFTER pivot value
///
/// # 参数
/// - `executor` - 命令执行器
///
/// # 返回值
/// - `Ok(RespValue::...)` - 执行成功
/// - `Err(AppError::...)` - 执行失败（键不存在、类型错误等）
pub(crate) fn execute_l_insert(
    executor: &CommandExecutor,
    key: String,
    pos: crate::storage::LInsertPosition,
    pivot: Bytes,
    value: Bytes,
) -> Result<RespValue> {
    let result = executor.storage.linsert(&key, pos, pivot, value)?;
    Ok(RespValue::Integer(result))
}

/// 执行 L_REM 命令
///
/// Redis 语法: LREM key count value
///
/// # 参数
/// - `executor` - 命令执行器
///
/// # 返回值
/// - `Ok(RespValue::...)` - 执行成功
/// - `Err(AppError::...)` - 执行失败（键不存在、类型错误等）
pub(crate) fn execute_l_rem(
    executor: &CommandExecutor,
    key: String,
    count: i64,
    value: Bytes,
) -> Result<RespValue> {
    let removed = executor.storage.lrem(&key, count, value)?;
    Ok(RespValue::Integer(removed))
}

/// 执行 L_TRIM 命令
///
/// Redis 语法: LTRIM key start stop
///
/// # 参数
/// - `executor` - 命令执行器
///
/// # 返回值
/// - `Ok(RespValue::...)` - 执行成功
/// - `Err(AppError::...)` - 执行失败（键不存在、类型错误等）
pub(crate) fn execute_l_trim(
    executor: &CommandExecutor,
    key: String,
    start: i64,
    stop: i64,
) -> Result<RespValue> {
    executor.storage.ltrim(&key, start, stop)?;
    Ok(RespValue::SimpleString("OK".to_string()))
}

/// 执行 L_POS 命令
///
/// Redis 语法: LPOS key element [RANK rank] [COUNT count] [MAXLEN maxlen]
///
/// # 参数
/// - `executor` - 命令执行器
///
/// # 返回值
/// - `Ok(RespValue::...)` - 执行成功
/// - `Err(AppError::...)` - 执行失败（键不存在、类型错误等）
pub(crate) fn execute_l_pos(
    executor: &CommandExecutor,
    key: String,
    value: Bytes,
    rank: i64,
    count: i64,
    maxlen: i64,
) -> Result<RespValue> {
    let positions = executor.storage.lpos(&key, value, rank, count, maxlen)?;
    if count == 0 {
        // 不指定 COUNT，返回第一个匹配位置或 nil
        if let Some(&pos) = positions.first() {
            Ok(RespValue::Integer(pos))
        } else {
            Ok(RespValue::BulkString(None))
        }
    } else {
        // 指定了 COUNT，返回位置数组
        let arr: Vec<RespValue> = positions.into_iter().map(RespValue::Integer).collect();
        Ok(RespValue::Array(arr))
    }
}

/// 执行 B_L_POP 命令
///
/// Redis 语法: BLPOP key [key ...] timeout
///
/// # 参数
/// - `executor` - 命令执行器
///
/// # 返回值
/// - `Ok(RespValue::...)` - 执行成功
/// - `Err(AppError::...)` - 执行失败（键不存在、类型错误等）
pub(crate) fn execute_b_l_pop(
    executor: &CommandExecutor,
    keys: Vec<String>,
    _timeout: f64,
) -> Result<RespValue> {
    // 非阻塞版本：直接尝试弹出（用于事务和 AOF 重放）
    for key in &keys {
        if let Ok(Some(value)) = executor.storage.lpop(key) {
            // 记录等效 LPOP 到 AOF
            let lpop_cmd = Command::LPop(key.clone());
            executor.record_applied_write(&lpop_cmd);
            return Ok(RespValue::Array(vec![
                RespValue::BulkString(Some(bytes::Bytes::from(key.clone()))),
                RespValue::BulkString(Some(value)),
            ]));
        }
    }
    Ok(RespValue::BulkString(None))
}

/// 执行 B_R_POP 命令
///
/// Redis 语法: BRPOP key [key ...] timeout
///
/// # 参数
/// - `executor` - 命令执行器
///
/// # 返回值
/// - `Ok(RespValue::...)` - 执行成功
/// - `Err(AppError::...)` - 执行失败（键不存在、类型错误等）
pub(crate) fn execute_b_r_pop(
    executor: &CommandExecutor,
    keys: Vec<String>,
    _timeout: f64,
) -> Result<RespValue> {
    // 非阻塞版本：直接尝试弹出（用于事务和 AOF 重放）
    for key in &keys {
        if let Ok(Some(value)) = executor.storage.rpop(key) {
            // 记录等效 RPOP 到 AOF
            let rpop_cmd = Command::RPop(key.clone());
            executor.record_applied_write(&rpop_cmd);
            return Ok(RespValue::Array(vec![
                RespValue::BulkString(Some(bytes::Bytes::from(key.clone()))),
                RespValue::BulkString(Some(value)),
            ]));
        }
    }
    Ok(RespValue::BulkString(None))
}

/// 执行 B_LMPOP 命令
///
/// Redis 语法: BLMPOP timeout numkeys key [key ...] LEFT|RIGHT [COUNT count]
///
/// # 参数
/// - `executor` - 命令执行器
///
/// # 返回值
/// - `Ok(RespValue::...)` - 执行成功
/// - `Err(AppError::...)` - 执行失败（键不存在、类型错误等）
pub(crate) fn execute_b_lmpop(
    executor: &CommandExecutor,
    keys: Vec<String>,
    left: bool,
    count: usize,
    _timeout: f64,
) -> Result<RespValue> {
    // 非阻塞版本：直接尝试弹出（用于事务和 AOF 重放）
    if let Ok(Some((key, values))) = executor.storage.lmpop(&keys, left, count) {
        executor.record_applied_write(&Command::Lmpop(keys.clone(), left, count));
        let mut arr = Vec::new();
        arr.push(RespValue::BulkString(Some(bytes::Bytes::from(key))));
        let vals: Vec<RespValue> = values
            .into_iter()
            .map(|v| RespValue::BulkString(Some(v)))
            .collect();
        arr.push(RespValue::Array(vals));
        Ok(RespValue::Array(arr))
    } else {
        Ok(RespValue::BulkString(None))
    }
}
