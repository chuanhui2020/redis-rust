//! String 命令执行器
use super::*;

use crate::error::Result;
use crate::protocol::RespValue;
use super::executor::CommandExecutor;

/// 执行 GET 命令
///
/// Redis 语法: GET key
///
/// # 参数
/// - `executor` - 命令执行器
///
/// # 返回值
/// - `Ok(RespValue::...)` - 执行成功
/// - `Err(AppError::...)` - 执行失败（键不存在、类型错误等）
pub(crate) fn execute_get(executor: &CommandExecutor, key: String) -> Result<RespValue> {
                match executor.storage.get(&key)? {
                    Some(value) => {
                        Ok(RespValue::BulkString(Some(value)))
                    }
                    None => Ok(RespValue::BulkString(None)),
                }
}

/// 执行 SET 命令
///
/// Redis 语法: SET key value [NX|XX] [GET] [EX seconds|PX milliseconds|EXAT timestamp|PXAT ms-timestamp|KEEPTTL]
///
/// # 参数
/// - `executor` - 命令执行器
///
/// # 返回值
/// - `Ok(RespValue::...)` - 执行成功
/// - `Err(AppError::...)` - 执行失败（键不存在、类型错误等）
pub(crate) fn execute_set(executor: &CommandExecutor, key: String, value: Bytes, options: crate::storage::SetOptions) -> Result<RespValue> {
                if options.is_plain() {
                    executor.storage.set_plain(key, value)?;
                    return Ok(RespValue::SimpleString("OK".to_string()));
                }
                let (ok, old_value) = executor.storage.set_with_options(key, value, &options)?;
                if options.get {
                    Ok(RespValue::BulkString(old_value))
                } else if ok {
                    Ok(RespValue::SimpleString("OK".to_string()))
                } else {
                    Ok(RespValue::BulkString(None))
                }
}

/// 执行 SET_EX 命令
///
/// Redis 语法: SET key value EX seconds
///
/// # 参数
/// - `executor` - 命令执行器
///
/// # 返回值
/// - `Ok(RespValue::...)` - 执行成功
/// - `Err(AppError::...)` - 执行失败（键不存在、类型错误等）
pub(crate) fn execute_set_ex(executor: &CommandExecutor, key: String, value: Bytes, ttl_ms: u64) -> Result<RespValue> {
                executor.storage.set_with_ttl(key, value, ttl_ms)?;
                Ok(RespValue::SimpleString("OK".to_string()))
}

/// 执行 DEL 命令
///
/// Redis 语法: DEL key [key ...]
///
/// # 参数
/// - `executor` - 命令执行器
///
/// # 返回值
/// - `Ok(RespValue::...)` - 执行成功
/// - `Err(AppError::...)` - 执行失败（键不存在、类型错误等）
pub(crate) fn execute_del(executor: &CommandExecutor, keys: Vec<String>) -> Result<RespValue> {
                let mut count = 0i64;
                for key in keys {
                    if executor.storage.del(&key)? {
                        count += 1;
                    }
                }
                Ok(RespValue::Integer(count))
}

/// 执行 EXISTS 命令
///
/// Redis 语法: EXISTS key [key ...]
///
/// # 参数
/// - `executor` - 命令执行器
///
/// # 返回值
/// - `Ok(RespValue::...)` - 执行成功
/// - `Err(AppError::...)` - 执行失败（键不存在、类型错误等）
pub(crate) fn execute_exists(executor: &CommandExecutor, keys: Vec<String>) -> Result<RespValue> {
                let mut count = 0i64;
                for key in keys {
                    if executor.storage.exists(&key)? {
                        count += 1;
                    }
                }
                Ok(RespValue::Integer(count))
}

/// 执行 FLUSH_ALL 命令
///
/// Redis 语法: 
///
/// # 参数
/// - `executor` - 命令执行器
///
/// # 返回值
/// - `Ok(RespValue::...)` - 执行成功
/// - `Err(AppError::...)` - 执行失败（键不存在、类型错误等）
pub(crate) fn execute_flush_all(executor: &CommandExecutor) -> Result<RespValue> {
                executor.storage.flush()?;
                Ok(RespValue::SimpleString("OK".to_string()))
}

/// 执行 LCS 命令
///
/// Redis 语法: LCS key1 key2 [LEN] [IDX] [MINMATCHLEN len] [WITHMATCHLEN]
///
/// # 参数
/// - `executor` - 命令执行器
///
/// # 返回值
/// - `Ok(RespValue::...)` - 执行成功
/// - `Err(AppError::...)` - 执行失败（键不存在、类型错误等）
pub(crate) fn execute_lcs(executor: &CommandExecutor, key1: String, key2: String, len: bool, idx: bool, _minmatchlen: usize, withmatchlen: bool) -> Result<RespValue> {
                let lcs_str = executor.storage.lcs(&key1, &key2)?;
                if len {
                    // LEN 模式：只返回长度
                    let length = lcs_str.as_ref().map(|s| s.len()).unwrap_or(0);
                    Ok(RespValue::Integer(length as i64))
                } else if idx {
                    // IDX 模式：返回匹配位置
                    // 简化实现：返回整个匹配的起点和终点
                    let mut arr = Vec::new();
                    if let Some(s) = lcs_str {
                        let v1 = match executor.storage.get(&key1)? {
                            Some(b) => String::from_utf8_lossy(&b).to_string(),
                            None => String::new(),
                        };
                        let v2 = match executor.storage.get(&key2)? {
                            Some(b) => String::from_utf8_lossy(&b).to_string(),
                            None => String::new(),
                        };
                        // 找到 LCS 在 v1 和 v2 中的位置
                        if !s.is_empty() && !v1.is_empty() && !v2.is_empty() {
                            let pos1 = v1.find(&s).unwrap_or(0) as i64;
                            let pos2 = v2.find(&s).unwrap_or(0) as i64;
                            let match_len = s.len() as i64;
                            let mut match_arr = Vec::new();
                            let m1 = vec![
                                RespValue::Integer(pos1),
                                RespValue::Integer(pos1 + match_len - 1),
                            ];
                            let m2 = vec![
                                RespValue::Integer(pos2),
                                RespValue::Integer(pos2 + match_len - 1),
                            ];
                            match_arr.push(RespValue::Array(m1));
                            match_arr.push(RespValue::Array(m2));
                            if withmatchlen {
                                match_arr.push(RespValue::Integer(match_len));
                            }
                            arr.push(RespValue::Array(match_arr));
                        }
                        arr.push(RespValue::Integer(v1.len() as i64));
                        arr.push(RespValue::Integer(v2.len() as i64));
                    } else {
                        arr.push(RespValue::Array(vec![]));
                        arr.push(RespValue::Integer(0));
                        arr.push(RespValue::Integer(0));
                    }
                    Ok(RespValue::Array(arr))
                } else {
                    // 默认模式：返回最长公共子串
                    match lcs_str {
                        Some(s) => Ok(RespValue::BulkString(Some(Bytes::from(s)))),
                        None => Ok(RespValue::BulkString(None)),
                    }
                }
}

/// 执行 M_GET 命令
///
/// Redis 语法: MGET key [key ...]
///
/// # 参数
/// - `executor` - 命令执行器
///
/// # 返回值
/// - `Ok(RespValue::...)` - 执行成功
/// - `Err(AppError::...)` - 执行失败（键不存在、类型错误等）
pub(crate) fn execute_m_get(executor: &CommandExecutor, keys: Vec<String>) -> Result<RespValue> {
                let values = executor.storage.mget(&keys)?;
                let resp_values: Vec<RespValue> = values
                    .into_iter()
                    .map(RespValue::BulkString)
                    .collect();
                Ok(RespValue::Array(resp_values))
}

/// 执行 M_SET 命令
///
/// Redis 语法: MSET key value [key value ...]
///
/// # 参数
/// - `executor` - 命令执行器
///
/// # 返回值
/// - `Ok(RespValue::...)` - 执行成功
/// - `Err(AppError::...)` - 执行失败（键不存在、类型错误等）
pub(crate) fn execute_m_set(executor: &CommandExecutor, pairs: Vec<(String, Bytes)>) -> Result<RespValue> {
                executor.storage.mset(&pairs)?;
                Ok(RespValue::SimpleString("OK".to_string()))
}

/// 执行 INCR 命令
///
/// Redis 语法: INCR key
///
/// # 参数
/// - `executor` - 命令执行器
///
/// # 返回值
/// - `Ok(RespValue::...)` - 执行成功
/// - `Err(AppError::...)` - 执行失败（键不存在、类型错误等）
pub(crate) fn execute_incr(executor: &CommandExecutor, key: String) -> Result<RespValue> {
                let new_val = executor.storage.incr(&key)?;
                Ok(RespValue::Integer(new_val))
}

/// 执行 DECR 命令
///
/// Redis 语法: DECR key
///
/// # 参数
/// - `executor` - 命令执行器
///
/// # 返回值
/// - `Ok(RespValue::...)` - 执行成功
/// - `Err(AppError::...)` - 执行失败（键不存在、类型错误等）
pub(crate) fn execute_decr(executor: &CommandExecutor, key: String) -> Result<RespValue> {
                let new_val = executor.storage.decr(&key)?;
                Ok(RespValue::Integer(new_val))
}

/// 执行 INCR_BY 命令
///
/// Redis 语法: INCRBY key delta
///
/// # 参数
/// - `executor` - 命令执行器
///
/// # 返回值
/// - `Ok(RespValue::...)` - 执行成功
/// - `Err(AppError::...)` - 执行失败（键不存在、类型错误等）
pub(crate) fn execute_incr_by(executor: &CommandExecutor, key: String, delta: i64) -> Result<RespValue> {
                let new_val = executor.storage.incrby(&key, delta)?;
                Ok(RespValue::Integer(new_val))
}

/// 执行 DECR_BY 命令
///
/// Redis 语法: DECRBY key delta
///
/// # 参数
/// - `executor` - 命令执行器
///
/// # 返回值
/// - `Ok(RespValue::...)` - 执行成功
/// - `Err(AppError::...)` - 执行失败（键不存在、类型错误等）
pub(crate) fn execute_decr_by(executor: &CommandExecutor, key: String, delta: i64) -> Result<RespValue> {
                let new_val = executor.storage.decrby(&key, delta)?;
                Ok(RespValue::Integer(new_val))
}

/// 执行 APPEND 命令
///
/// Redis 语法: APPEND key value
///
/// # 参数
/// - `executor` - 命令执行器
///
/// # 返回值
/// - `Ok(RespValue::...)` - 执行成功
/// - `Err(AppError::...)` - 执行失败（键不存在、类型错误等）
pub(crate) fn execute_append(executor: &CommandExecutor, key: String, value: Bytes) -> Result<RespValue> {
                let new_len = executor.storage.append(&key, value)?;
                Ok(RespValue::Integer(new_len as i64))
}

/// 执行 SET_NX 命令
///
/// Redis 语法: SETNX key value
///
/// # 参数
/// - `executor` - 命令执行器
///
/// # 返回值
/// - `Ok(RespValue::...)` - 执行成功
/// - `Err(AppError::...)` - 执行失败（键不存在、类型错误等）
pub(crate) fn execute_set_nx(executor: &CommandExecutor, key: String, value: Bytes) -> Result<RespValue> {
                let result = if executor.storage.setnx(key, value)? {
                    1i64
                } else {
                    0i64
                };
                Ok(RespValue::Integer(result))
}

/// 执行 SET_EX_CMD 命令
///
/// Redis 语法: SETEX key seconds value
///
/// # 参数
/// - `executor` - 命令执行器
///
/// # 返回值
/// - `Ok(RespValue::...)` - 执行成功
/// - `Err(AppError::...)` - 执行失败（键不存在、类型错误等）
pub(crate) fn execute_set_ex_cmd(executor: &CommandExecutor, key: String, value: Bytes, seconds: u64) -> Result<RespValue> {
                executor.storage.setex(key, seconds, value)?;
                Ok(RespValue::SimpleString("OK".to_string()))
}

/// 执行 P_SET_EX 命令
///
/// Redis 语法: PSETEX key milliseconds value
///
/// # 参数
/// - `executor` - 命令执行器
///
/// # 返回值
/// - `Ok(RespValue::...)` - 执行成功
/// - `Err(AppError::...)` - 执行失败（键不存在、类型错误等）
pub(crate) fn execute_p_set_ex(executor: &CommandExecutor, key: String, value: Bytes, ms: u64) -> Result<RespValue> {
                executor.storage.psetex(key, ms, value)?;
                Ok(RespValue::SimpleString("OK".to_string()))
}

/// 执行 GET_SET 命令
///
/// Redis 语法: GETSET key value
///
/// # 参数
/// - `executor` - 命令执行器
///
/// # 返回值
/// - `Ok(RespValue::...)` - 执行成功
/// - `Err(AppError::...)` - 执行失败（键不存在、类型错误等）
pub(crate) fn execute_get_set(executor: &CommandExecutor, key: String, value: Bytes) -> Result<RespValue> {
                match executor.storage.getset(&key, value)? {
                    Some(data) => Ok(RespValue::BulkString(Some(data))),
                    None => Ok(RespValue::BulkString(None)),
                }
}

/// 执行 GET_DEL 命令
///
/// Redis 语法: GETDEL key
///
/// # 参数
/// - `executor` - 命令执行器
///
/// # 返回值
/// - `Ok(RespValue::...)` - 执行成功
/// - `Err(AppError::...)` - 执行失败（键不存在、类型错误等）
pub(crate) fn execute_get_del(executor: &CommandExecutor, key: String) -> Result<RespValue> {
                match executor.storage.getdel(&key)? {
                    Some(data) => Ok(RespValue::BulkString(Some(data))),
                    None => Ok(RespValue::BulkString(None)),
                }
}

/// 执行 GET_EX 命令
///
/// Redis 语法: GETEX key [EX seconds|PX milliseconds|EXAT timestamp|PXAT ms-timestamp|PERSIST]
///
/// # 参数
/// - `executor` - 命令执行器
///
/// # 返回值
/// - `Ok(RespValue::...)` - 执行成功
/// - `Err(AppError::...)` - 执行失败（键不存在、类型错误等）
pub(crate) fn execute_get_ex(executor: &CommandExecutor, key: String, opt: GetExOption) -> Result<RespValue> {
                match executor.storage.getex(&key, opt)? {
                    Some(data) => Ok(RespValue::BulkString(Some(data))),
                    None => Ok(RespValue::BulkString(None)),
                }
}

/// 执行 M_SET_NX 命令
///
/// Redis 语法: MSETNX key value [key value ...]
///
/// # 参数
/// - `executor` - 命令执行器
///
/// # 返回值
/// - `Ok(RespValue::...)` - 执行成功
/// - `Err(AppError::...)` - 执行失败（键不存在、类型错误等）
pub(crate) fn execute_m_set_nx(executor: &CommandExecutor, pairs: Vec<(String, Bytes)>) -> Result<RespValue> {
                let result = executor.storage.msetnx(&pairs)?;
                Ok(RespValue::Integer(result))
}

/// 执行 INCR_BY_FLOAT 命令
///
/// Redis 语法: INCRBYFLOAT key increment
///
/// # 参数
/// - `executor` - 命令执行器
///
/// # 返回值
/// - `Ok(RespValue::...)` - 执行成功
/// - `Err(AppError::...)` - 执行失败（键不存在、类型错误等）
pub(crate) fn execute_incr_by_float(executor: &CommandExecutor, key: String, delta: f64) -> Result<RespValue> {
                let new_val = executor.storage.incrbyfloat(&key, delta)?;
                Ok(RespValue::BulkString(Some(Bytes::from(new_val))))
}

/// 执行 SET_RANGE 命令
///
/// Redis 语法: SETRANGE key offset value
///
/// # 参数
/// - `executor` - 命令执行器
///
/// # 返回值
/// - `Ok(RespValue::...)` - 执行成功
/// - `Err(AppError::...)` - 执行失败（键不存在、类型错误等）
pub(crate) fn execute_set_range(executor: &CommandExecutor, key: String, offset: usize, value: Bytes) -> Result<RespValue> {
                let new_len = executor.storage.setrange(&key, offset, value)?;
                Ok(RespValue::Integer(new_len as i64))
}

/// 执行 GET_RANGE 命令
///
/// Redis 语法: GETRANGE key start end
///
/// # 参数
/// - `executor` - 命令执行器
///
/// # 返回值
/// - `Ok(RespValue::...)` - 执行成功
/// - `Err(AppError::...)` - 执行失败（键不存在、类型错误等）
pub(crate) fn execute_get_range(executor: &CommandExecutor, key: String, start: i64, end: i64) -> Result<RespValue> {
                match executor.storage.getrange(&key, start, end)? {
                    Some(data) => Ok(RespValue::BulkString(Some(data))),
                    None => Ok(RespValue::BulkString(None)),
                }
}

/// 执行 STR_LEN 命令
///
/// Redis 语法: STRLEN key
///
/// # 参数
/// - `executor` - 命令执行器
///
/// # 返回值
/// - `Ok(RespValue::...)` - 执行成功
/// - `Err(AppError::...)` - 执行失败（键不存在、类型错误等）
pub(crate) fn execute_str_len(executor: &CommandExecutor, key: String) -> Result<RespValue> {
                let len = executor.storage.strlen(&key)?;
                Ok(RespValue::Integer(len as i64))
}

