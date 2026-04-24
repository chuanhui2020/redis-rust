//! Hash 命令执行器
use super::*;

use crate::error::Result;
use crate::protocol::RespValue;
use super::executor::CommandExecutor;

/// 执行 H_SET 命令
///
/// Redis 语法: HSET key field value [field value ...]
///
/// # 参数
/// - `executor` - 命令执行器
///
/// # 返回值
/// - `Ok(RespValue::...)` - 执行成功
/// - `Err(AppError::...)` - 执行失败（键不存在、类型错误等）
pub(crate) fn execute_h_set(executor: &CommandExecutor, key: String, pairs: Vec<(String, Bytes)>) -> Result<RespValue> {
                let mut count = 0i64;
                for (field, value) in pairs {
                    count += executor.storage.hset(&key, field, value)?;
                }
                Ok(RespValue::Integer(count))
}

/// 执行 H_GET 命令
///
/// Redis 语法: HGET key field
///
/// # 参数
/// - `executor` - 命令执行器
///
/// # 返回值
/// - `Ok(RespValue::...)` - 执行成功
/// - `Err(AppError::...)` - 执行失败（键不存在、类型错误等）
pub(crate) fn execute_h_get(executor: &CommandExecutor, key: String, field: String) -> Result<RespValue> {
                match executor.storage.hget(&key, &field)? {
                    Some(value) => Ok(RespValue::BulkString(Some(value))),
                    None => Ok(RespValue::BulkString(None)),
                }
}

/// 执行 H_STR_LEN 命令
///
/// Redis 语法: HSTRLEN key field
///
/// # 参数
/// - `executor` - 命令执行器
///
/// # 返回值
/// - `Ok(RespValue::...)` - 执行成功
/// - `Err(AppError::...)` - 执行失败（键不存在、类型错误等）
pub(crate) fn execute_h_str_len(executor: &CommandExecutor, key: String, field: String) -> Result<RespValue> {
                match executor.storage.hget(&key, &field)? {
                    Some(value) => Ok(RespValue::Integer(value.len() as i64)),
                    None => Ok(RespValue::Integer(0)),
                }
}

/// 执行 H_DEL 命令
///
/// Redis 语法: HDEL key field [field ...]
///
/// # 参数
/// - `executor` - 命令执行器
///
/// # 返回值
/// - `Ok(RespValue::...)` - 执行成功
/// - `Err(AppError::...)` - 执行失败（键不存在、类型错误等）
pub(crate) fn execute_h_del(executor: &CommandExecutor, key: String, fields: Vec<String>) -> Result<RespValue> {
                let count = executor.storage.hdel(&key, &fields)?;
                Ok(RespValue::Integer(count))
}

/// 执行 H_EXISTS 命令
///
/// Redis 语法: HEXISTS key field
///
/// # 参数
/// - `executor` - 命令执行器
///
/// # 返回值
/// - `Ok(RespValue::...)` - 执行成功
/// - `Err(AppError::...)` - 执行失败（键不存在、类型错误等）
pub(crate) fn execute_h_exists(executor: &CommandExecutor, key: String, field: String) -> Result<RespValue> {
                let result = if executor.storage.hexists(&key, &field)? {
                    1i64
                } else {
                    0i64
                };
                Ok(RespValue::Integer(result))
}

/// 执行 H_GET_ALL 命令
///
/// Redis 语法: HGETALL key
///
/// # 参数
/// - `executor` - 命令执行器
///
/// # 返回值
/// - `Ok(RespValue::...)` - 执行成功
/// - `Err(AppError::...)` - 执行失败（键不存在、类型错误等）
pub(crate) fn execute_h_get_all(executor: &CommandExecutor, key: String) -> Result<RespValue> {
                let pairs = executor.storage.hgetall(&key)?;
                let mut resp_values = Vec::new();
                for (field, value) in pairs {
                    resp_values.push(RespValue::BulkString(Some(Bytes::from(field))));
                    resp_values.push(RespValue::BulkString(Some(value)));
                }
                Ok(RespValue::Array(resp_values))
}

/// 执行 H_LEN 命令
///
/// Redis 语法: HLEN key
///
/// # 参数
/// - `executor` - 命令执行器
///
/// # 返回值
/// - `Ok(RespValue::...)` - 执行成功
/// - `Err(AppError::...)` - 执行失败（键不存在、类型错误等）
pub(crate) fn execute_h_len(executor: &CommandExecutor, key: String) -> Result<RespValue> {
                let len = executor.storage.hlen(&key)?;
                Ok(RespValue::Integer(len as i64))
}

/// 执行 H_M_SET 命令
///
/// Redis 语法: HMSET key field value [field value ...]
///
/// # 参数
/// - `executor` - 命令执行器
///
/// # 返回值
/// - `Ok(RespValue::...)` - 执行成功
/// - `Err(AppError::...)` - 执行失败（键不存在、类型错误等）
pub(crate) fn execute_h_m_set(executor: &CommandExecutor, key: String, pairs: Vec<(String, Bytes)>) -> Result<RespValue> {
                executor.storage.hmset(&key, &pairs)?;
                Ok(RespValue::SimpleString("OK".to_string()))
}

/// 执行 H_M_GET 命令
///
/// Redis 语法: HMGET key field [field ...]
///
/// # 参数
/// - `executor` - 命令执行器
///
/// # 返回值
/// - `Ok(RespValue::...)` - 执行成功
/// - `Err(AppError::...)` - 执行失败（键不存在、类型错误等）
pub(crate) fn execute_h_m_get(executor: &CommandExecutor, key: String, fields: Vec<String>) -> Result<RespValue> {
                let values = executor.storage.hmget(&key, &fields)?;
                let resp_values: Vec<RespValue> = values
                    .into_iter()
                    .map(RespValue::BulkString)
                    .collect();
                Ok(RespValue::Array(resp_values))
}

/// 执行 H_INCR_BY 命令
///
/// Redis 语法: HINCRBY key field increment
///
/// # 参数
/// - `executor` - 命令执行器
///
/// # 返回值
/// - `Ok(RespValue::...)` - 执行成功
/// - `Err(AppError::...)` - 执行失败（键不存在、类型错误等）
pub(crate) fn execute_h_incr_by(executor: &CommandExecutor, key: String, field: String, delta: i64) -> Result<RespValue> {
                let new_val = executor.storage.hincrby(&key, field, delta)?;
                Ok(RespValue::Integer(new_val))
}

/// 执行 H_INCR_BY_FLOAT 命令
///
/// Redis 语法: HINCRBYFLOAT key field increment
///
/// # 参数
/// - `executor` - 命令执行器
///
/// # 返回值
/// - `Ok(RespValue::...)` - 执行成功
/// - `Err(AppError::...)` - 执行失败（键不存在、类型错误等）
pub(crate) fn execute_h_incr_by_float(executor: &CommandExecutor, key: String, field: String, delta: f64) -> Result<RespValue> {
                let new_val = executor.storage.hincrbyfloat(&key, field, delta)?;
                Ok(RespValue::BulkString(Some(Bytes::from(new_val))))
}

/// 执行 H_KEYS 命令
///
/// Redis 语法: HKEYS key
///
/// # 参数
/// - `executor` - 命令执行器
///
/// # 返回值
/// - `Ok(RespValue::...)` - 执行成功
/// - `Err(AppError::...)` - 执行失败（键不存在、类型错误等）
pub(crate) fn execute_h_keys(executor: &CommandExecutor, key: String) -> Result<RespValue> {
                let keys = executor.storage.hkeys(&key)?;
                let resp_values: Vec<RespValue> = keys.into_iter().map(|k| RespValue::BulkString(Some(Bytes::from(k)))).collect();
                Ok(RespValue::Array(resp_values))
}

/// 执行 H_VALS 命令
///
/// Redis 语法: HVALS key
///
/// # 参数
/// - `executor` - 命令执行器
///
/// # 返回值
/// - `Ok(RespValue::...)` - 执行成功
/// - `Err(AppError::...)` - 执行失败（键不存在、类型错误等）
pub(crate) fn execute_h_vals(executor: &CommandExecutor, key: String) -> Result<RespValue> {
                let vals = executor.storage.hvals(&key)?;
                let resp_values: Vec<RespValue> = vals.into_iter().map(|v| RespValue::BulkString(Some(v))).collect();
                Ok(RespValue::Array(resp_values))
}

/// 执行 H_SET_NX 命令
///
/// Redis 语法: HSETNX key field value
///
/// # 参数
/// - `executor` - 命令执行器
///
/// # 返回值
/// - `Ok(RespValue::...)` - 执行成功
/// - `Err(AppError::...)` - 执行失败（键不存在、类型错误等）
pub(crate) fn execute_h_set_nx(executor: &CommandExecutor, key: String, field: String, value: Bytes) -> Result<RespValue> {
                let result = executor.storage.hsetnx(&key, field, value)?;
                Ok(RespValue::Integer(result))
}

/// 执行 H_RAND_FIELD 命令
///
/// Redis 语法: HRANDFIELD key [count [WITHVALUES]]
///
/// # 参数
/// - `executor` - 命令执行器
///
/// # 返回值
/// - `Ok(RespValue::...)` - 执行成功
/// - `Err(AppError::...)` - 执行失败（键不存在、类型错误等）
pub(crate) fn execute_h_rand_field(executor: &CommandExecutor, key: String, count: i64, with_values: bool) -> Result<RespValue> {
                let result = executor.storage.hrandfield(&key, count, with_values)?;
                if count == 1 && !with_values {
                    // 单字段不带值，返回单个 BulkString
                    if let Some((field, _)) = result.first() {
                        Ok(RespValue::BulkString(Some(Bytes::from(field.clone()))))
                    } else {
                        Ok(RespValue::BulkString(None))
                    }
                } else {
                    let mut parts = Vec::new();
                    for (field, value) in result {
                        parts.push(RespValue::BulkString(Some(Bytes::from(field))));
                        if let Some(v) = value {
                            parts.push(RespValue::BulkString(Some(v)));
                        }
                    }
                    Ok(RespValue::Array(parts))
                }
}

/// 执行 H_SCAN 命令
///
/// Redis 语法: HSCAN key cursor [MATCH pattern] [COUNT count]
///
/// # 参数
/// - `executor` - 命令执行器
///
/// # 返回值
/// - `Ok(RespValue::...)` - 执行成功
/// - `Err(AppError::...)` - 执行失败（键不存在、类型错误等）
pub(crate) fn execute_h_scan(executor: &CommandExecutor, key: String, cursor: usize, pattern: String, count: usize) -> Result<RespValue> {
                let (new_cursor, fields) = executor.storage.hscan(&key, cursor, &pattern, count)?;
                let mut parts: Vec<RespValue> = vec![
                    RespValue::BulkString(Some(Bytes::from(new_cursor.to_string()))),
                ];
                let field_values: Vec<RespValue> = fields
                    .into_iter()
                    .flat_map(|(f, v)| {
                        vec![
                            RespValue::BulkString(Some(Bytes::from(f))),
                            RespValue::BulkString(Some(v)),
                        ]
                    })
                    .collect();
                parts.push(RespValue::Array(field_values));
                Ok(RespValue::Array(parts))
}

/// 执行 H_EXPIRE 命令
///
/// Redis 语法: HEXPIRE key field [field ...] seconds
///
/// # 参数
/// - `executor` - 命令执行器
///
/// # 返回值
/// - `Ok(RespValue::...)` - 执行成功
/// - `Err(AppError::...)` - 执行失败（键不存在、类型错误等）
pub(crate) fn execute_h_expire(executor: &CommandExecutor, key: String, fields: Vec<String>, seconds: u64) -> Result<RespValue> {
                let result = executor.storage.hexpire(&key, &fields, seconds)?;
                let arr: Vec<RespValue> = result
                    .into_iter()
                    .map(RespValue::Integer)
                    .collect();
                Ok(RespValue::Array(arr))
}

/// 执行 H_P_EXPIRE 命令
///
/// Redis 语法: HPEXPIRE key field [field ...] milliseconds
///
/// # 参数
/// - `executor` - 命令执行器
///
/// # 返回值
/// - `Ok(RespValue::...)` - 执行成功
/// - `Err(AppError::...)` - 执行失败（键不存在、类型错误等）
pub(crate) fn execute_h_p_expire(executor: &CommandExecutor, key: String, fields: Vec<String>, ms: u64) -> Result<RespValue> {
                let result = executor.storage.hpexpire(&key, &fields, ms)?;
                let arr: Vec<RespValue> = result
                    .into_iter()
                    .map(RespValue::Integer)
                    .collect();
                Ok(RespValue::Array(arr))
}

/// 执行 H_EXPIRE_AT 命令
///
/// Redis 语法: HEXPIREAT key field [field ...] timestamp-seconds
///
/// # 参数
/// - `executor` - 命令执行器
///
/// # 返回值
/// - `Ok(RespValue::...)` - 执行成功
/// - `Err(AppError::...)` - 执行失败（键不存在、类型错误等）
pub(crate) fn execute_h_expire_at(executor: &CommandExecutor, key: String, fields: Vec<String>, ts: u64) -> Result<RespValue> {
                let result = executor.storage.hexpireat(&key, &fields, ts)?;
                let arr: Vec<RespValue> = result
                    .into_iter()
                    .map(RespValue::Integer)
                    .collect();
                Ok(RespValue::Array(arr))
}

/// 执行 H_P_EXPIRE_AT 命令
///
/// Redis 语法: HPEXPIREAT key field [field ...] timestamp-ms
///
/// # 参数
/// - `executor` - 命令执行器
///
/// # 返回值
/// - `Ok(RespValue::...)` - 执行成功
/// - `Err(AppError::...)` - 执行失败（键不存在、类型错误等）
pub(crate) fn execute_h_p_expire_at(executor: &CommandExecutor, key: String, fields: Vec<String>, ts: u64) -> Result<RespValue> {
                let result = executor.storage.hpexpireat(&key, &fields, ts)?;
                let arr: Vec<RespValue> = result
                    .into_iter()
                    .map(RespValue::Integer)
                    .collect();
                Ok(RespValue::Array(arr))
}

/// 执行 H_TTL 命令
///
/// Redis 语法: HTTL key field [field ...]
///
/// # 参数
/// - `executor` - 命令执行器
///
/// # 返回值
/// - `Ok(RespValue::...)` - 执行成功
/// - `Err(AppError::...)` - 执行失败（键不存在、类型错误等）
pub(crate) fn execute_h_ttl(executor: &CommandExecutor, key: String, fields: Vec<String>) -> Result<RespValue> {
                let result = executor.storage.httl(&key, &fields)?;
                let arr: Vec<RespValue> = result
                    .into_iter()
                    .map(RespValue::Integer)
                    .collect();
                Ok(RespValue::Array(arr))
}

/// 执行 H_P_TTL 命令
///
/// Redis 语法: HPTTL key field [field ...]
///
/// # 参数
/// - `executor` - 命令执行器
///
/// # 返回值
/// - `Ok(RespValue::...)` - 执行成功
/// - `Err(AppError::...)` - 执行失败（键不存在、类型错误等）
pub(crate) fn execute_h_p_ttl(executor: &CommandExecutor, key: String, fields: Vec<String>) -> Result<RespValue> {
                let result = executor.storage.hpttl(&key, &fields)?;
                let arr: Vec<RespValue> = result
                    .into_iter()
                    .map(RespValue::Integer)
                    .collect();
                Ok(RespValue::Array(arr))
}

/// 执行 H_EXPIRE_TIME 命令
///
/// Redis 语法: HEXPIRETIME key field [field ...]
///
/// # 参数
/// - `executor` - 命令执行器
///
/// # 返回值
/// - `Ok(RespValue::...)` - 执行成功
/// - `Err(AppError::...)` - 执行失败（键不存在、类型错误等）
pub(crate) fn execute_h_expire_time(executor: &CommandExecutor, key: String, fields: Vec<String>) -> Result<RespValue> {
                let result = executor.storage.hexpiretime(&key, &fields)?;
                let arr: Vec<RespValue> = result
                    .into_iter()
                    .map(RespValue::Integer)
                    .collect();
                Ok(RespValue::Array(arr))
}

/// 执行 H_P_EXPIRE_TIME 命令
///
/// Redis 语法: HPEXPIRETIME key field [field ...]
///
/// # 参数
/// - `executor` - 命令执行器
///
/// # 返回值
/// - `Ok(RespValue::...)` - 执行成功
/// - `Err(AppError::...)` - 执行失败（键不存在、类型错误等）
pub(crate) fn execute_h_p_expire_time(executor: &CommandExecutor, key: String, fields: Vec<String>) -> Result<RespValue> {
                let result = executor.storage.hpexpiretime(&key, &fields)?;
                let arr: Vec<RespValue> = result
                    .into_iter()
                    .map(RespValue::Integer)
                    .collect();
                Ok(RespValue::Array(arr))
}

/// 执行 H_PERSIST 命令
///
/// Redis 语法: HPERSIST key field [field ...]
///
/// # 参数
/// - `executor` - 命令执行器
///
/// # 返回值
/// - `Ok(RespValue::...)` - 执行成功
/// - `Err(AppError::...)` - 执行失败（键不存在、类型错误等）
pub(crate) fn execute_h_persist(executor: &CommandExecutor, key: String, fields: Vec<String>) -> Result<RespValue> {
                let result = executor.storage.hpersist(&key, &fields)?;
                let arr: Vec<RespValue> = result
                    .into_iter()
                    .map(RespValue::Integer)
                    .collect();
                Ok(RespValue::Array(arr))
}

/// 执行 H_GET_DEL 命令
///
/// Redis 语法: HGETDEL key field [field ...]
///
/// # 参数
/// - `executor` - 命令执行器
///
/// # 返回值
/// - `Ok(RespValue::...)` - 执行成功
/// - `Err(AppError::...)` - 执行失败（键不存在、类型错误等）
pub(crate) fn execute_h_get_del(executor: &CommandExecutor, key: String, fields: Vec<String>) -> Result<RespValue> {
                // 对每个 field 先 hget 获取值，然后 hdel 删除
                let mut values = Vec::new();
                for field in &fields {
                    let val = executor.storage.hget(&key, field)?;
                    values.push(RespValue::BulkString(val));
                }
                executor.storage.hdel(&key, &fields)?;
                Ok(RespValue::Array(values))
}

/// 执行 H_GET_EX 命令
///
/// Redis 语法: HGETEX key [EX seconds|PX milliseconds|EXAT timestamp|PXAT ms-timestamp|PERSIST] field [field ...]
///
/// # 参数
/// - `executor` - 命令执行器
///
/// # 返回值
/// - `Ok(RespValue::...)` - 执行成功
/// - `Err(AppError::...)` - 执行失败（键不存在、类型错误等）
pub(crate) fn execute_h_get_ex(executor: &CommandExecutor, key: String, opt: GetExOption, fields: Vec<String>) -> Result<RespValue> {
                // 先获取所有字段值
                let values = executor.storage.hmget(&key, &fields)?;
                let resp_values: Vec<RespValue> = values
                    .into_iter()
                    .map(RespValue::BulkString)
                    .collect();
                // 根据选项设置过期时间
                match opt {
                    GetExOption::Persist => {
                        executor.storage.hpersist(&key, &fields)?;
                    }
                    GetExOption::Ex(seconds) => {
                        executor.storage.hexpire(&key, &fields, seconds)?;
                    }
                    GetExOption::Px(ms) => {
                        executor.storage.hpexpire(&key, &fields, ms)?;
                    }
                    GetExOption::ExAt(ts) => {
                        executor.storage.hexpireat(&key, &fields, ts)?;
                    }
                    GetExOption::PxAt(ts) => {
                        executor.storage.hpexpireat(&key, &fields, ts)?;
                    }
                }
                Ok(RespValue::Array(resp_values))
}

/// 执行 H_SET_EX 命令
///
/// Redis 语法: HSETEX key seconds field value [field value ...]
///
/// # 参数
/// - `executor` - 命令执行器
///
/// # 返回值
/// - `Ok(RespValue::...)` - 执行成功
/// - `Err(AppError::...)` - 执行失败（键不存在、类型错误等）
pub(crate) fn execute_h_set_ex(executor: &CommandExecutor, key: String, seconds: u64, pairs: Vec<(String, Bytes)>) -> Result<RespValue> {
                // 先 hset 设置字段
                let mut count = 0i64;
                for (field, value) in &pairs {
                    count += executor.storage.hset(&key, field.clone(), value.clone())?;
                }
                // 然后 hexpire 设置过期时间
                let fields: Vec<String> = pairs.into_iter().map(|(f, _)| f).collect();
                executor.storage.hexpire(&key, &fields, seconds)?;
                Ok(RespValue::Integer(count))
}

