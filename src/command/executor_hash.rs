use super::*;

use crate::error::Result;
use crate::protocol::RespValue;
use super::executor::CommandExecutor;

pub(crate) fn execute_h_set(executor: &CommandExecutor, key: String, pairs: Vec<(String, Bytes)>) -> Result<RespValue> {
                let mut count = 0i64;
                for (field, value) in pairs {
                    count += executor.storage.hset(&key, field, value)?;
                }
                Ok(RespValue::Integer(count))
}

pub(crate) fn execute_h_get(executor: &CommandExecutor, key: String, field: String) -> Result<RespValue> {
                match executor.storage.hget(&key, &field)? {
                    Some(value) => Ok(RespValue::BulkString(Some(value))),
                    None => Ok(RespValue::BulkString(None)),
                }
}

pub(crate) fn execute_h_del(executor: &CommandExecutor, key: String, fields: Vec<String>) -> Result<RespValue> {
                let count = executor.storage.hdel(&key, &fields)?;
                Ok(RespValue::Integer(count))
}

pub(crate) fn execute_h_exists(executor: &CommandExecutor, key: String, field: String) -> Result<RespValue> {
                let result = if executor.storage.hexists(&key, &field)? {
                    1i64
                } else {
                    0i64
                };
                Ok(RespValue::Integer(result))
}

pub(crate) fn execute_h_get_all(executor: &CommandExecutor, key: String) -> Result<RespValue> {
                let pairs = executor.storage.hgetall(&key)?;
                let mut resp_values = Vec::new();
                for (field, value) in pairs {
                    resp_values.push(RespValue::BulkString(Some(Bytes::from(field))));
                    resp_values.push(RespValue::BulkString(Some(value)));
                }
                Ok(RespValue::Array(resp_values))
}

pub(crate) fn execute_h_len(executor: &CommandExecutor, key: String) -> Result<RespValue> {
                let len = executor.storage.hlen(&key)?;
                Ok(RespValue::Integer(len as i64))
}

pub(crate) fn execute_h_m_set(executor: &CommandExecutor, key: String, pairs: Vec<(String, Bytes)>) -> Result<RespValue> {
                executor.storage.hmset(&key, &pairs)?;
                Ok(RespValue::SimpleString("OK".to_string()))
}

pub(crate) fn execute_h_m_get(executor: &CommandExecutor, key: String, fields: Vec<String>) -> Result<RespValue> {
                let values = executor.storage.hmget(&key, &fields)?;
                let resp_values: Vec<RespValue> = values
                    .into_iter()
                    .map(RespValue::BulkString)
                    .collect();
                Ok(RespValue::Array(resp_values))
}

pub(crate) fn execute_h_incr_by(executor: &CommandExecutor, key: String, field: String, delta: i64) -> Result<RespValue> {
                let new_val = executor.storage.hincrby(&key, field, delta)?;
                Ok(RespValue::Integer(new_val))
}

pub(crate) fn execute_h_incr_by_float(executor: &CommandExecutor, key: String, field: String, delta: f64) -> Result<RespValue> {
                let new_val = executor.storage.hincrbyfloat(&key, field, delta)?;
                Ok(RespValue::BulkString(Some(Bytes::from(new_val))))
}

pub(crate) fn execute_h_keys(executor: &CommandExecutor, key: String) -> Result<RespValue> {
                let keys = executor.storage.hkeys(&key)?;
                let resp_values: Vec<RespValue> = keys.into_iter().map(|k| RespValue::BulkString(Some(Bytes::from(k)))).collect();
                Ok(RespValue::Array(resp_values))
}

pub(crate) fn execute_h_vals(executor: &CommandExecutor, key: String) -> Result<RespValue> {
                let vals = executor.storage.hvals(&key)?;
                let resp_values: Vec<RespValue> = vals.into_iter().map(|v| RespValue::BulkString(Some(v))).collect();
                Ok(RespValue::Array(resp_values))
}

pub(crate) fn execute_h_set_nx(executor: &CommandExecutor, key: String, field: String, value: Bytes) -> Result<RespValue> {
                let result = executor.storage.hsetnx(&key, field, value)?;
                Ok(RespValue::Integer(result))
}

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

pub(crate) fn execute_h_expire(executor: &CommandExecutor, key: String, fields: Vec<String>, seconds: u64) -> Result<RespValue> {
                let result = executor.storage.hexpire(&key, &fields, seconds)?;
                let arr: Vec<RespValue> = result
                    .into_iter()
                    .map(RespValue::Integer)
                    .collect();
                Ok(RespValue::Array(arr))
}

pub(crate) fn execute_h_p_expire(executor: &CommandExecutor, key: String, fields: Vec<String>, ms: u64) -> Result<RespValue> {
                let result = executor.storage.hpexpire(&key, &fields, ms)?;
                let arr: Vec<RespValue> = result
                    .into_iter()
                    .map(RespValue::Integer)
                    .collect();
                Ok(RespValue::Array(arr))
}

pub(crate) fn execute_h_expire_at(executor: &CommandExecutor, key: String, fields: Vec<String>, ts: u64) -> Result<RespValue> {
                let result = executor.storage.hexpireat(&key, &fields, ts)?;
                let arr: Vec<RespValue> = result
                    .into_iter()
                    .map(RespValue::Integer)
                    .collect();
                Ok(RespValue::Array(arr))
}

pub(crate) fn execute_h_p_expire_at(executor: &CommandExecutor, key: String, fields: Vec<String>, ts: u64) -> Result<RespValue> {
                let result = executor.storage.hpexpireat(&key, &fields, ts)?;
                let arr: Vec<RespValue> = result
                    .into_iter()
                    .map(RespValue::Integer)
                    .collect();
                Ok(RespValue::Array(arr))
}

pub(crate) fn execute_h_ttl(executor: &CommandExecutor, key: String, fields: Vec<String>) -> Result<RespValue> {
                let result = executor.storage.httl(&key, &fields)?;
                let arr: Vec<RespValue> = result
                    .into_iter()
                    .map(RespValue::Integer)
                    .collect();
                Ok(RespValue::Array(arr))
}

pub(crate) fn execute_h_p_ttl(executor: &CommandExecutor, key: String, fields: Vec<String>) -> Result<RespValue> {
                let result = executor.storage.hpttl(&key, &fields)?;
                let arr: Vec<RespValue> = result
                    .into_iter()
                    .map(RespValue::Integer)
                    .collect();
                Ok(RespValue::Array(arr))
}

pub(crate) fn execute_h_expire_time(executor: &CommandExecutor, key: String, fields: Vec<String>) -> Result<RespValue> {
                let result = executor.storage.hexpiretime(&key, &fields)?;
                let arr: Vec<RespValue> = result
                    .into_iter()
                    .map(RespValue::Integer)
                    .collect();
                Ok(RespValue::Array(arr))
}

pub(crate) fn execute_h_p_expire_time(executor: &CommandExecutor, key: String, fields: Vec<String>) -> Result<RespValue> {
                let result = executor.storage.hpexpiretime(&key, &fields)?;
                let arr: Vec<RespValue> = result
                    .into_iter()
                    .map(RespValue::Integer)
                    .collect();
                Ok(RespValue::Array(arr))
}

pub(crate) fn execute_h_persist(executor: &CommandExecutor, key: String, fields: Vec<String>) -> Result<RespValue> {
                let result = executor.storage.hpersist(&key, &fields)?;
                let arr: Vec<RespValue> = result
                    .into_iter()
                    .map(RespValue::Integer)
                    .collect();
                Ok(RespValue::Array(arr))
}

