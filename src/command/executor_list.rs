use super::*;

use crate::error::Result;
use crate::protocol::RespValue;
use super::executor::CommandExecutor;

pub(crate) fn execute_lmpop(executor: &CommandExecutor, keys: Vec<String>, left: bool, count: usize) -> Result<RespValue> {
                match executor.storage.lmpop(&keys, left, count)? {
                    Some((key, values)) => {
                        let mut arr = Vec::new();
                        arr.push(RespValue::BulkString(Some(Bytes::from(key))));
                        let vals: Vec<RespValue> = values.into_iter()
                            .map(|v| RespValue::BulkString(Some(v)))
                            .collect();
                        arr.push(RespValue::Array(vals));
                        Ok(RespValue::Array(arr))
                    }
                    None => Ok(RespValue::BulkString(None)),
                }
}

pub(crate) fn execute_l_push(executor: &CommandExecutor, key: String, values: Vec<Bytes>) -> Result<RespValue> {
                let len = executor.storage.lpush(&key, values)?;
                Ok(RespValue::Integer(len as i64))
}

pub(crate) fn execute_r_push(executor: &CommandExecutor, key: String, values: Vec<Bytes>) -> Result<RespValue> {
                let len = executor.storage.rpush(&key, values)?;
                Ok(RespValue::Integer(len as i64))
}

pub(crate) fn execute_l_pop(executor: &CommandExecutor, key: String) -> Result<RespValue> {
                match executor.storage.lpop(&key)? {
                    Some(value) => Ok(RespValue::BulkString(Some(value))),
                    None => Ok(RespValue::BulkString(None)),
                }
}

pub(crate) fn execute_r_pop(executor: &CommandExecutor, key: String) -> Result<RespValue> {
                match executor.storage.rpop(&key)? {
                    Some(value) => Ok(RespValue::BulkString(Some(value))),
                    None => Ok(RespValue::BulkString(None)),
                }
}

pub(crate) fn execute_l_len(executor: &CommandExecutor, key: String) -> Result<RespValue> {
                let len = executor.storage.llen(&key)?;
                Ok(RespValue::Integer(len as i64))
}

pub(crate) fn execute_l_range(executor: &CommandExecutor, key: String, start: i64, stop: i64) -> Result<RespValue> {
                let values = executor.storage.lrange(&key, start, stop)?;
                let resp_values: Vec<RespValue> = values
                    .into_iter()
                    .map(|v| RespValue::BulkString(Some(v)))
                    .collect();
                Ok(RespValue::Array(resp_values))
}

pub(crate) fn execute_l_index(executor: &CommandExecutor, key: String, index: i64) -> Result<RespValue> {
                match executor.storage.lindex(&key, index)? {
                    Some(value) => Ok(RespValue::BulkString(Some(value))),
                    None => Ok(RespValue::BulkString(None)),
                }
}

pub(crate) fn execute_l_set(executor: &CommandExecutor, key: String, index: i64, value: Bytes) -> Result<RespValue> {
                executor.storage.lset(&key, index, value)?;
                Ok(RespValue::SimpleString("OK".to_string()))
}

pub(crate) fn execute_l_insert(executor: &CommandExecutor, key: String, pos: crate::storage::LInsertPosition, pivot: Bytes, value: Bytes) -> Result<RespValue> {
                let result = executor.storage.linsert(&key, pos, pivot, value)?;
                Ok(RespValue::Integer(result))
}

pub(crate) fn execute_l_rem(executor: &CommandExecutor, key: String, count: i64, value: Bytes) -> Result<RespValue> {
                let removed = executor.storage.lrem(&key, count, value)?;
                Ok(RespValue::Integer(removed))
}

pub(crate) fn execute_l_trim(executor: &CommandExecutor, key: String, start: i64, stop: i64) -> Result<RespValue> {
                executor.storage.ltrim(&key, start, stop)?;
                Ok(RespValue::SimpleString("OK".to_string()))
}

pub(crate) fn execute_l_pos(executor: &CommandExecutor, key: String, value: Bytes, rank: i64, count: i64, maxlen: i64) -> Result<RespValue> {
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
                    let arr: Vec<RespValue> = positions.into_iter().map(|p| RespValue::Integer(p)).collect();
                    Ok(RespValue::Array(arr))
                }
}

