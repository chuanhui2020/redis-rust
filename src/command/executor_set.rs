use super::*;

use crate::error::Result;
use crate::protocol::RespValue;
use super::executor::CommandExecutor;

pub(crate) fn execute_s_add(executor: &CommandExecutor, key: String, members: Vec<Bytes>) -> Result<RespValue> {
                let count = executor.storage.sadd(&key, members)?;
                Ok(RespValue::Integer(count))
}

pub(crate) fn execute_s_rem(executor: &CommandExecutor, key: String, members: Vec<Bytes>) -> Result<RespValue> {
                let count = executor.storage.srem(&key, &members)?;
                Ok(RespValue::Integer(count))
}

pub(crate) fn execute_s_members(executor: &CommandExecutor, key: String) -> Result<RespValue> {
                let members = executor.storage.smembers(&key)?;
                let resp_values: Vec<RespValue> = members
                    .into_iter()
                    .map(|m| RespValue::BulkString(Some(m)))
                    .collect();
                Ok(RespValue::Array(resp_values))
}

pub(crate) fn execute_s_is_member(executor: &CommandExecutor, key: String, member: Bytes) -> Result<RespValue> {
                let result = if executor.storage.sismember(&key, &member)? {
                    1i64
                } else {
                    0i64
                };
                Ok(RespValue::Integer(result))
}

pub(crate) fn execute_s_card(executor: &CommandExecutor, key: String) -> Result<RespValue> {
                let len = executor.storage.scard(&key)?;
                Ok(RespValue::Integer(len as i64))
}

pub(crate) fn execute_s_inter(executor: &CommandExecutor, keys: Vec<String>) -> Result<RespValue> {
                let members = executor.storage.sinter(&keys)?;
                let resp_values: Vec<RespValue> = members
                    .into_iter()
                    .map(|m| RespValue::BulkString(Some(m)))
                    .collect();
                Ok(RespValue::Array(resp_values))
}

pub(crate) fn execute_s_union(executor: &CommandExecutor, keys: Vec<String>) -> Result<RespValue> {
                let members = executor.storage.sunion(&keys)?;
                let resp_values: Vec<RespValue> = members
                    .into_iter()
                    .map(|m| RespValue::BulkString(Some(m)))
                    .collect();
                Ok(RespValue::Array(resp_values))
}

pub(crate) fn execute_s_diff(executor: &CommandExecutor, keys: Vec<String>) -> Result<RespValue> {
                let members = executor.storage.sdiff(&keys)?;
                let resp_values: Vec<RespValue> = members
                    .into_iter()
                    .map(|m| RespValue::BulkString(Some(m)))
                    .collect();
                Ok(RespValue::Array(resp_values))
}

pub(crate) fn execute_s_pop(executor: &CommandExecutor, key: String, count: i64) -> Result<RespValue> {
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

pub(crate) fn execute_s_rand_member(executor: &CommandExecutor, key: String, count: i64) -> Result<RespValue> {
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

pub(crate) fn execute_s_move(executor: &CommandExecutor, source: String, destination: String, member: Bytes) -> Result<RespValue> {
                let result = if executor.storage.smove(&source, &destination, member)? {
                    1i64
                } else {
                    0i64
                };
                Ok(RespValue::Integer(result))
}

pub(crate) fn execute_s_inter_store(executor: &CommandExecutor, destination: String, keys: Vec<String>) -> Result<RespValue> {
                let count = executor.storage.sinterstore(&destination, &keys)?;
                Ok(RespValue::Integer(count as i64))
}

pub(crate) fn execute_s_union_store(executor: &CommandExecutor, destination: String, keys: Vec<String>) -> Result<RespValue> {
                let count = executor.storage.sunionstore(&destination, &keys)?;
                Ok(RespValue::Integer(count as i64))
}

pub(crate) fn execute_s_diff_store(executor: &CommandExecutor, destination: String, keys: Vec<String>) -> Result<RespValue> {
                let count = executor.storage.sdiffstore(&destination, &keys)?;
                Ok(RespValue::Integer(count as i64))
}

pub(crate) fn execute_s_scan(executor: &CommandExecutor, key: String, cursor: usize, pattern: String, count: usize) -> Result<RespValue> {
                let (new_cursor, members) = executor.storage.sscan(&key, cursor, &pattern, count)?;
                let mut parts: Vec<RespValue> = vec![
                    RespValue::BulkString(Some(Bytes::from(new_cursor.to_string()))),
                ];
                let member_values: Vec<RespValue> = members
                    .into_iter()
                    .map(|m| RespValue::BulkString(Some(m)))
                    .collect();
                parts.push(RespValue::Array(member_values));
                Ok(RespValue::Array(parts))
}

