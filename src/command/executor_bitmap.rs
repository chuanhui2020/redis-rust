
use crate::error::Result;
use crate::protocol::RespValue;
use super::executor::CommandExecutor;

pub(crate) fn execute_set_bit(executor: &CommandExecutor, key: String, offset: usize, value: bool) -> Result<RespValue> {
                let old_val = executor.storage.setbit(&key, offset, value)?;
                Ok(RespValue::Integer(old_val))
}

pub(crate) fn execute_get_bit(executor: &CommandExecutor, key: String, offset: usize) -> Result<RespValue> {
                let val = executor.storage.getbit(&key, offset)?;
                Ok(RespValue::Integer(val))
}

pub(crate) fn execute_bit_count(executor: &CommandExecutor, key: String, start: isize, end: isize, is_byte: bool) -> Result<RespValue> {
                let count = executor.storage.bitcount(&key, start, end, is_byte)?;
                Ok(RespValue::Integer(count as i64))
}

pub(crate) fn execute_bit_op(executor: &CommandExecutor, op: String, destkey: String, keys: Vec<String>) -> Result<RespValue> {
                let len = executor.storage.bitop(&op, &destkey, &keys)?;
                Ok(RespValue::Integer(len as i64))
}

pub(crate) fn execute_bit_pos(executor: &CommandExecutor, key: String, bit: u8, start: isize, end: isize, is_byte: bool) -> Result<RespValue> {
                let pos = executor.storage.bitpos(&key, bit, start, end, is_byte)?;
                Ok(RespValue::Integer(pos))
}

pub(crate) fn execute_bit_field(executor: &CommandExecutor, key: String, ops: Vec<crate::storage::BitFieldOp>) -> Result<RespValue> {
                let results = executor.storage.bitfield(&key, &ops)?;
                let resp_values: Vec<RespValue> = results
                    .into_iter()
                    .map(|r| match r {
                        crate::storage::BitFieldResult::Value(v) => RespValue::Integer(v),
                        crate::storage::BitFieldResult::Nil => RespValue::BulkString(None),
                    })
                    .collect();
                Ok(RespValue::Array(resp_values))
}

pub(crate) fn execute_bit_field_ro(executor: &CommandExecutor, key: String, ops: Vec<crate::storage::BitFieldOp>) -> Result<RespValue> {
                let results = executor.storage.bitfield_ro(&key, &ops)?;
                let resp_values: Vec<RespValue> = results
                    .into_iter()
                    .map(|r| match r {
                        crate::storage::BitFieldResult::Value(v) => RespValue::Integer(v),
                        crate::storage::BitFieldResult::Nil => RespValue::BulkString(None),
                    })
                    .collect();
                Ok(RespValue::Array(resp_values))
}

