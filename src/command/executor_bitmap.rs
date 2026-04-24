
use crate::error::Result;
use crate::protocol::RespValue;
use super::executor::CommandExecutor;

/// 执行 SET_BIT 命令
///
/// Redis 语法: SETBIT key offset value
///
/// # 参数
/// - `executor` - 命令执行器
///
/// # 返回值
/// - `Ok(RespValue::...)` - 执行成功
/// - `Err(AppError::...)` - 执行失败（键不存在、类型错误等）
pub(crate) fn execute_set_bit(executor: &CommandExecutor, key: String, offset: usize, value: bool) -> Result<RespValue> {
                let old_val = executor.storage.setbit(&key, offset, value)?;
                Ok(RespValue::Integer(old_val))
}

/// 执行 GET_BIT 命令
///
/// Redis 语法: GETBIT key offset
///
/// # 参数
/// - `executor` - 命令执行器
///
/// # 返回值
/// - `Ok(RespValue::...)` - 执行成功
/// - `Err(AppError::...)` - 执行失败（键不存在、类型错误等）
pub(crate) fn execute_get_bit(executor: &CommandExecutor, key: String, offset: usize) -> Result<RespValue> {
                let val = executor.storage.getbit(&key, offset)?;
                Ok(RespValue::Integer(val))
}

/// 执行 BIT_COUNT 命令
///
/// Redis 语法: BITCOUNT key [start end [BYTE|BIT]]
///
/// # 参数
/// - `executor` - 命令执行器
///
/// # 返回值
/// - `Ok(RespValue::...)` - 执行成功
/// - `Err(AppError::...)` - 执行失败（键不存在、类型错误等）
pub(crate) fn execute_bit_count(executor: &CommandExecutor, key: String, start: isize, end: isize, is_byte: bool) -> Result<RespValue> {
                let count = executor.storage.bitcount(&key, start, end, is_byte)?;
                Ok(RespValue::Integer(count as i64))
}

/// 执行 BIT_OP 命令
///
/// Redis 语法: BITOP AND|OR|XOR|NOT destkey key [key ...]
///
/// # 参数
/// - `executor` - 命令执行器
///
/// # 返回值
/// - `Ok(RespValue::...)` - 执行成功
/// - `Err(AppError::...)` - 执行失败（键不存在、类型错误等）
pub(crate) fn execute_bit_op(executor: &CommandExecutor, op: String, destkey: String, keys: Vec<String>) -> Result<RespValue> {
                let len = executor.storage.bitop(&op, &destkey, &keys)?;
                Ok(RespValue::Integer(len as i64))
}

/// 执行 BIT_POS 命令
///
/// Redis 语法: BITPOS key bit [start [end [BYTE|BIT]]]
///
/// # 参数
/// - `executor` - 命令执行器
///
/// # 返回值
/// - `Ok(RespValue::...)` - 执行成功
/// - `Err(AppError::...)` - 执行失败（键不存在、类型错误等）
pub(crate) fn execute_bit_pos(executor: &CommandExecutor, key: String, bit: u8, start: isize, end: isize, is_byte: bool) -> Result<RespValue> {
                let pos = executor.storage.bitpos(&key, bit, start, end, is_byte)?;
                Ok(RespValue::Integer(pos))
}

/// 执行 BIT_FIELD 命令
///
/// Redis 语法: BITFIELD key [GET type offset] [SET type offset value] [INCRBY type offset increment] [OVERFLOW WRAP|SAT|FAIL] ...
///
/// # 参数
/// - `executor` - 命令执行器
///
/// # 返回值
/// - `Ok(RespValue::...)` - 执行成功
/// - `Err(AppError::...)` - 执行失败（键不存在、类型错误等）
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

/// 执行 BIT_FIELD_RO 命令
///
/// Redis 语法: BITFIELD_RO key [GET type offset] ...
///
/// # 参数
/// - `executor` - 命令执行器
///
/// # 返回值
/// - `Ok(RespValue::...)` - 执行成功
/// - `Err(AppError::...)` - 执行失败（键不存在、类型错误等）
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

