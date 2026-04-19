use super::*;

use crate::error::{AppError, Result};
use crate::protocol::RespValue;
use super::parser::CommandParser;

impl CommandParser {
    /// 解析 SETBIT 命令：SETBIT key offset value
    pub(crate) fn parse_setbit(&self, arr: &[RespValue]) -> Result<Command> {
        if arr.len() != 4 {
            return Err(AppError::Command(
                "SETBIT 命令需要 3 个参数".to_string(),
            ));
        }
        let key = self.extract_string(&arr[1])?;
        let offset: usize = self.extract_string(&arr[2])?.parse().map_err(|_| {
            AppError::Command("SETBIT 的 offset 必须是非负整数".to_string())
        })?;
        let value: u8 = self.extract_string(&arr[3])?.parse().map_err(|_| {
            AppError::Command("SETBIT 的 value 必须是 0 或 1".to_string())
        })?;
        if value != 0 && value != 1 {
            return Err(AppError::Command(
                "SETBIT 的 value 必须是 0 或 1".to_string(),
            ));
        }
        Ok(Command::SetBit(key, offset, value == 1))
    }


    /// 解析 GETBIT 命令：GETBIT key offset
    pub(crate) fn parse_getbit(&self, arr: &[RespValue]) -> Result<Command> {
        if arr.len() != 3 {
            return Err(AppError::Command(
                "GETBIT 命令需要 2 个参数".to_string(),
            ));
        }
        let key = self.extract_string(&arr[1])?;
        let offset: usize = self.extract_string(&arr[2])?.parse().map_err(|_| {
            AppError::Command("GETBIT 的 offset 必须是非负整数".to_string())
        })?;
        Ok(Command::GetBit(key, offset))
    }


    /// 解析 BITCOUNT 命令：BITCOUNT key [start end [BYTE|BIT]]
    pub(crate) fn parse_bitcount(&self, arr: &[RespValue]) -> Result<Command> {
        if arr.len() < 2 || arr.len() > 5 {
            return Err(AppError::Command(
                "BITCOUNT 命令参数数量错误".to_string(),
            ));
        }
        let key = self.extract_string(&arr[1])?;
        let start = if arr.len() > 2 {
            self.extract_string(&arr[2])?.parse().map_err(|_| {
                AppError::Command("BITCOUNT 的 start 必须是整数".to_string())
            })?
        } else {
            0
        };
        let end = if arr.len() > 3 {
            self.extract_string(&arr[3])?.parse().map_err(|_| {
                AppError::Command("BITCOUNT 的 end 必须是整数".to_string())
            })?
        } else {
            -1
        };
        let is_byte = if arr.len() > 4 {
            let unit = self.extract_string(&arr[4])?.to_ascii_uppercase();
            if unit == "BYTE" {
                true
            } else if unit == "BIT" {
                false
            } else {
                return Err(AppError::Command(
                    "BITCOUNT 的单位只能是 BYTE 或 BIT".to_string(),
                ));
            }
        } else {
            true
        };
        Ok(Command::BitCount(key, start, end, is_byte))
    }


    /// 解析 BITOP 命令：BITOP AND|OR|XOR|NOT destkey key [key ...]
    pub(crate) fn parse_bitop(&self, arr: &[RespValue]) -> Result<Command> {
        if arr.len() < 4 {
            return Err(AppError::Command(
                "BITOP 命令参数数量错误".to_string(),
            ));
        }
        let op = self.extract_string(&arr[1])?.to_ascii_uppercase();
        if op != "AND" && op != "OR" && op != "XOR" && op != "NOT" {
            return Err(AppError::Command(
                "BITOP 的操作只能是 AND|OR|XOR|NOT".to_string(),
            ));
        }
        let destkey = self.extract_string(&arr[2])?;
        let keys: Vec<String> = arr[3..]
            .iter()
            .map(|v| self.extract_string(v))
            .collect::<Result<Vec<String>>>()?;
        if op == "NOT" && keys.len() != 1 {
            return Err(AppError::Command(
                "BITOP NOT 只能接受一个 key".to_string(),
            ));
        }
        Ok(Command::BitOp(op, destkey, keys))
    }


    /// 解析 BITPOS 命令：BITPOS key bit [start [end [BYTE|BIT]]]
    pub(crate) fn parse_bitpos(&self, arr: &[RespValue]) -> Result<Command> {
        if arr.len() < 3 || arr.len() > 6 {
            return Err(AppError::Command(
                "BITPOS 命令参数数量错误".to_string(),
            ));
        }
        let key = self.extract_string(&arr[1])?;
        let bit: u8 = self.extract_string(&arr[2])?.parse().map_err(|_| {
            AppError::Command("BITPOS 的 bit 必须是 0 或 1".to_string())
        })?;
        if bit != 0 && bit != 1 {
            return Err(AppError::Command(
                "BITPOS 的 bit 必须是 0 或 1".to_string(),
            ));
        }
        let start = if arr.len() > 3 {
            self.extract_string(&arr[3])?.parse().map_err(|_| {
                AppError::Command("BITPOS 的 start 必须是整数".to_string())
            })?
        } else {
            0
        };
        let end = if arr.len() > 4 {
            self.extract_string(&arr[4])?.parse().map_err(|_| {
                AppError::Command("BITPOS 的 end 必须是整数".to_string())
            })?
        } else {
            -1
        };
        let is_byte = if arr.len() > 5 {
            let unit = self.extract_string(&arr[5])?.to_ascii_uppercase();
            if unit == "BYTE" {
                true
            } else if unit == "BIT" {
                false
            } else {
                return Err(AppError::Command(
                    "BITPOS 的单位只能是 BYTE 或 BIT".to_string(),
                ));
            }
        } else {
            true
        };
        Ok(Command::BitPos(key, bit, start, end, is_byte))
    }


    /// 解析 BITFIELD 命令：BITFIELD key [GET type offset] [SET type offset value] [INCRBY type offset increment] [OVERFLOW WRAP|SAT|FAIL] ...
    pub(crate) fn parse_bitfield(&self, arr: &[RespValue]) -> Result<Command> {
        if arr.len() < 2 {
            return Err(AppError::Command(
                "BITFIELD 命令需要至少 1 个参数".to_string(),
            ));
        }
        let key = self.extract_string(&arr[1])?;
        let mut ops = Vec::new();
        let mut i = 2;
        while i < arr.len() {
            let cmd = self.extract_string(&arr[i])?.to_ascii_uppercase();
            match cmd.as_str() {
                "GET" => {
                    if i + 2 >= arr.len() {
                        return Err(AppError::Command("BITFIELD GET 需要 type 和 offset".to_string()));
                    }
                    let enc = crate::storage::BitFieldEncoding::parse(&self.extract_string(&arr[i + 1])?)?;
                    let off = crate::storage::BitFieldOffset::parse(&self.extract_string(&arr[i + 2])?)?;
                    ops.push(crate::storage::BitFieldOp::Get(enc, off));
                    i += 3;
                }
                "SET" => {
                    if i + 3 >= arr.len() {
                        return Err(AppError::Command("BITFIELD SET 需要 type offset value".to_string()));
                    }
                    let enc = crate::storage::BitFieldEncoding::parse(&self.extract_string(&arr[i + 1])?)?;
                    let off = crate::storage::BitFieldOffset::parse(&self.extract_string(&arr[i + 2])?)?;
                    let value: i64 = self.extract_string(&arr[i + 3])?.parse().map_err(|_| {
                        AppError::Command("BITFIELD SET value 必须是整数".to_string())
                    })?;
                    ops.push(crate::storage::BitFieldOp::Set(enc, off, value));
                    i += 4;
                }
                "INCRBY" => {
                    if i + 3 >= arr.len() {
                        return Err(AppError::Command("BITFIELD INCRBY 需要 type offset increment".to_string()));
                    }
                    let enc = crate::storage::BitFieldEncoding::parse(&self.extract_string(&arr[i + 1])?)?;
                    let off = crate::storage::BitFieldOffset::parse(&self.extract_string(&arr[i + 2])?)?;
                    let inc: i64 = self.extract_string(&arr[i + 3])?.parse().map_err(|_| {
                        AppError::Command("BITFIELD INCRBY increment 必须是整数".to_string())
                    })?;
                    ops.push(crate::storage::BitFieldOp::IncrBy(enc, off, inc));
                    i += 4;
                }
                "OVERFLOW" => {
                    if i + 1 >= arr.len() {
                        return Err(AppError::Command("BITFIELD OVERFLOW 需要策略".to_string()));
                    }
                    let strategy = self.extract_string(&arr[i + 1])?.to_ascii_uppercase();
                    let overflow = match strategy.as_str() {
                        "WRAP" => crate::storage::BitFieldOverflow::Wrap,
                        "SAT" => crate::storage::BitFieldOverflow::Sat,
                        "FAIL" => crate::storage::BitFieldOverflow::Fail,
                        _ => return Err(AppError::Command("BITFIELD OVERFLOW 必须是 WRAP|SAT|FAIL".to_string())),
                    };
                    ops.push(crate::storage::BitFieldOp::Overflow(overflow));
                    i += 2;
                }
                _ => {
                    return Err(AppError::Command(format!("BITFIELD 不支持的子命令: {}", cmd)));
                }
            }
        }
        Ok(Command::BitField(key, ops))
    }


    /// 解析 BITFIELD_RO 命令：BITFIELD_RO key [GET type offset] ...
    pub(crate) fn parse_bitfield_ro(&self, arr: &[RespValue]) -> Result<Command> {
        if arr.len() < 2 {
            return Err(AppError::Command(
                "BITFIELD_RO 命令需要至少 1 个参数".to_string(),
            ));
        }
        let key = self.extract_string(&arr[1])?;
        let mut ops = Vec::new();
        let mut i = 2;
        while i < arr.len() {
            let cmd = self.extract_string(&arr[i])?.to_ascii_uppercase();
            if cmd != "GET" {
                return Err(AppError::Command("BITFIELD_RO 只支持 GET 操作".to_string()));
            }
            if i + 2 >= arr.len() {
                return Err(AppError::Command("BITFIELD_RO GET 需要 type 和 offset".to_string()));
            }
            let enc = crate::storage::BitFieldEncoding::parse(&self.extract_string(&arr[i + 1])?)?;
            let off = crate::storage::BitFieldOffset::parse(&self.extract_string(&arr[i + 2])?)?;
            ops.push(crate::storage::BitFieldOp::Get(enc, off));
            i += 3;
        }
        Ok(Command::BitFieldRo(key, ops))
    }


}
