//! Bitmap 命令 RESP 序列化
use super::*;

use crate::protocol::RespValue;

/// 将 Command::SetBit 序列化为 RESP 数组
///
/// 对应 Redis 命令: SETBIT key offset value
///
/// # 参数
/// - `cmd` - Command 枚举引用（预期为 Command::SetBit 变体）
///
/// # 返回值
/// RESP 数组，适合写入 AOF 或发送给副本
///
/// # panic
/// 如果传入的 cmd 不是 Command::SetBit 变体，将触发 unreachable!()
pub(crate) fn to_resp_set_bit(cmd: &Command) -> RespValue {
    match cmd {
        Command::SetBit(key, offset, value) => {
                RespValue::Array(vec![
                    bulk("SETBIT"),
                    bulk(key),
                    bulk(&offset.to_string()),
                    bulk(if *value { "1" } else { "0" }),
                ])
        }
        _ => unreachable!(),
    }
}

/// 将 Command::GetBit 序列化为 RESP 数组
///
/// 对应 Redis 命令: GETBIT key offset
///
/// # 参数
/// - `cmd` - Command 枚举引用（预期为 Command::GetBit 变体）
///
/// # 返回值
/// RESP 数组，适合写入 AOF 或发送给副本
///
/// # panic
/// 如果传入的 cmd 不是 Command::GetBit 变体，将触发 unreachable!()
pub(crate) fn to_resp_get_bit(cmd: &Command) -> RespValue {
    match cmd {
        Command::GetBit(key, offset) => {
                RespValue::Array(vec![
                    bulk("GETBIT"),
                    bulk(key),
                    bulk(&offset.to_string()),
                ])
        }
        _ => unreachable!(),
    }
}

/// 将 Command::BitCount 序列化为 RESP 数组
///
/// 对应 Redis 命令: BITCOUNT key [start end [BYTE|BIT]]
///
/// # 参数
/// - `cmd` - Command 枚举引用（预期为 Command::BitCount 变体）
///
/// # 返回值
/// RESP 数组，适合写入 AOF 或发送给副本
///
/// # panic
/// 如果传入的 cmd 不是 Command::BitCount 变体，将触发 unreachable!()
pub(crate) fn to_resp_bit_count(cmd: &Command) -> RespValue {
    match cmd {
        Command::BitCount(key, start, end, is_byte) => {
                let mut parts = vec![bulk("BITCOUNT"), bulk(key)];
                parts.push(bulk(&start.to_string()));
                parts.push(bulk(&end.to_string()));
                parts.push(bulk(if *is_byte { "BYTE" } else { "BIT" }));
                RespValue::Array(parts)
        }
        _ => unreachable!(),
    }
}

/// 将 Command::BitOp 序列化为 RESP 数组
///
/// 对应 Redis 命令: BITOP AND|OR|XOR|NOT destkey key [key ...]
///
/// # 参数
/// - `cmd` - Command 枚举引用（预期为 Command::BitOp 变体）
///
/// # 返回值
/// RESP 数组，适合写入 AOF 或发送给副本
///
/// # panic
/// 如果传入的 cmd 不是 Command::BitOp 变体，将触发 unreachable!()
pub(crate) fn to_resp_bit_op(cmd: &Command) -> RespValue {
    match cmd {
        Command::BitOp(op, destkey, keys) => {
                let mut parts = vec![bulk("BITOP"), bulk(op), bulk(destkey)];
                for key in keys {
                    parts.push(bulk(key));
                }
                RespValue::Array(parts)
        }
        _ => unreachable!(),
    }
}

/// 将 Command::BitPos 序列化为 RESP 数组
///
/// 对应 Redis 命令: BITPOS key bit [start [end [BYTE|BIT]]]
///
/// # 参数
/// - `cmd` - Command 枚举引用（预期为 Command::BitPos 变体）
///
/// # 返回值
/// RESP 数组，适合写入 AOF 或发送给副本
///
/// # panic
/// 如果传入的 cmd 不是 Command::BitPos 变体，将触发 unreachable!()
pub(crate) fn to_resp_bit_pos(cmd: &Command) -> RespValue {
    match cmd {
        Command::BitPos(key, bit, start, end, is_byte) => {
                let mut parts = vec![
                    bulk("BITPOS"),
                    bulk(key),
                    bulk(&bit.to_string()),
                    bulk(&start.to_string()),
                    bulk(&end.to_string()),
                ];
                parts.push(bulk(if *is_byte { "BYTE" } else { "BIT" }));
                RespValue::Array(parts)
        }
        _ => unreachable!(),
    }
}

/// 将 Command::BitField 序列化为 RESP 数组
///
/// 对应 Redis 命令: BITFIELD key [GET type offset] [SET type offset value] [INCRBY type offset increment] [OVERFLOW WRAP|SAT|FAIL] ...
///
/// # 参数
/// - `cmd` - Command 枚举引用（预期为 Command::BitField 变体）
///
/// # 返回值
/// RESP 数组，适合写入 AOF 或发送给副本
///
/// # panic
/// 如果传入的 cmd 不是 Command::BitField 变体，将触发 unreachable!()
pub(crate) fn to_resp_bit_field(cmd: &Command) -> RespValue {
    match cmd {
        Command::BitField(key, ops) => {
                let mut parts = vec![bulk("BITFIELD"), bulk(key)];
                for op in ops {
                    match op {
                        crate::storage::BitFieldOp::Get(enc, off) => {
                            parts.push(bulk("GET"));
                            let type_str = format!("{}{}", if enc.signed { "i" } else { "u" }, enc.bits);
                            parts.push(bulk(&type_str));
                            let off_str = match off {
                                crate::storage::BitFieldOffset::Num(n) => n.to_string(),
                                crate::storage::BitFieldOffset::Hash(n) => format!("#{}", n),
                            };
                            parts.push(bulk(&off_str));
                        }
                        crate::storage::BitFieldOp::Set(enc, off, value) => {
                            parts.push(bulk("SET"));
                            let type_str = format!("{}{}", if enc.signed { "i" } else { "u" }, enc.bits);
                            parts.push(bulk(&type_str));
                            let off_str = match off {
                                crate::storage::BitFieldOffset::Num(n) => n.to_string(),
                                crate::storage::BitFieldOffset::Hash(n) => format!("#{}", n),
                            };
                            parts.push(bulk(&off_str));
                            parts.push(bulk(&value.to_string()));
                        }
                        crate::storage::BitFieldOp::IncrBy(enc, off, inc) => {
                            parts.push(bulk("INCRBY"));
                            let type_str = format!("{}{}", if enc.signed { "i" } else { "u" }, enc.bits);
                            parts.push(bulk(&type_str));
                            let off_str = match off {
                                crate::storage::BitFieldOffset::Num(n) => n.to_string(),
                                crate::storage::BitFieldOffset::Hash(n) => format!("#{}", n),
                            };
                            parts.push(bulk(&off_str));
                            parts.push(bulk(&inc.to_string()));
                        }
                        crate::storage::BitFieldOp::Overflow(o) => {
                            parts.push(bulk("OVERFLOW"));
                            let strategy = match o {
                                crate::storage::BitFieldOverflow::Wrap => "WRAP",
                                crate::storage::BitFieldOverflow::Sat => "SAT",
                                crate::storage::BitFieldOverflow::Fail => "FAIL",
                            };
                            parts.push(bulk(strategy));
                        }
                    }
                }
                RespValue::Array(parts)
        }
        _ => unreachable!(),
    }
}

/// 将 Command::BitFieldRo 序列化为 RESP 数组
///
/// 对应 Redis 命令: BITFIELD_RO key [GET type offset] ...
///
/// # 参数
/// - `cmd` - Command 枚举引用（预期为 Command::BitFieldRo 变体）
///
/// # 返回值
/// RESP 数组，适合写入 AOF 或发送给副本
///
/// # panic
/// 如果传入的 cmd 不是 Command::BitFieldRo 变体，将触发 unreachable!()
pub(crate) fn to_resp_bit_field_ro(cmd: &Command) -> RespValue {
    match cmd {
        Command::BitFieldRo(key, ops) => {
                let mut parts = vec![bulk("BITFIELD_RO"), bulk(key)];
                for op in ops {
                    if let crate::storage::BitFieldOp::Get(enc, off) = op {
                        parts.push(bulk("GET"));
                        let type_str = format!("{}{}", if enc.signed { "i" } else { "u" }, enc.bits);
                        parts.push(bulk(&type_str));
                        let off_str = match off {
                            crate::storage::BitFieldOffset::Num(n) => n.to_string(),
                            crate::storage::BitFieldOffset::Hash(n) => format!("#{}", n),
                        };
                        parts.push(bulk(&off_str));
                    }
                }
                RespValue::Array(parts)
        }
        _ => unreachable!(),
    }
}

