//! Geo 命令 RESP 序列化
use super::*;

use crate::protocol::RespValue;

/// 将 Command::GeoAdd 序列化为 RESP 数组
///
/// 对应 Redis 命令: GEOADD key [NX|XX] [CH] longitude latitude member [longitude latitude member ...]
///
/// # 参数
/// - `cmd` - Command 枚举引用（预期为 Command::GeoAdd 变体）
///
/// # 返回值
/// RESP 数组，适合写入 AOF 或发送给副本
///
/// # panic
/// 如果传入的 cmd 不是 Command::GeoAdd 变体，将触发 unreachable!()
pub(crate) fn to_resp_geo_add(cmd: &Command) -> RespValue {
    match cmd {
        Command::GeoAdd(key, items) => {
            let mut parts = vec![bulk("GEOADD"), bulk(key)];
            for (lon, lat, member) in items {
                parts.push(bulk(&lon.to_string()));
                parts.push(bulk(&lat.to_string()));
                parts.push(bulk(member));
            }
            RespValue::Array(parts)
        }
        _ => unreachable!(),
    }
}

/// 将 Command::GeoDist 序列化为 RESP 数组
///
/// 对应 Redis 命令: GEODIST key member1 member2 [m|km|ft|mi]
///
/// # 参数
/// - `cmd` - Command 枚举引用（预期为 Command::GeoDist 变体）
///
/// # 返回值
/// RESP 数组，适合写入 AOF 或发送给副本
///
/// # panic
/// 如果传入的 cmd 不是 Command::GeoDist 变体，将触发 unreachable!()
pub(crate) fn to_resp_geo_dist(cmd: &Command) -> RespValue {
    match cmd {
        Command::GeoDist(key, m1, m2, unit) => {
            let mut parts = vec![bulk("GEODIST"), bulk(key), bulk(m1), bulk(m2)];
            if !unit.is_empty() && unit != "m" {
                parts.push(bulk(unit));
            }
            RespValue::Array(parts)
        }
        _ => unreachable!(),
    }
}

/// 将 Command::GeoHash 序列化为 RESP 数组
///
/// 对应 Redis 命令: GEOHASH key member [member ...]
///
/// # 参数
/// - `cmd` - Command 枚举引用（预期为 Command::GeoHash 变体）
///
/// # 返回值
/// RESP 数组，适合写入 AOF 或发送给副本
///
/// # panic
/// 如果传入的 cmd 不是 Command::GeoHash 变体，将触发 unreachable!()
pub(crate) fn to_resp_geo_hash(cmd: &Command) -> RespValue {
    match cmd {
        Command::GeoHash(key, members) => {
            let mut parts = vec![bulk("GEOHASH"), bulk(key)];
            for member in members {
                parts.push(bulk(member));
            }
            RespValue::Array(parts)
        }
        _ => unreachable!(),
    }
}

/// 将 Command::GeoPos 序列化为 RESP 数组
///
/// 对应 Redis 命令: GEOPOS key member [member ...]
///
/// # 参数
/// - `cmd` - Command 枚举引用（预期为 Command::GeoPos 变体）
///
/// # 返回值
/// RESP 数组，适合写入 AOF 或发送给副本
///
/// # panic
/// 如果传入的 cmd 不是 Command::GeoPos 变体，将触发 unreachable!()
pub(crate) fn to_resp_geo_pos(cmd: &Command) -> RespValue {
    match cmd {
        Command::GeoPos(key, members) => {
            let mut parts = vec![bulk("GEOPOS"), bulk(key)];
            for member in members {
                parts.push(bulk(member));
            }
            RespValue::Array(parts)
        }
        _ => unreachable!(),
    }
}
