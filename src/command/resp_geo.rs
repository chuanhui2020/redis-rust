use super::*;

use crate::protocol::RespValue;

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

pub(crate) fn to_resp_geo_dist(cmd: &Command) -> RespValue {
    match cmd {
        Command::GeoDist(key, m1, m2, unit) => {
                let mut parts = vec![
                    bulk("GEODIST"),
                    bulk(key),
                    bulk(m1),
                    bulk(m2),
                ];
                if !unit.is_empty() && unit != "m" {
                    parts.push(bulk(unit));
                }
                RespValue::Array(parts)
        }
        _ => unreachable!(),
    }
}

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

