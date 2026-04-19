use super::*;

use crate::error::Result;
use crate::protocol::RespValue;
use super::executor::CommandExecutor;

pub(crate) fn execute_geo_add(executor: &CommandExecutor, key: String, items: Vec<(f64, f64, String)>) -> Result<RespValue> {
                let count = executor.storage.geoadd(&key, items)?;
                Ok(RespValue::Integer(count))
}

pub(crate) fn execute_geo_dist(executor: &CommandExecutor, key: String, member1: String, member2: String, unit: String) -> Result<RespValue> {
                match executor.storage.geodist(&key, &member1, &member2, &unit)? {
                    Some(dist) => {
                        let trimmed = format!("{:.17}", dist).trim_end_matches('0').trim_end_matches('.').to_string();
                        Ok(RespValue::BulkString(Some(Bytes::from(trimmed))))
                    }
                    None => Ok(RespValue::BulkString(None)),
                }
}

pub(crate) fn execute_geo_hash(executor: &CommandExecutor, key: String, members: Vec<String>) -> Result<RespValue> {
                let hashes = executor.storage.geohash(&key, &members)?;
                let resp_values: Vec<RespValue> = hashes
                    .into_iter()
                    .map(|h| RespValue::BulkString(h.map(|s| Bytes::from(s))))
                    .collect();
                Ok(RespValue::Array(resp_values))
}

pub(crate) fn execute_geo_pos(executor: &CommandExecutor, key: String, members: Vec<String>) -> Result<RespValue> {
                let positions = executor.storage.geopos(&key, &members)?;
                let resp_values: Vec<RespValue> = positions
                    .into_iter()
                    .map(|opt| {
                        match opt {
                            Some((lon, lat)) => {
                                let parts = vec![
                                    RespValue::BulkString(Some(Bytes::from(
                                        format!("{:.17}", lon).trim_end_matches('0').trim_end_matches('.').to_string()
                                    ))),
                                    RespValue::BulkString(Some(Bytes::from(
                                        format!("{:.17}", lat).trim_end_matches('0').trim_end_matches('.').to_string()
                                    ))),
                                ];
                                RespValue::Array(parts)
                            }
                            None => RespValue::BulkString(None),
                        }
                    })
                    .collect();
                Ok(RespValue::Array(resp_values))
}

pub(crate) fn execute_geo_search(executor: &CommandExecutor, key: String, center_lon: f64, center_lat: f64, by_radius: Option<f64>, by_box: Option<(f64, f64)>, order: Option<String>, count: usize, withcoord: bool, withdist: bool, withhash: bool) -> Result<RespValue> {
                let results = executor.storage.geosearch(
                    &key, center_lon, center_lat, by_radius, by_box,
                    order.as_deref(), count,
                )?;
                let mut resp_values = Vec::new();
                for (member, dist, lon, lat, hash) in results {
                    let mut item_parts = Vec::new();
                    // 成员名
                    item_parts.push(RespValue::BulkString(Some(Bytes::from(member))));
                    // 距离
                    if withdist {
                        let dist_s = format!("{:.17}", dist / 1000.0);
                        let dist_str = dist_s.trim_end_matches('0').trim_end_matches('.').to_string();
                        item_parts.push(RespValue::BulkString(Some(Bytes::from(dist_str))));
                    }
                    // hash
                    if withhash {
                        item_parts.push(RespValue::Integer(hash as i64));
                    }
                    // 坐标
                    if withcoord {
                        let coord_parts = vec![
                            RespValue::BulkString(Some(Bytes::from(
                                format!("{:.17}", lon).trim_end_matches('0').trim_end_matches('.').to_string()
                            ))),
                            RespValue::BulkString(Some(Bytes::from(
                                format!("{:.17}", lat).trim_end_matches('0').trim_end_matches('.').to_string()
                            ))),
                        ];
                        item_parts.push(RespValue::Array(coord_parts));
                    }
                    if item_parts.len() == 1 {
                        resp_values.push(item_parts.into_iter().next().unwrap());
                    } else {
                        resp_values.push(RespValue::Array(item_parts));
                    }
                }
                Ok(RespValue::Array(resp_values))
}

pub(crate) fn execute_geo_search_store(executor: &CommandExecutor, destination: String, source: String, center_lon: f64, center_lat: f64, by_radius: Option<f64>, by_box: Option<(f64, f64)>, order: Option<String>, count: usize, storedist: bool) -> Result<RespValue> {
                let count_result = executor.storage.geosearchstore(
                    &destination, &source, center_lon, center_lat, by_radius, by_box,
                    order.as_deref(), count, storedist,
                )?;
                Ok(RespValue::Integer(count_result as i64))
}

