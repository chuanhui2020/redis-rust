//! Geo 数据类型操作（GEOADD/GEODIST/GEOSEARCH 等）

use super::*;

impl StorageEngine {
    pub fn encode_geohash(lon: f64, lat: f64) -> u64 {
        let mut lon_min = -180.0;
        let mut lon_max = 180.0;
        let mut lat_min = -85.05112878;
        let mut lat_max = 85.05112878;
        let mut hash = 0u64;

        for _ in 0..26 {
            // 经度位
            let lon_mid = (lon_min + lon_max) / 2.0;
            hash = (hash << 1) | if lon >= lon_mid { 1 } else { 0 };
            if lon >= lon_mid {
                lon_min = lon_mid;
            } else {
                lon_max = lon_mid;
            }

            // 纬度位
            let lat_mid = (lat_min + lat_max) / 2.0;
            hash = (hash << 1) | if lat >= lat_mid { 1 } else { 0 };
            if lat >= lat_mid {
                lat_min = lat_mid;
            } else {
                lat_max = lat_mid;
            }
        }

        hash
    }

    /// 将 52 位 GeoHash 整数解码为经度纬度
    pub fn decode_geohash(hash: u64) -> (f64, f64) {
        let mut lon_min = -180.0;
        let mut lon_max = 180.0;
        let mut lat_min = -85.05112878;
        let mut lat_max = 85.05112878;

        for i in (0..52).rev() {
            let bit = (hash >> i) & 1;
            if i % 2 == 1 {
                // 经度位
                let mid = (lon_min + lon_max) / 2.0;
                if bit == 1 {
                    lon_min = mid;
                } else {
                    lon_max = mid;
                }
            } else {
                // 纬度位
                let mid = (lat_min + lat_max) / 2.0;
                if bit == 1 {
                    lat_min = mid;
                } else {
                    lat_max = mid;
                }
            }
        }

        let lon = (lon_min + lon_max) / 2.0;
        let lat = (lat_min + lat_max) / 2.0;
        (lon, lat)
    }

    /// 将 GeoHash 整数编码为 base32 字符串（11 位）
    pub fn geohash_base32(hash: u64) -> String {
        const BASE32: &[u8] = b"0123456789bcdefghjkmnpqrstuvwxyz";
        let mut result = String::with_capacity(11);
        for i in 0..11 {
            let shift = 47 - i * 5;
            let bits = if shift >= 0 {
                (hash >> shift as u32) & 0x1F
            } else {
                (hash << (-shift) as u32) & 0x1F
            };
            result.push(BASE32[bits as usize] as char);
        }
        result
    }

    /// 使用 Haversine 公式计算两点间的球面距离（米）
    pub fn haversine(lon1: f64, lat1: f64, lon2: f64, lat2: f64) -> f64 {
        const R: f64 = 6371000.0; // 地球半径（米）
        let dlat = (lat2 - lat1).to_radians();
        let dlon = (lon2 - lon1).to_radians();
        let a = (dlat / 2.0).sin().powi(2)
            + lat1.to_radians().cos() * lat2.to_radians().cos() * (dlon / 2.0).sin().powi(2);
        let c = 2.0 * a.sqrt().atan2((1.0 - a).sqrt());
        R * c
    }

    /// 距离单位转换
    pub fn convert_distance(meters: f64, unit: &str) -> f64 {
        match unit.to_ascii_lowercase().as_str() {
            "km" => meters / 1000.0,
            "mi" => meters / 1609.344,
            "ft" => meters / 0.3048,
            _ => meters,
        }
    }

    /// 添加地理位置到有序集合
    /// items: (经度, 纬度, 成员名)
    /// 返回新增成员数量
    pub fn geoadd(&self, key: &str, items: Vec<(f64, f64, String)>) -> Result<i64> {
        // 验证经纬度范围
        for (lon, lat, _) in &items {
            if *lon < -180.0 || *lon > 180.0 {
                return Err(AppError::Command("经度范围必须在 [-180, 180]".to_string()));
            }
            if *lat < -85.05112878 || *lat > 85.05112878 {
                return Err(AppError::Command(
                    "纬度范围必须在 [-85.05112878, 85.05112878]".to_string(),
                ));
            }
        }

        let pairs: Vec<(f64, String)> = items
            .into_iter()
            .map(|(lon, lat, member)| {
                let hash = Self::encode_geohash(lon, lat);
                (hash as f64, member)
            })
            .collect();

        self.zadd(key, pairs)
    }

    /// 计算两个成员之间的距离
    /// unit: m/km/mi/ft，默认 m
    pub fn geodist(
        &self,
        key: &str,
        member1: &str,
        member2: &str,
        unit: &str,
    ) -> Result<Option<f64>> {
        let db = self.db();
        let mut map = db
            .inner
            .get_shard(key)
            .write()
            .map_err(|e| AppError::Storage(format!("锁中毒: {}", e)))?;

        match map.get(key) {
            Some(v) => {
                if v.is_expired() {
                    map.remove(key);
                    Ok(None)
                } else {
                    Self::check_zset_type(&v.value)?;
                    match &v.value {
                        StorageValue::ZSet(zset) => {
                            let score1 = zset.member_to_score.get(member1).copied();
                            let score2 = zset.member_to_score.get(member2).copied();
                            match (score1, score2) {
                                (Some(s1), Some(s2)) => {
                                    let (lon1, lat1) = Self::decode_geohash(s1 as u64);
                                    let (lon2, lat2) = Self::decode_geohash(s2 as u64);
                                    let meters = Self::haversine(lon1, lat1, lon2, lat2);
                                    Ok(Some(Self::convert_distance(meters, unit)))
                                }
                                _ => Ok(None),
                            }
                        }
                        _ => unreachable!(),
                    }
                }
            }
            None => Ok(None),
        }
    }

    /// 获取成员的 GeoHash 字符串（base32 编码，11 位）
    pub fn geohash(&self, key: &str, members: &[String]) -> Result<Vec<Option<String>>> {
        let db = self.db();
        let mut map = db
            .inner
            .get_shard(key)
            .write()
            .map_err(|e| AppError::Storage(format!("锁中毒: {}", e)))?;

        match map.get(key) {
            Some(v) => {
                if v.is_expired() {
                    map.remove(key);
                    Ok(members.iter().map(|_| None).collect())
                } else {
                    Self::check_zset_type(&v.value)?;
                    match &v.value {
                        StorageValue::ZSet(zset) => {
                            let result: Vec<Option<String>> = members
                                .iter()
                                .map(|member| {
                                    zset.member_to_score
                                        .get(member)
                                        .map(|&score| Self::geohash_base32(score as u64))
                                })
                                .collect();
                            Ok(result)
                        }
                        _ => unreachable!(),
                    }
                }
            }
            None => Ok(members.iter().map(|_| None).collect()),
        }
    }

    /// 获取成员的经纬度坐标
    pub fn geopos(&self, key: &str, members: &[String]) -> Result<Vec<Option<(f64, f64)>>> {
        let db = self.db();
        let mut map = db
            .inner
            .get_shard(key)
            .write()
            .map_err(|e| AppError::Storage(format!("锁中毒: {}", e)))?;

        match map.get(key) {
            Some(v) => {
                if v.is_expired() {
                    map.remove(key);
                    Ok(members.iter().map(|_| None).collect())
                } else {
                    Self::check_zset_type(&v.value)?;
                    match &v.value {
                        StorageValue::ZSet(zset) => {
                            let result: Vec<Option<(f64, f64)>> = members
                                .iter()
                                .map(|member| {
                                    zset.member_to_score
                                        .get(member)
                                        .map(|&score| Self::decode_geohash(score as u64))
                                })
                                .collect();
                            Ok(result)
                        }
                        _ => unreachable!(),
                    }
                }
            }
            None => Ok(members.iter().map(|_| None).collect()),
        }
    }

    /// 搜索指定范围内的地理位置成员
    /// center_lon/center_lat: 中心点坐标
    /// by_radius: 圆形搜索 (半径米数)
    /// by_box: 矩形搜索 (宽度米数, 高度米数)
    /// order: "ASC" 或 "DESC" 按距离排序，None 不排序
    /// count: 限制返回数量，0 表示不限制
    /// 返回 [(成员名, 距离米, 经度, 纬度, geohash)]
    pub fn geosearch(
        &self,
        key: &str,
        center_lon: f64,
        center_lat: f64,
        by_radius: Option<f64>,
        by_box: Option<(f64, f64)>,
        order: Option<&str>,
        count: usize,
    ) -> Result<Vec<(String, f64, f64, f64, u64)>> {
        let db = self.db();
        let mut map = db
            .inner
            .get_shard(key)
            .write()
            .map_err(|e| AppError::Storage(format!("锁中毒: {}", e)))?;

        let zset = match map.get(key) {
            Some(v) => {
                if v.is_expired() {
                    map.remove(key);
                    return Ok(vec![]);
                }
                Self::check_zset_type(&v.value)?;
                match &v.value {
                    StorageValue::ZSet(z) => z,
                    _ => unreachable!(),
                }
            }
            None => return Ok(vec![]),
        };

        let mut results: Vec<(String, f64, f64, f64, u64)> = Vec::new();

        for (member, &score) in &zset.member_to_score {
            let hash = score as u64;
            let (lon, lat) = Self::decode_geohash(hash);

            let in_range = if let Some(radius_m) = by_radius {
                let dist = Self::haversine(center_lon, center_lat, lon, lat);
                dist <= radius_m
            } else if let Some((width_m, height_m)) = by_box {
                // 矩形搜索：检查经纬度是否在范围内
                // 近似计算：1 度纬度 ≈ 111 km，1 度经度 ≈ 111 km * cos(中心纬度)
                let lat_range = height_m / 2.0 / 111000.0;
                let lon_range = width_m / 2.0 / (111000.0 * center_lat.to_radians().cos());
                (lon - center_lon).abs() <= lon_range && (lat - center_lat).abs() <= lat_range
            } else {
                true
            };

            if in_range {
                let dist = Self::haversine(center_lon, center_lat, lon, lat);
                results.push((member.clone(), dist, lon, lat, hash));
            }
        }

        // 排序
        if let Some(order_str) = order {
            match order_str.to_ascii_uppercase().as_str() {
                "ASC" => results
                    .sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal)),
                "DESC" => results
                    .sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal)),
                _ => {}
            }
        }

        // 限制数量
        if count > 0 && results.len() > count {
            results.truncate(count);
        }

        Ok(results)
    }

    /// 将搜索结果存储到 destination
    /// storedist: true 存储距离作为 score，false 存储 geohash 作为 score
    /// 返回存储的成员数量
    pub fn geosearchstore(
        &self,
        destination: &str,
        source: &str,
        center_lon: f64,
        center_lat: f64,
        by_radius: Option<f64>,
        by_box: Option<(f64, f64)>,
        order: Option<&str>,
        count: usize,
        storedist: bool,
    ) -> Result<usize> {
        let results = self.geosearch(
            source, center_lon, center_lat, by_radius, by_box, order, count,
        )?;

        if results.is_empty() {
            let db = self.db();
            let mut map = db
                .inner
                .get_shard(destination)
                .write()
                .map_err(|e| AppError::Storage(format!("锁中毒: {}", e)))?;
            map.remove(destination);
self.bump_version(&mut map, destination);
            return Ok(0);
        }

        let mut zset = ZSetData::new();
        for (member, dist, _lon, _lat, hash) in results {
            let score = if storedist { dist } else { hash as f64 };
            zset.add(member, score);
        }

        let db = self.db();
        let mut map = db
            .inner
            .get_shard(destination)
            .write()
            .map_err(|e| AppError::Storage(format!("锁中毒: {}", e)))?;
        let result_count = zset.member_to_score.len();
        map.insert(destination.to_string(), Entry::new(StorageValue::ZSet(zset)));
self.bump_version(&mut map, destination);
        Ok(result_count)
    }
}
