//! Geo 命令解析器

use super::*;

use super::parser::CommandParser;
use crate::error::{AppError, Result};
use crate::protocol::RespValue;

impl CommandParser {
    /// 解析 GEOADD 命令：GEOADD key longitude latitude member [longitude latitude member ...]
    pub(crate) fn parse_geoadd(&self, arr: &[RespValue]) -> Result<Command> {
        if arr.len() < 5 || !(arr.len() - 2).is_multiple_of(3) {
            return Err(AppError::Command(
                "GEOADD 命令参数数量错误，需要 key 和至少一组 (lon lat member)".to_string(),
            ));
        }
        let key = self.extract_string(&arr[1])?;
        let mut items = Vec::new();
        let mut idx = 2;
        while idx < arr.len() {
            let lon: f64 = self
                .extract_string(&arr[idx])?
                .parse()
                .map_err(|_| AppError::Command("GEOADD 的 longitude 必须是数字".to_string()))?;
            let lat: f64 = self
                .extract_string(&arr[idx + 1])?
                .parse()
                .map_err(|_| AppError::Command("GEOADD 的 latitude 必须是数字".to_string()))?;
            let member = self.extract_string(&arr[idx + 2])?;
            items.push((lon, lat, member));
            idx += 3;
        }
        Ok(Command::GeoAdd(key, items))
    }

    /// 解析 GEODIST 命令：GEODIST key member1 member2 [m|km|ft|mi]
    pub(crate) fn parse_geodist(&self, arr: &[RespValue]) -> Result<Command> {
        if arr.len() < 4 || arr.len() > 5 {
            return Err(AppError::Command("GEODIST 命令参数数量错误".to_string()));
        }
        let key = self.extract_string(&arr[1])?;
        let member1 = self.extract_string(&arr[2])?;
        let member2 = self.extract_string(&arr[3])?;
        let unit = if arr.len() == 5 {
            self.extract_string(&arr[4])?
        } else {
            "m".to_string()
        };
        Ok(Command::GeoDist(key, member1, member2, unit))
    }

    /// 解析 GEOHASH 命令：GEOHASH key member [member ...]
    pub(crate) fn parse_geohash(&self, arr: &[RespValue]) -> Result<Command> {
        if arr.len() < 3 {
            return Err(AppError::Command(
                "GEOHASH 命令需要至少 2 个参数".to_string(),
            ));
        }
        let key = self.extract_string(&arr[1])?;
        let members: Vec<String> = arr[2..]
            .iter()
            .map(|v| self.extract_string(v))
            .collect::<Result<Vec<String>>>()?;
        Ok(Command::GeoHash(key, members))
    }

    /// 解析 GEOPOS 命令：GEOPOS key member [member ...]
    pub(crate) fn parse_geopos(&self, arr: &[RespValue]) -> Result<Command> {
        if arr.len() < 3 {
            return Err(AppError::Command(
                "GEOPOS 命令需要至少 2 个参数".to_string(),
            ));
        }
        let key = self.extract_string(&arr[1])?;
        let members: Vec<String> = arr[2..]
            .iter()
            .map(|v| self.extract_string(v))
            .collect::<Result<Vec<String>>>()?;
        Ok(Command::GeoPos(key, members))
    }

    /// 解析 GEOSEARCH 命令
    pub(crate) fn parse_geosearch(&self, arr: &[RespValue]) -> Result<Command> {
        if arr.len() < 7 {
            return Err(AppError::Command("GEOSEARCH 命令参数数量错误".to_string()));
        }
        let key = self.extract_string(&arr[1])?;

        let mut idx = 2;
        let center_lon;
        let center_lat;
        let mut by_radius: Option<f64> = None;
        let mut by_box: Option<(f64, f64)> = None;
        let mut order: Option<String> = None;
        let mut count = 0usize;
        let mut withcoord = false;
        let mut withdist = false;
        let mut withhash = false;

        // FROMMEMBER member | FROMLONLAT lon lat
        let from_type = self.extract_string(&arr[idx])?.to_ascii_uppercase();
        if from_type == "FROMMEMBER" {
            return Err(AppError::Command(
                "GEOSEARCH FROMMEMBER 暂不支持，请使用 FROMLONLAT".to_string(),
            ));
        } else if from_type == "FROMLONLAT" {
            idx += 1;
            center_lon = self
                .extract_string(&arr[idx])?
                .parse()
                .map_err(|_| AppError::Command("GEOSEARCH 的 lon 必须是数字".to_string()))?;
            idx += 1;
            center_lat = self
                .extract_string(&arr[idx])?
                .parse()
                .map_err(|_| AppError::Command("GEOSEARCH 的 lat 必须是数字".to_string()))?;
            idx += 1;
        } else {
            return Err(AppError::Command(format!(
                "GEOSEARCH 需要 FROMMEMBER 或 FROMLONLAT，得到: {}",
                from_type
            )));
        }

        // BYRADIUS radius unit | BYBOX width height unit
        let by_type = self.extract_string(&arr[idx])?.to_ascii_uppercase();
        if by_type == "BYRADIUS" {
            idx += 1;
            let radius: f64 = self
                .extract_string(&arr[idx])?
                .parse()
                .map_err(|_| AppError::Command("GEOSEARCH 的 radius 必须是数字".to_string()))?;
            idx += 1;
            let unit = self.extract_string(&arr[idx])?.to_ascii_lowercase();
            let radius_m = match unit.as_str() {
                "m" => radius,
                "km" => radius * 1000.0,
                "mi" => radius * 1609.344,
                "ft" => radius * 0.3048,
                _ => {
                    return Err(AppError::Command(
                        "GEOSEARCH 单位必须是 m|km|mi|ft".to_string(),
                    ));
                }
            };
            by_radius = Some(radius_m);
            idx += 1;
        } else if by_type == "BYBOX" {
            idx += 1;
            let width: f64 = self
                .extract_string(&arr[idx])?
                .parse()
                .map_err(|_| AppError::Command("GEOSEARCH 的 width 必须是数字".to_string()))?;
            idx += 1;
            let height: f64 = self
                .extract_string(&arr[idx])?
                .parse()
                .map_err(|_| AppError::Command("GEOSEARCH 的 height 必须是数字".to_string()))?;
            idx += 1;
            let unit = self.extract_string(&arr[idx])?.to_ascii_lowercase();
            let width_m = match unit.as_str() {
                "m" => width,
                "km" => width * 1000.0,
                "mi" => width * 1609.344,
                "ft" => width * 0.3048,
                _ => {
                    return Err(AppError::Command(
                        "GEOSEARCH 单位必须是 m|km|mi|ft".to_string(),
                    ));
                }
            };
            let height_m = match unit.as_str() {
                "m" => height,
                "km" => height * 1000.0,
                "mi" => height * 1609.344,
                "ft" => height * 0.3048,
                _ => {
                    return Err(AppError::Command(
                        "GEOSEARCH 单位必须是 m|km|mi|ft".to_string(),
                    ));
                }
            };
            by_box = Some((width_m, height_m));
            idx += 1;
        } else {
            return Err(AppError::Command(format!(
                "GEOSEARCH 需要 BYRADIUS 或 BYBOX，得到: {}",
                by_type
            )));
        }

        // 可选参数
        while idx < arr.len() {
            let flag = self.extract_string(&arr[idx])?.to_ascii_uppercase();
            if flag == "ASC" || flag == "DESC" {
                order = Some(flag);
                idx += 1;
            } else if flag == "COUNT" {
                idx += 1;
                if idx >= arr.len() {
                    return Err(AppError::Command("GEOSEARCH COUNT 缺少数量".to_string()));
                }
                count = self
                    .extract_string(&arr[idx])?
                    .parse()
                    .map_err(|_| AppError::Command("GEOSEARCH COUNT 必须是整数".to_string()))?;
                idx += 1;
            } else if flag == "WITHCOORD" {
                withcoord = true;
                idx += 1;
            } else if flag == "WITHDIST" {
                withdist = true;
                idx += 1;
            } else if flag == "WITHHASH" {
                withhash = true;
                idx += 1;
            } else {
                return Err(AppError::Command(format!("GEOSEARCH 未知参数: {}", flag)));
            }
        }

        Ok(Command::GeoSearch(
            key, center_lon, center_lat, by_radius, by_box, order, count, withcoord, withdist,
            withhash,
        ))
    }

    /// 解析 GEORADIUS 命令：GEORADIUS key longitude latitude radius m|km|ft|mi [WITHCOORD] [WITHDIST] [WITHHASH] [COUNT count] [ASC|DESC] [STORE key] [STOREDIST key]
    pub(crate) fn parse_georadius(&self, arr: &[RespValue]) -> Result<Command> {
        if arr.len() < 6 {
            return Err(AppError::Command("GEORADIUS 命令参数数量错误".to_string()));
        }
        let key = self.extract_string(&arr[1])?;
        let lon: f64 = self
            .extract_string(&arr[2])?
            .parse()
            .map_err(|_| AppError::Command("GEORADIUS 的 longitude 必须是数字".to_string()))?;
        let lat: f64 = self
            .extract_string(&arr[3])?
            .parse()
            .map_err(|_| AppError::Command("GEORADIUS 的 latitude 必须是数字".to_string()))?;
        let radius: f64 = self
            .extract_string(&arr[4])?
            .parse()
            .map_err(|_| AppError::Command("GEORADIUS 的 radius 必须是数字".to_string()))?;
        let unit = self.extract_string(&arr[5])?.to_ascii_lowercase();
        let radius_m = match unit.as_str() {
            "m" => radius,
            "km" => radius * 1000.0,
            "mi" => radius * 1609.344,
            "ft" => radius * 0.3048,
            _ => {
                return Err(AppError::Command(
                    "GEORADIUS 单位必须是 m|km|mi|ft".to_string(),
                ));
            }
        };

        let mut idx = 6;
        let mut withcoord = false;
        let mut withdist = false;
        let mut withhash = false;
        let mut count = 0usize;
        let mut order: Option<String> = None;
        let mut store_key: Option<String> = None;
        let mut store_dist_key: Option<String> = None;

        while idx < arr.len() {
            let flag = self.extract_string(&arr[idx])?.to_ascii_uppercase();
            if flag == "WITHCOORD" {
                withcoord = true;
                idx += 1;
            } else if flag == "WITHDIST" {
                withdist = true;
                idx += 1;
            } else if flag == "WITHHASH" {
                withhash = true;
                idx += 1;
            } else if flag == "COUNT" {
                idx += 1;
                if idx >= arr.len() {
                    return Err(AppError::Command("GEORADIUS COUNT 缺少数量".to_string()));
                }
                count = self
                    .extract_string(&arr[idx])?
                    .parse()
                    .map_err(|_| AppError::Command("GEORADIUS COUNT 必须是整数".to_string()))?;
                idx += 1;
            } else if flag == "ASC" || flag == "DESC" {
                order = Some(flag);
                idx += 1;
            } else if flag == "STORE" {
                idx += 1;
                if idx >= arr.len() {
                    return Err(AppError::Command("GEORADIUS STORE 缺少 key".to_string()));
                }
                store_key = Some(self.extract_string(&arr[idx])?);
                idx += 1;
            } else if flag == "STOREDIST" {
                idx += 1;
                if idx >= arr.len() {
                    return Err(AppError::Command(
                        "GEORADIUS STOREDIST 缺少 key".to_string(),
                    ));
                }
                store_dist_key = Some(self.extract_string(&arr[idx])?);
                idx += 1;
            } else {
                return Err(AppError::Command(format!("GEORADIUS 未知参数: {}", flag)));
            }
        }

        Ok(Command::GeoRadius(
            key,
            lon,
            lat,
            radius_m,
            unit,
            withcoord,
            withdist,
            withhash,
            count,
            order,
            store_key,
            store_dist_key,
        ))
    }

    /// 解析 GEORADIUSBYMEMBER 命令：GEORADIUSBYMEMBER key member radius m|km|ft|mi [WITHCOORD] [WITHDIST] [WITHHASH] [COUNT count] [ASC|DESC]
    pub(crate) fn parse_georadiusbymember(&self, arr: &[RespValue]) -> Result<Command> {
        if arr.len() < 5 {
            return Err(AppError::Command(
                "GEORADIUSBYMEMBER 命令参数数量错误".to_string(),
            ));
        }
        let key = self.extract_string(&arr[1])?;
        let member = self.extract_string(&arr[2])?;
        let radius: f64 = self
            .extract_string(&arr[3])?
            .parse()
            .map_err(|_| AppError::Command("GEORADIUSBYMEMBER 的 radius 必须是数字".to_string()))?;
        let unit = self.extract_string(&arr[4])?.to_ascii_lowercase();
        let radius_m = match unit.as_str() {
            "m" => radius,
            "km" => radius * 1000.0,
            "mi" => radius * 1609.344,
            "ft" => radius * 0.3048,
            _ => {
                return Err(AppError::Command(
                    "GEORADIUSBYMEMBER 单位必须是 m|km|mi|ft".to_string(),
                ));
            }
        };

        let mut idx = 5;
        let mut withcoord = false;
        let mut withdist = false;
        let mut withhash = false;
        let mut count = 0usize;
        let mut order: Option<String> = None;

        while idx < arr.len() {
            let flag = self.extract_string(&arr[idx])?.to_ascii_uppercase();
            if flag == "WITHCOORD" {
                withcoord = true;
                idx += 1;
            } else if flag == "WITHDIST" {
                withdist = true;
                idx += 1;
            } else if flag == "WITHHASH" {
                withhash = true;
                idx += 1;
            } else if flag == "COUNT" {
                idx += 1;
                if idx >= arr.len() {
                    return Err(AppError::Command(
                        "GEORADIUSBYMEMBER COUNT 缺少数量".to_string(),
                    ));
                }
                count = self.extract_string(&arr[idx])?.parse().map_err(|_| {
                    AppError::Command("GEORADIUSBYMEMBER COUNT 必须是整数".to_string())
                })?;
                idx += 1;
            } else if flag == "ASC" || flag == "DESC" {
                order = Some(flag);
                idx += 1;
            } else {
                return Err(AppError::Command(format!(
                    "GEORADIUSBYMEMBER 未知参数: {}",
                    flag
                )));
            }
        }

        Ok(Command::GeoRadiusByMember(
            key, member, radius_m, unit, withcoord, withdist, withhash, count, order,
        ))
    }

    /// 解析 GEORADIUS_RO 命令：GEORADIUS_RO key longitude latitude radius m|km|ft|mi [WITHCOORD] [WITHDIST] [WITHHASH] [COUNT count] [ASC|DESC]
    pub(crate) fn parse_georadius_ro(&self, arr: &[RespValue]) -> Result<Command> {
        if arr.len() < 6 {
            return Err(AppError::Command(
                "GEORADIUS_RO 命令参数数量错误".to_string(),
            ));
        }
        let key = self.extract_string(&arr[1])?;
        let lon: f64 = self
            .extract_string(&arr[2])?
            .parse()
            .map_err(|_| AppError::Command("GEORADIUS_RO 的 longitude 必须是数字".to_string()))?;
        let lat: f64 = self
            .extract_string(&arr[3])?
            .parse()
            .map_err(|_| AppError::Command("GEORADIUS_RO 的 latitude 必须是数字".to_string()))?;
        let radius: f64 = self
            .extract_string(&arr[4])?
            .parse()
            .map_err(|_| AppError::Command("GEORADIUS_RO 的 radius 必须是数字".to_string()))?;
        let unit = self.extract_string(&arr[5])?.to_ascii_lowercase();
        let radius_m = match unit.as_str() {
            "m" => radius,
            "km" => radius * 1000.0,
            "mi" => radius * 1609.344,
            "ft" => radius * 0.3048,
            _ => {
                return Err(AppError::Command(
                    "GEORADIUS_RO 单位必须是 m|km|mi|ft".to_string(),
                ));
            }
        };

        let mut idx = 6;
        let mut withcoord = false;
        let mut withdist = false;
        let mut withhash = false;
        let mut count = 0usize;
        let mut order: Option<String> = None;

        while idx < arr.len() {
            let flag = self.extract_string(&arr[idx])?.to_ascii_uppercase();
            if flag == "WITHCOORD" {
                withcoord = true;
                idx += 1;
            } else if flag == "WITHDIST" {
                withdist = true;
                idx += 1;
            } else if flag == "WITHHASH" {
                withhash = true;
                idx += 1;
            } else if flag == "COUNT" {
                idx += 1;
                if idx >= arr.len() {
                    return Err(AppError::Command("GEORADIUS_RO COUNT 缺少数量".to_string()));
                }
                count = self
                    .extract_string(&arr[idx])?
                    .parse()
                    .map_err(|_| AppError::Command("GEORADIUS_RO COUNT 必须是整数".to_string()))?;
                idx += 1;
            } else if flag == "ASC" || flag == "DESC" {
                order = Some(flag);
                idx += 1;
            } else if flag == "STORE" || flag == "STOREDIST" {
                return Err(AppError::Command(format!("GEORADIUS_RO 不支持 {}", flag)));
            } else {
                return Err(AppError::Command(format!(
                    "GEORADIUS_RO 未知参数: {}",
                    flag
                )));
            }
        }

        Ok(Command::GeoRadiusRo(
            key, lon, lat, radius_m, unit, withcoord, withdist, withhash, count, order,
        ))
    }

    /// 解析 GEORADIUSBYMEMBER_RO 命令：GEORADIUSBYMEMBER_RO key member radius m|km|ft|mi [WITHCOORD] [WITHDIST] [WITHHASH] [COUNT count] [ASC|DESC]
    pub(crate) fn parse_georadiusbymember_ro(&self, arr: &[RespValue]) -> Result<Command> {
        if arr.len() < 5 {
            return Err(AppError::Command(
                "GEORADIUSBYMEMBER_RO 命令参数数量错误".to_string(),
            ));
        }
        let key = self.extract_string(&arr[1])?;
        let member = self.extract_string(&arr[2])?;
        let radius: f64 = self.extract_string(&arr[3])?.parse().map_err(|_| {
            AppError::Command("GEORADIUSBYMEMBER_RO 的 radius 必须是数字".to_string())
        })?;
        let unit = self.extract_string(&arr[4])?.to_ascii_lowercase();
        let radius_m = match unit.as_str() {
            "m" => radius,
            "km" => radius * 1000.0,
            "mi" => radius * 1609.344,
            "ft" => radius * 0.3048,
            _ => {
                return Err(AppError::Command(
                    "GEORADIUSBYMEMBER_RO 单位必须是 m|km|mi|ft".to_string(),
                ));
            }
        };

        let mut idx = 5;
        let mut withcoord = false;
        let mut withdist = false;
        let mut withhash = false;
        let mut count = 0usize;
        let mut order: Option<String> = None;

        while idx < arr.len() {
            let flag = self.extract_string(&arr[idx])?.to_ascii_uppercase();
            if flag == "WITHCOORD" {
                withcoord = true;
                idx += 1;
            } else if flag == "WITHDIST" {
                withdist = true;
                idx += 1;
            } else if flag == "WITHHASH" {
                withhash = true;
                idx += 1;
            } else if flag == "COUNT" {
                idx += 1;
                if idx >= arr.len() {
                    return Err(AppError::Command(
                        "GEORADIUSBYMEMBER_RO COUNT 缺少数量".to_string(),
                    ));
                }
                count = self.extract_string(&arr[idx])?.parse().map_err(|_| {
                    AppError::Command("GEORADIUSBYMEMBER_RO COUNT 必须是整数".to_string())
                })?;
                idx += 1;
            } else if flag == "ASC" || flag == "DESC" {
                order = Some(flag);
                idx += 1;
            } else if flag == "STORE" || flag == "STOREDIST" {
                return Err(AppError::Command(format!(
                    "GEORADIUSBYMEMBER_RO 不支持 {}",
                    flag
                )));
            } else {
                return Err(AppError::Command(format!(
                    "GEORADIUSBYMEMBER_RO 未知参数: {}",
                    flag
                )));
            }
        }

        Ok(Command::GeoRadiusByMemberRo(
            key, member, radius_m, unit, withcoord, withdist, withhash, count, order,
        ))
    }

    /// 解析 GEOSEARCHSTORE 命令
    pub(crate) fn parse_geosearchstore(&self, arr: &[RespValue]) -> Result<Command> {
        if arr.len() < 8 {
            return Err(AppError::Command(
                "GEOSEARCHSTORE 命令参数数量错误".to_string(),
            ));
        }
        let destination = self.extract_string(&arr[1])?;
        let source = self.extract_string(&arr[2])?;

        let mut idx = 3;
        let center_lon;
        let center_lat;
        let mut by_radius: Option<f64> = None;
        let mut by_box: Option<(f64, f64)> = None;
        let mut order: Option<String> = None;
        let mut count = 0usize;
        let mut storedist = false;

        // FROMMEMBER member | FROMLONLAT lon lat
        let from_type = self.extract_string(&arr[idx])?.to_ascii_uppercase();
        if from_type == "FROMMEMBER" {
            return Err(AppError::Command(
                "GEOSEARCHSTORE FROMMEMBER 暂不支持，请使用 FROMLONLAT".to_string(),
            ));
        } else if from_type == "FROMLONLAT" {
            idx += 1;
            center_lon = self
                .extract_string(&arr[idx])?
                .parse()
                .map_err(|_| AppError::Command("GEOSEARCHSTORE 的 lon 必须是数字".to_string()))?;
            idx += 1;
            center_lat = self
                .extract_string(&arr[idx])?
                .parse()
                .map_err(|_| AppError::Command("GEOSEARCHSTORE 的 lat 必须是数字".to_string()))?;
            idx += 1;
        } else {
            return Err(AppError::Command(format!(
                "GEOSEARCHSTORE 需要 FROMMEMBER 或 FROMLONLAT，得到: {}",
                from_type
            )));
        }

        // BYRADIUS radius unit | BYBOX width height unit
        let by_type = self.extract_string(&arr[idx])?.to_ascii_uppercase();
        if by_type == "BYRADIUS" {
            idx += 1;
            let radius: f64 = self.extract_string(&arr[idx])?.parse().map_err(|_| {
                AppError::Command("GEOSEARCHSTORE 的 radius 必须是数字".to_string())
            })?;
            idx += 1;
            let unit = self.extract_string(&arr[idx])?.to_ascii_lowercase();
            let radius_m = match unit.as_str() {
                "m" => radius,
                "km" => radius * 1000.0,
                "mi" => radius * 1609.344,
                "ft" => radius * 0.3048,
                _ => {
                    return Err(AppError::Command(
                        "GEOSEARCHSTORE 单位必须是 m|km|mi|ft".to_string(),
                    ));
                }
            };
            by_radius = Some(radius_m);
            idx += 1;
        } else if by_type == "BYBOX" {
            idx += 1;
            let width: f64 = self
                .extract_string(&arr[idx])?
                .parse()
                .map_err(|_| AppError::Command("GEOSEARCHSTORE 的 width 必须是数字".to_string()))?;
            idx += 1;
            let height: f64 = self.extract_string(&arr[idx])?.parse().map_err(|_| {
                AppError::Command("GEOSEARCHSTORE 的 height 必须是数字".to_string())
            })?;
            idx += 1;
            let unit = self.extract_string(&arr[idx])?.to_ascii_lowercase();
            let width_m = match unit.as_str() {
                "m" => width,
                "km" => width * 1000.0,
                "mi" => width * 1609.344,
                "ft" => width * 0.3048,
                _ => {
                    return Err(AppError::Command(
                        "GEOSEARCHSTORE 单位必须是 m|km|mi|ft".to_string(),
                    ));
                }
            };
            let height_m = match unit.as_str() {
                "m" => height,
                "km" => height * 1000.0,
                "mi" => height * 1609.344,
                "ft" => height * 0.3048,
                _ => {
                    return Err(AppError::Command(
                        "GEOSEARCHSTORE 单位必须是 m|km|mi|ft".to_string(),
                    ));
                }
            };
            by_box = Some((width_m, height_m));
            idx += 1;
        } else {
            return Err(AppError::Command(format!(
                "GEOSEARCHSTORE 需要 BYRADIUS 或 BYBOX，得到: {}",
                by_type
            )));
        }

        // 可选参数
        while idx < arr.len() {
            let flag = self.extract_string(&arr[idx])?.to_ascii_uppercase();
            if flag == "ASC" || flag == "DESC" {
                order = Some(flag);
                idx += 1;
            } else if flag == "COUNT" {
                idx += 1;
                if idx >= arr.len() {
                    return Err(AppError::Command(
                        "GEOSEARCHSTORE COUNT 缺少数量".to_string(),
                    ));
                }
                count = self.extract_string(&arr[idx])?.parse().map_err(|_| {
                    AppError::Command("GEOSEARCHSTORE COUNT 必须是整数".to_string())
                })?;
                idx += 1;
            } else if flag == "STOREDIST" {
                storedist = true;
                idx += 1;
            } else {
                return Err(AppError::Command(format!(
                    "GEOSEARCHSTORE 未知参数: {}",
                    flag
                )));
            }
        }

        Ok(Command::GeoSearchStore(
            destination,
            source,
            center_lon,
            center_lat,
            by_radius,
            by_box,
            order,
            count,
            storedist,
        ))
    }
}
