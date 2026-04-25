//! Sorted Set 高级操作（ZUNIONSTORE/ZINTERSTORE/ZDIFF 等集合运算）

use crate::error::{AppError, Result};
use crate::storage::{StorageEngine, StorageValue, ZSetData};
use ordered_float::OrderedFloat;
use std::collections::HashMap;
use std::sync::Arc;

impl StorageEngine {
    pub fn zunionstore(
        &self,
        destination: &str,
        keys: &[String],
        weights: Option<&[f64]>,
        aggregate: &str,
    ) -> Result<usize> {
        let db = self.db();
        let mut union_scores: HashMap<String, Vec<f64>> = HashMap::new();

        for (idx, key) in keys.iter().enumerate() {
            let weight = weights.map(|w| w.get(idx).copied().unwrap_or(1.0)).unwrap_or(1.0);
            let map = db.inner.get_shard(key).read()
                .map_err(|e| AppError::Storage(format!("锁中毒: {}", e)))?;
            if let Some(v) = map.get(key) {
                if Self::is_key_expired(&db, key) {
                    continue;
                }
                if let StorageValue::ZSet(z) = v {
                    for (member, score) in &z.member_to_score {
                        union_scores
                            .entry(member.clone())
                            .or_default()
                            .push(*score * weight);
                    }
                }
            }
        }

        let result_len = union_scores.len();

        let mut result = ZSetData::new();
        for (member, scores) in union_scores {
            let final_score = match aggregate.to_ascii_uppercase().as_str() {
                "MIN" => scores.into_iter().fold(f64::INFINITY, f64::min),
                "MAX" => scores.into_iter().fold(f64::NEG_INFINITY, f64::max),
                _ => scores.iter().sum(),
            };
            result.add(member, final_score);
        }

        let mut map = db.inner.get_shard(destination).write()
            .map_err(|e| AppError::Storage(format!("锁中毒: {}", e)))?;
        map.insert(destination.to_string(), StorageValue::ZSet(result));
        self.bump_version(destination);
        self.touch(destination);
        Ok(result_len)
    }
    pub fn zinterstore(
        &self,
        destination: &str,
        keys: &[String],
        weights: Option<&[f64]>,
        aggregate: &str,
    ) -> Result<usize> {
        let db = self.db();

        if keys.is_empty() {
            let mut map = db.inner.get_shard(destination).write()
                .map_err(|e| AppError::Storage(format!("锁中毒: {}", e)))?;
            map.remove(destination);
            self.bump_version(destination);
            return Ok(0);
        }

        let mut first = true;
        let mut inter_members: HashMap<String, Vec<f64>> = HashMap::new();

        for (idx, key) in keys.iter().enumerate() {
            let weight = weights.map(|w| w.get(idx).copied().unwrap_or(1.0)).unwrap_or(1.0);
            let mut current_members: HashMap<String, f64> = HashMap::new();

            let map = db.inner.get_shard(key).read()
                .map_err(|e| AppError::Storage(format!("锁中毒: {}", e)))?;
            if let Some(v) = map.get(key)
                && !Self::is_key_expired(&db, key)
                    && let StorageValue::ZSet(z) = v {
                        for (member, score) in &z.member_to_score {
                            current_members.insert(member.clone(), *score * weight);
                        }
                    }

            if first {
                for (member, score) in current_members {
                    inter_members.insert(member, vec![score]);
                }
                first = false;
            } else {
                inter_members.retain(|member, scores| {
                    if let Some(score) = current_members.get(member) {
                        scores.push(*score);
                        true
                    } else {
                        false
                    }
                });
            }
        }

        let result_len = inter_members.len();

        let mut result = ZSetData::new();
        for (member, scores) in inter_members {
            let final_score = match aggregate.to_ascii_uppercase().as_str() {
                "MIN" => scores.into_iter().fold(f64::INFINITY, f64::min),
                "MAX" => scores.into_iter().fold(f64::NEG_INFINITY, f64::max),
                _ => scores.iter().sum(),
            };
            result.add(member, final_score);
        }

        let mut map = db.inner.get_shard(destination).write()
            .map_err(|e| AppError::Storage(format!("锁中毒: {}", e)))?;
        map.insert(destination.to_string(), StorageValue::ZSet(result));
        self.bump_version(destination);
        self.touch(destination);
        Ok(result_len)
    }
    pub fn zdiff(&self, keys: &[String], with_scores: bool) -> Result<Vec<(String, f64)>> {
        if keys.is_empty() {
            return Ok(vec![]);
        }

        let db = self.db();
        let mut first = true;
        let mut diff_members: HashMap<String, f64> = HashMap::new();

        for key in keys {
            let map = db.inner.get_shard(key).read()
                .map_err(|e| AppError::Storage(format!("锁中毒: {}", e)))?;
            let current: HashMap<String, f64> = if let Some(v) = map.get(key) {
                if Self::is_key_expired(&db, key) {
                    HashMap::new()
                } else if let StorageValue::ZSet(z) = v {
                    z.member_to_score.clone()
                } else {
                    HashMap::new()
                }
            } else {
                HashMap::new()
            };

            if first {
                diff_members = current;
                first = false;
            } else {
                diff_members.retain(|member, _| !current.contains_key(member));
            }
        }

        let mut result: Vec<(String, f64)> = diff_members.into_iter().collect();
        // 按分数升序、成员字典序排序
        result.sort_by(|a, b| {
            a.1.partial_cmp(&b.1)
                .unwrap_or(std::cmp::Ordering::Equal)
                .then_with(|| a.0.cmp(&b.0))
        });
        if !with_scores {
            // 即使 with_scores 为 false，为了接口统一仍返回 (String, f64)
            // 调用方根据需要只使用 member
        }
        Ok(result)
    }
    pub fn zdiffstore(&self, destination: &str, keys: &[String]) -> Result<usize> {
        let db = self.db();

        if keys.is_empty() {
            let mut map = db.inner.get_shard(destination).write()
                .map_err(|e| AppError::Storage(format!("锁中毒: {}", e)))?;
            map.remove(destination);
            self.bump_version(destination);
            return Ok(0);
        }

        let mut first = true;
        let mut diff_members: HashMap<String, f64> = HashMap::new();

        for key in keys {
            let map = db.inner.get_shard(key).read()
                .map_err(|e| AppError::Storage(format!("锁中毒: {}", e)))?;
            let current: HashMap<String, f64> = if let Some(v) = map.get(key) {
                if Self::is_key_expired(&db, key) {
                    HashMap::new()
                } else if let StorageValue::ZSet(z) = v {
                    z.member_to_score.clone()
                } else {
                    HashMap::new()
                }
            } else {
                HashMap::new()
            };

            if first {
                diff_members = current;
                first = false;
            } else {
                diff_members.retain(|member, _| !current.contains_key(member));
            }
        }

        let mut result = ZSetData::new();
        for (member, score) in diff_members {
            result.add(member, score);
        }
        let count = result.member_to_score.len();
        let mut map = db.inner.get_shard(destination).write()
            .map_err(|e| AppError::Storage(format!("锁中毒: {}", e)))?;
        map.insert(destination.to_string(), StorageValue::ZSet(result));
        self.bump_version(destination);
        self.touch(destination);
        Ok(count)
    }
    pub fn zinter(
        &self,
        keys: &[String],
        weights: Option<&[f64]>,
        aggregate: &str,
        _with_scores: bool,
    ) -> Result<Vec<(String, f64)>> {
        if keys.is_empty() {
            return Ok(vec![]);
        }

        let db = self.db();
        let mut first = true;
        let mut inter_members: HashMap<String, Vec<f64>> = HashMap::new();

        for (idx, key) in keys.iter().enumerate() {
            let weight = weights.map(|w| w.get(idx).copied().unwrap_or(1.0)).unwrap_or(1.0);
            let mut current_members: HashMap<String, f64> = HashMap::new();

            let map = db.inner.get_shard(key).read()
                .map_err(|e| AppError::Storage(format!("锁中毒: {}", e)))?;
            if let Some(v) = map.get(key)
                && !Self::is_key_expired(&db, key)
                    && let StorageValue::ZSet(z) = v {
                        for (member, score) in &z.member_to_score {
                            current_members.insert(member.clone(), *score * weight);
                        }
                    }

            if first {
                for (member, score) in current_members {
                    inter_members.insert(member, vec![score]);
                }
                first = false;
            } else {
                inter_members.retain(|member, scores| {
                    if let Some(score) = current_members.get(member) {
                        scores.push(*score);
                        true
                    } else {
                        false
                    }
                });
            }
        }

        let mut result: Vec<(String, f64)> = Vec::new();
        for (member, scores) in inter_members {
            let final_score = match aggregate.to_ascii_uppercase().as_str() {
                "MIN" => scores.into_iter().fold(f64::INFINITY, f64::min),
                "MAX" => scores.into_iter().fold(f64::NEG_INFINITY, f64::max),
                _ => scores.iter().sum(),
            };
            result.push((member, final_score));
        }
        result.sort_by(|a, b| {
            a.1.partial_cmp(&b.1)
                .unwrap_or(std::cmp::Ordering::Equal)
                .then_with(|| a.0.cmp(&b.0))
        });
        Ok(result)
    }
    pub fn zunion(
        &self,
        keys: &[String],
        weights: Option<&[f64]>,
        aggregate: &str,
        _with_scores: bool,
    ) -> Result<Vec<(String, f64)>> {
        let db = self.db();
        let mut union_scores: HashMap<String, Vec<f64>> = HashMap::new();

        for (idx, key) in keys.iter().enumerate() {
            let weight = weights.map(|w| w.get(idx).copied().unwrap_or(1.0)).unwrap_or(1.0);
            let map = db.inner.get_shard(key).read()
                .map_err(|e| AppError::Storage(format!("锁中毒: {}", e)))?;
            if let Some(v) = map.get(key) {
                if Self::is_key_expired(&db, key) {
                    continue;
                }
                if let StorageValue::ZSet(z) = v {
                    for (member, score) in &z.member_to_score {
                        union_scores
                            .entry(member.clone())
                            .or_default()
                            .push(*score * weight);
                    }
                }
            }
        }

        let mut result: Vec<(String, f64)> = Vec::new();
        for (member, scores) in union_scores {
            let final_score = match aggregate.to_ascii_uppercase().as_str() {
                "MIN" => scores.into_iter().fold(f64::INFINITY, f64::min),
                "MAX" => scores.into_iter().fold(f64::NEG_INFINITY, f64::max),
                _ => scores.iter().sum(),
            };
            result.push((member, final_score));
        }
        result.sort_by(|a, b| {
            a.1.partial_cmp(&b.1)
                .unwrap_or(std::cmp::Ordering::Equal)
                .then_with(|| a.0.cmp(&b.0))
        });
        Ok(result)
    }
    pub fn zrangestore(
        &self,
        dst: &str,
        src: &str,
        min: &str,
        max: &str,
        by_score: bool,
        by_lex: bool,
        rev: bool,
        limit_offset: usize,
        limit_count: usize,
    ) -> Result<usize> {
        let db = self.db();
        let result = {
            let map = db.inner.get_shard(src).read()
                .map_err(|e| AppError::Storage(format!("锁中毒: {}", e)))?;
            match map.get(src) {
                Some(v) => {
                    if Self::is_key_expired(&db, src) {
                        vec![]
                    } else {
                        Self::check_zset_type(v)?;
                        match v {
                            StorageValue::ZSet(z) => {
                                let pairs: Vec<(String, f64)> = if by_score {
                                    let min_score: f64 = min.parse().map_err(|_| {
                                        AppError::Command("ZRANGESTORE BYSCORE min 必须是数字".to_string())
                                    })?;
                                    let max_score: f64 = max.parse().map_err(|_| {
                                        AppError::Command("ZRANGESTORE BYSCORE max 必须是数字".to_string())
                                    })?;
                                    if rev {
                                        z.score_to_member
                                            .range(..=(OrderedFloat(max_score), String::from("\u{10FFFF}")))
                                            .rev()
                                            .filter(|((s, _), _)| s.into_inner() >= min_score)
                                            .map(|((s, m), _)| (m.clone(), s.into_inner()))
                                            .collect()
                                    } else {
                                        z.range_by_score(min_score, max_score)
                                    }
                                } else if by_lex {
                                    let lex_result = if rev {
                                        z.rev_range_by_lex(min, max)
                                    } else {
                                        z.range_by_lex(min, max)
                                    };
                                    lex_result?
                                        .into_iter()
                                        .map(|m| {
                                            let score = z.member_to_score.get(&m).copied().unwrap_or(0.0);
                                            (m, score)
                                        })
                                        .collect()
                                } else {
                                    let start: isize = min.parse().map_err(|_| {
                                        AppError::Command("ZRANGESTORE start 必须是整数".to_string())
                                    })?;
                                    let stop: isize = max.parse().map_err(|_| {
                                        AppError::Command("ZRANGESTORE stop 必须是整数".to_string())
                                    })?;
                                    if rev {
                                        z.rev_range_by_rank(start, stop)
                                    } else {
                                        z.range_by_rank(start, stop)
                                    }
                                };
                                // 应用 LIMIT
                                if limit_count > 0 {
                                    pairs.into_iter().skip(limit_offset).take(limit_count).collect()
                                } else {
                                    pairs
                                }
                            }
                            _ => unreachable!(),
                        }
                    }
                }
                None => vec![],
            }
        };

        let mut new_zset = ZSetData::new();
        for (member, score) in &result {
            new_zset.add(member.clone(), *score);
        }
        let count = new_zset.member_to_score.len();
        let mut map = db.inner.get_shard(dst).write()
            .map_err(|e| AppError::Storage(format!("锁中毒: {}", e)))?;
        if count > 0 {
            map.insert(dst.to_string(), StorageValue::ZSet(new_zset));
            self.bump_version(dst);
            self.touch(dst);
        } else {
            map.remove(dst);
            self.bump_version(dst);
        }
        Ok(count)
    }
    pub fn zmpop(
        &self,
        keys: &[String],
        min_or_max: bool,
        count: usize,
    ) -> Result<Option<(String, Vec<(String, f64)>)>> {
        self.evict_if_needed()?;
        let db = self.db();

        for key in keys {
            let mut map = db
                .inner
                .get_shard(key)
                .write()
                .map_err(|e| AppError::Storage(format!("锁中毒: {}", e)))?;
            if let Some(v) = map.get_mut(key) {
                if Self::is_key_expired(&db, key) {
                    continue;
                }
                Self::check_zset_type(v)?;
                match v {
                    StorageValue::ZSet(z) => {
                        let result = if min_or_max {
                            z.pop_min(count)
                        } else {
                            z.pop_max(count)
                        };
                        if z.member_to_score.is_empty() {
                            map.remove(key);
                        }
                        if !result.is_empty() {
                            self.bump_version(key);
                            self.touch(key);
                            return Ok(Some((key.clone(), result)));
                        }
                    }
                    _ => unreachable!(),
                }
            }
        }
        Ok(None)
    }
    pub async fn bzmpop(
        &self,
        keys: &[String],
        min_or_max: bool,
        count: usize,
        timeout: f64,
    ) -> Result<Option<(String, Vec<(String, f64)>)>> {
        // 先尝试非阻塞执行
        if let Some(result) = self.zmpop(keys, min_or_max, count)? {
            return Ok(Some(result));
        }

        let db = self.db();
        let notify = Arc::new(tokio::sync::Notify::new());

        // 注册等待者
        {
            let mut waiters_map = db.blocking_waiters.write().unwrap();
            for key in keys {
                waiters_map
                    .entry(key.clone())
                    .or_default()
                    .push(notify.clone());
            }
        }

        let deadline = if timeout > 0.0 {
            Some(tokio::time::Instant::now() + tokio::time::Duration::from_secs_f64(timeout))
        } else {
            None
        };

        loop {
            let wait_result = if let Some(deadline) = deadline {
                match tokio::time::timeout(deadline - tokio::time::Instant::now(), notify.notified()).await {
                    Ok(()) => true,
                    Err(_) => false,
                }
            } else {
                notify.notified().await;
                true
            };

            if !wait_result {
                break;
            }

            if let Some(result) = self.zmpop(keys, min_or_max, count)? {
                // 清理等待者
                let mut waiters_map = db.blocking_waiters.write().unwrap();
                for key in keys {
                    if let Some(list) = waiters_map.get_mut(key) {
                        list.retain(|n| !Arc::ptr_eq(n, &notify));
                        if list.is_empty() {
                            waiters_map.remove(key);
                        }
                    }
                }
                return Ok(Some(result));
            }
        }

        // 超时清理
        let mut waiters_map = db.blocking_waiters.write().unwrap();
        for key in keys {
            if let Some(list) = waiters_map.get_mut(key) {
                list.retain(|n| !Arc::ptr_eq(n, &notify));
                if list.is_empty() {
                    waiters_map.remove(key);
                }
            }
        }
        Ok(None)
    }
    pub async fn bzpopmin(
        &self,
        keys: &[String],
        timeout: f64,
    ) -> Result<Option<(String, String, f64)>> {
        if let Some((key, mut pairs)) = self.zmpop(keys, true, 1)?
            && let Some((member, score)) = pairs.pop() {
                return Ok(Some((key, member, score)));
            }

        let db = self.db();
        let notify = Arc::new(tokio::sync::Notify::new());

        {
            let mut waiters_map = db.blocking_waiters.write().unwrap();
            for key in keys {
                waiters_map
                    .entry(key.clone())
                    .or_default()
                    .push(notify.clone());
            }
        }

        let deadline = if timeout > 0.0 {
            Some(tokio::time::Instant::now() + tokio::time::Duration::from_secs_f64(timeout))
        } else {
            None
        };

        loop {
            let wait_result = if let Some(deadline) = deadline {
                match tokio::time::timeout(deadline - tokio::time::Instant::now(), notify.notified()).await {
                    Ok(()) => true,
                    Err(_) => false,
                }
            } else {
                notify.notified().await;
                true
            };

            if !wait_result {
                break;
            }

            if let Some((key, mut pairs)) = self.zmpop(keys, true, 1)?
                && let Some((member, score)) = pairs.pop() {
                    let mut waiters_map = db.blocking_waiters.write().unwrap();
                    for key in keys {
                        if let Some(list) = waiters_map.get_mut(key) {
                            list.retain(|n| !Arc::ptr_eq(n, &notify));
                            if list.is_empty() {
                                waiters_map.remove(key);
                            }
                        }
                    }
                    return Ok(Some((key, member, score)));
                }
        }

        let mut waiters_map = db.blocking_waiters.write().unwrap();
        for key in keys {
            if let Some(list) = waiters_map.get_mut(key) {
                list.retain(|n| !Arc::ptr_eq(n, &notify));
                if list.is_empty() {
                    waiters_map.remove(key);
                }
            }
        }
        Ok(None)
    }
    pub fn zintercard(&self, keys: &[String], limit: usize) -> Result<usize> {
        if keys.is_empty() {
            return Ok(0);
        }

        let db = self.db();
        let mut first = true;
        let mut inter_members: HashMap<String, Vec<f64>> = HashMap::new();

        for key in keys {
            let mut current_members: HashMap<String, f64> = HashMap::new();

            let map = db.inner.get_shard(key).read()
                .map_err(|e| AppError::Storage(format!("锁中毒: {}", e)))?;
            if let Some(v) = map.get(key)
                && !Self::is_key_expired(&db, key)
                    && let StorageValue::ZSet(z) = v {
                        for (member, score) in &z.member_to_score {
                            current_members.insert(member.clone(), *score);
                        }
                    }

            if first {
                for (member, score) in current_members {
                    inter_members.insert(member, vec![score]);
                }
                first = false;
            } else {
                inter_members.retain(|member, scores| {
                    if let Some(score) = current_members.get(member) {
                        scores.push(*score);
                        true
                    } else {
                        false
                    }
                });
            }

            if limit > 0 && inter_members.len() >= limit {
                return Ok(limit);
            }
        }

        Ok(inter_members.len())
    }

    pub async fn bzpopmax(
        &self,
        keys: &[String],
        timeout: f64,
    ) -> Result<Option<(String, String, f64)>> {
        if let Some((key, mut pairs)) = self.zmpop(keys, false, 1)?
            && let Some((member, score)) = pairs.pop() {
                return Ok(Some((key, member, score)));
            }

        let db = self.db();
        let notify = Arc::new(tokio::sync::Notify::new());

        {
            let mut waiters_map = db.blocking_waiters.write().unwrap();
            for key in keys {
                waiters_map
                    .entry(key.clone())
                    .or_default()
                    .push(notify.clone());
            }
        }

        let deadline = if timeout > 0.0 {
            Some(tokio::time::Instant::now() + tokio::time::Duration::from_secs_f64(timeout))
        } else {
            None
        };

        loop {
            let wait_result = if let Some(deadline) = deadline {
                match tokio::time::timeout(deadline - tokio::time::Instant::now(), notify.notified()).await {
                    Ok(()) => true,
                    Err(_) => false,
                }
            } else {
                notify.notified().await;
                true
            };

            if !wait_result {
                break;
            }

            if let Some((key, mut pairs)) = self.zmpop(keys, false, 1)?
                && let Some((member, score)) = pairs.pop() {
                    let mut waiters_map = db.blocking_waiters.write().unwrap();
                    for key in keys {
                        if let Some(list) = waiters_map.get_mut(key) {
                            list.retain(|n| !Arc::ptr_eq(n, &notify));
                            if list.is_empty() {
                                waiters_map.remove(key);
                            }
                        }
                    }
                    return Ok(Some((key, member, score)));
                }
        }

        let mut waiters_map = db.blocking_waiters.write().unwrap();
        for key in keys {
            if let Some(list) = waiters_map.get_mut(key) {
                list.retain(|n| !Arc::ptr_eq(n, &notify));
                if list.is_empty() {
                    waiters_map.remove(key);
                }
            }
        }
        Ok(None)
    }
}
