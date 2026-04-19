// Keyspace 通知模块，实现 Redis 风格的键空间事件发布

use std::sync::{Arc, RwLock};

use bytes::Bytes;

use crate::command::Command;
use crate::pubsub::PubSubManager;

/// Keyspace 事件类型位掩码
pub const NOTIFY_KEYSPACE: u16 = 1 << 0; // K
pub const NOTIFY_KEYEVENT: u16 = 1 << 1; // E
pub const NOTIFY_GENERIC: u16 = 1 << 2;  // g
pub const NOTIFY_STRING: u16 = 1 << 3;   // $
pub const NOTIFY_LIST: u16 = 1 << 4;     // l
pub const NOTIFY_SET: u16 = 1 << 5;      // s
pub const NOTIFY_HASH: u16 = 1 << 6;     // h
pub const NOTIFY_ZSET: u16 = 1 << 7;     // z
pub const NOTIFY_EXPIRED: u16 = 1 << 8;  // x
pub const NOTIFY_EVICTED: u16 = 1 << 9;  // e

/// 所有数据类型的掩码（不含 K/E/x/e）
const NOTIFY_ALL_TYPES: u16 = NOTIFY_GENERIC
    | NOTIFY_STRING
    | NOTIFY_LIST
    | NOTIFY_SET
    | NOTIFY_HASH
    | NOTIFY_ZSET
    | NOTIFY_EXPIRED
    | NOTIFY_EVICTED;

/// Keyspace 事件配置
#[derive(Debug, Clone)]
pub struct NotifyKeyspaceEvents {
    /// 位掩码
    flags: u16,
    /// 原始配置字符串（用于 CONFIG GET 返回）
    raw: String,
}

impl NotifyKeyspaceEvents {
    /// 创建空配置（默认关闭）
    pub fn empty() -> Self {
        Self {
            flags: 0,
            raw: String::new(),
        }
    }

    /// 从配置字符串解析
    pub fn from_str(s: &str) -> Self {
        let mut flags = 0u16;
        for c in s.chars() {
            match c {
                'K' => flags |= NOTIFY_KEYSPACE,
                'E' => flags |= NOTIFY_KEYEVENT,
                'g' => flags |= NOTIFY_GENERIC,
                '$' => flags |= NOTIFY_STRING,
                'l' => flags |= NOTIFY_LIST,
                's' => flags |= NOTIFY_SET,
                'h' => flags |= NOTIFY_HASH,
                'z' => flags |= NOTIFY_ZSET,
                'x' => flags |= NOTIFY_EXPIRED,
                'e' => flags |= NOTIFY_EVICTED,
                'A' => flags |= NOTIFY_ALL_TYPES,
                _ => {}
            }
        }
        Self {
            flags,
            raw: s.to_string(),
        }
    }

    /// 获取原始配置字符串
    pub fn raw(&self) -> &str {
        &self.raw
    }

    /// 是否启用 keyspace 通知（K）
    pub fn keyspace_enabled(&self) -> bool {
        self.flags & NOTIFY_KEYSPACE != 0
    }

    /// 是否启用 keyevent 通知（E）
    pub fn keyevent_enabled(&self) -> bool {
        self.flags & NOTIFY_KEYEVENT != 0
    }

    /// 检查特定事件类型是否被启用
    pub fn type_enabled(&self, type_flag: u16) -> bool {
        self.flags & type_flag != 0
    }

    /// 判断是否对某个命令需要发送通知
    pub fn should_notify(&self, event_type: u16) -> bool {
        self.flags & event_type != 0
    }
}

impl Default for NotifyKeyspaceEvents {
    fn default() -> Self {
        Self::empty()
    }
}

/// 命令对应的事件信息
pub struct EventInfo {
    /// 事件名称（如 "set", "del"）
    pub name: &'static str,
    /// 事件类型掩码（如 NOTIFY_STRING, NOTIFY_GENERIC）
    pub type_flag: u16,
    /// 受影响的 key（如果有）
    pub key: Option<String>,
}

/// Keyspace 通知器
#[derive(Debug, Clone)]
pub struct KeyspaceNotifier {
    /// PubSub 管理器
    pubsub: PubSubManager,
    /// 事件配置
    config: Arc<RwLock<NotifyKeyspaceEvents>>,
}

impl KeyspaceNotifier {
    /// 创建新的通知器
    pub fn new(pubsub: PubSubManager) -> Self {
        Self {
            pubsub,
            config: Arc::new(RwLock::new(NotifyKeyspaceEvents::empty())),
        }
    }

    /// 设置事件配置
    pub fn set_config(&self, config: NotifyKeyspaceEvents) {
        let mut guard = self.config.write().unwrap();
        *guard = config;
    }

    /// 获取当前配置
    pub fn config(&self) -> NotifyKeyspaceEvents {
        self.config.read().unwrap().clone()
    }

    /// 对命令发送 keyspace 通知
    pub fn notify_command(&self, cmd: &Command, db: usize) {
        let config = self.config.read().unwrap().clone();
        if config.flags == 0 {
            return;
        }

        let Some(info) = Self::extract_event_info(cmd) else {
            return;
        };

        if !config.should_notify(info.type_flag) {
            return;
        }

        if let Some(ref key) = info.key {
            if config.keyspace_enabled() {
                let channel = format!("__keyspace@{}__:{}", db, key);
                let _ = self.pubsub.publish(&channel, Bytes::from(info.name));
            }
            if config.keyevent_enabled() {
                let channel = format!("__keyevent@{}__:{}", db, info.name);
                let _ = self.pubsub.publish(&channel, Bytes::from(key.clone()));
            }
        }
    }

    /// 发送过期事件通知（由 storage 在读操作清理过期 key 时调用）
    pub fn notify_expired(&self, db: usize, key: &str) {
        let config = self.config.read().unwrap().clone();
        if config.flags == 0 {
            return;
        }
        if !config.should_notify(NOTIFY_EXPIRED) {
            return;
        }
        if config.keyspace_enabled() {
            let channel = format!("__keyspace@{}__:{}", db, key);
            let _ = self.pubsub.publish(&channel, Bytes::from("expired"));
        }
        if config.keyevent_enabled() {
            let channel = format!("__keyevent@{}__:{}", db, "expired");
            let _ = self.pubsub.publish(&channel, Bytes::from(key.to_string()));
        }
    }

    /// 发送淘汰事件通知（由 storage 在 evict 时调用）
    pub fn notify_evicted(&self, db: usize, key: &str) {
        let config = self.config.read().unwrap().clone();
        if config.flags == 0 {
            return;
        }
        if !config.should_notify(NOTIFY_EVICTED) {
            return;
        }
        if config.keyspace_enabled() {
            let channel = format!("__keyspace@{}__:{}", db, key);
            let _ = self.pubsub.publish(&channel, Bytes::from("evicted"));
        }
        if config.keyevent_enabled() {
            let channel = format!("__keyevent@{}__:{}", db, "evicted");
            let _ = self.pubsub.publish(&channel, Bytes::from(key.to_string()));
        }
    }

    /// 从命令提取事件信息
    fn extract_event_info(cmd: &Command) -> Option<EventInfo> {
        match cmd {
            // 字符串命令
            Command::Set(key, _, _) => Some(EventInfo {
                name: "set",
                type_flag: NOTIFY_STRING,
                key: Some(key.clone()),
            }),
            Command::SetEx(key, _, _) => Some(EventInfo {
                name: "set",
                type_flag: NOTIFY_STRING,
                key: Some(key.clone()),
            }),
            Command::SetExCmd(key, _, _) => Some(EventInfo {
                name: "set",
                type_flag: NOTIFY_STRING,
                key: Some(key.clone()),
            }),
            Command::PSetEx(key, _, _) => Some(EventInfo {
                name: "set",
                type_flag: NOTIFY_STRING,
                key: Some(key.clone()),
            }),
            Command::SetNx(key, _) => Some(EventInfo {
                name: "set",
                type_flag: NOTIFY_STRING,
                key: Some(key.clone()),
            }),
            Command::GetSet(key, _) => Some(EventInfo {
                name: "set",
                type_flag: NOTIFY_STRING,
                key: Some(key.clone()),
            }),
            Command::GetDel(key) => Some(EventInfo {
                name: "del",
                type_flag: NOTIFY_STRING,
                key: Some(key.clone()),
            }),
            Command::GetEx(key, _) => Some(EventInfo {
                name: "set",
                type_flag: NOTIFY_STRING,
                key: Some(key.clone()),
            }),
            Command::MSet(pairs) => pairs.first().map(|(k, _)| EventInfo {
                name: "set",
                type_flag: NOTIFY_STRING,
                key: Some(k.clone()),
            }),
            Command::MSetNx(pairs) => pairs.first().map(|(k, _)| EventInfo {
                name: "set",
                type_flag: NOTIFY_STRING,
                key: Some(k.clone()),
            }),
            Command::Append(key, _) => Some(EventInfo {
                name: "append",
                type_flag: NOTIFY_STRING,
                key: Some(key.clone()),
            }),
            Command::Incr(key) => Some(EventInfo {
                name: "incr",
                type_flag: NOTIFY_STRING,
                key: Some(key.clone()),
            }),
            Command::Decr(key) => Some(EventInfo {
                name: "decr",
                type_flag: NOTIFY_STRING,
                key: Some(key.clone()),
            }),
            Command::IncrBy(key, _) => Some(EventInfo {
                name: "incrby",
                type_flag: NOTIFY_STRING,
                key: Some(key.clone()),
            }),
            Command::DecrBy(key, _) => Some(EventInfo {
                name: "decrby",
                type_flag: NOTIFY_STRING,
                key: Some(key.clone()),
            }),
            Command::IncrByFloat(key, _) => Some(EventInfo {
                name: "incrbyfloat",
                type_flag: NOTIFY_STRING,
                key: Some(key.clone()),
            }),
            Command::SetRange(key, _, _) => Some(EventInfo {
                name: "setrange",
                type_flag: NOTIFY_STRING,
                key: Some(key.clone()),
            }),

            // 通用命令
            Command::Del(keys) => keys.first().map(|k| EventInfo {
                name: "del",
                type_flag: NOTIFY_GENERIC,
                key: Some(k.clone()),
            }),
            Command::Unlink(keys) => keys.first().map(|k| EventInfo {
                name: "del",
                type_flag: NOTIFY_GENERIC,
                key: Some(k.clone()),
            }),
            Command::FlushAll => Some(EventInfo {
                name: "flushdb",
                type_flag: NOTIFY_GENERIC,
                key: None,
            }),
            Command::Expire(key, _) => Some(EventInfo {
                name: "expire",
                type_flag: NOTIFY_GENERIC,
                key: Some(key.clone()),
            }),
            Command::PExpire(key, _) => Some(EventInfo {
                name: "expire",
                type_flag: NOTIFY_GENERIC,
                key: Some(key.clone()),
            }),
            Command::Persist(key) => Some(EventInfo {
                name: "persist",
                type_flag: NOTIFY_GENERIC,
                key: Some(key.clone()),
            }),
            Command::Rename(key, newkey) => Some(EventInfo {
                name: "rename_to",
                type_flag: NOTIFY_GENERIC,
                key: Some(newkey.clone()),
            }),
            Command::Copy(key, _, _) => Some(EventInfo {
                name: "copy",
                type_flag: NOTIFY_GENERIC,
                key: Some(key.clone()),
            }),
            Command::Restore(key, _, _, _) => Some(EventInfo {
                name: "restore",
                type_flag: NOTIFY_GENERIC,
                key: Some(key.clone()),
            }),

            // 列表命令
            Command::LPush(key, _) => Some(EventInfo {
                name: "lpush",
                type_flag: NOTIFY_LIST,
                key: Some(key.clone()),
            }),
            Command::RPush(key, _) => Some(EventInfo {
                name: "rpush",
                type_flag: NOTIFY_LIST,
                key: Some(key.clone()),
            }),
            Command::LPop(key) => Some(EventInfo {
                name: "lpop",
                type_flag: NOTIFY_LIST,
                key: Some(key.clone()),
            }),
            Command::RPop(key) => Some(EventInfo {
                name: "rpop",
                type_flag: NOTIFY_LIST,
                key: Some(key.clone()),
            }),
            Command::LSet(key, _, _) => Some(EventInfo {
                name: "lset",
                type_flag: NOTIFY_LIST,
                key: Some(key.clone()),
            }),
            Command::LInsert(key, _, _, _) => Some(EventInfo {
                name: "linsert",
                type_flag: NOTIFY_LIST,
                key: Some(key.clone()),
            }),
            Command::LRem(key, _, _) => Some(EventInfo {
                name: "lrem",
                type_flag: NOTIFY_LIST,
                key: Some(key.clone()),
            }),
            Command::LTrim(key, _, _) => Some(EventInfo {
                name: "ltrim",
                type_flag: NOTIFY_LIST,
                key: Some(key.clone()),
            }),
            Command::BLPop(keys, _) => keys.first().map(|k| EventInfo {
                name: "blpop",
                type_flag: NOTIFY_LIST,
                key: Some(k.clone()),
            }),
            Command::BRPop(keys, _) => keys.first().map(|k| EventInfo {
                name: "brpop",
                type_flag: NOTIFY_LIST,
                key: Some(k.clone()),
            }),
            Command::BLmove(key, _, _, _, _) => Some(EventInfo {
                name: "blmove",
                type_flag: NOTIFY_LIST,
                key: Some(key.clone()),
            }),
            Command::Lmove(key, _, _, _) => Some(EventInfo {
                name: "lmove",
                type_flag: NOTIFY_LIST,
                key: Some(key.clone()),
            }),

            // 哈希命令
            Command::HSet(key, _) => Some(EventInfo {
                name: "hset",
                type_flag: NOTIFY_HASH,
                key: Some(key.clone()),
            }),
            Command::HDel(key, _) => Some(EventInfo {
                name: "hdel",
                type_flag: NOTIFY_HASH,
                key: Some(key.clone()),
            }),
            Command::HMSet(key, _) => Some(EventInfo {
                name: "hset",
                type_flag: NOTIFY_HASH,
                key: Some(key.clone()),
            }),
            Command::HSetNx(key, _, _) => Some(EventInfo {
                name: "hset",
                type_flag: NOTIFY_HASH,
                key: Some(key.clone()),
            }),
            Command::HIncrBy(key, _, _) => Some(EventInfo {
                name: "hincrby",
                type_flag: NOTIFY_HASH,
                key: Some(key.clone()),
            }),
            Command::HIncrByFloat(key, _, _) => Some(EventInfo {
                name: "hincrbyfloat",
                type_flag: NOTIFY_HASH,
                key: Some(key.clone()),
            }),
            Command::HExpire(key, _, _) => Some(EventInfo {
                name: "hexpire",
                type_flag: NOTIFY_HASH,
                key: Some(key.clone()),
            }),
            Command::HPExpire(key, _, _) => Some(EventInfo {
                name: "hexpire",
                type_flag: NOTIFY_HASH,
                key: Some(key.clone()),
            }),
            Command::HExpireAt(key, _, _) => Some(EventInfo {
                name: "hexpire",
                type_flag: NOTIFY_HASH,
                key: Some(key.clone()),
            }),
            Command::HPExpireAt(key, _, _) => Some(EventInfo {
                name: "hexpire",
                type_flag: NOTIFY_HASH,
                key: Some(key.clone()),
            }),
            Command::HPersist(key, _) => Some(EventInfo {
                name: "hpersist",
                type_flag: NOTIFY_HASH,
                key: Some(key.clone()),
            }),

            // 集合命令
            Command::SAdd(key, _) => Some(EventInfo {
                name: "sadd",
                type_flag: NOTIFY_SET,
                key: Some(key.clone()),
            }),
            Command::SRem(key, _) => Some(EventInfo {
                name: "srem",
                type_flag: NOTIFY_SET,
                key: Some(key.clone()),
            }),
            Command::SPop(key, _) => Some(EventInfo {
                name: "spop",
                type_flag: NOTIFY_SET,
                key: Some(key.clone()),
            }),
            Command::SMove(key, _, _) => Some(EventInfo {
                name: "smove",
                type_flag: NOTIFY_SET,
                key: Some(key.clone()),
            }),
            Command::SInterStore(key, _) => Some(EventInfo {
                name: "sinterstore",
                type_flag: NOTIFY_SET,
                key: Some(key.clone()),
            }),
            Command::SUnionStore(key, _) => Some(EventInfo {
                name: "sunionstore",
                type_flag: NOTIFY_SET,
                key: Some(key.clone()),
            }),
            Command::SDiffStore(key, _) => Some(EventInfo {
                name: "sdiffstore",
                type_flag: NOTIFY_SET,
                key: Some(key.clone()),
            }),

            // 有序集合命令
            Command::ZAdd(key, _) => Some(EventInfo {
                name: "zadd",
                type_flag: NOTIFY_ZSET,
                key: Some(key.clone()),
            }),
            Command::ZRem(key, _) => Some(EventInfo {
                name: "zrem",
                type_flag: NOTIFY_ZSET,
                key: Some(key.clone()),
            }),
            Command::ZIncrBy(key, _, _) => Some(EventInfo {
                name: "zincrby",
                type_flag: NOTIFY_ZSET,
                key: Some(key.clone()),
            }),
            Command::ZPopMin(key, _) => Some(EventInfo {
                name: "zpopmin",
                type_flag: NOTIFY_ZSET,
                key: Some(key.clone()),
            }),
            Command::ZPopMax(key, _) => Some(EventInfo {
                name: "zpopmax",
                type_flag: NOTIFY_ZSET,
                key: Some(key.clone()),
            }),
            Command::ZUnionStore(key, _, _, _) => Some(EventInfo {
                name: "zunionstore",
                type_flag: NOTIFY_ZSET,
                key: Some(key.clone()),
            }),
            Command::ZInterStore(key, _, _, _) => Some(EventInfo {
                name: "zinterstore",
                type_flag: NOTIFY_ZSET,
                key: Some(key.clone()),
            }),
            Command::ZDiffStore(key, _) => Some(EventInfo {
                name: "zdiffstore",
                type_flag: NOTIFY_ZSET,
                key: Some(key.clone()),
            }),
            Command::ZRangeStore(key, _, _, _, _, _, _, _, _) => Some(EventInfo {
                name: "zrangestore",
                type_flag: NOTIFY_ZSET,
                key: Some(key.clone()),
            }),
            Command::ZMpop(keys, _, _) => keys.first().map(|k| EventInfo {
                name: "zmpop",
                type_flag: NOTIFY_ZSET,
                key: Some(k.clone()),
            }),
            Command::BZPopMin(keys, _) => keys.first().map(|k| EventInfo {
                name: "zpopmin",
                type_flag: NOTIFY_ZSET,
                key: Some(k.clone()),
            }),
            Command::BZPopMax(keys, _) => keys.first().map(|k| EventInfo {
                name: "zpopmax",
                type_flag: NOTIFY_ZSET,
                key: Some(k.clone()),
            }),
            Command::BZMpop(_, keys, _, _) => keys.first().map(|k| EventInfo {
                name: "zmpop",
                type_flag: NOTIFY_ZSET,
                key: Some(k.clone()),
            }),

            // 其他写命令归类为 generic
            Command::SetBit(key, _, _) => Some(EventInfo {
                name: "setbit",
                type_flag: NOTIFY_GENERIC,
                key: Some(key.clone()),
            }),
            Command::BitOp(_, _, _) => Some(EventInfo {
                name: "bitop",
                type_flag: NOTIFY_GENERIC,
                key: None,
            }),
            Command::BitField(key, _) => Some(EventInfo {
                name: "bitfield",
                type_flag: NOTIFY_GENERIC,
                key: Some(key.clone()),
            }),
            Command::PfAdd(key, _) => Some(EventInfo {
                name: "pfadd",
                type_flag: NOTIFY_GENERIC,
                key: Some(key.clone()),
            }),
            Command::PfMerge(key, _) => Some(EventInfo {
                name: "pfmerge",
                type_flag: NOTIFY_GENERIC,
                key: Some(key.clone()),
            }),
            Command::GeoAdd(key, _) => Some(EventInfo {
                name: "geoadd",
                type_flag: NOTIFY_GENERIC,
                key: Some(key.clone()),
            }),
            Command::GeoSearchStore(key, _, _, _, _, _, _, _, _) => Some(EventInfo {
                name: "geosearchstore",
                type_flag: NOTIFY_GENERIC,
                key: Some(key.clone()),
            }),
            Command::XAdd(key, _, _, _, _, _) => Some(EventInfo {
                name: "xadd",
                type_flag: NOTIFY_GENERIC,
                key: Some(key.clone()),
            }),
            Command::XDel(key, _) => Some(EventInfo {
                name: "xdel",
                type_flag: NOTIFY_GENERIC,
                key: Some(key.clone()),
            }),
            Command::XTrim(key, _, _) => Some(EventInfo {
                name: "xtrim",
                type_flag: NOTIFY_GENERIC,
                key: Some(key.clone()),
            }),
            Command::XSetId(key, _) => Some(EventInfo {
                name: "xsetid",
                type_flag: NOTIFY_GENERIC,
                key: Some(key.clone()),
            }),
            Command::Sort(key, _, _, _, _, _, _, store) => {
                if store.is_some() {
                    Some(EventInfo {
                        name: "sort_store",
                        type_flag: NOTIFY_GENERIC,
                        key: Some(key.clone()),
                    })
                } else {
                    None
                }
            }

            // 读命令不产生通知
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_config_empty() {
        let cfg = NotifyKeyspaceEvents::from_str("");
        assert!(!cfg.keyspace_enabled());
        assert!(!cfg.keyevent_enabled());
        assert!(!cfg.should_notify(NOTIFY_STRING));
    }

    #[test]
    fn test_parse_config_kea() {
        let cfg = NotifyKeyspaceEvents::from_str("KEA");
        assert!(cfg.keyspace_enabled());
        assert!(cfg.keyevent_enabled());
        assert!(cfg.should_notify(NOTIFY_STRING));
        assert!(cfg.should_notify(NOTIFY_LIST));
        assert!(cfg.should_notify(NOTIFY_GENERIC));
        assert!(cfg.should_notify(NOTIFY_EXPIRED));
        assert!(cfg.should_notify(NOTIFY_EVICTED));
    }

    #[test]
    fn test_parse_config_kg() {
        let cfg = NotifyKeyspaceEvents::from_str("Kg");
        assert!(cfg.keyspace_enabled());
        assert!(!cfg.keyevent_enabled());
        assert!(cfg.should_notify(NOTIFY_GENERIC));
        assert!(!cfg.should_notify(NOTIFY_STRING));
    }

    #[test]
    fn test_keyspace_notifier_notify_set() {
        let pubsub = PubSubManager::new();
        let notifier = KeyspaceNotifier::new(pubsub.clone());
        notifier.set_config(NotifyKeyspaceEvents::from_str("KE$"));

        let mut rx_ks = pubsub.subscribe("__keyspace@0__:mykey");
        let mut rx_ke = pubsub.subscribe("__keyevent@0__:set");

        let cmd = Command::Set(
            "mykey".to_string(),
            Bytes::from("val"),
            crate::storage::SetOptions::default(),
        );
        notifier.notify_command(&cmd, 0);

        assert_eq!(rx_ks.try_recv().unwrap(), Bytes::from("set"));
        assert_eq!(rx_ke.try_recv().unwrap(), Bytes::from("mykey"));
    }

    #[test]
    fn test_keyspace_only() {
        let pubsub = PubSubManager::new();
        let notifier = KeyspaceNotifier::new(pubsub.clone());
        notifier.set_config(NotifyKeyspaceEvents::from_str("K$"));

        let mut rx_ks = pubsub.subscribe("__keyspace@0__:mykey");
        let mut rx_ke = pubsub.subscribe("__keyevent@0__:set");

        let cmd = Command::Set(
            "mykey".to_string(),
            Bytes::from("val"),
            crate::storage::SetOptions::default(),
        );
        notifier.notify_command(&cmd, 0);

        assert_eq!(rx_ks.try_recv().unwrap(), Bytes::from("set"));
        assert!(rx_ke.try_recv().is_err()); // keyevent 未启用
    }

    #[test]
    fn test_keyevent_only() {
        let pubsub = PubSubManager::new();
        let notifier = KeyspaceNotifier::new(pubsub.clone());
        notifier.set_config(NotifyKeyspaceEvents::from_str("E$"));

        let mut rx_ks = pubsub.subscribe("__keyspace@0__:mykey");
        let mut rx_ke = pubsub.subscribe("__keyevent@0__:set");

        let cmd = Command::Set(
            "mykey".to_string(),
            Bytes::from("val"),
            crate::storage::SetOptions::default(),
        );
        notifier.notify_command(&cmd, 0);

        assert!(rx_ks.try_recv().is_err()); // keyspace 未启用
        assert_eq!(rx_ke.try_recv().unwrap(), Bytes::from("mykey"));
    }

    #[test]
    fn test_type_filter_string_only() {
        let pubsub = PubSubManager::new();
        let notifier = KeyspaceNotifier::new(pubsub.clone());
        notifier.set_config(NotifyKeyspaceEvents::from_str("KE$"));

        let mut rx = pubsub.subscribe("__keyevent@0__:set");

        // 字符串命令应触发
        let cmd_set = Command::Set(
            "k".to_string(),
            Bytes::from("v"),
            crate::storage::SetOptions::default(),
        );
        notifier.notify_command(&cmd_set, 0);
        assert_eq!(rx.try_recv().unwrap(), Bytes::from("k"));

        // 列表命令不应触发（未配置 l）
        let cmd_lpush = Command::LPush("list".to_string(), vec![Bytes::from("a")]);
        notifier.notify_command(&cmd_lpush, 0);
        assert!(rx.try_recv().is_err());
    }

    #[test]
    fn test_expired_notification() {
        let pubsub = PubSubManager::new();
        let notifier = KeyspaceNotifier::new(pubsub.clone());
        notifier.set_config(NotifyKeyspaceEvents::from_str("KEx"));

        let mut rx_ks = pubsub.subscribe("__keyspace@0__:oldkey");
        let mut rx_ke = pubsub.subscribe("__keyevent@0__:expired");

        notifier.notify_expired(0, "oldkey");

        assert_eq!(rx_ks.try_recv().unwrap(), Bytes::from("expired"));
        assert_eq!(rx_ke.try_recv().unwrap(), Bytes::from("oldkey"));
    }

    #[test]
    fn test_evicted_notification() {
        let pubsub = PubSubManager::new();
        let notifier = KeyspaceNotifier::new(pubsub.clone());
        notifier.set_config(NotifyKeyspaceEvents::from_str("KEe"));

        let mut rx_ks = pubsub.subscribe("__keyspace@0__:bigkey");
        let mut rx_ke = pubsub.subscribe("__keyevent@0__:evicted");

        notifier.notify_evicted(0, "bigkey");

        assert_eq!(rx_ks.try_recv().unwrap(), Bytes::from("evicted"));
        assert_eq!(rx_ke.try_recv().unwrap(), Bytes::from("bigkey"));
    }
}
