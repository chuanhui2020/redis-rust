// ACL 权限系统模块
// 实现 Redis 风格的访问控制列表，支持用户管理、命令权限、Key 模式和频道权限

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, RwLock};

use crate::error::{AppError, Result};
use rand::Rng;

/// ACL 用户
#[derive(Debug, Clone)]
pub struct AclUser {
    /// 用户名
    pub name: String,
    /// 密码哈希列表（SHA256）
    pub passwords: Vec<String>,
    /// 是否启用
    pub enabled: bool,
    /// 是否无密码（nopass）
    pub nopass: bool,
    /// 命令权限
    pub commands: AclCommands,
    /// Key 模式权限
    pub keys: AclKeys,
    /// 频道模式权限
    pub channels: AclChannels,
}

impl AclUser {
    /// 创建默认用户（所有权限，无密码）
    pub fn default_user() -> Self {
        Self {
            name: "default".to_string(),
            passwords: vec![],
            enabled: true,
            nopass: true,
            commands: AclCommands::AllCommands,
            keys: AclKeys::AllKeys,
            channels: AclChannels::AllChannels,
        }
    }

    /// 创建新用户，默认禁用、无权限
    pub fn new(name: &str) -> Self {
        Self {
            name: name.to_string(),
            passwords: vec![],
            enabled: false,
            nopass: false,
            commands: AclCommands::NoCommands,
            keys: AclKeys::Specific(vec![]),
            channels: AclChannels::Specific(vec![]),
        }
    }

    /// 返回用户的 ACL 规则字符串列表
    pub fn to_rules(&self) -> Vec<String> {
        let mut rules = Vec::new();
        if self.enabled {
            rules.push("on".to_string());
        } else {
            rules.push("off".to_string());
        }
        for pwd in &self.passwords {
            rules.push(format!(">#{}", pwd));
        }
        if self.nopass {
            rules.push("nopass".to_string());
        }
        match &self.commands {
            AclCommands::AllCommands => rules.push("allcommands".to_string()),
            AclCommands::NoCommands => rules.push("nocommands".to_string()),
            AclCommands::Specific { allow, deny } => {
                for cmd in allow {
                    rules.push(format!("+{}", cmd));
                }
                for cmd in deny {
                    rules.push(format!("-{}", cmd));
                }
            }
        }
        match &self.keys {
            AclKeys::AllKeys => rules.push("allkeys".to_string()),
            AclKeys::Specific(patterns) => {
                if patterns.is_empty() {
                    rules.push("resetkeys".to_string());
                } else {
                    for p in patterns {
                        rules.push(format!("~{}", p));
                    }
                }
            }
        }
        match &self.channels {
            AclChannels::AllChannels => rules.push("allchannels".to_string()),
            AclChannels::Specific(patterns) => {
                if patterns.is_empty() {
                    rules.push("resetchannels".to_string());
                } else {
                    for p in patterns {
                        rules.push(format!("&{}", p));
                    }
                }
            }
        }
        rules
    }
}

/// 命令权限
#[derive(Debug, Clone)]
pub enum AclCommands {
    /// 允许所有命令
    AllCommands,
    /// 禁止所有命令
    NoCommands,
    /// 具体的允许/禁止集合
    Specific {
        allow: HashSet<String>,
        deny: HashSet<String>,
    },
}

/// Key 模式权限
#[derive(Debug, Clone)]
pub enum AclKeys {
    /// 允许所有 Key
    AllKeys,
    /// 只允许匹配指定模式的 Key
    Specific(Vec<String>),
}

/// 频道模式权限
#[derive(Debug, Clone)]
pub enum AclChannels {
    /// 允许所有频道
    AllChannels,
    /// 只允许匹配指定模式的频道
    Specific(Vec<String>),
}

/// ACL 拒绝日志条目
#[derive(Debug, Clone)]
pub struct AclLogEntry {
    pub reason: String,
    pub context: String,
    pub object: String,
    pub username: String,
    pub age_ms: u64,
}

/// ACL 管理器
#[derive(Debug, Clone)]
pub struct AclManager {
    inner: Arc<RwLock<AclManagerInner>>,
}

#[derive(Debug)]
struct AclManagerInner {
    users: HashMap<String, AclUser>,
    log: Vec<AclLogEntry>,
}

impl AclManager {
    /// 创建新的 ACL 管理器，包含默认用户
    pub fn new() -> Self {
        let mut users = HashMap::new();
        users.insert("default".to_string(), AclUser::default_user());
        Self {
            inner: Arc::new(RwLock::new(AclManagerInner {
                users,
                log: Vec::new(),
            })),
        }
    }

    /// 设置/修改用户
    pub fn setuser(&self, name: &str, rules: &[&str]) -> Result<()> {
        let mut inner = self.inner.write().map_err(|e| {
            AppError::Storage(format!("ACL 锁中毒: {}", e))
        })?;

        let user = inner.users.entry(name.to_string()).or_insert_with(|| AclUser::new(name));

        for rule in rules {
            let rule = rule.trim();
            if rule.is_empty() {
                continue;
            }

            match rule {
                "on" => user.enabled = true,
                "off" => user.enabled = false,
                "nopass" => {
                    user.nopass = true;
                    user.passwords.clear();
                }
                "allcommands" => user.commands = AclCommands::AllCommands,
                "nocommands" => user.commands = AclCommands::NoCommands,
                "allkeys" => user.keys = AclKeys::AllKeys,
                "resetkeys" => user.keys = AclKeys::Specific(vec![]),
                "allchannels" => user.channels = AclChannels::AllChannels,
                "resetchannels" => user.channels = AclChannels::Specific(vec![]),
                _ => {
                    if let Some(password) = rule.strip_prefix('>') {
                        // 添加密码（明文，存储时转为 SHA256）
                        let hash = hash_password(password);
                        if !user.passwords.contains(&hash) {
                            user.passwords.push(hash);
                        }
                        user.nopass = false;
                    } else if let Some(password) = rule.strip_prefix('<') {
                        // 移除密码
                        let hash = hash_password(password);
                        user.passwords.retain(|p| p != &hash);
                    } else if let Some(cmd) = rule.strip_prefix('+') {
                        if cmd.starts_with('@') {
                            // 允许命令类别
                            let category = &cmd[1..];
                            let cmds = get_category_commands(category);
                            match &mut user.commands {
                                AclCommands::AllCommands => {}
                                AclCommands::NoCommands => {
                                    let mut allow = HashSet::new();
                                    for c in cmds {
                                        allow.insert(c.to_lowercase());
                                    }
                                    user.commands = AclCommands::Specific { allow, deny: HashSet::new() };
                                }
                                AclCommands::Specific { allow, .. } => {
                                    for c in cmds {
                                        allow.insert(c.to_lowercase());
                                    }
                                }
                            }
                        } else {
                            // 允许单个命令
                            match &mut user.commands {
                                AclCommands::AllCommands => {}
                                AclCommands::NoCommands => {
                                    let mut allow = HashSet::new();
                                    allow.insert(cmd.to_lowercase());
                                    user.commands = AclCommands::Specific { allow, deny: HashSet::new() };
                                }
                                AclCommands::Specific { allow, deny } => {
                                    allow.insert(cmd.to_lowercase());
                                    deny.remove(cmd);
                                }
                            }
                        }
                    } else if let Some(cmd) = rule.strip_prefix('-') {
                        if cmd.starts_with('@') {
                            // 禁止命令类别
                            let category = &cmd[1..];
                            let cmds = get_category_commands(category);
                            match &mut user.commands {
                                AclCommands::AllCommands => {
                                    // 从所有命令中排除这些
                                    let mut deny = HashSet::new();
                                    for c in cmds {
                                        deny.insert(c.to_lowercase());
                                    }
                                    user.commands = AclCommands::Specific {
                                        allow: get_all_commands(),
                                        deny,
                                    };
                                }
                                AclCommands::NoCommands => {}
                                AclCommands::Specific { allow, deny } => {
                                    for c in cmds {
                                        let c = c.to_lowercase();
                                        deny.insert(c.clone());
                                        allow.remove(&c);
                                    }
                                }
                            }
                        } else {
                            // 禁止单个命令
                            match &mut user.commands {
                                AclCommands::AllCommands => {
                                    let mut deny = HashSet::new();
                                    deny.insert(cmd.to_lowercase());
                                    user.commands = AclCommands::Specific {
                                        allow: get_all_commands(),
                                        deny,
                                    };
                                }
                                AclCommands::NoCommands => {}
                                AclCommands::Specific { allow, deny } => {
                                    allow.remove(cmd);
                                    deny.insert(cmd.to_lowercase());
                                }
                            }
                        }
                    } else if let Some(pattern) = rule.strip_prefix('~') {
                        match &mut user.keys {
                            AclKeys::AllKeys => {
                                user.keys = AclKeys::Specific(vec![pattern.to_string()]);
                            }
                            AclKeys::Specific(patterns) => {
                                if !patterns.contains(&pattern.to_string()) {
                                    patterns.push(pattern.to_string());
                                }
                            }
                        }
                    } else if let Some(pattern) = rule.strip_prefix('&') {
                        match &mut user.channels {
                            AclChannels::AllChannels => {
                                user.channels = AclChannels::Specific(vec![pattern.to_string()]);
                            }
                            AclChannels::Specific(patterns) => {
                                if !patterns.contains(&pattern.to_string()) {
                                    patterns.push(pattern.to_string());
                                }
                            }
                        }
                    }
                    // 其他未知规则忽略
                }
            }
        }

        Ok(())
    }

    /// 获取用户信息
    pub fn getuser(&self, name: &str) -> Result<Option<AclUser>> {
        let inner = self.inner.read().map_err(|e| {
            AppError::Storage(format!("ACL 锁中毒: {}", e))
        })?;
        Ok(inner.users.get(name).cloned())
    }

    /// 删除用户，返回删除数量
    pub fn deluser(&self, names: &[&str]) -> Result<usize> {
        let mut inner = self.inner.write().map_err(|e| {
            AppError::Storage(format!("ACL 锁中毒: {}", e))
        })?;
        let mut count = 0;
        for name in names {
            if *name == "default" {
                continue; // 不能删除默认用户
            }
            if inner.users.remove(*name).is_some() {
                count += 1;
            }
        }
        Ok(count)
    }

    /// 列出所有用户的 ACL 规则字符串
    pub fn list(&self) -> Result<Vec<String>> {
        let inner = self.inner.read().map_err(|e| {
            AppError::Storage(format!("ACL 锁中毒: {}", e))
        })?;
        let mut result = Vec::new();
        for (_, user) in &inner.users {
            let rules = user.to_rules().join(" ");
            result.push(format!("user {} {}", user.name, rules));
        }
        Ok(result)
    }

    /// 返回命令类别
    /// 无参数时返回所有类别名称，有参数时返回类别下的命令
    pub fn cat(&self, category: Option<&str>) -> Result<Vec<String>> {
        let categories: Vec<&str> = vec!["read", "write", "admin", "all"];
        match category {
            None => Ok(categories.into_iter().map(|s| s.to_string()).collect()),
            Some(cat) => Ok(get_category_commands(cat)),
        }
    }

    /// 返回当前用户名
    pub fn whoami(&self, username: &str) -> Result<String> {
        Ok(username.to_string())
    }

    /// 返回 ACL 拒绝日志
    pub fn log(&self, count: Option<usize>) -> Result<Vec<AclLogEntry>> {
        let inner = self.inner.read().map_err(|e| {
            AppError::Storage(format!("ACL 锁中毒: {}", e))
        })?;
        let n = count.unwrap_or(10);
        let result: Vec<AclLogEntry> = inner.log.iter().rev().take(n).cloned().collect();
        Ok(result)
    }

    /// 重置 ACL 日志
    pub fn log_reset(&self) -> Result<()> {
        let mut inner = self.inner.write().map_err(|e| {
            AppError::Storage(format!("ACL 锁中毒: {}", e))
        })?;
        inner.log.clear();
        Ok(())
    }

    /// 记录 ACL 拒绝日志
    pub fn log_deny(&self, username: &str, reason: &str, context: &str, object: &str) -> Result<()> {
        let mut inner = self.inner.write().map_err(|e| {
            AppError::Storage(format!("ACL 锁中毒: {}", e))
        })?;
        inner.log.push(AclLogEntry {
            reason: reason.to_string(),
            context: context.to_string(),
            object: object.to_string(),
            username: username.to_string(),
            age_ms: 0,
        });
        // 只保留最近 128 条日志
        if inner.log.len() > 128 {
            inner.log.remove(0);
        }
        Ok(())
    }

    /// 生成随机密码
    pub fn genpass(&self, bits: Option<usize>) -> Result<String> {
        let bits = bits.unwrap_or(256);
        if bits < 64 || bits > 4096 || bits % 8 != 0 {
            return Err(AppError::Command(
                "ACL GENPASS bits 必须是 64-4096 之间的 8 的倍数".to_string(),
            ));
        }
        let bytes = bits / 8;
        let mut rng = rand::thread_rng();
        let mut buf = vec![0u8; bytes];
        rng.fill(&mut buf[..]);
        Ok(hex::encode(&buf))
    }

    /// 检查命令权限
    pub fn check_command(&self, username: &str, cmd: &str, keys: &[&str]) -> Result<bool> {
        let inner = self.inner.read().map_err(|e| {
            AppError::Storage(format!("ACL 锁中毒: {}", e))
        })?;

        let user = match inner.users.get(username) {
            Some(u) => u,
            None => return Ok(false),
        };

        if !user.enabled {
            return Ok(false);
        }

        // 检查命令权限
        let allowed = match &user.commands {
            AclCommands::AllCommands => true,
            AclCommands::NoCommands => false,
            AclCommands::Specific { allow, deny } => {
                let cmd_lower = cmd.to_lowercase();
                if deny.contains(&cmd_lower) {
                    false
                } else {
                    allow.contains(&cmd_lower)
                }
            }
        };

        if !allowed {
            return Ok(false);
        }

        // 检查 Key 权限
        let keys_ok = match &user.keys {
            AclKeys::AllKeys => true,
            AclKeys::Specific(patterns) => {
                if patterns.is_empty() {
                    false
                } else {
                    keys.iter().all(|key| {
                        patterns.iter().any(|pattern| glob_match(pattern, key))
                    })
                }
            }
        };

        Ok(keys_ok)
    }

    /// 检查频道权限
    pub fn check_channel(&self, username: &str, channel: &str) -> Result<bool> {
        let inner = self.inner.read().map_err(|e| {
            AppError::Storage(format!("ACL 锁中毒: {}", e))
        })?;

        let user = match inner.users.get(username) {
            Some(u) => u,
            None => return Ok(false),
        };

        if !user.enabled {
            return Ok(false);
        }

        Ok(match &user.channels {
            AclChannels::AllChannels => true,
            AclChannels::Specific(patterns) => {
                if patterns.is_empty() {
                    false
                } else {
                    patterns.iter().any(|pattern| glob_match(pattern, channel))
                }
            }
        })
    }

    /// 验证用户名密码
    pub fn authenticate(&self, username: &str, password: &str) -> Result<bool> {
        let inner = self.inner.read().map_err(|e| {
            AppError::Storage(format!("ACL 锁中毒: {}", e))
        })?;

        let user = match inner.users.get(username) {
            Some(u) => u,
            None => return Ok(false),
        };

        if !user.enabled {
            return Ok(false);
        }

        if user.nopass {
            return Ok(true);
        }

        let hash = hash_password(password);
        Ok(user.passwords.contains(&hash))
    }
}

impl Default for AclManager {
    fn default() -> Self {
        Self::new()
    }
}

/// 密码哈希（使用 SHA1）
fn hash_password(input: &str) -> String {
    use sha1::{Sha1, Digest};
    let mut hasher = Sha1::new();
    hasher.update(input.as_bytes());
    format!("{:x}", hasher.finalize())
}

/// 获取命令类别中的所有命令
fn get_category_commands(category: &str) -> Vec<String> {
    let cmds: Vec<&str> = match category.to_lowercase().as_str() {
        "read" => vec![
            "get", "mget", "exists", "ttl", "pttl", "type", "strlen", "getrange",
            "hget", "hmget", "hgetall", "hkeys", "hvals", "hlen", "hexists", "hstrlen", "hscan",
            "lrange", "llen", "lindex",
            "smembers", "sismember", "scard", "srandmember", "sscan",
            "zrange", "zrevrange", "zrangebyscore", "zrevrangebyscore", "zrangebylex", "zrevrangebylex",
            "zcard", "zcount", "zlexcount", "zscore", "zmscore", "zrank", "zrevrank", "zscan",
            "xrange", "xrevrange", "xlen", "xread", "xreadgroup", "xinfo", "xpending",
            "keys", "scan", "dbsize", "info", "client", "command",
            "pfcount", "pfmerge",
            "bitcount", "bitpos", "getbit", "bitfield", "bitfield_ro",
            "geoadd", "geodist", "geohash", "geopos", "georadius", "georadiusbymember", "geosearch",
        ],
        "write" => vec![
            "set", "mset", "del", "expire", "pexpire", "expireat", "pexpireat", "persist",
            "incr", "decr", "incrby", "decrby", "append", "setnx", "setex", "psetex", "getset", "getdel",
            "hset", "hmset", "hsetnx", "hdel", "hincrby", "hincrbyfloat", "hexpire", "hpexpire",
            "lpush", "rpush", "lpop", "rpop", "lrem", "ltrim", "lset", "linsert", "lmove", "blmove",
            "sadd", "srem", "spop", "smove", "sdiffstore", "sinterstore", "sunionstore",
            "zadd", "zrem", "zincrby", "zunionstore", "zinterstore", "zdiffstore", "zrangestore",
            "xadd", "xtrim", "xdel", "xgroup", "xack", "xclaim", "xautoclaim",
            "rename", "renamenx", "flushall", "flushdb",
            "pfadd", "pfmerge",
            "setbit", "setrange",
            "copy", "move",
        ],
        "admin" => vec![
            "config", "debug", "shutdown", "bgsave", "bgrewriteaof", "save", "lastsave",
            "acl", "auth", "client", "slowlog", "monitor", "info", "role", "replicaof",
            "slaveof", "sync", "psync",
        ],
        "all" => {
            let all = get_all_commands();
            return all.into_iter().collect();
        }
        _ => vec![],
    };
    cmds.into_iter().map(|s| s.to_string()).collect()
}

/// 获取所有命令
fn get_all_commands() -> HashSet<String> {
    let mut all = HashSet::new();
    for cat in &["read", "write", "admin"] {
        for cmd in get_category_commands(cat) {
            all.insert(cmd);
        }
    }
    // 添加一些未分类的命令
    let extras = vec![
        "ping", "echo", "select", "quit", "multi", "exec", "discard", "watch", "unwatch",
        "subscribe", "unsubscribe", "psubscribe", "punsubscribe", "publish",
        "eval", "evalsha", "script", "slowlog", "time", "readonly", "readwrite",
        "memory", "latency", "module", "hello", "reset",
    ];
    for cmd in extras {
        all.insert(cmd.to_string());
    }
    all
}

/// 简化的 glob 匹配：支持 * 和 ?
fn glob_match(pattern: &str, text: &str) -> bool {
    let mut pattern_chars = pattern.chars().peekable();
    let mut text_chars = text.chars().peekable();

    while let Some(p) = pattern_chars.peek() {
        match p {
            '*' => {
                pattern_chars.next();
                if pattern_chars.peek().is_none() {
                    return true;
                }
                // 尝试匹配剩余部分
                let remaining_pattern: String = pattern_chars.collect();
                for i in 0..text.len() {
                    if glob_match(&remaining_pattern, &text[i..]) {
                        return true;
                    }
                }
                return false;
            }
            '?' => {
                pattern_chars.next();
                if text_chars.next().is_none() {
                    return false;
                }
            }
            _ => {
                let pc = pattern_chars.next().unwrap();
                let tc = match text_chars.next() {
                    Some(c) => c,
                    None => return false,
                };
                if pc != tc {
                    return false;
                }
            }
        }
    }

    text_chars.peek().is_none()
}

// 简单的 hex 编码（避免依赖 hex crate）
mod hex {
    pub fn encode(data: &[u8]) -> String {
        const HEX: &[u8; 16] = b"0123456789abcdef";
        let mut result = String::with_capacity(data.len() * 2);
        for &byte in data {
            result.push(HEX[(byte >> 4) as usize] as char);
            result.push(HEX[(byte & 0x0f) as usize] as char);
        }
        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_user() {
        let acl = AclManager::new();
        let user = acl.getuser("default").unwrap().unwrap();
        assert!(user.enabled);
        assert!(user.nopass);
        assert!(matches!(user.commands, AclCommands::AllCommands));
        assert!(matches!(user.keys, AclKeys::AllKeys));
    }

    #[test]
    fn test_setuser_create() {
        let acl = AclManager::new();
        acl.setuser("alice", &["on", ">password123", "+get", "+set", "~foo:*", "&chat:*"]).unwrap();

        let user = acl.getuser("alice").unwrap().unwrap();
        assert!(user.enabled);
        assert!(!user.nopass);
        assert_eq!(user.passwords.len(), 1);

        // 验证密码
        assert!(acl.authenticate("alice", "password123").unwrap());
        assert!(!acl.authenticate("alice", "wrong").unwrap());
    }

    #[test]
    fn test_command_permission() {
        let acl = AclManager::new();
        acl.setuser("bob", &["on", "nopass", "+get", "-set", "allkeys"]).unwrap();

        assert!(acl.check_command("bob", "get", &["key1"]).unwrap());
        assert!(!acl.check_command("bob", "set", &["key1"]).unwrap());
        assert!(!acl.check_command("bob", "del", &["key1"]).unwrap());
    }

    #[test]
    fn test_key_pattern_permission() {
        let acl = AclManager::new();
        acl.setuser("carol", &["on", "nopass", "allcommands", "~data:*", "~cache:*"]).unwrap();

        assert!(acl.check_command("carol", "get", &["data:foo"]).unwrap());
        assert!(acl.check_command("carol", "get", &["cache:bar"]).unwrap());
        assert!(!acl.check_command("carol", "get", &["other"]).unwrap());
    }

    #[test]
    fn test_category_permission() {
        let acl = AclManager::new();
        acl.setuser("dave", &["on", "nopass", "+@read", "allkeys"]).unwrap();

        assert!(acl.check_command("dave", "get", &["key"]).unwrap());
        assert!(acl.check_command("dave", "hget", &["key"]).unwrap());
        assert!(!acl.check_command("dave", "set", &["key"]).unwrap());
    }

    #[test]
    fn test_deluser() {
        let acl = AclManager::new();
        acl.setuser("eve", &["on", "nopass"]).unwrap();
        assert!(acl.getuser("eve").unwrap().is_some());

        let count = acl.deluser(&["eve"]).unwrap();
        assert_eq!(count, 1);
        assert!(acl.getuser("eve").unwrap().is_none());

        // 不能删除默认用户
        let count = acl.deluser(&["default"]).unwrap();
        assert_eq!(count, 0);
        assert!(acl.getuser("default").unwrap().is_some());
    }

    #[test]
    fn test_acl_cat() {
        let acl = AclManager::new();
        let cats = acl.cat(None).unwrap();
        assert!(cats.contains(&"read".to_string()));
        assert!(cats.contains(&"write".to_string()));
        assert!(cats.contains(&"admin".to_string()));

        let read_cmds = acl.cat(Some("read")).unwrap();
        assert!(read_cmds.contains(&"get".to_string()));
        assert!(read_cmds.contains(&"hget".to_string()));
    }

    #[test]
    fn test_genpass() {
        let acl = AclManager::new();
        let pass = acl.genpass(Some(128)).unwrap();
        assert_eq!(pass.len(), 32); // 128 bits = 16 bytes = 32 hex chars

        let pass = acl.genpass(Some(256)).unwrap();
        assert_eq!(pass.len(), 64); // 256 bits = 32 bytes = 64 hex chars
    }

    #[test]
    fn test_acl_log() {
        let acl = AclManager::new();
        acl.log_deny("alice", "command", "toplevel", "get").unwrap();
        acl.log_deny("alice", "key", "toplevel", "secret").unwrap();

        let logs = acl.log(None).unwrap();
        assert_eq!(logs.len(), 2);
        assert_eq!(logs[0].reason, "key");
        assert_eq!(logs[1].reason, "command");

        acl.log_reset().unwrap();
        let logs = acl.log(None).unwrap();
        assert_eq!(logs.len(), 0);
    }

    #[test]
    fn test_glob_match() {
        assert!(glob_match("*", "anything"));
        assert!(glob_match("data:*", "data:foo"));
        assert!(!glob_match("data:*", "cache:foo"));
        assert!(glob_match("?at", "cat"));
        assert!(!glob_match("?at", "chat"));
        assert!(glob_match("*foo*", "barfoobar"));
    }

    #[test]
    fn test_channel_permission() {
        let acl = AclManager::new();
        acl.setuser("pub", &["on", "nopass", "allcommands", "allkeys", "&news:*"]).unwrap();

        assert!(acl.check_channel("pub", "news:sports").unwrap());
        assert!(!acl.check_channel("pub", "chat:general").unwrap());
    }

    #[test]
    fn test_off_user_denied() {
        let acl = AclManager::new();
        acl.setuser("frank", &["on", "nopass", "allcommands", "allkeys"]).unwrap();
        assert!(acl.check_command("frank", "get", &["key"]).unwrap());

        acl.setuser("frank", &["off"]).unwrap();
        assert!(!acl.check_command("frank", "get", &["key"]).unwrap());
        assert!(!acl.authenticate("frank", "").unwrap());
    }
}
