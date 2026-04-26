//! 事务处理模块（MULTI/EXEC/WATCH）
use std::collections::HashMap;
use tokio::io::BufWriter;
use tokio::net::TcpStream;
use crate::command::Command;
use crate::error::Result;
use crate::protocol::RespValue;
use crate::pubsub::PubSubManager;
use crate::storage::StorageEngine;
use super::ReplyMode;
use super::handler::{ConnectionHandler, write_resp, send_reply};
use std::sync::atomic::Ordering;

/// 在事务中处理命令（已执行 MULTI 后）
///
/// 事务处理流程：客户端发送 MULTI -> 命令入队 -> EXEC 时原子执行 / DISCARD 时放弃。
/// 此函数处理的是**已入 MULTI 之后**的所有命令，包括：
/// - EXEC: 检查 WATCH、原子执行队列中所有命令
/// - DISCARD: 放弃事务，清空队列
/// - MULTI: 返回嵌套错误
/// - WATCH: 返回不允许错误
/// - 其他命令: 入队，返回 QUEUED
///
/// # 参数
/// - `cmd`: 当前接收到的命令
/// - `in_transaction`: 事务状态标志，EXEC/DISCARD 时会被置为 false
/// - `tx_queue`: 事务命令队列，非事务命令在此累积
/// - `watched`: WATCH 监控的 key -> 版本号映射，用于 EXEC 时的乐观锁检查
/// - `tx_storage`: 存储引擎引用，用于 WATCH 版本检查
/// - `stream`: TCP 写入流
/// - `handler`: 连接处理器（编码器 + 执行器）
/// - `reply_mode`: 回复模式
/// - `pubsub`: 发布订阅管理器，事务中 PUBLISH 需要在此直接执行
///
/// # 返回值
/// 返回 Ok(true) 表示继续处理后续命令；Ok(false) 表示连接应关闭。
///
/// # 并发模型
/// WATCH/EXEC 的乐观锁通过 storage.get_version(key) 实现，EXEC 时比较 key 的版本号
/// 是否与 WATCH 时一致。版本号由存储引擎在每次写操作时递增，无需显式锁。
///
/// # 架构说明
/// 事务中的 PUBLISH 命令不经过 executor.execute（因其副作用是 pubsub 而非存储），
/// 而是直接调用 pubsub.publish。其余命令通过 handler.executor.execute 执行。
pub(crate) async fn handle_in_transaction(
    cmd: Command,
    in_transaction: &mut bool,
    tx_queue: &mut Vec<Command>,
    watched: &mut HashMap<String, u64>,
    tx_storage: &StorageEngine,
    stream: &mut BufWriter<TcpStream>,
    handler: &ConnectionHandler,
    reply_mode: &mut ReplyMode,
    pubsub: &PubSubManager,
) -> Result<bool> {
    match cmd {
        Command::Exec => {
            *in_transaction = false;
            let check_passed = if watched.is_empty() {
                true
            } else {
                match tx_storage.watch_check(watched) {
                    Ok(true) => true,
                    Ok(false) => false,
                    Err(e) => {
                        let resp = RespValue::Error(format!("ERR {}", e));
                        let _ = write_resp(stream, handler, &resp).await;
                        if !watched.is_empty() {
                            tx_storage.watch_count.fetch_sub(1, Ordering::Relaxed);
                        }
                        watched.clear();
                        tx_queue.clear();
                        return Ok(true);
                    }
                }
            };
            if !watched.is_empty() {
                tx_storage.watch_count.fetch_sub(1, Ordering::Relaxed);
            }
            watched.clear();
            if !check_passed {
                tx_queue.clear();
                let resp = RespValue::BulkString(None);
                if let Err(e) = write_resp(stream, handler, &resp).await {
                    log::error!("写入响应失败: {}", e);
                    return Ok(false);
                }
                return Ok(true);
            }
            let mut results = Vec::new();
            for queued_cmd in tx_queue.drain(..) {
                let result = match queued_cmd {
                    Command::Publish(channel, message) => {
                        match pubsub.publish(&channel, message) {
                            Ok(count) => RespValue::Integer(count as i64),
                            Err(e) => RespValue::Error(format!("ERR {}", e)),
                        }
                    }
                    other => {
                        match handler.executor.execute(other) {
                            Ok(resp) => resp,
                            Err(e) => RespValue::Error(format!("ERR {}", e)),
                        }
                    }
                };
                results.push(result);
            }
            let resp = RespValue::Array(results);
            if let Err(e) = send_reply(stream, handler, &resp, reply_mode).await {
                log::error!("写入响应失败: {}", e);
                return Ok(false);
            }
            Ok(true)
        }
        Command::Discard => {
            *in_transaction = false;
            tx_queue.clear();
            if !watched.is_empty() {
                tx_storage.watch_count.fetch_sub(1, Ordering::Relaxed);
            }
            watched.clear();
            let resp = RespValue::SimpleString("OK".to_string());
            if let Err(e) = write_resp(stream, handler, &resp).await {
                log::error!("写入响应失败: {}", e);
                return Ok(false);
            }
            Ok(true)
        }
        Command::Multi => {
            let resp = RespValue::Error(
                "ERR MULTI calls can not be nested".to_string()
            );
            if let Err(e) = write_resp(stream, handler, &resp).await {
                log::error!("写入响应失败: {}", e);
                return Ok(false);
            }
            Ok(true)
        }
        Command::Watch(_) => {
            let resp = RespValue::Error(
                "ERR WATCH inside MULTI is not allowed".to_string()
            );
            if let Err(e) = write_resp(stream, handler, &resp).await {
                log::error!("写入响应失败: {}", e);
                return Ok(false);
            }
            Ok(true)
        }
        other => {
            tx_queue.push(other);
            let resp = RespValue::SimpleString("QUEUED".to_string());
            if let Err(e) = write_resp(stream, handler, &resp).await {
                log::error!("写入响应失败: {}", e);
                return Ok(false);
            }
            Ok(true)
        }
    }
}

/// 处理事务初始化相关命令（执行 MULTI 前）
///
/// 此函数处理的是**尚未进入 MULTI 状态**时收到的事务相关命令：
/// - MULTI: 标记进入事务状态，清空队列
/// - WATCH: 记录 key 的当前版本号，用于后续 EXEC 的乐观锁检查
/// - EXEC / DISCARD: 返回错误（未 MULTI 不能 EXEC/DISCARD）
/// - UNWATCH: 清空监控列表
///
/// # 参数
/// - `cmd`: 当前接收到的命令
/// - `in_transaction`: 事务状态标志，MULTI 时置为 true
/// - `tx_queue`: 事务命令队列，MULTI 时清空
/// - `watched`: WATCH 监控映射
/// - `tx_storage`: 存储引擎引用，用于获取 key 版本号
/// - `stream`: TCP 写入流
/// - `handler`: 连接处理器
///
/// # 返回值
/// 返回 Ok(true) 表示继续处理；Ok(false) 表示连接应关闭。
/// 对于非事务命令（如 GET/SET），返回 Ok(true) 让调用方继续分发。
///
/// # 与 handle_in_transaction 的关系
/// 客户端命令 ---> 是否 in_transaction?
///     |-- 否 -> handle_transaction_init (本函数)
///     |          |-- MULTI -> in_transaction = true
///     |          |-- WATCH -> 记录版本号
///     |          \-- 其他 -> 返回 true，由外部继续处理
///     \-- 是 -> handle_in_transaction
///                |-- EXEC -> 检查 WATCH + 执行队列
///                |-- DISCARD -> 清空
///                \-- 其他 -> 入队 (QUEUED)
pub(crate) async fn handle_transaction_init(
    cmd: Command,
    in_transaction: &mut bool,
    tx_queue: &mut Vec<Command>,
    watched: &mut HashMap<String, u64>,
    tx_storage: &StorageEngine,
    stream: &mut BufWriter<TcpStream>,
    handler: &ConnectionHandler,
) -> Result<bool> {
    match cmd {
        Command::Multi => {
            *in_transaction = true;
            tx_queue.clear();
            let resp = RespValue::SimpleString("OK".to_string());
            if let Err(e) = write_resp(stream, handler, &resp).await {
                log::error!("写入响应失败: {}", e);
                return Ok(false);
            }
            Ok(true)
        }
        Command::Watch(keys) => {
            let was_watching = !watched.is_empty();
            watched.clear();
            for key in keys {
                match tx_storage.get_version(&key) {
                    Ok(ver) => {
                        watched.insert(key, ver);
                    }
                    Err(e) => {
                        log::warn!("获取 key {} 版本号失败: {}", key, e);
                    }
                }
            }
            if !watched.is_empty() && !was_watching {
                tx_storage.watch_count.fetch_add(1, Ordering::Relaxed);
            } else if watched.is_empty() && was_watching {
                tx_storage.watch_count.fetch_sub(1, Ordering::Relaxed);
            }
            let resp = RespValue::SimpleString("OK".to_string());
            if let Err(e) = write_resp(stream, handler, &resp).await {
                log::error!("写入响应失败: {}", e);
                return Ok(false);
            }
            Ok(true)
        }
        Command::Exec => {
            let resp = RespValue::Error(
                "ERR EXEC without MULTI".to_string()
            );
            if let Err(e) = write_resp(stream, handler, &resp).await {
                log::error!("写入响应失败: {}", e);
                return Ok(false);
            }
            Ok(true)
        }
        Command::Discard => {
            let resp = RespValue::Error(
                "ERR DISCARD without MULTI".to_string()
            );
            if let Err(e) = write_resp(stream, handler, &resp).await {
                log::error!("写入响应失败: {}", e);
                return Ok(false);
            }
            Ok(true)
        }
        Command::Unwatch => {
            if !watched.is_empty() {
                tx_storage.watch_count.fetch_sub(1, Ordering::Relaxed);
            }
            watched.clear();
            let resp = RespValue::SimpleString("OK".to_string());
            if let Err(e) = write_resp(stream, handler, &resp).await {
                log::error!("写入响应失败: {}", e);
                return Ok(false);
            }
            Ok(true)
        }
        _ => Ok(true),
    }
}
