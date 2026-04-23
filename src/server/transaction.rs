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
            // 检查 WATCH 的 key 版本是否变化
            let check_passed = if watched.is_empty() {
                true
            } else {
                match tx_storage.watch_check(watched) {
                    Ok(true) => true,
                    Ok(false) => false,
                    Err(e) => {
                        let resp = RespValue::Error(format!("ERR {}", e));
                        let _ = write_resp(stream, handler, &resp).await;
                        watched.clear();
                        tx_queue.clear();
                        return Ok(true);
                    }
                }
            };
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
            // 依次执行队列中的命令
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
