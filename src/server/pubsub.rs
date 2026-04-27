//! 发布订阅命令处理模块

use super::handler::{ConnectionHandler, write_resp};
use super::{ClientMessage, SubscriptionState};
use crate::command::Command;
use crate::error::Result;
use crate::protocol::RespValue;
use crate::pubsub::PubSubManager;
use tokio::io::BufWriter;
use tokio::net::TcpStream;
use tokio::sync::mpsc;

/// 处理发布订阅相关命令
/// 包括 SUBSCRIBE、UNSUBSCRIBE、PSUBSCRIBE、PUNSUBSCRIBE、PUBLISH 等
pub(crate) async fn handle_pubsub_command(
    cmd: Command,
    is_subscribed: &mut bool,
    sub_state: &mut SubscriptionState,
    msg_tx: &mpsc::UnboundedSender<ClientMessage>,
    pubsub: &PubSubManager,
    stream: &mut BufWriter<TcpStream>,
    handler: &ConnectionHandler,
) -> Result<bool> {
    match cmd {
        Command::Subscribe(channels) => {
            *is_subscribed = true;
            for ch in channels {
                let rx = pubsub.subscribe(&ch);
                let tx = msg_tx.clone();
                let ch_clone = ch.clone();
                let handle = tokio::spawn(async move {
                    let mut rx = rx;
                    loop {
                        match rx.recv().await {
                            Ok(data) => {
                                if tx
                                    .send(ClientMessage::Message(ch_clone.clone(), data))
                                    .is_err()
                                {
                                    break;
                                }
                            }
                            Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
                            Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => {}
                        }
                    }
                });
                sub_state.channels.insert(ch.clone(), handle.abort_handle());

                let count = sub_state.total();
                let resp = RespValue::Array(vec![
                    super::bulk("subscribe"),
                    super::bulk(&ch),
                    RespValue::Integer(count as i64),
                ]);
                if let Err(e) = write_resp(stream, handler, &resp).await {
                    log::error!("写入响应失败: {}", e);
                    return Ok(false);
                }
            }
            Ok(true)
        }
        Command::Unsubscribe(channels) => {
            let to_unsub = if channels.is_empty() {
                // 无参数则取消所有精确订阅
                sub_state.channels.keys().cloned().collect::<Vec<_>>()
            } else {
                channels
            };
            for ch in to_unsub {
                if let Some(handle) = sub_state.channels.remove(&ch) {
                    handle.abort();
                    pubsub.unsubscribe(&ch);
                }
                let count = sub_state.total();
                let resp = RespValue::Array(vec![
                    super::bulk("unsubscribe"),
                    super::bulk(&ch),
                    RespValue::Integer(count as i64),
                ]);
                if let Err(e) = write_resp(stream, handler, &resp).await {
                    log::error!("写入响应失败: {}", e);
                    return Ok(false);
                }
            }
            if sub_state.total() == 0 {
                *is_subscribed = false;
            }
            Ok(true)
        }
        Command::PSubscribe(patterns) => {
            *is_subscribed = true;
            for pat in patterns {
                let rx = pubsub.psubscribe(&pat);
                let tx = msg_tx.clone();
                let pat_clone = pat.clone();
                let handle = tokio::spawn(async move {
                    let mut rx = rx;
                    loop {
                        match rx.recv().await {
                            Ok((ch, data)) => {
                                if tx
                                    .send(ClientMessage::PMessage(pat_clone.clone(), ch, data))
                                    .is_err()
                                {
                                    break;
                                }
                            }
                            Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
                            Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => {}
                        }
                    }
                });
                sub_state
                    .patterns
                    .insert(pat.clone(), handle.abort_handle());

                let count = sub_state.total();
                let resp = RespValue::Array(vec![
                    super::bulk("psubscribe"),
                    super::bulk(&pat),
                    RespValue::Integer(count as i64),
                ]);
                if let Err(e) = write_resp(stream, handler, &resp).await {
                    log::error!("写入响应失败: {}", e);
                    return Ok(false);
                }
            }
            Ok(true)
        }
        Command::PUnsubscribe(patterns) => {
            let to_unsub = if patterns.is_empty() {
                sub_state.patterns.keys().cloned().collect::<Vec<_>>()
            } else {
                patterns
            };
            for pat in to_unsub {
                if let Some(handle) = sub_state.patterns.remove(&pat) {
                    handle.abort();
                    pubsub.punsubscribe(&pat);
                }
                let count = sub_state.total();
                let resp = RespValue::Array(vec![
                    super::bulk("punsubscribe"),
                    super::bulk(&pat),
                    RespValue::Integer(count as i64),
                ]);
                if let Err(e) = write_resp(stream, handler, &resp).await {
                    log::error!("写入响应失败: {}", e);
                    return Ok(false);
                }
            }
            if sub_state.total() == 0 {
                *is_subscribed = false;
            }
            Ok(true)
        }
        Command::Publish(channel, message) => {
            match pubsub.publish(&channel, message) {
                Ok(count) => {
                    let resp = RespValue::Integer(count as i64);
                    if let Err(e) = write_resp(stream, handler, &resp).await {
                        log::error!("写入响应失败: {}", e);
                        return Ok(false);
                    }
                }
                Err(e) => {
                    let resp = RespValue::Error(format!("ERR {}", e));
                    if let Err(e) = write_resp(stream, handler, &resp).await {
                        log::error!("写入错误响应失败: {}", e);
                        return Ok(false);
                    }
                }
            }
            Ok(true)
        }
        Command::PubSubChannels(pattern) => {
            let channels = pubsub.channels(pattern.as_deref());
            let resp = RespValue::Array(channels.into_iter().map(|ch| super::bulk(&ch)).collect());
            if let Err(e) = write_resp(stream, handler, &resp).await {
                log::error!("写入响应失败: {}", e);
                return Ok(false);
            }
            Ok(true)
        }
        Command::PubSubNumSub(channels) => {
            let counts = pubsub.numsub(&channels);
            let mut arr = Vec::new();
            for (ch, count) in counts {
                arr.push(super::bulk(&ch));
                arr.push(RespValue::Integer(count as i64));
            }
            let resp = RespValue::Array(arr);
            if let Err(e) = write_resp(stream, handler, &resp).await {
                log::error!("写入响应失败: {}", e);
                return Ok(false);
            }
            Ok(true)
        }
        Command::PubSubNumPat => {
            let count = pubsub.numpat();
            let resp = RespValue::Integer(count as i64);
            if let Err(e) = write_resp(stream, handler, &resp).await {
                log::error!("写入响应失败: {}", e);
                return Ok(false);
            }
            Ok(true)
        }
        Command::SSubscribe(channels) => {
            *is_subscribed = true;
            for ch in channels {
                let rx = pubsub.ssubscribe(&ch);
                let tx = msg_tx.clone();
                let ch_clone = ch.clone();
                let handle = tokio::spawn(async move {
                    let mut rx = rx;
                    loop {
                        match rx.recv().await {
                            Ok(data) => {
                                if tx
                                    .send(ClientMessage::SMessage(ch_clone.clone(), data))
                                    .is_err()
                                {
                                    break;
                                }
                            }
                            Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
                            Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => {}
                        }
                    }
                });
                sub_state
                    .shard_channels
                    .insert(ch.clone(), handle.abort_handle());

                let count = sub_state.total();
                let resp = RespValue::Array(vec![
                    super::bulk("ssubscribe"),
                    super::bulk(&ch),
                    RespValue::Integer(count as i64),
                ]);
                if let Err(e) = write_resp(stream, handler, &resp).await {
                    log::error!("写入响应失败: {}", e);
                    return Ok(false);
                }
            }
            Ok(true)
        }
        Command::SUnsubscribe(channels) => {
            let to_unsub = if channels.is_empty() {
                // 无参数则取消所有分片订阅
                sub_state.shard_channels.keys().cloned().collect::<Vec<_>>()
            } else {
                channels
            };
            for ch in to_unsub {
                if let Some(handle) = sub_state.shard_channels.remove(&ch) {
                    handle.abort();
                    pubsub.sunsubscribe(&ch);
                }
                let count = sub_state.total();
                let resp = RespValue::Array(vec![
                    super::bulk("sunsubscribe"),
                    super::bulk(&ch),
                    RespValue::Integer(count as i64),
                ]);
                if let Err(e) = write_resp(stream, handler, &resp).await {
                    log::error!("写入响应失败: {}", e);
                    return Ok(false);
                }
            }
            if sub_state.total() == 0 {
                *is_subscribed = false;
            }
            Ok(true)
        }
        Command::SPublish(channel, message) => {
            match pubsub.spublish(&channel, message) {
                Ok(count) => {
                    let resp = RespValue::Integer(count as i64);
                    if let Err(e) = write_resp(stream, handler, &resp).await {
                        log::error!("写入响应失败: {}", e);
                        return Ok(false);
                    }
                }
                Err(e) => {
                    let resp = RespValue::Error(format!("ERR {}", e));
                    if let Err(e) = write_resp(stream, handler, &resp).await {
                        log::error!("写入错误响应失败: {}", e);
                        return Ok(false);
                    }
                }
            }
            Ok(true)
        }
        Command::PubSubShardChannels(pattern) => {
            let channels = pubsub.shard_channels(pattern.as_deref());
            let resp = RespValue::Array(channels.into_iter().map(|ch| super::bulk(&ch)).collect());
            if let Err(e) = write_resp(stream, handler, &resp).await {
                log::error!("写入响应失败: {}", e);
                return Ok(false);
            }
            Ok(true)
        }
        Command::PubSubShardNumSub(channels) => {
            let counts = pubsub.shard_numsub(&channels);
            let mut arr = Vec::new();
            for (ch, count) in counts {
                arr.push(super::bulk(&ch));
                arr.push(RespValue::Integer(count as i64));
            }
            let resp = RespValue::Array(arr);
            if let Err(e) = write_resp(stream, handler, &resp).await {
                log::error!("写入响应失败: {}", e);
                return Ok(false);
            }
            Ok(true)
        }
        _ => Ok(true),
    }
}

/// 处理转发给客户端的发布订阅消息
/// 将频道消息写入客户端连接流
pub(crate) async fn handle_pubsub_message(
    maybe_msg: Option<ClientMessage>,
    stream: &mut BufWriter<TcpStream>,
    handler: &ConnectionHandler,
) -> Result<bool> {
    match maybe_msg {
        Some(ClientMessage::Message(channel, data)) => {
            let resp = RespValue::Array(vec![
                super::bulk("message"),
                super::bulk(&channel),
                super::bulk_bytes(&data),
            ]);
            if let Err(e) = write_resp(stream, handler, &resp).await {
                log::error!("写入消息失败: {}", e);
                return Ok(false);
            }
            Ok(true)
        }
        Some(ClientMessage::PMessage(pattern, channel, data)) => {
            let resp = RespValue::Array(vec![
                super::bulk("pmessage"),
                super::bulk(&pattern),
                super::bulk(&channel),
                super::bulk_bytes(&data),
            ]);
            if let Err(e) = write_resp(stream, handler, &resp).await {
                log::error!("写入消息失败: {}", e);
                return Ok(false);
            }
            Ok(true)
        }
        Some(ClientMessage::SMessage(channel, data)) => {
            let resp = RespValue::Array(vec![
                super::bulk("smessage"),
                super::bulk(&channel),
                super::bulk_bytes(&data),
            ]);
            if let Err(e) = write_resp(stream, handler, &resp).await {
                log::error!("写入消息失败: {}", e);
                return Ok(false);
            }
            Ok(true)
        }
        None => {
            // 所有 sender 已关闭，正常退出
            log::debug!("消息通道已关闭");
            Ok(true)
        }
    }
}
