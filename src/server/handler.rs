//! 连接处理器和响应写入模块
use tokio::io::{AsyncWriteExt, BufWriter};
use tokio::net::TcpStream;
use crate::protocol::{RespParser, RespValue};
use crate::command::{CommandParser, CommandExecutor};
use super::ReplyMode;

pub(crate) static RESP_OK: &[u8] = b"+OK\r\n";
pub(crate) static RESP_PONG: &[u8] = b"+PONG\r\n";
pub(crate) static RESP_ZERO: &[u8] = b":0\r\n";
pub(crate) static RESP_ONE: &[u8] = b":1\r\n";
pub(crate) static RESP_NULL: &[u8] = b"$-1\r\n";

/// 直接写入预编码的静态 RESP 字节，跳过 RespValue 构造和编码
pub(crate) async fn write_resp_bytes(
    stream: &mut BufWriter<TcpStream>,
    bytes: &[u8],
) -> std::io::Result<()> {
    stream.write_all(bytes).await
}

/// 复用缓冲区编码 RESP 值并写入（避免每次分配新 Vec/Bytes）
pub(crate) async fn write_resp_buf(
    stream: &mut BufWriter<TcpStream>,
    handler: &ConnectionHandler,
    resp: &RespValue,
    encode_buf: &mut Vec<u8>,
) -> std::io::Result<()> {
    encode_buf.clear();
    handler.parser.encode_append(resp, encode_buf);
    stream.write_all(encode_buf).await
}

/// 客户端连接处理器，聚合协议解析、命令解析和命令执行三个核心组件
///
/// # 架构说明
/// `ConnectionHandler` 是连接处理主循环 (`handle_connection`) 的核心依赖，负责：
/// - `parser`: 将 [`RespValue`] 编码为字节流，以及解析客户端发送的 RESP 协议数据
/// - `cmd_parser`: 将解析后的 [`RespValue`] 转换为具体的 [`Command`] 枚举
/// - `executor`: 执行命令并返回 [`RespValue`] 响应
///
/// 三者通过此结构体聚合，避免在连接处理函数中传递过多独立参数。
#[derive(Debug)]
pub struct ConnectionHandler {
    /// RESP 协议解析器，负责编码/解码 RESP 数据
    pub(crate) parser: RespParser,
    /// 命令解析器，负责将 RESP 数组解析为具体 Command 枚举
    pub(crate) cmd_parser: CommandParser,
    /// 命令执行器，负责调用底层存储引擎完成命令执行
    pub(crate) executor: CommandExecutor,
}

/// 将 RESP 值编码为字节流并写入 TCP 连接
///
/// # 参数
/// - `stream` - 带缓冲的 TCP 写入流，`BufWriter` 批量减少系统调用次数
/// - `handler` - 包含 RESP 编码器的连接处理器
/// - `resp` - 待发送的 RESP 值
///
/// # 性能说明
/// 数据仅写入 `BufWriter` 内部缓冲区，实际 flush 由调用方在 pipeline 批量处理结束后统一执行。
pub(crate) async fn write_resp(
    stream: &mut BufWriter<TcpStream>,
    handler: &ConnectionHandler,
    resp: &RespValue,
) -> std::io::Result<()> {
    let mut buf = Vec::with_capacity(64);
    handler.parser.encode_append(resp, &mut buf);
    stream.write_all(&buf).await
}

/// 根据回复模式发送响应
///
/// 支持三种回复模式：
/// - `ReplyMode::On` — 正常发送响应（默认）
/// - `ReplyMode::Off` — 静默模式，不发送任何响应（CLIENT REPLY OFF）
/// - `ReplyMode::Skip` — 跳过当前命令的响应，之后恢复为 On（CLIENT REPLY SKIP）
///
/// # 参数
/// - `stream` - 带缓冲的 TCP 写入流
/// - `handler` - 包含 RESP 编码器的连接处理器
/// - `resp` - 待发送的 RESP 值
/// - `reply_mode` - 当前回复模式，会被 Skip 状态自动重置为 On
///
/// # 与 Redis 客户端的交互
/// `CLIENT REPLY` 命令用于 pipeline 场景中减少网络往返，提升批量操作性能。
pub(crate) async fn send_reply(
    stream: &mut BufWriter<TcpStream>,
    handler: &ConnectionHandler,
    resp: &RespValue,
    reply_mode: &mut ReplyMode,
) -> std::io::Result<()> {
    match *reply_mode {
        ReplyMode::Off => Ok(()),
        ReplyMode::Skip => {
            *reply_mode = ReplyMode::On;
            Ok(())
        }
        ReplyMode::On => write_resp(stream, handler, resp).await,
    }
}
