use tokio::io::{AsyncWriteExt, BufWriter};
use tokio::net::TcpStream;
use crate::protocol::{RespParser, RespValue};
use crate::command::{CommandParser, CommandExecutor};
use super::ReplyMode;


/// 客户端连接处理器
#[derive(Debug)]
pub struct ConnectionHandler {
    /// RESP 协议解析器
    pub(crate) parser: RespParser,
    /// 命令解析器
    pub(crate) cmd_parser: CommandParser,
    /// 命令执行器
    pub(crate) executor: CommandExecutor,
}

/// 将 RESP 值编码并写入流
pub(crate) async fn write_resp(
    stream: &mut BufWriter<TcpStream>,
    handler: &ConnectionHandler,
    resp: &RespValue,
) -> std::io::Result<()> {
    let encoded = handler.parser.encode(resp);
    stream.write_all(&encoded).await
}

/// 根据回复模式发送响应
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
