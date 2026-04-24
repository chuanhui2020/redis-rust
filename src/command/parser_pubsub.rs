//! Pub/Sub 命令解析器
use crate::error::{AppError, Result};
use crate::protocol::RespValue;
use super::{Command, CommandParser};

impl CommandParser {
    /// 解析 SUBSCRIBE 命令
    ///
    /// Redis 语法: SUBSCRIBE channel [channel ...]
    ///
    /// # 参数
    /// - `arr` - RESP 数组，arr[0] 为命令名，后续为命令参数
    ///
    /// # 返回值
    /// - `Ok(Command::Subscribe(...))` - 解析成功
    /// - `Err(AppError::Command)` - 参数不足或格式错误
    pub(crate) fn parse_subscribe(&self, arr: &[RespValue]) -> Result<Command> {
        if arr.len() < 2 {
            return Err(AppError::Command(
                "SUBSCRIBE 命令需要至少 1 个参数".to_string(),
            ));
        }
        let channels = arr[1..]
            .iter()
            .map(|v| self.extract_string(v))
            .collect::<Result<Vec<String>>>()?;
        Ok(Command::Subscribe(channels))
    }
    /// 解析 UNSUBSCRIBE 命令
    ///
    /// Redis 语法: UNSUBSCRIBE [channel ...]
    ///
    /// # 参数
    /// - `arr` - RESP 数组，arr[0] 为命令名，后续为命令参数
    ///
    /// # 返回值
    /// - `Ok(Command::Unsubscribe(...))` - 解析成功
    /// - `Err(AppError::Command)` - 参数不足或格式错误
    pub(crate) fn parse_unsubscribe(&self, arr: &[RespValue]) -> Result<Command> {
        let channels = if arr.len() > 1 {
            arr[1..]
                .iter()
                .map(|v| self.extract_string(v))
                .collect::<Result<Vec<String>>>()?
        } else {
            vec![]
        };
        Ok(Command::Unsubscribe(channels))
    }
    /// 解析 PUBLISH 命令
    ///
    /// Redis 语法: PUBLISH channel message
    ///
    /// # 参数
    /// - `arr` - RESP 数组，arr[0] 为命令名，后续为命令参数
    ///
    /// # 返回值
    /// - `Ok(Command::Publish(...))` - 解析成功
    /// - `Err(AppError::Command)` - 参数不足或格式错误
    pub(crate) fn parse_publish(&self, arr: &[RespValue]) -> Result<Command> {
        if arr.len() != 3 {
            return Err(AppError::Command(
                "PUBLISH 命令需要 2 个参数".to_string(),
            ));
        }
        let channel = self.extract_string(&arr[1])?;
        let message = self.extract_bytes(&arr[2])?;
        Ok(Command::Publish(channel, message))
    }
    /// 解析 UNKNOWN 命令
    ///
    /// Redis 语法: 
    ///
    /// # 参数
    /// - `arr` - RESP 数组，arr[0] 为命令名，后续为命令参数
    ///
    /// # 返回值
    /// - `Ok(Command::Unknown(...))` - 解析成功
    /// - `Err(AppError::Command)` - 参数不足或格式错误
    pub(crate) fn parse_psubscribe(&self, arr: &[RespValue]) -> Result<Command> {
        if arr.len() < 2 {
            return Err(AppError::Command(
                "PSUBSCRIBE 命令需要至少 1 个参数".to_string(),
            ));
        }
        let patterns = arr[1..]
            .iter()
            .map(|v| self.extract_string(v))
            .collect::<Result<Vec<String>>>()?;
        Ok(Command::PSubscribe(patterns))
    }
    /// 解析 UNKNOWN 命令
    ///
    /// Redis 语法: 
    ///
    /// # 参数
    /// - `arr` - RESP 数组，arr[0] 为命令名，后续为命令参数
    ///
    /// # 返回值
    /// - `Ok(Command::Unknown(...))` - 解析成功
    /// - `Err(AppError::Command)` - 参数不足或格式错误
    pub(crate) fn parse_punsubscribe(&self, arr: &[RespValue]) -> Result<Command> {
        let patterns = if arr.len() > 1 {
            arr[1..]
                .iter()
                .map(|v| self.extract_string(v))
                .collect::<Result<Vec<String>>>()?
        } else {
            vec![]
        };
        Ok(Command::PUnsubscribe(patterns))
    }
    /// 解析 UNKNOWN 命令
    ///
    /// Redis 语法: 
    ///
    /// # 参数
    /// - `arr` - RESP 数组，arr[0] 为命令名，后续为命令参数
    ///
    /// # 返回值
    /// - `Ok(Command::Unknown(...))` - 解析成功
    /// - `Err(AppError::Command)` - 参数不足或格式错误
    pub(crate) fn parse_ssubscribe(&self, arr: &[RespValue]) -> Result<Command> {
        if arr.len() < 2 {
            return Err(AppError::Command(
                "SSUBSCRIBE 命令需要至少 1 个参数".to_string(),
            ));
        }
        let channels = arr[1..]
            .iter()
            .map(|v| self.extract_string(v))
            .collect::<Result<Vec<String>>>()?;
        Ok(Command::SSubscribe(channels))
    }
    /// 解析 UNKNOWN 命令
    ///
    /// Redis 语法: 
    ///
    /// # 参数
    /// - `arr` - RESP 数组，arr[0] 为命令名，后续为命令参数
    ///
    /// # 返回值
    /// - `Ok(Command::Unknown(...))` - 解析成功
    /// - `Err(AppError::Command)` - 参数不足或格式错误
    pub(crate) fn parse_sunsubscribe(&self, arr: &[RespValue]) -> Result<Command> {
        let channels = if arr.len() > 1 {
            arr[1..]
                .iter()
                .map(|v| self.extract_string(v))
                .collect::<Result<Vec<String>>>()?
        } else {
            vec![]
        };
        Ok(Command::SUnsubscribe(channels))
    }
    /// 解析 UNKNOWN 命令
    ///
    /// Redis 语法: 
    ///
    /// # 参数
    /// - `arr` - RESP 数组，arr[0] 为命令名，后续为命令参数
    ///
    /// # 返回值
    /// - `Ok(Command::Unknown(...))` - 解析成功
    /// - `Err(AppError::Command)` - 参数不足或格式错误
    pub(crate) fn parse_spublish(&self, arr: &[RespValue]) -> Result<Command> {
        if arr.len() != 3 {
            return Err(AppError::Command(
                "SPUBLISH 命令需要 2 个参数".to_string(),
            ));
        }
        let channel = self.extract_string(&arr[1])?;
        let message = self.extract_bytes(&arr[2])?;
        Ok(Command::SPublish(channel, message))
    }
    /// 解析 UNKNOWN 命令
    ///
    /// Redis 语法: 
    ///
    /// # 参数
    /// - `arr` - RESP 数组，arr[0] 为命令名，后续为命令参数
    ///
    /// # 返回值
    /// - `Ok(Command::Unknown(...))` - 解析成功
    /// - `Err(AppError::Command)` - 参数不足或格式错误
    pub(crate) fn parse_pubsub(&self, arr: &[RespValue]) -> Result<Command> {
        if arr.len() < 2 {
            return Err(AppError::Command(
                "PUBSUB 命令需要子命令".to_string(),
            ));
        }
        let subcmd = self.extract_string(&arr[1])?.to_ascii_uppercase();
        match subcmd.as_str() {
            "CHANNELS" => {
                let pattern = if arr.len() > 2 {
                    Some(self.extract_string(&arr[2])?)
                } else {
                    None
                };
                Ok(Command::PubSubChannels(pattern))
            }
            "NUMSUB" => {
                let channels = if arr.len() > 2 {
                    arr[2..]
                        .iter()
                        .map(|v| self.extract_string(v))
                        .collect::<Result<Vec<String>>>()?
                } else {
                    vec![]
                };
                Ok(Command::PubSubNumSub(channels))
            }
            "NUMPAT" => {
                if arr.len() != 2 {
                    return Err(AppError::Command(
                        "PUBSUB NUMPAT 不需要参数".to_string(),
                    ));
                }
                Ok(Command::PubSubNumPat)
            }
            "SHARDCHANNELS" => {
                let pattern = if arr.len() > 2 {
                    Some(self.extract_string(&arr[2])?)
                } else {
                    None
                };
                Ok(Command::PubSubShardChannels(pattern))
            }
            "SHARDNUMSUB" => {
                let channels = if arr.len() > 2 {
                    arr[2..]
                        .iter()
                        .map(|v| self.extract_string(v))
                        .collect::<Result<Vec<String>>>()?
                } else {
                    vec![]
                };
                Ok(Command::PubSubShardNumSub(channels))
            }
            _ => Err(AppError::Command(
                format!("未知的 PUBSUB 子命令: {}", subcmd),
            )),
        }
    }
}
