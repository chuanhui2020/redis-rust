use crate::error::{AppError, Result};
use crate::protocol::RespValue;
use super::{Command, CommandParser};

impl CommandParser {
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
}
