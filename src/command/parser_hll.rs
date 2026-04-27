//! HyperLogLog 命令解析器

use super::*;

use super::parser::CommandParser;
use crate::error::{AppError, Result};
use crate::protocol::RespValue;

impl CommandParser {
    /// 解析 PFADD 命令：PFADD key element [element ...]
    pub(crate) fn parse_pfadd(&self, arr: &[RespValue]) -> Result<Command> {
        if arr.len() < 3 {
            return Err(AppError::Command("PFADD 命令需要至少 2 个参数".to_string()));
        }
        let key = self.extract_string(&arr[1])?;
        let elements: Vec<String> = arr[2..]
            .iter()
            .map(|v| self.extract_string(v))
            .collect::<Result<Vec<String>>>()?;
        Ok(Command::PfAdd(key, elements))
    }

    /// 解析 PFCOUNT 命令：PFCOUNT key [key ...]
    pub(crate) fn parse_pfcount(&self, arr: &[RespValue]) -> Result<Command> {
        if arr.len() < 2 {
            return Err(AppError::Command(
                "PFCOUNT 命令需要至少 1 个参数".to_string(),
            ));
        }
        let keys: Vec<String> = arr[1..]
            .iter()
            .map(|v| self.extract_string(v))
            .collect::<Result<Vec<String>>>()?;
        Ok(Command::PfCount(keys))
    }

    /// 解析 PFMERGE 命令：PFMERGE destkey sourcekey [sourcekey ...]
    pub(crate) fn parse_pfmerge(&self, arr: &[RespValue]) -> Result<Command> {
        if arr.len() < 3 {
            return Err(AppError::Command(
                "PFMERGE 命令需要至少 2 个参数".to_string(),
            ));
        }
        let destkey = self.extract_string(&arr[1])?;
        let sourcekeys: Vec<String> = arr[2..]
            .iter()
            .map(|v| self.extract_string(v))
            .collect::<Result<Vec<String>>>()?;
        Ok(Command::PfMerge(destkey, sourcekeys))
    }
}
