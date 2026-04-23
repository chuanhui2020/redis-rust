use super::*;

use crate::error::{AppError, Result};
use crate::protocol::RespValue;
use super::parser::CommandParser;

impl CommandParser {
    /// 解析 XADD 命令：XADD key [NOMKSTREAM] [MAXLEN|MINID [=|~] threshold] *|id field value [field value ...]
    pub(crate) fn parse_xadd(&self, arr: &[RespValue]) -> Result<Command> {
        if arr.len() < 5 {
            return Err(AppError::Command(
                "XADD 命令需要至少 4 个参数".to_string(),
            ));
        }
        let key = self.extract_string(&arr[1])?;
        let mut i = 2;
        let mut nomkstream = false;
        let mut max_len = None;
        let mut min_id = None;

        // 解析选项
        while i < arr.len() {
            let opt = self.extract_string(&arr[i])?.to_ascii_uppercase();
            match opt.as_str() {
                "NOMKSTREAM" => {
                    nomkstream = true;
                    i += 1;
                }
                "MAXLEN" => {
                    i += 1;
                    if i < arr.len() {
                        let next = self.extract_string(&arr[i])?;
                        if next == "~" || next == "=" {
                            i += 1; // 忽略 ~ 或 =
                        }
                        if i < arr.len() {
                            if next == "~" || next == "=" {
                                max_len = Some(self.extract_string(&arr[i])?.parse().map_err(|_| {
                                    AppError::Command("XADD MAXLEN threshold 必须是整数".to_string())
                                })?);
                                i += 1;
                            } else {
                                max_len = Some(next.parse().map_err(|_| {
                                    AppError::Command("XADD MAXLEN threshold 必须是整数".to_string())
                                })?);
                                i += 1;
                            }
                        } else {
                            return Err(AppError::Command("XADD MAXLEN 需要 threshold".to_string()));
                        }
                    } else {
                        return Err(AppError::Command("XADD MAXLEN 需要 threshold".to_string()));
                    }
                }
                "MINID" => {
                    i += 1;
                    if i < arr.len() {
                        let next = self.extract_string(&arr[i])?;
                        if next == "~" || next == "=" {
                            i += 1;
                        }
                        if i < arr.len() {
                            min_id = Some(self.extract_string(&arr[i])?);
                            i += 1;
                        } else {
                            return Err(AppError::Command("XADD MINID 需要 id".to_string()));
                        }
                    } else {
                        return Err(AppError::Command("XADD MINID 需要 id".to_string()));
                    }
                }
                _ => break,
            }
        }

        if i >= arr.len() {
            return Err(AppError::Command("XADD 需要 ID 和字段".to_string()));
        }
        let id = self.extract_string(&arr[i])?;
        i += 1;

        if !(arr.len() - i).is_multiple_of(2) {
            return Err(AppError::Command("XADD 字段必须是 field-value 对".to_string()));
        }

        let mut fields = Vec::new();
        while i < arr.len() {
            let field = self.extract_string(&arr[i])?;
            let value = self.extract_string(&arr[i + 1])?;
            fields.push((field, value));
            i += 2;
        }

        Ok(Command::XAdd(key, id, fields, nomkstream, max_len, min_id))
    }


    /// 解析 XLEN 命令：XLEN key
    pub(crate) fn parse_xlen(&self, arr: &[RespValue]) -> Result<Command> {
        if arr.len() != 2 {
            return Err(AppError::Command("XLEN 命令需要 1 个参数".to_string()));
        }
        let key = self.extract_string(&arr[1])?;
        Ok(Command::XLen(key))
    }


    /// 解析 XRANGE 命令：XRANGE key start end [COUNT count]
    pub(crate) fn parse_xrange(&self, arr: &[RespValue]) -> Result<Command> {
        if arr.len() < 4 {
            return Err(AppError::Command("XRANGE 命令需要至少 3 个参数".to_string()));
        }
        let key = self.extract_string(&arr[1])?;
        let start = self.extract_string(&arr[2])?;
        let end = self.extract_string(&arr[3])?;
        let mut count = None;
        if arr.len() >= 6 {
            let opt = self.extract_string(&arr[4])?.to_ascii_uppercase();
            if opt == "COUNT" {
                count = Some(self.extract_string(&arr[5])?.parse().map_err(|_| {
                    AppError::Command("XRANGE COUNT 必须是整数".to_string())
                })?);
            }
        }
        Ok(Command::XRange(key, start, end, count))
    }


    /// 解析 XREVRANGE 命令：XREVRANGE key end start [COUNT count]
    pub(crate) fn parse_xrevrange(&self, arr: &[RespValue]) -> Result<Command> {
        if arr.len() < 4 {
            return Err(AppError::Command("XREVRANGE 命令需要至少 3 个参数".to_string()));
        }
        let key = self.extract_string(&arr[1])?;
        let end = self.extract_string(&arr[2])?;
        let start = self.extract_string(&arr[3])?;
        let mut count = None;
        if arr.len() >= 6 {
            let opt = self.extract_string(&arr[4])?.to_ascii_uppercase();
            if opt == "COUNT" {
                count = Some(self.extract_string(&arr[5])?.parse().map_err(|_| {
                    AppError::Command("XREVRANGE COUNT 必须是整数".to_string())
                })?);
            }
        }
        Ok(Command::XRevRange(key, end, start, count))
    }


    /// 解析 XTRIM 命令：XTRIM key MAXLEN|MINID [=|~] threshold
    pub(crate) fn parse_xtrim(&self, arr: &[RespValue]) -> Result<Command> {
        if arr.len() < 4 {
            return Err(AppError::Command("XTRIM 命令需要至少 3 个参数".to_string()));
        }
        let key = self.extract_string(&arr[1])?;
        let strategy = self.extract_string(&arr[2])?;
        let mut threshold_idx = 3;
        let threshold_str = self.extract_string(&arr[3])?;
        if threshold_str == "~" || threshold_str == "=" {
            threshold_idx = 4;
        }
        let threshold = self.extract_string(&arr[threshold_idx])?;
        Ok(Command::XTrim(key, strategy, threshold))
    }


    /// 解析 XDEL 命令：XDEL key id [id ...]
    pub(crate) fn parse_xdel(&self, arr: &[RespValue]) -> Result<Command> {
        if arr.len() < 3 {
            return Err(AppError::Command("XDEL 命令需要至少 2 个参数".to_string()));
        }
        let key = self.extract_string(&arr[1])?;
        let ids: Vec<String> = arr[2..]
            .iter()
            .map(|v| self.extract_string(v))
            .collect::<Result<Vec<_>>>()?;
        Ok(Command::XDel(key, ids))
    }


    /// 解析 XREAD 命令：XREAD [COUNT count] [BLOCK milliseconds] STREAMS key [key ...] id [id ...]
    pub(crate) fn parse_xread(&self, arr: &[RespValue]) -> Result<Command> {
        if arr.len() < 4 {
            return Err(AppError::Command("XREAD 命令需要至少 3 个参数".to_string()));
        }
        let mut count = None;
        let mut i = 1;
        while i < arr.len() {
            let opt = self.extract_string(&arr[i])?.to_ascii_uppercase();
            match opt.as_str() {
                "COUNT" => {
                    if i + 1 >= arr.len() {
                        return Err(AppError::Command("XREAD COUNT 需要参数".to_string()));
                    }
                    count = Some(self.extract_string(&arr[i + 1])?.parse().map_err(|_| {
                        AppError::Command("XREAD COUNT 必须是整数".to_string())
                    })?);
                    i += 2;
                }
                "BLOCK" => {
                    // BLOCK 由 server.rs 处理，这里只解析 COUNT 和 STREAMS
                    i += 2;
                }
                "STREAMS" => {
                    break;
                }
                _ => {
                    return Err(AppError::Command(format!("XREAD 不支持的选项: {}", opt)));
                }
            }
        }
        if i >= arr.len() || !self.extract_string(&arr[i])?.eq_ignore_ascii_case("STREAMS") {
            return Err(AppError::Command("XREAD 需要 STREAMS 关键字".to_string()));
        }
        i += 1;
        let remaining = arr.len() - i;
        if !remaining.is_multiple_of(2) {
            return Err(AppError::Command("XREAD STREAMS 后面需要成对的 key 和 id".to_string()));
        }
        let num_streams = remaining / 2;
        let keys: Vec<String> = arr[i..i + num_streams]
            .iter()
            .map(|v| self.extract_string(v))
            .collect::<Result<Vec<_>>>()?;
        let ids: Vec<String> = arr[i + num_streams..]
            .iter()
            .map(|v| self.extract_string(v))
            .collect::<Result<Vec<_>>>()?;
        Ok(Command::XRead(keys, ids, count))
    }


    /// 解析 XSETID 命令：XSETID key id
    pub(crate) fn parse_xsetid(&self, arr: &[RespValue]) -> Result<Command> {
        if arr.len() != 3 {
            return Err(AppError::Command("XSETID 命令需要 2 个参数".to_string()));
        }
        let key = self.extract_string(&arr[1])?;
        let id = self.extract_string(&arr[2])?;
        Ok(Command::XSetId(key, id))
    }


    pub(crate) fn parse_xgroup(&self, arr: &[RespValue]) -> Result<Command> {
        if arr.len() < 2 {
            return Err(AppError::Command("XGROUP 需要子命令".to_string()));
        }
        let sub = self.extract_string(&arr[1])?.to_ascii_uppercase();
        match sub.as_str() {
            "CREATE" => {
                if arr.len() < 5 {
                    return Err(AppError::Command("XGROUP CREATE 需要 key groupname id".to_string()));
                }
                let key = self.extract_string(&arr[2])?;
                let group = self.extract_string(&arr[3])?;
                let id = self.extract_string(&arr[4])?;
                let mut mkstream = false;
                for item in arr.iter().skip(5) {
                    if self.extract_string(item)?.eq_ignore_ascii_case("MKSTREAM") {
                        mkstream = true;
                    }
                }
                Ok(Command::XGroupCreate(key, group, id, mkstream))
            }
            "DESTROY" => {
                if arr.len() < 4 {
                    return Err(AppError::Command("XGROUP DESTROY 需要 key groupname".to_string()));
                }
                let key = self.extract_string(&arr[2])?;
                let group = self.extract_string(&arr[3])?;
                Ok(Command::XGroupDestroy(key, group))
            }
            "SETID" => {
                if arr.len() < 5 {
                    return Err(AppError::Command("XGROUP SETID 需要 key groupname id".to_string()));
                }
                let key = self.extract_string(&arr[2])?;
                let group = self.extract_string(&arr[3])?;
                let id = self.extract_string(&arr[4])?;
                Ok(Command::XGroupSetId(key, group, id))
            }
            "DELCONSUMER" => {
                if arr.len() < 5 {
                    return Err(AppError::Command("XGROUP DELCONSUMER 需要 key groupname consumername".to_string()));
                }
                let key = self.extract_string(&arr[2])?;
                let group = self.extract_string(&arr[3])?;
                let consumer = self.extract_string(&arr[4])?;
                Ok(Command::XGroupDelConsumer(key, group, consumer))
            }
            "CREATECONSUMER" => {
                if arr.len() < 5 {
                    return Err(AppError::Command("XGROUP CREATECONSUMER 需要 key groupname consumername".to_string()));
                }
                let key = self.extract_string(&arr[2])?;
                let group = self.extract_string(&arr[3])?;
                let consumer = self.extract_string(&arr[4])?;
                Ok(Command::XGroupCreateConsumer(key, group, consumer))
            }
            _ => Err(AppError::Command(format!("XGROUP 不支持的子命令: {}", sub))),
        }
    }


    pub(crate) fn parse_xreadgroup(&self, arr: &[RespValue]) -> Result<Command> {
        if arr.len() < 7 {
            return Err(AppError::Command("XREADGROUP 参数不足".to_string()));
        }
        let g = self.extract_string(&arr[1])?.to_ascii_uppercase();
        if g != "GROUP" {
            return Err(AppError::Command("XREADGROUP 需要 GROUP 关键字".to_string()));
        }
        let group = self.extract_string(&arr[2])?;
        let consumer = self.extract_string(&arr[3])?;
        let mut i = 4;
        let mut count = None;
        let mut noack = false;
        while i < arr.len() {
            let opt = self.extract_string(&arr[i])?.to_ascii_uppercase();
            match opt.as_str() {
                "COUNT" => {
                    i += 1;
                    count = Some(self.extract_string(&arr[i])?.parse::<usize>().map_err(|_| {
                        AppError::Command("COUNT 必须是整数".to_string())
                    })?);
                }
                "NOACK" => { noack = true; }
                "STREAMS" => { i += 1; break; }
                _ => {}
            }
            i += 1;
        }
        let remaining = arr.len() - i;
        if remaining == 0 || !remaining.is_multiple_of(2) {
            return Err(AppError::Command("XREADGROUP STREAMS 参数不匹配".to_string()));
        }
        let num_streams = remaining / 2;
        let keys: Vec<String> = arr[i..i + num_streams]
            .iter()
            .map(|v| self.extract_string(v))
            .collect::<Result<Vec<_>>>()?;
        let ids: Vec<String> = arr[i + num_streams..]
            .iter()
            .map(|v| self.extract_string(v))
            .collect::<Result<Vec<_>>>()?;
        Ok(Command::XReadGroup(group, consumer, keys, ids, count, noack))
    }


    pub(crate) fn parse_xack(&self, arr: &[RespValue]) -> Result<Command> {
        if arr.len() < 4 {
            return Err(AppError::Command("XACK 需要 key group id".to_string()));
        }
        let key = self.extract_string(&arr[1])?;
        let group = self.extract_string(&arr[2])?;
        let ids: Vec<String> = arr[3..]
            .iter()
            .map(|v| self.extract_string(v))
            .collect::<Result<Vec<_>>>()?;
        Ok(Command::XAck(key, group, ids))
    }


    pub(crate) fn parse_xclaim(&self, arr: &[RespValue]) -> Result<Command> {
        if arr.len() < 6 {
            return Err(AppError::Command("XCLAIM 参数不足".to_string()));
        }
        let key = self.extract_string(&arr[1])?;
        let group = self.extract_string(&arr[2])?;
        let consumer = self.extract_string(&arr[3])?;
        let min_idle: u64 = self.extract_string(&arr[4])?.parse().map_err(|_| {
            AppError::Command("min-idle-time 必须是整数".to_string())
        })?;
        let mut ids = Vec::new();
        let mut justid = false;
        for item in arr.iter().skip(5) {
            let s = self.extract_string(item)?;
            if s.eq_ignore_ascii_case("JUSTID") {
                justid = true;
            } else {
                ids.push(s);
            }
        }
        Ok(Command::XClaim(key, group, consumer, min_idle, ids, justid))
    }


    pub(crate) fn parse_xautoclaim(&self, arr: &[RespValue]) -> Result<Command> {
        if arr.len() < 6 {
            return Err(AppError::Command("XAUTOCLAIM 参数不足".to_string()));
        }
        let key = self.extract_string(&arr[1])?;
        let group = self.extract_string(&arr[2])?;
        let consumer = self.extract_string(&arr[3])?;
        let min_idle: u64 = self.extract_string(&arr[4])?.parse().map_err(|_| {
            AppError::Command("min-idle-time 必须是整数".to_string())
        })?;
        let start = self.extract_string(&arr[5])?;
        let mut count = 100usize;
        let mut justid = false;
        let mut i = 6;
        while i < arr.len() {
            let opt = self.extract_string(&arr[i])?.to_ascii_uppercase();
            match opt.as_str() {
                "COUNT" => {
                    i += 1;
                    count = self.extract_string(&arr[i])?.parse().map_err(|_| {
                        AppError::Command("COUNT 必须是整数".to_string())
                    })?;
                }
                "JUSTID" => { justid = true; }
                _ => {}
            }
            i += 1;
        }
        Ok(Command::XAutoClaim(key, group, consumer, min_idle, start, count, justid))
    }


    pub(crate) fn parse_xpending(&self, arr: &[RespValue]) -> Result<Command> {
        if arr.len() < 3 {
            return Err(AppError::Command("XPENDING 需要 key group".to_string()));
        }
        let key = self.extract_string(&arr[1])?;
        let group = self.extract_string(&arr[2])?;
        if arr.len() == 3 {
            return Ok(Command::XPending(key, group, None, None, None, None));
        }
        if arr.len() >= 6 {
            let start = self.extract_string(&arr[3])?;
            let end = self.extract_string(&arr[4])?;
            let count: usize = self.extract_string(&arr[5])?.parse().map_err(|_| {
                AppError::Command("COUNT 必须是整数".to_string())
            })?;
            let consumer = if arr.len() >= 7 {
                Some(self.extract_string(&arr[6])?)
            } else {
                None
            };
            Ok(Command::XPending(key, group, Some(start), Some(end), Some(count), consumer))
        } else {
            Err(AppError::Command("XPENDING 参数不足".to_string()))
        }
    }


    pub(crate) fn parse_xinfo(&self, arr: &[RespValue]) -> Result<Command> {
        if arr.len() < 3 {
            return Err(AppError::Command("XINFO 需要子命令".to_string()));
        }
        let sub = self.extract_string(&arr[1])?.to_ascii_uppercase();
        match sub.as_str() {
            "STREAM" => {
                let key = self.extract_string(&arr[2])?;
                let full = arr.len() > 3 && self.extract_string(&arr[3])?.eq_ignore_ascii_case("FULL");
                Ok(Command::XInfoStream(key, full))
            }
            "GROUPS" => {
                let key = self.extract_string(&arr[2])?;
                Ok(Command::XInfoGroups(key))
            }
            "CONSUMERS" => {
                if arr.len() < 4 {
                    return Err(AppError::Command("XINFO CONSUMERS 需要 key groupname".to_string()));
                }
                let key = self.extract_string(&arr[2])?;
                let group = self.extract_string(&arr[3])?;
                Ok(Command::XInfoConsumers(key, group))
            }
            _ => Err(AppError::Command(format!("XINFO 不支持的子命令: {}", sub))),
        }
    }


}
