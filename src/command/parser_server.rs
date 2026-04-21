use crate::error::{AppError, Result};
use crate::protocol::RespValue;
use super::{Command, CommandParser};

impl CommandParser {
    pub(crate) fn parse_ping(&self, arr: &[RespValue]) -> Result<Command> {
        if arr.len() > 2 {
            return Err(AppError::Command(
                "PING 命令参数过多".to_string(),
            ));
        }

        let message = if arr.len() == 2 {
            Some(self.extract_string(&arr[1])?)
        } else {
            None
        };

        Ok(Command::Ping(message))
    }
    pub(crate) fn parse_dbsize(&self, arr: &[RespValue]) -> Result<Command> {
        if arr.len() != 1 {
            return Err(AppError::Command(
                "DBSIZE 命令不需要参数".to_string(),
            ));
        }
        Ok(Command::DbSize)
    }
    pub(crate) fn parse_info(&self, arr: &[RespValue]) -> Result<Command> {
        if arr.len() > 2 {
            return Err(AppError::Command(
                "INFO 命令参数过多".to_string(),
            ));
        }
        let section = if arr.len() == 2 {
            Some(self.extract_string(&arr[1])?)
        } else {
            None
        };
        Ok(Command::Info(section))
    }
    pub(crate) fn parse_bgrewriteaof(&self, arr: &[RespValue]) -> Result<Command> {
        if arr.len() != 1 {
            return Err(AppError::Command(
                "BGREWRITEAOF 命令不需要参数".to_string(),
            ));
        }
        Ok(Command::BgRewriteAof)
    }
    pub(crate) fn parse_select(&self, arr: &[RespValue]) -> Result<Command> {
        if arr.len() != 2 {
            return Err(AppError::Command(
                "SELECT 命令需要 1 个参数".to_string(),
            ));
        }
        let index: usize = self.extract_string(&arr[1])?.parse().map_err(|_| {
            AppError::Command("SELECT 的参数必须是整数".to_string())
        })?;
        Ok(Command::Select(index))
    }
    pub(crate) fn parse_auth(&self, arr: &[RespValue]) -> Result<Command> {
        if arr.len() == 2 {
            // AUTH password（默认用户 default）
            let password = self.extract_string(&arr[1])?;
            Ok(Command::Auth("default".to_string(), password))
        } else if arr.len() == 3 {
            // AUTH username password
            let username = self.extract_string(&arr[1])?;
            let password = self.extract_string(&arr[2])?;
            Ok(Command::Auth(username, password))
        } else {
            Err(AppError::Command(
                "AUTH 命令需要 1 或 2 个参数".to_string(),
            ))
        }
    }
    pub(crate) fn parse_acl(&self, arr: &[RespValue]) -> Result<Command> {
        if arr.len() < 2 {
            return Err(AppError::Command(
                "ACL 命令需要子命令".to_string(),
            ));
        }
        let sub = self.extract_string(&arr[1])?.to_ascii_uppercase();
        match sub.as_str() {
            "SETUSER" => {
                if arr.len() < 3 {
                    return Err(AppError::Command(
                        "ACL SETUSER 需要用户名".to_string(),
                    ));
                }
                let username = self.extract_string(&arr[2])?;
                let rules: Vec<String> = arr[3..].iter()
                    .map(|v| self.extract_string(v))
                    .collect::<Result<Vec<_>>>()?;
                Ok(Command::AclSetUser(username, rules))
            }
            "GETUSER" => {
                if arr.len() != 3 {
                    return Err(AppError::Command(
                        "ACL GETUSER 需要 1 个参数".to_string(),
                    ));
                }
                let username = self.extract_string(&arr[2])?;
                Ok(Command::AclGetUser(username))
            }
            "DELUSER" => {
                if arr.len() < 3 {
                    return Err(AppError::Command(
                        "ACL DELUSER 需要至少 1 个用户名".to_string(),
                    ));
                }
                let names: Vec<String> = arr[2..].iter()
                    .map(|v| self.extract_string(v))
                    .collect::<Result<Vec<_>>>()?;
                Ok(Command::AclDelUser(names))
            }
            "LIST" => Ok(Command::AclList),
            "CAT" => {
                if arr.len() == 2 {
                    Ok(Command::AclCat(None))
                } else {
                    let category = self.extract_string(&arr[2])?;
                    Ok(Command::AclCat(Some(category)))
                }
            }
            "WHOAMI" => Ok(Command::AclWhoAmI),
            "LOG" => {
                if arr.len() == 2 {
                    Ok(Command::AclLog(None))
                } else {
                    let arg = self.extract_string(&arr[2])?.to_ascii_uppercase();
                    Ok(Command::AclLog(Some(arg)))
                }
            }
            "GENPASS" => {
                if arr.len() == 2 {
                    Ok(Command::AclGenPass(None))
                } else {
                    let bits: usize = self.extract_string(&arr[2])?.parse().map_err(|_| {
                        AppError::Command("ACL GENPASS bits 必须是整数".to_string())
                    })?;
                    Ok(Command::AclGenPass(Some(bits)))
                }
            }
            _ => Err(AppError::Command(
                format!("ACL 未知子命令: {}", sub),
            )),
        }
    }
    pub(crate) fn parse_client(&self, arr: &[RespValue]) -> Result<Command> {
        if arr.len() < 2 {
            return Err(AppError::Command(
                "CLIENT 命令需要子命令".to_string(),
            ));
        }
        let sub = self.extract_string(&arr[1])?.to_ascii_uppercase();
        match sub.as_str() {
            "SETNAME" => {
                if arr.len() != 3 {
                    return Err(AppError::Command(
                        "CLIENT SETNAME 命令需要 1 个参数".to_string(),
                    ));
                }
                let name = self.extract_string(&arr[2])?;
                Ok(Command::ClientSetName(name))
            }
            "GETNAME" => {
                if arr.len() != 2 {
                    return Err(AppError::Command(
                        "CLIENT GETNAME 命令不需要参数".to_string(),
                    ));
                }
                Ok(Command::ClientGetName)
            }
            "LIST" => {
                if arr.len() != 2 {
                    return Err(AppError::Command(
                        "CLIENT LIST 命令不需要参数".to_string(),
                    ));
                }
                Ok(Command::ClientList)
            }
            "ID" => {
                if arr.len() != 2 {
                    return Err(AppError::Command(
                        "CLIENT ID 命令不需要参数".to_string(),
                    ));
                }
                Ok(Command::ClientId)
            }
            "INFO" => {
                if arr.len() != 2 {
                    return Err(AppError::Command(
                        "CLIENT INFO 命令不需要参数".to_string(),
                    ));
                }
                Ok(Command::ClientInfo)
            }
            "KILL" => {
                let mut id = None;
                let mut addr = None;
                let mut user = None;
                let mut skipme = true;
                let mut i = 2;
                while i < arr.len() {
                    let arg = self.extract_string(&arr[i])?.to_ascii_uppercase();
                    match arg.as_str() {
                        "ID" => {
                            i += 1;
                            if i < arr.len() {
                                id = Some(self.extract_string(&arr[i])?.parse().map_err(|_| {
                                    AppError::Command("CLIENT KILL ID 必须是整数".to_string())
                                })?);
                            }
                        }
                        "ADDR" => {
                            i += 1;
                            if i < arr.len() {
                                addr = Some(self.extract_string(&arr[i])?);
                            }
                        }
                        "USER" => {
                            i += 1;
                            if i < arr.len() {
                                user = Some(self.extract_string(&arr[i])?);
                            }
                        }
                        "SKIPME" => {
                            i += 1;
                            if i < arr.len() {
                                let val = self.extract_string(&arr[i])?.to_ascii_lowercase();
                                skipme = val == "yes";
                            }
                        }
                        _ => {}
                    }
                    i += 1;
                }
                Ok(Command::ClientKill { id, addr, user, skipme })
            }
            "PAUSE" => {
                if arr.len() < 3 {
                    return Err(AppError::Command(
                        "CLIENT PAUSE 需要 timeout 参数".to_string(),
                    ));
                }
                let timeout: u64 = self.extract_string(&arr[2])?.parse().map_err(|_| {
                    AppError::Command("CLIENT PAUSE timeout 必须是整数".to_string())
                })?;
                let mode = if arr.len() >= 4 {
                    self.extract_string(&arr[3])?.to_ascii_uppercase()
                } else {
                    "ALL".to_string()
                };
                Ok(Command::ClientPause(timeout, mode))
            }
            "UNPAUSE" => {
                if arr.len() != 2 {
                    return Err(AppError::Command(
                        "CLIENT UNPAUSE 命令不需要参数".to_string(),
                    ));
                }
                Ok(Command::ClientUnpause)
            }
            "NO-EVICT" => {
                if arr.len() != 3 {
                    return Err(AppError::Command(
                        "CLIENT NO-EVICT 需要 ON|OFF 参数".to_string(),
                    ));
                }
                let flag = match self.extract_string(&arr[2])?.to_ascii_uppercase().as_str() {
                    "ON" => true,
                    "OFF" => false,
                    _ => return Err(AppError::Command(
                        "CLIENT NO-EVICT 参数必须是 ON 或 OFF".to_string(),
                    )),
                };
                Ok(Command::ClientNoEvict(flag))
            }
            "NO-TOUCH" => {
                if arr.len() != 3 {
                    return Err(AppError::Command(
                        "CLIENT NO-TOUCH 需要 ON|OFF 参数".to_string(),
                    ));
                }
                let flag = match self.extract_string(&arr[2])?.to_ascii_uppercase().as_str() {
                    "ON" => true,
                    "OFF" => false,
                    _ => return Err(AppError::Command(
                        "CLIENT NO-TOUCH 参数必须是 ON 或 OFF".to_string(),
                    )),
                };
                Ok(Command::ClientNoTouch(flag))
            }
            "REPLY" => {
                if arr.len() != 3 {
                    return Err(AppError::Command(
                        "CLIENT REPLY 需要 ON|OFF|SKIP 参数".to_string(),
                    ));
                }
                let mode = match self.extract_string(&arr[2])?.to_ascii_uppercase().as_str() {
                    "ON" => crate::server::ReplyMode::On,
                    "OFF" => crate::server::ReplyMode::Off,
                    "SKIP" => crate::server::ReplyMode::Skip,
                    _ => return Err(AppError::Command(
                        "CLIENT REPLY 参数必须是 ON、OFF 或 SKIP".to_string(),
                    )),
                };
                Ok(Command::ClientReply(mode))
            }
            "UNBLOCK" => {
                if arr.len() < 3 {
                    return Err(AppError::Command(
                        "CLIENT UNBLOCK 需要 client-id 参数".to_string(),
                    ));
                }
                let target_id: u64 = self.extract_string(&arr[2])?.parse().map_err(|_| {
                    AppError::Command("CLIENT UNBLOCK client-id 必须是整数".to_string())
                })?;
                let reason = if arr.len() >= 4 {
                    self.extract_string(&arr[3])?.to_ascii_uppercase()
                } else {
                    "TIMEOUT".to_string()
                };
                Ok(Command::ClientUnblock(target_id, reason))
            }
            _ => Err(AppError::Command(
                format!("CLIENT 未知子命令: {}", sub),
            )),
        }
    }
    pub(crate) fn parse_sort(&self, arr: &[RespValue]) -> Result<Command> {
        if arr.len() < 2 {
            return Err(AppError::Command(
                "SORT 命令需要至少 1 个参数".to_string(),
            ));
        }
        let key = self.extract_string(&arr[1])?;
        let mut by_pattern = None;
        let mut get_patterns = Vec::new();
        let mut limit_offset = None;
        let mut limit_count = None;
        let mut asc = true;
        let mut alpha = false;
        let mut store_key = None;

        let mut i = 2;
        while i < arr.len() {
            let arg = self.extract_string(&arr[i])?.to_ascii_uppercase();
            match arg.as_str() {
                "BY" => {
                    if i + 1 >= arr.len() {
                        return Err(AppError::Command("SORT BY 需要参数".to_string()));
                    }
                    by_pattern = Some(self.extract_string(&arr[i + 1])?);
                    i += 2;
                }
                "LIMIT" => {
                    if i + 2 >= arr.len() {
                        return Err(AppError::Command("SORT LIMIT 需要 2 个参数".to_string()));
                    }
                    limit_offset = Some(self.extract_string(&arr[i + 1])?.parse::<isize>().map_err(|_| {
                        AppError::Command("SORT LIMIT offset 必须是整数".to_string())
                    })?);
                    limit_count = Some(self.extract_string(&arr[i + 2])?.parse::<isize>().map_err(|_| {
                        AppError::Command("SORT LIMIT count 必须是整数".to_string())
                    })?);
                    i += 3;
                }
                "GET" => {
                    if i + 1 >= arr.len() {
                        return Err(AppError::Command("SORT GET 需要参数".to_string()));
                    }
                    get_patterns.push(self.extract_string(&arr[i + 1])?);
                    i += 2;
                }
                "ASC" => {
                    asc = true;
                    i += 1;
                }
                "DESC" => {
                    asc = false;
                    i += 1;
                }
                "ALPHA" => {
                    alpha = true;
                    i += 1;
                }
                "STORE" => {
                    if i + 1 >= arr.len() {
                        return Err(AppError::Command("SORT STORE 需要参数".to_string()));
                    }
                    store_key = Some(self.extract_string(&arr[i + 1])?);
                    i += 2;
                }
                _ => {
                    return Err(AppError::Command(format!("SORT 未知参数: {}", arg)));
                }
            }
        }

        Ok(Command::Sort(key, by_pattern, get_patterns, limit_offset, limit_count, asc, alpha, store_key))
    }
    pub(crate) fn parse_eval(&self, arr: &[RespValue]) -> Result<Command> {
        if arr.len() < 2 {
            return Err(AppError::Command(
                "EVAL 命令需要至少 1 个参数".to_string(),
            ));
        }
        let script = self.extract_string(&arr[1])?;
        if arr.len() < 3 {
            return Ok(Command::Eval(script, Vec::new(), Vec::new()));
        }
        let numkeys: usize = self.extract_string(&arr[2])?.parse().map_err(|_| {
            AppError::Command("EVAL numkeys 必须是整数".to_string())
        })?;
        if arr.len() < 3 + numkeys {
            return Err(AppError::Command(
                "EVAL 提供的 key 数量不足".to_string(),
            ));
        }
        let mut keys = Vec::new();
        for i in 0..numkeys {
            keys.push(self.extract_string(&arr[3 + i])?);
        }
        let mut args = Vec::new();
        for i in (3 + numkeys)..arr.len() {
            args.push(self.extract_string(&arr[i])?);
        }
        Ok(Command::Eval(script, keys, args))
    }
    pub(crate) fn parse_evalsha(&self, arr: &[RespValue]) -> Result<Command> {
        if arr.len() < 2 {
            return Err(AppError::Command(
                "EVALSHA 命令需要至少 1 个参数".to_string(),
            ));
        }
        let sha1 = self.extract_string(&arr[1])?;
        if arr.len() < 3 {
            return Ok(Command::EvalSha(sha1, Vec::new(), Vec::new()));
        }
        let numkeys: usize = self.extract_string(&arr[2])?.parse().map_err(|_| {
            AppError::Command("EVALSHA numkeys 必须是整数".to_string())
        })?;
        if arr.len() < 3 + numkeys {
            return Err(AppError::Command(
                "EVALSHA 提供的 key 数量不足".to_string(),
            ));
        }
        let mut keys = Vec::new();
        for i in 0..numkeys {
            keys.push(self.extract_string(&arr[3 + i])?);
        }
        let mut args = Vec::new();
        for i in (3 + numkeys)..arr.len() {
            args.push(self.extract_string(&arr[i])?);
        }
        Ok(Command::EvalSha(sha1, keys, args))
    }
    pub(crate) fn parse_script(&self, arr: &[RespValue]) -> Result<Command> {
        if arr.len() < 2 {
            return Err(AppError::Command(
                "SCRIPT 命令需要子命令".to_string(),
            ));
        }
        let sub = self.extract_string(&arr[1])?.to_ascii_uppercase();
        match sub.as_str() {
            "LOAD" => {
                if arr.len() != 3 {
                    return Err(AppError::Command(
                        "SCRIPT LOAD 命令需要 1 个参数".to_string(),
                    ));
                }
                let script = self.extract_string(&arr[2])?;
                Ok(Command::ScriptLoad(script))
            }
            "EXISTS" => {
                if arr.len() < 3 {
                    return Err(AppError::Command(
                        "SCRIPT EXISTS 命令需要至少 1 个参数".to_string(),
                    ));
                }
                let mut sha1s = Vec::new();
                for i in 2..arr.len() {
                    sha1s.push(self.extract_string(&arr[i])?);
                }
                Ok(Command::ScriptExists(sha1s))
            }
            "FLUSH" => {
                if arr.len() != 2 {
                    return Err(AppError::Command(
                        "SCRIPT FLUSH 命令不需要参数".to_string(),
                    ));
                }
                Ok(Command::ScriptFlush)
            }
            _ => Err(AppError::Command(
                format!("SCRIPT 未知子命令: {}", sub),
            )),
        }
    }
    pub(crate) fn parse_function(&self, arr: &[RespValue]) -> Result<Command> {
        if arr.len() < 2 {
            return Err(AppError::Command(
                "FUNCTION 命令需要子命令".to_string(),
            ));
        }
        let sub = self.extract_string(&arr[1])?.to_ascii_uppercase();
        match sub.as_str() {
            "LOAD" => {
                let mut replace = false;
                let mut code_idx = 2;
                if arr.len() > 2 {
                    let arg = self.extract_string(&arr[2])?.to_ascii_uppercase();
                    if arg == "REPLACE" {
                        replace = true;
                        code_idx = 3;
                    }
                }
                if arr.len() <= code_idx {
                    return Err(AppError::Command(
                        "FUNCTION LOAD 需要函数代码".to_string(),
                    ));
                }
                let code = self.extract_string(&arr[code_idx])?;
                Ok(Command::FunctionLoad(code, replace))
            }
            "DELETE" => {
                if arr.len() != 3 {
                    return Err(AppError::Command(
                        "FUNCTION DELETE 需要 1 个参数".to_string(),
                    ));
                }
                let lib = self.extract_string(&arr[2])?;
                Ok(Command::FunctionDelete(lib))
            }
            "LIST" => {
                let mut pattern = None;
                let mut withcode = false;
                let mut i = 2;
                while i < arr.len() {
                    let arg = self.extract_string(&arr[i])?.to_ascii_uppercase();
                    match arg.as_str() {
                        "LIBRARYNAME" => {
                            i += 1;
                            if i < arr.len() {
                                pattern = Some(self.extract_string(&arr[i])?);
                            }
                        }
                        "WITHCODE" => withcode = true,
                        _ => {}
                    }
                    i += 1;
                }
                Ok(Command::FunctionList(pattern, withcode))
            }
            "DUMP" => Ok(Command::FunctionDump),
            "RESTORE" => {
                if arr.len() < 3 {
                    return Err(AppError::Command(
                        "FUNCTION RESTORE 需要序列化数据".to_string(),
                    ));
                }
                let data = self.extract_string(&arr[2])?;
                let policy = if arr.len() >= 4 {
                    self.extract_string(&arr[3])?.to_ascii_uppercase()
                } else {
                    "FLUSH".to_string()
                };
                Ok(Command::FunctionRestore(data, policy))
            }
            "STATS" => Ok(Command::FunctionStats),
            "FLUSH" => {
                let async_mode = if arr.len() >= 3 {
                    let arg = self.extract_string(&arr[2])?.to_ascii_uppercase();
                    arg == "ASYNC"
                } else {
                    false
                };
                Ok(Command::FunctionFlush(async_mode))
            }
            _ => Err(AppError::Command(
                format!("FUNCTION 未知子命令: {}", sub),
            )),
        }
    }
    pub(crate) fn parse_fcall(&self, arr: &[RespValue]) -> Result<Command> {
        if arr.len() < 2 {
            return Err(AppError::Command(
                "FCALL 命令需要函数名".to_string(),
            ));
        }
        let name = self.extract_string(&arr[1])?;
        if arr.len() < 3 {
            return Ok(Command::FCall(name, vec![], vec![]));
        }
        let numkeys: usize = self.extract_string(&arr[2])?.parse().map_err(|_| {
            AppError::Command("FCALL numkeys 必须是整数".to_string())
        })?;
        if arr.len() < 3 + numkeys {
            return Err(AppError::Command(
                "FCALL 提供的 key 数量不足".to_string(),
            ));
        }
        let mut keys = Vec::new();
        for i in 0..numkeys {
            keys.push(self.extract_string(&arr[3 + i])?);
        }
        let mut args = Vec::new();
        for i in (3 + numkeys)..arr.len() {
            args.push(self.extract_string(&arr[i])?);
        }
        Ok(Command::FCall(name, keys, args))
    }
    pub(crate) fn parse_fcall_ro(&self, arr: &[RespValue]) -> Result<Command> {
        if arr.len() < 2 {
            return Err(AppError::Command(
                "FCALL_RO 命令需要函数名".to_string(),
            ));
        }
        let name = self.extract_string(&arr[1])?;
        if arr.len() < 3 {
            return Ok(Command::FCallRO(name, vec![], vec![]));
        }
        let numkeys: usize = self.extract_string(&arr[2])?.parse().map_err(|_| {
            AppError::Command("FCALL_RO numkeys 必须是整数".to_string())
        })?;
        if arr.len() < 3 + numkeys {
            return Err(AppError::Command(
                "FCALL_RO 提供的 key 数量不足".to_string(),
            ));
        }
        let mut keys = Vec::new();
        for i in 0..numkeys {
            keys.push(self.extract_string(&arr[3 + i])?);
        }
        let mut args = Vec::new();
        for i in (3 + numkeys)..arr.len() {
            args.push(self.extract_string(&arr[i])?);
        }
        Ok(Command::FCallRO(name, keys, args))
    }
    pub(crate) fn parse_eval_ro(&self, arr: &[RespValue]) -> Result<Command> {
        if arr.len() < 2 {
            return Err(AppError::Command(
                "EVAL_RO 命令需要脚本".to_string(),
            ));
        }
        let script = self.extract_string(&arr[1])?;
        if arr.len() < 3 {
            return Ok(Command::EvalRO(script, Vec::new(), Vec::new()));
        }
        let numkeys: usize = self.extract_string(&arr[2])?.parse().map_err(|_| {
            AppError::Command("EVAL_RO numkeys 必须是整数".to_string())
        })?;
        if arr.len() < 3 + numkeys {
            return Err(AppError::Command(
                "EVAL_RO 提供的 key 数量不足".to_string(),
            ));
        }
        let mut keys = Vec::new();
        for i in 0..numkeys {
            keys.push(self.extract_string(&arr[3 + i])?);
        }
        let mut args = Vec::new();
        for i in (3 + numkeys)..arr.len() {
            args.push(self.extract_string(&arr[i])?);
        }
        Ok(Command::EvalRO(script, keys, args))
    }
    pub(crate) fn parse_evalsha_ro(&self, arr: &[RespValue]) -> Result<Command> {
        if arr.len() < 2 {
            return Err(AppError::Command(
                "EVALSHA_RO 命令需要 sha1".to_string(),
            ));
        }
        let sha1 = self.extract_string(&arr[1])?;
        if arr.len() < 3 {
            return Ok(Command::EvalShaRO(sha1, Vec::new(), Vec::new()));
        }
        let numkeys: usize = self.extract_string(&arr[2])?.parse().map_err(|_| {
            AppError::Command("EVALSHA_RO numkeys 必须是整数".to_string())
        })?;
        if arr.len() < 3 + numkeys {
            return Err(AppError::Command(
                "EVALSHA_RO 提供的 key 数量不足".to_string(),
            ));
        }
        let mut keys = Vec::new();
        for i in 0..numkeys {
            keys.push(self.extract_string(&arr[3 + i])?);
        }
        let mut args = Vec::new();
        for i in (3 + numkeys)..arr.len() {
            args.push(self.extract_string(&arr[i])?);
        }
        Ok(Command::EvalShaRO(sha1, keys, args))
    }
    pub(crate) fn parse_config(&self, arr: &[RespValue]) -> Result<Command> {
        if arr.len() < 2 {
            return Err(AppError::Command(
                "CONFIG 命令需要子命令".to_string(),
            ));
        }

        let sub = self.extract_string(&arr[1])?.to_ascii_uppercase();
        match sub.as_str() {
            "GET" => {
                if arr.len() != 3 {
                    return Err(AppError::Command(
                        "CONFIG GET 需要 1 个参数".to_string(),
                    ));
                }
                let key = self.extract_string(&arr[2])?;
                Ok(Command::ConfigGet(key))
            }
            "SET" => {
                if arr.len() != 4 {
                    return Err(AppError::Command(
                        "CONFIG SET 需要 2 个参数".to_string(),
                    ));
                }
                let key = self.extract_string(&arr[2])?;
                let value = self.extract_string(&arr[3])?;
                Ok(Command::ConfigSet(key, value))
            }
            "REWRITE" => Ok(Command::ConfigRewrite),
            "RESETSTAT" => Ok(Command::ConfigResetStat),
            _ => Ok(Command::Unknown(format!("CONFIG {}", sub))),
        }
    }
    pub(crate) fn parse_memory(&self, arr: &[RespValue]) -> Result<Command> {
        if arr.len() < 2 {
            return Err(AppError::Command(
                "MEMORY 命令需要子命令".to_string(),
            ));
        }
        let sub = self.extract_string(&arr[1])?.to_ascii_uppercase();
        match sub.as_str() {
            "USAGE" => {
                if arr.len() < 3 {
                    return Err(AppError::Command(
                        "MEMORY USAGE 需要 key 参数".to_string(),
                    ));
                }
                let key = self.extract_string(&arr[2])?;
                let mut samples = None;
                let mut i = 3;
                while i < arr.len() {
                    let arg = self.extract_string(&arr[i])?.to_ascii_uppercase();
                    if arg == "SAMPLES" && i + 1 < arr.len() {
                        samples = Some(self.extract_string(&arr[i + 1])?.parse().map_err(|_| {
                            AppError::Command("MEMORY USAGE SAMPLES 必须是整数".to_string())
                        })?);
                        i += 2;
                    } else {
                        i += 1;
                    }
                }
                Ok(Command::MemoryUsage(key, samples))
            }
            "DOCTOR" => Ok(Command::MemoryDoctor),
            _ => Err(AppError::Command(format!("MEMORY 未知子命令: {}", sub))),
        }
    }
    pub(crate) fn parse_latency(&self, arr: &[RespValue]) -> Result<Command> {
        if arr.len() < 2 {
            return Err(AppError::Command(
                "LATENCY 命令需要子命令".to_string(),
            ));
        }
        let sub = self.extract_string(&arr[1])?.to_ascii_uppercase();
        match sub.as_str() {
            "LATEST" => Ok(Command::LatencyLatest),
            "HISTORY" => {
                if arr.len() != 3 {
                    return Err(AppError::Command(
                        "LATENCY HISTORY 需要事件名".to_string(),
                    ));
                }
                let event = self.extract_string(&arr[2])?;
                Ok(Command::LatencyHistory(event))
            }
            "RESET" => {
                let events: Vec<String> = arr[2..].iter()
                    .map(|v| self.extract_string(v))
                    .collect::<Result<Vec<_>>>()?;
                Ok(Command::LatencyReset(events))
            }
            _ => Err(AppError::Command(format!("LATENCY 未知子命令: {}", sub))),
        }
    }
    pub(crate) fn parse_hello(&self, arr: &[RespValue]) -> Result<Command> {
        let protover: u8 = if arr.len() >= 2 {
            self.extract_string(&arr[1])?.parse().map_err(|_| {
                AppError::Command("HELLO protover 必须是整数".to_string())
            })?
        } else {
            2
        };
        let mut auth = None;
        let mut setname = None;
        let mut i = 2;
        while i < arr.len() {
            let arg = self.extract_string(&arr[i])?.to_ascii_uppercase();
            match arg.as_str() {
                "AUTH" => {
                    if i + 2 < arr.len() {
                        let username = self.extract_string(&arr[i + 1])?;
                        let password = self.extract_string(&arr[i + 2])?;
                        auth = Some((username, password));
                        i += 3;
                        continue;
                    }
                }
                "SETNAME" => {
                    if i + 1 < arr.len() {
                        setname = Some(self.extract_string(&arr[i + 1])?);
                        i += 2;
                        continue;
                    }
                }
                _ => {}
            }
            i += 1;
        }
        Ok(Command::Hello(protover, auth, setname))
    }
    pub(crate) fn parse_slowlog(&self, arr: &[RespValue]) -> Result<Command> {
        if arr.len() < 2 {
            return Err(AppError::Command(
                "SLOWLOG 命令需要子命令".to_string(),
            ));
        }
        let sub = self.extract_string(&arr[1])?.to_ascii_uppercase();
        match sub.as_str() {
            "GET" => {
                let count = if arr.len() >= 3 {
                    self.extract_string(&arr[2])?.parse().map_err(|_| {
                        AppError::Command("SLOWLOG GET count 必须是整数".to_string())
                    })?
                } else {
                    10
                };
                Ok(Command::SlowLogGet(count))
            }
            "LEN" => {
                if arr.len() != 2 {
                    return Err(AppError::Command(
                        "SLOWLOG LEN 不需要参数".to_string(),
                    ));
                }
                Ok(Command::SlowLogLen)
            }
            "RESET" => {
                if arr.len() != 2 {
                    return Err(AppError::Command(
                        "SLOWLOG RESET 不需要参数".to_string(),
                    ));
                }
                Ok(Command::SlowLogReset)
            }
            _ => Err(AppError::Command(
                format!("SLOWLOG 未知子命令: {}", sub),
            )),
        }
    }
    pub(crate) fn parse_object(&self, arr: &[RespValue]) -> Result<Command> {
        if arr.len() < 2 {
            return Err(AppError::Command(
                "OBJECT 命令需要子命令".to_string(),
            ));
        }
        let sub = self.extract_string(&arr[1])?.to_ascii_uppercase();
        match sub.as_str() {
            "ENCODING" => {
                if arr.len() != 3 {
                    return Err(AppError::Command(
                        "OBJECT ENCODING 需要 1 个 key 参数".to_string(),
                    ));
                }
                Ok(Command::ObjectEncoding(self.extract_string(&arr[2])?))
            }
            "REFCOUNT" => {
                if arr.len() != 3 {
                    return Err(AppError::Command(
                        "OBJECT REFCOUNT 需要 1 个 key 参数".to_string(),
                    ));
                }
                Ok(Command::ObjectRefCount(self.extract_string(&arr[2])?))
            }
            "IDLETIME" => {
                if arr.len() != 3 {
                    return Err(AppError::Command(
                        "OBJECT IDLETIME 需要 1 个 key 参数".to_string(),
                    ));
                }
                Ok(Command::ObjectIdleTime(self.extract_string(&arr[2])?))
            }
            "HELP" => Ok(Command::ObjectHelp),
            _ => Err(AppError::Command(
                format!("OBJECT 未知子命令: {}", sub),
            )),
        }
    }
    pub(crate) fn parse_debug(&self, arr: &[RespValue]) -> Result<Command> {
        if arr.len() < 2 {
            return Err(AppError::Command(
                "DEBUG 命令需要子命令".to_string(),
            ));
        }
        let sub = self.extract_string(&arr[1])?.to_ascii_uppercase();
        match sub.as_str() {
            "SET-ACTIVE-EXPIRE" => {
                if arr.len() != 3 {
                    return Err(AppError::Command(
                        "DEBUG SET-ACTIVE-EXPIRE 需要 1 个参数 (0|1)".to_string(),
                    ));
                }
                let flag = match self.extract_string(&arr[2])?.as_str() {
                    "0" => false,
                    "1" => true,
                    _ => return Err(AppError::Command(
                        "DEBUG SET-ACTIVE-EXPIRE 参数必须是 0 或 1".to_string(),
                    )),
                };
                Ok(Command::DebugSetActiveExpire(flag))
            }
            "SLEEP" => {
                if arr.len() != 3 {
                    return Err(AppError::Command(
                        "DEBUG SLEEP 需要 1 个参数 (seconds)".to_string(),
                    ));
                }
                let seconds: f64 = self.extract_string(&arr[2])?.parse().map_err(|_| {
                    AppError::Command("DEBUG SLEEP 参数必须是数字".to_string())
                })?;
                Ok(Command::DebugSleep(seconds))
            }
            "OBJECT" => {
                if arr.len() != 3 {
                    return Err(AppError::Command(
                        "DEBUG OBJECT 需要 1 个 key 参数".to_string(),
                    ));
                }
                Ok(Command::DebugObject(self.extract_string(&arr[2])?))
            }
            _ => Err(AppError::Command(
                format!("DEBUG 未知子命令: {}", sub),
            )),
        }
    }
    pub(crate) fn parse_echo(&self, arr: &[RespValue]) -> Result<Command> {
        if arr.len() != 2 {
            return Err(AppError::Command(
                "ECHO 命令需要 1 个参数".to_string(),
            ));
        }
        Ok(Command::Echo(self.extract_string(&arr[1])?))
    }
    pub(crate) fn parse_swapdb(&self, arr: &[RespValue]) -> Result<Command> {
        if arr.len() != 3 {
            return Err(AppError::Command(
                "SWAPDB 命令需要 2 个参数".to_string(),
            ));
        }
        let idx1: usize = self.extract_string(&arr[1])?.parse().map_err(|_| {
            AppError::Command("SWAPDB 索引必须是整数".to_string())
        })?;
        let idx2: usize = self.extract_string(&arr[2])?.parse().map_err(|_| {
            AppError::Command("SWAPDB 索引必须是整数".to_string())
        })?;
        Ok(Command::SwapDb(idx1, idx2))
    }
    pub(crate) fn parse_shutdown(&self, arr: &[RespValue]) -> Result<Command> {
        let opt = if arr.len() >= 2 {
            Some(self.extract_string(&arr[1])?.to_ascii_uppercase())
        } else {
            None
        };
        Ok(Command::Shutdown(opt))
    }
}
