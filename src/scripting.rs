// Lua 脚本执行模块

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use bytes::Bytes;
use mlua::{Lua, Value, Variadic};

use crate::command::{CommandExecutor, CommandParser};
use crate::error::{AppError, Result};
use crate::protocol::RespValue;
use crate::storage::StorageEngine;

/// 函数库信息
#[derive(Debug, Clone)]
pub struct FunctionLibrary {
    /// 库名
    pub name: String,
    /// 引擎名（目前只支持 lua）
    pub engine: String,
    /// 完整源码
    pub code: String,
    /// 库中的函数列表
    pub functions: HashMap<String, FunctionInfo>,
}

/// 函数信息
#[derive(Debug, Clone)]
pub struct FunctionInfo {
    /// 函数名
    pub name: String,
    /// 函数标志（如 no-writes, allow-stale 等）
    pub flags: Vec<String>,
    /// Lua 字节码
    pub bytecode: Vec<u8>,
}

/// Lua 脚本引擎，管理脚本缓存、Lua 虚拟机和函数库
#[derive(Debug, Clone)]
pub struct ScriptEngine {
    /// 脚本缓存：SHA1 → 脚本源码
    scripts: Arc<Mutex<HashMap<String, String>>>,
    /// 已加载的函数库
    libraries: Arc<Mutex<HashMap<String, FunctionLibrary>>>,
}

impl ScriptEngine {
    /// 创建新的脚本引擎
    pub fn new() -> Self {
        Self {
            scripts: Arc::new(Mutex::new(HashMap::new())),
            libraries: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// 执行 Lua 脚本
    pub fn eval(
        &self,
        script: &str,
        keys: Vec<String>,
        args: Vec<String>,
        storage: StorageEngine,
    ) -> Result<RespValue> {
        let lua = Lua::new();
        let executor = CommandExecutor::new(storage);

        // 注册 redis 表
        let redis_table = lua.create_table()?;

        // redis.call
        let exec_call = executor.clone();
        let call_fn = lua.create_function(move |lua, (cmd_name, lua_args): (String, Variadic<Value>)| {
            match execute_redis_command(&exec_call, &cmd_name, &lua_args) {
                Ok(resp) => resp_value_to_lua(lua, &resp),
                Err(e) => {
                    Err(mlua::Error::RuntimeError(format!("ERR {}", e)))
                }
            }
        })?;
        redis_table.set("call", call_fn)?;

        // redis.pcall
        let exec_pcall = executor.clone();
        let pcall_fn = lua.create_function(move |lua, (cmd_name, lua_args): (String, Variadic<Value>)| {
            match execute_redis_command(&exec_pcall, &cmd_name, &lua_args) {
                Ok(resp) => resp_value_to_lua(lua, &resp),
                Err(e) => {
                    let err_table = lua.create_table()?;
                    err_table.set("err", format!("ERR {}", e))?;
                    Ok(Value::Table(err_table))
                }
            }
        })?;
        redis_table.set("pcall", pcall_fn)?;

        lua.globals().set("redis", redis_table)?;

        // 设置 KEYS 表
        let keys_table = lua.create_table()?;
        for (i, key) in keys.iter().enumerate() {
            keys_table.set(i + 1, key.as_str())?;
        }
        lua.globals().set("KEYS", keys_table)?;

        // 设置 ARGV 表
        let argv_table = lua.create_table()?;
        for (i, arg) in args.iter().enumerate() {
            argv_table.set(i + 1, arg.as_str())?;
        }
        lua.globals().set("ARGV", argv_table)?;

        // 执行脚本
        let result: Value = lua.load(script).eval()?;

        // 转换 Lua 返回值为 RespValue
        lua_value_to_resp(&result)
    }

    /// 通过 SHA1 执行已缓存的脚本
    pub fn evalsha(
        &self,
        sha1: &str,
        keys: Vec<String>,
        args: Vec<String>,
        storage: StorageEngine,
    ) -> Result<RespValue> {
        let scripts = self.scripts.lock().map_err(|e| {
            AppError::Storage(format!("脚本缓存锁中毒: {}", e))
        })?;
        match scripts.get(sha1) {
            Some(script) => {
                let script = script.clone();
                drop(scripts);
                self.eval(&script, keys, args, storage)
            }
            None => Err(AppError::Command(
                "NOSCRIPT No matching script. Please use EVAL.".to_string(),
            )),
        }
    }

    /// 缓存脚本并返回 SHA1
    pub fn script_load(&self, script: &str) -> Result<String> {
        let sha1 = compute_sha1(script);
        let mut scripts = self.scripts.lock().map_err(|e| {
            AppError::Storage(format!("脚本缓存锁中毒: {}", e))
        })?;
        scripts.insert(sha1.clone(), script.to_string());
        Ok(sha1)
    }

    /// 检查脚本是否已缓存
    pub fn script_exists(&self, sha1s: &[String]) -> Result<Vec<bool>> {
        let scripts = self.scripts.lock().map_err(|e| {
            AppError::Storage(format!("脚本缓存锁中毒: {}", e))
        })?;
        Ok(sha1s.iter().map(|s| scripts.contains_key(s)).collect())
    }

    /// 清空脚本缓存
    pub fn script_flush(&self) -> Result<()> {
        let mut scripts = self.scripts.lock().map_err(|e| {
            AppError::Storage(format!("脚本缓存锁中毒: {}", e))
        })?;
        scripts.clear();
        Ok(())
    }

    // ---------- Function 系统 ----------

    /// 加载函数库
    /// 代码格式：#!lua name=mylib\n redis.register_function("func1", function(keys, args) ... end)
    pub fn function_load(&self, code: &str, replace: bool) -> Result<String> {
        let (engine, lib_name, body) = parse_shebang(code)?;

        if engine != "lua" {
            return Err(AppError::Command(format!(
                "不支持的函数引擎: {}", engine
            )));
        }

        {
            let libs = self.libraries.lock().map_err(|e| {
                AppError::Storage(format!("函数库锁中毒: {}", e))
            })?;
            if libs.contains_key(&lib_name) && !replace {
                return Err(AppError::Command(
                    "函数库已存在，使用 REPLACE 覆盖".to_string(),
                ));
            }
        }

        let lua = Lua::new();
        let registered = Arc::new(Mutex::new(Vec::<(String, Vec<u8>, Vec<String>)>::new()));

        // 注册 redis.register_function
        let redis_table = lua.create_table()?;
        let reg = registered.clone();
        let register_fn = lua.create_function(move |_lua, args: Variadic<Value>| {
            if args.len() < 2 {
                return Err(mlua::Error::RuntimeError(
                    "redis.register_function 需要至少 2 个参数".to_string(),
                ));
            }
            let name = match &args[0] {
                Value::String(s) => s.to_str()?.to_string(),
                _ => {
                    return Err(mlua::Error::RuntimeError(
                        "函数名必须是字符串".to_string(),
                    ))
                }
            };
            let func = match &args[1] {
                Value::Function(f) => f.clone(),
                _ => {
                    return Err(mlua::Error::RuntimeError(
                        "第二个参数必须是函数".to_string(),
                    ))
                }
            };
            let mut flags = Vec::new();
            for i in 2..args.len() {
                match &args[i] {
                    Value::String(s) => flags.push(s.to_str()?.to_string()),
                    _ => {}
                }
            }
            let bytecode = func.dump(false);
            let mut r = reg.lock().map_err(|e| {
                mlua::Error::RuntimeError(format!("锁中毒: {}", e))
            })?;
            r.push((name, bytecode, flags));
            Ok(())
        })?;
        redis_table.set("register_function", register_fn)?;
        lua.globals().set("redis", redis_table)?;

        // 执行库代码
        lua.load(&body).exec().map_err(|e| {
            AppError::Command(format!("函数库加载失败: {}", e))
        })?;

        // 收集注册的函数
        let mut r = registered.lock().map_err(|e| {
            AppError::Storage(format!("锁中毒: {}", e))
        })?;
        let mut functions = HashMap::new();
        for (name, bytecode, flags) in r.drain(..) {
            functions.insert(
                name.clone(),
                FunctionInfo {
                    name: name.clone(),
                    flags,
                    bytecode,
                },
            );
        }

        let mut libs = self.libraries.lock().map_err(|e| {
            AppError::Storage(format!("函数库锁中毒: {}", e))
        })?;
        libs.insert(
            lib_name.clone(),
            FunctionLibrary {
                name: lib_name.clone(),
                engine,
                code: code.to_string(),
                functions,
            },
        );

        Ok(lib_name)
    }

    /// 删除函数库
    pub fn function_delete(&self, library_name: &str) -> Result<bool> {
        let mut libs = self.libraries.lock().map_err(|e| {
            AppError::Storage(format!("函数库锁中毒: {}", e))
        })?;
        Ok(libs.remove(library_name).is_some())
    }

    /// 列出函数库信息
    pub fn function_list(
        &self,
        library_pattern: Option<&str>,
        withcode: bool,
    ) -> Result<Vec<(String, String, String, Vec<(String, Vec<String>)>)>> {
        let libs = self.libraries.lock().map_err(|e| {
            AppError::Storage(format!("函数库锁中毒: {}", e))
        })?;
        let mut result = Vec::new();
        for (name, lib) in libs.iter() {
            if let Some(pattern) = library_pattern {
                if !glob_match(pattern, name) {
                    continue;
                }
            }
            let funcs: Vec<(String, Vec<String>)> = lib
                .functions
                .iter()
                .map(|(n, info)| (n.clone(), info.flags.clone()))
                .collect();
            let code = if withcode {
                lib.code.clone()
            } else {
                String::new()
            };
            result.push((name.clone(), lib.engine.clone(), code, funcs));
        }
        Ok(result)
    }

    /// 序列化所有函数库
    pub fn function_dump(&self) -> Result<String> {
        let libs = self.libraries.lock().map_err(|e| {
            AppError::Storage(format!("函数库锁中毒: {}", e))
        })?;
        let mut lines = Vec::new();
        lines.push(libs.len().to_string());
        for (name, lib) in libs.iter() {
            lines.push(name.clone());
            lines.push(lib.engine.clone());
            let code_lines: Vec<&str> = lib.code.lines().collect();
            lines.push(code_lines.len().to_string());
            for line in code_lines {
                lines.push(line.to_string());
            }
            lines.push(lib.functions.len().to_string());
            for (fname, info) in lib.functions.iter() {
                lines.push(fname.clone());
                lines.push(info.flags.join(","));
                lines.push(info.bytecode.len().to_string());
                lines.push(hex_encode(&info.bytecode));
            }
        }
        Ok(lines.join("\n"))
    }

    /// 从序列化数据恢复函数库
    pub fn function_restore(&self, data: &str, policy: &str) -> Result<()> {
        let policy = policy.to_ascii_uppercase();
        match policy.as_str() {
            "FLUSH" => {
                self.function_flush_lib()?;
            }
            "APPEND" | "REPLACE" => {}
            _ => {
                return Err(AppError::Command(
                    "FUNCTION RESTORE policy 必须是 FLUSH/APPEND/REPLACE".to_string(),
                ))
            }
        }

        let mut iter = data.lines();
        let num_libs: usize = iter
            .next()
            .unwrap_or("0")
            .parse()
            .map_err(|_| AppError::Command("FUNCTION RESTORE 数据格式错误".to_string()))?;

        for _ in 0..num_libs {
            let name = iter
                .next()
                .ok_or_else(|| AppError::Command("FUNCTION RESTORE 数据格式错误".to_string()))?
                .to_string();
            let engine = iter
                .next()
                .ok_or_else(|| AppError::Command("FUNCTION RESTORE 数据格式错误".to_string()))?
                .to_string();
            let num_code_lines: usize = iter
                .next()
                .unwrap_or("0")
                .parse()
                .map_err(|_| AppError::Command("FUNCTION RESTORE 数据格式错误".to_string()))?;
            let mut code_lines = Vec::new();
            for _ in 0..num_code_lines {
                code_lines.push(iter.next().unwrap_or("").to_string());
            }
            let code = code_lines.join("\n");

            let num_funcs: usize = iter
                .next()
                .unwrap_or("0")
                .parse()
                .map_err(|_| AppError::Command("FUNCTION RESTORE 数据格式错误".to_string()))?;

            let mut functions = HashMap::new();
            for _ in 0..num_funcs {
                let fname = iter
                    .next()
                    .ok_or_else(|| AppError::Command("FUNCTION RESTORE 数据格式错误".to_string()))?
                    .to_string();
                let flags_str = iter
                    .next()
                    .ok_or_else(|| AppError::Command("FUNCTION RESTORE 数据格式错误".to_string()))?
                    .to_string();
                let flags: Vec<String> = if flags_str.is_empty() {
                    vec![]
                } else {
                    flags_str.split(',').map(|s| s.to_string()).collect()
                };
                let _bc_len: usize = iter
                    .next()
                    .unwrap_or("0")
                    .parse()
                    .map_err(|_| AppError::Command("FUNCTION RESTORE 数据格式错误".to_string()))?;
                let bc_hex = iter
                    .next()
                    .ok_or_else(|| AppError::Command("FUNCTION RESTORE 数据格式错误".to_string()))?;
                let bytecode = hex_decode(bc_hex).map_err(|_| {
                    AppError::Command("FUNCTION RESTORE 字节码格式错误".to_string())
                })?;
                functions.insert(
                    fname.clone(),
                    FunctionInfo {
                        name: fname.clone(),
                        flags,
                        bytecode,
                    },
                );
            }

            {
                let mut libs = self.libraries.lock().map_err(|e| {
                    AppError::Storage(format!("函数库锁中毒: {}", e))
                })?;
                if policy == "APPEND" && libs.contains_key(&name) {
                    continue;
                }
                libs.insert(
                    name.clone(),
                    FunctionLibrary {
                        name,
                        engine,
                        code,
                        functions,
                    },
                );
            }
        }

        Ok(())
    }

    /// 返回函数库统计信息
    pub fn function_stats(&self) -> Result<(usize, usize)> {
        let libs = self.libraries.lock().map_err(|e| {
            AppError::Storage(format!("函数库锁中毒: {}", e))
        })?;
        let lib_count = libs.len();
        let func_count = libs.values().map(|l| l.functions.len()).sum();
        Ok((lib_count, func_count))
    }

    /// 清空所有函数库
    pub fn function_flush(&self, _async_mode: bool) -> Result<()> {
        self.function_flush_lib()
    }

    fn function_flush_lib(&self) -> Result<()> {
        let mut libs = self.libraries.lock().map_err(|e| {
            AppError::Storage(format!("函数库锁中毒: {}", e))
        })?;
        libs.clear();
        Ok(())
    }

    /// 调用已注册的函数
    pub fn fcall(
        &self,
        function_name: &str,
        keys: Vec<String>,
        args: Vec<String>,
        storage: StorageEngine,
    ) -> Result<RespValue> {
        let func_info = self.find_function(function_name)?;
        self.execute_function(&func_info, keys, args, storage)
    }

    /// 只读调用已注册的函数（目前和 fcall 相同）
    pub fn fcall_ro(
        &self,
        function_name: &str,
        keys: Vec<String>,
        args: Vec<String>,
        storage: StorageEngine,
    ) -> Result<RespValue> {
        self.fcall(function_name, keys, args, storage)
    }

    /// 查找函数
    fn find_function(&self, function_name: &str) -> Result<FunctionInfo> {
        let libs = self.libraries.lock().map_err(|e| {
            AppError::Storage(format!("函数库锁中毒: {}", e))
        })?;
        for lib in libs.values() {
            if let Some(info) = lib.functions.get(function_name) {
                return Ok(info.clone());
            }
        }
        Err(AppError::Command(format!(
            "函数 '{}' 不存在",
            function_name
        )))
    }

    /// 执行函数
    fn execute_function(
        &self,
        func_info: &FunctionInfo,
        keys: Vec<String>,
        args: Vec<String>,
        storage: StorageEngine,
    ) -> Result<RespValue> {
        let lua = Lua::new();
        let executor = CommandExecutor::new(storage);

        // 注册 redis.call
        let exec_call = executor.clone();
        let call_fn = lua.create_function(
            move |lua, (cmd_name, lua_args): (String, Variadic<Value>)| {
                match execute_redis_command(&exec_call, &cmd_name, &lua_args) {
                    Ok(resp) => resp_value_to_lua(lua, &resp),
                    Err(e) => Err(mlua::Error::RuntimeError(format!("ERR {}", e))),
                }
            },
        )?;

        // 注册 redis.pcall
        let exec_pcall = executor.clone();
        let pcall_fn = lua.create_function(
            move |lua, (cmd_name, lua_args): (String, Variadic<Value>)| {
                match execute_redis_command(&exec_pcall, &cmd_name, &lua_args) {
                    Ok(resp) => resp_value_to_lua(lua, &resp),
                    Err(e) => {
                        let err_table = lua.create_table()?;
                        err_table.set("err", format!("ERR {}", e))?;
                        Ok(Value::Table(err_table))
                    }
                }
            },
        )?;

        let redis_table = lua.create_table()?;
        redis_table.set("call", call_fn)?;
        redis_table.set("pcall", pcall_fn)?;
        lua.globals().set("redis", redis_table)?;

        // 设置 KEYS 和 ARGV
        let keys_table = lua.create_table()?;
        for (i, key) in keys.iter().enumerate() {
            keys_table.set(i + 1, key.as_str())?;
        }
        lua.globals().set("KEYS", keys_table.clone())?;

        let argv_table = lua.create_table()?;
        for (i, arg) in args.iter().enumerate() {
            argv_table.set(i + 1, arg.as_str())?;
        }
        lua.globals().set("ARGV", argv_table.clone())?;

        // 加载函数字节码
        let func: mlua::Function = lua.load(&func_info.bytecode).into_function().map_err(|e| {
            AppError::Command(format!("函数字节码加载失败: {}", e))
        })?;

        // 调用函数
        let result: Value = func.call((keys_table, argv_table)).map_err(|e| {
            AppError::Command(format!("函数执行失败: {}", e))
        })?;

        lua_value_to_resp(&result)
    }
}

impl Default for ScriptEngine {
    fn default() -> Self {
        Self::new()
    }
}

/// 执行 Redis 命令（供 Lua redis.call/pcall 使用）
fn execute_redis_command(
    executor: &CommandExecutor,
    cmd_name: &str,
    lua_args: &Variadic<Value>,
) -> Result<RespValue> {
    // 将 Lua 参数转换为字符串
    let mut str_args = Vec::new();
    for arg in lua_args.iter() {
        str_args.push(lua_arg_to_string(arg)?);
    }

    // 构建 RESP 数组
    let mut resp_parts = vec![RespValue::BulkString(Some(Bytes::from(cmd_name.to_ascii_uppercase())))];
    for arg in str_args {
        resp_parts.push(RespValue::BulkString(Some(Bytes::from(arg))));
    }
    let resp = RespValue::Array(resp_parts);

    // 解析并执行命令
    let parser = CommandParser::new();
    let cmd = parser.parse(resp)?;
    executor.execute(cmd)
}

/// 将 Lua 参数转换为字符串（供 Redis 命令使用）
fn lua_arg_to_string(value: &Value) -> Result<String> {
    match value {
        Value::String(s) => Ok(s.to_str()?.to_string()),
        Value::Integer(i) => Ok(i.to_string()),
        Value::Number(n) => Ok(n.to_string()),
        Value::Boolean(true) => Ok("1".to_string()),
        Value::Boolean(false) => Ok("".to_string()),
        Value::Nil => Ok("".to_string()),
        _ => Err(AppError::Command(format!(
            "不支持的 Lua 参数类型: {:?}",
            value
        ))),
    }
}

/// 将 RespValue 转换为 Lua Value
fn resp_value_to_lua<'a>(lua: &'a Lua, resp: &RespValue) -> mlua::Result<Value<'a>> {
    match resp {
        RespValue::SimpleString(s) => Ok(Value::String(lua.create_string(s)?)),
        RespValue::Error(s) => Ok(Value::String(lua.create_string(s)?)),
        RespValue::Integer(i) => Ok(Value::Integer(*i)),
        RespValue::BulkString(Some(b)) => {
            Ok(Value::String(lua.create_string(String::from_utf8_lossy(b).as_bytes())?))
        }
        RespValue::BulkString(None) => Ok(Value::Nil),
        RespValue::Array(arr) => {
            let table = lua.create_table()?;
            for (i, item) in arr.iter().enumerate() {
                table.set(i + 1, resp_value_to_lua(lua, item)?)?;
            }
            Ok(Value::Table(table))
        }
    }
}

/// 将 Lua Value 转换为 RespValue
fn lua_value_to_resp(value: &Value) -> Result<RespValue> {
    match value {
        Value::Integer(i) => Ok(RespValue::Integer(*i)),
        Value::Number(n) => Ok(RespValue::Integer(*n as i64)),
        Value::String(s) => Ok(RespValue::BulkString(Some(Bytes::from(
            s.to_str()?.to_string()
        )))),
        Value::Boolean(true) => Ok(RespValue::Integer(1)),
        Value::Boolean(false) => Ok(RespValue::BulkString(None)),
        Value::Nil => Ok(RespValue::BulkString(None)),
        Value::Table(t) => {
            let mut arr = Vec::new();
            // 尝试按数组部分遍历
            for i in 1..=t.raw_len() {
                let v: Value = t.get(i)?;
                arr.push(lua_value_to_resp(&v)?);
            }
            Ok(RespValue::Array(arr))
        }
        _ => Ok(RespValue::BulkString(None)),
    }
}

/// 计算字符串的 SHA1 哈希
fn compute_sha1(s: &str) -> String {
    use sha1::{Sha1, Digest};
    let mut hasher = Sha1::new();
    hasher.update(s.as_bytes());
    format!("{:x}", hasher.finalize())
}

/// 解析函数库的 #! 头部
/// 格式：#!lua name=mylib
fn parse_shebang(code: &str) -> Result<(String, String, String)> {
    let trimmed = code.trim_start();
    if !trimmed.starts_with("#!") {
        return Err(AppError::Command(
            "函数库代码必须以 #! 开头".to_string(),
        ));
    }
    let end = trimmed.find('\n').unwrap_or(trimmed.len());
    let shebang = trimmed[2..end].trim();
    let body = trimmed[end..].to_string();

    let parts: Vec<&str> = shebang.split_whitespace().collect();
    if parts.is_empty() {
        return Err(AppError::Command("#! 行格式错误".to_string()));
    }

    let engine = parts[0].to_string();
    let mut lib_name = None;

    for part in &parts[1..] {
        if let Some(pos) = part.find('=') {
            let key = &part[..pos];
            let value = &part[pos + 1..];
            if key == "name" {
                lib_name = Some(value.to_string());
            }
        }
    }

    let lib_name = lib_name.ok_or_else(|| {
        AppError::Command("#! 行必须包含 name=library_name".to_string())
    })?;

    Ok((engine, lib_name, body))
}

/// 简化的 glob 匹配：支持 * 和 ?
fn glob_match(pattern: &str, text: &str) -> bool {
    let mut pattern_chars = pattern.chars().peekable();
    let mut text_chars = text.chars().peekable();

    while let Some(p) = pattern_chars.peek() {
        match p {
            '*' => {
                pattern_chars.next();
                if pattern_chars.peek().is_none() {
                    return true;
                }
                let remaining_pattern: String = pattern_chars.collect();
                for i in 0..=text.len() {
                    if glob_match(&remaining_pattern, &text[i..]) {
                        return true;
                    }
                }
                return false;
            }
            '?' => {
                pattern_chars.next();
                if text_chars.next().is_none() {
                    return false;
                }
            }
            _ => {
                let pc = pattern_chars.next().unwrap();
                let tc = match text_chars.next() {
                    Some(c) => c,
                    None => return false,
                };
                if pc != tc {
                    return false;
                }
            }
        }
    }

    text_chars.peek().is_none()
}

/// hex 编码
fn hex_encode(data: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut result = String::with_capacity(data.len() * 2);
    for &byte in data {
        result.push(HEX[(byte >> 4) as usize] as char);
        result.push(HEX[(byte & 0x0f) as usize] as char);
    }
    result
}

/// hex 解码
fn hex_decode(s: &str) -> Result<Vec<u8>> {
    if s.len() % 2 != 0 {
        return Err(AppError::Command("hex 字符串长度必须是偶数".to_string()));
    }
    let mut result = Vec::with_capacity(s.len() / 2);
    for i in (0..s.len()).step_by(2) {
        let hi = s.as_bytes()[i];
        let lo = s.as_bytes()[i + 1];
        let h = hex_char_value(hi)?;
        let l = hex_char_value(lo)?;
        result.push((h << 4) | l);
    }
    Ok(result)
}

fn hex_char_value(c: u8) -> Result<u8> {
    match c {
        b'0'..=b'9' => Ok(c - b'0'),
        b'a'..=b'f' => Ok(c - b'a' + 10),
        b'A'..=b'F' => Ok(c - b'A' + 10),
        _ => Err(AppError::Command("非法 hex 字符".to_string())),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_engine() -> (ScriptEngine, StorageEngine) {
        let engine = ScriptEngine::new();
        let storage = StorageEngine::new();
        (engine, storage)
    }

    #[test]
    fn test_eval_simple_return() {
        let (engine, storage) = make_engine();
        let resp = engine.eval("return 42", vec![], vec![], storage).unwrap();
        assert_eq!(resp, RespValue::Integer(42));
    }

    #[test]
    fn test_eval_string_return() {
        let (engine, storage) = make_engine();
        let resp = engine.eval("return 'hello'", vec![], vec![], storage).unwrap();
        assert_eq!(resp, RespValue::BulkString(Some(Bytes::from("hello"))));
    }

    #[test]
    fn test_eval_boolean_return() {
        let (engine, storage) = make_engine();
        let resp = engine.eval("return true", vec![], vec![], storage.clone()).unwrap();
        assert_eq!(resp, RespValue::Integer(1));

        let resp = engine.eval("return false", vec![], vec![], storage).unwrap();
        assert_eq!(resp, RespValue::BulkString(None));
    }

    #[test]
    fn test_eval_table_return() {
        let (engine, storage) = make_engine();
        let resp = engine.eval("return {1, 2, 3}", vec![], vec![], storage).unwrap();
        match resp {
            RespValue::Array(arr) => {
                assert_eq!(arr.len(), 3);
                assert_eq!(arr[0], RespValue::Integer(1));
                assert_eq!(arr[1], RespValue::Integer(2));
                assert_eq!(arr[2], RespValue::Integer(3));
            }
            _ => panic!("期望 Array，得到 {:?}", resp),
        }
    }

    #[test]
    fn test_eval_redis_call_set_get() {
        let (engine, storage) = make_engine();
        let script = r#"
            redis.call('SET', 'mykey', 'myvalue')
            return redis.call('GET', 'mykey')
        "#;
        let resp = engine.eval(script, vec![], vec![], storage).unwrap();
        assert_eq!(resp, RespValue::BulkString(Some(Bytes::from("myvalue"))));
    }

    #[test]
    fn test_eval_keys_and_argv() {
        let (engine, storage) = make_engine();
        let script = r#"
            redis.call('SET', KEYS[1], ARGV[1])
            return redis.call('GET', KEYS[1])
        "#;
        let resp = engine.eval(script, vec!["key1".to_string()], vec!["val1".to_string()], storage).unwrap();
        assert_eq!(resp, RespValue::BulkString(Some(Bytes::from("val1"))));
    }

    #[test]
    fn test_eval_pcall_error() {
        let (engine, storage) = make_engine();
        let script = r#"
            local result = redis.pcall('SET', 'key')
            if type(result) == 'table' and result.err then
                return result.err
            end
            return result
        "#;
        let resp = engine.eval(script, vec![], vec![], storage).unwrap();
        match resp {
            RespValue::BulkString(Some(b)) => {
                let s = String::from_utf8_lossy(&b);
                assert!(s.contains("ERR"));
            }
            _ => panic!("期望包含错误的 BulkString，得到 {:?}", resp),
        }
    }

    #[test]
    fn test_script_load_and_exists() {
        let (engine, _) = make_engine();
        let sha1 = engine.script_load("return 1").unwrap();
        assert_eq!(sha1.len(), 40);

        let exists = engine.script_exists(&[sha1.clone()]).unwrap();
        assert_eq!(exists, vec![true]);

        let exists2 = engine.script_exists(&[sha1.clone(), "nonexistent".to_string()]).unwrap();
        assert_eq!(exists2, vec![true, false]);
    }

    #[test]
    fn test_evalsha() {
        let (engine, storage) = make_engine();
        let sha1 = engine.script_load("return 100").unwrap();
        let resp = engine.evalsha(&sha1, vec![], vec![], storage).unwrap();
        assert_eq!(resp, RespValue::Integer(100));
    }

    #[test]
    fn test_script_flush() {
        let (engine, _) = make_engine();
        let sha1 = engine.script_load("return 1").unwrap();
        assert!(engine.script_exists(&[sha1.clone()]).unwrap()[0]);

        engine.script_flush().unwrap();
        assert!(!engine.script_exists(&[sha1.clone()]).unwrap()[0]);
    }

    #[test]
    fn test_eval_nil_return() {
        let (engine, storage) = make_engine();
        let resp = engine.eval("return nil", vec![], vec![], storage).unwrap();
        assert_eq!(resp, RespValue::BulkString(None));
    }

    #[test]
    fn test_eval_redis_call_del() {
        let (engine, storage) = make_engine();
        // 先设置 key
        let _ = engine.eval("redis.call('SET', 'delkey', 'val')", vec![], vec![], storage.clone());
        // 删除并返回结果
        let resp = engine.eval("return redis.call('DEL', 'delkey')", vec![], vec![], storage).unwrap();
        assert_eq!(resp, RespValue::Integer(1));
    }

    #[test]
    fn test_evalsha_not_found() {
        let (engine, storage) = make_engine();
        let result = engine.evalsha("aabbccdd", vec![], vec![], storage);
        assert!(result.is_err());
    }

    // ---------- Function 系统测试 ----------

    #[test]
    fn test_function_load() {
        let (engine, _) = make_engine();
        let code = r#"#!lua name=mylib
redis.register_function("hello", function(keys, args)
    return "world"
end)
"#;
        let name = engine.function_load(code, false).unwrap();
        assert_eq!(name, "mylib");

        // 重复加载（不 replace）应失败
        let result = engine.function_load(code, false);
        assert!(result.is_err());
    }

    #[test]
    fn test_function_load_replace() {
        let (engine, _) = make_engine();
        let code = r#"#!lua name=mylib
redis.register_function("hello", function(keys, args)
    return "world"
end)
"#;
        engine.function_load(code, false).unwrap();

        let code2 = r#"#!lua name=mylib
redis.register_function("hello2", function(keys, args)
    return "world2"
end)
"#;
        let name = engine.function_load(code2, true).unwrap();
        assert_eq!(name, "mylib");

        let list = engine.function_list(None, false).unwrap();
        assert_eq!(list.len(), 1);
        assert_eq!(list[0].3.len(), 1);
        assert_eq!(list[0].3[0].0, "hello2");
    }

    #[test]
    fn test_fcall() {
        let (engine, storage) = make_engine();
        let code = r#"#!lua name=mylib
redis.register_function("hello", function(keys, args)
    return "world"
end)
"#;
        engine.function_load(code, false).unwrap();

        let resp = engine.fcall("hello", vec![], vec![], storage).unwrap();
        assert_eq!(resp, RespValue::BulkString(Some(Bytes::from("world"))));
    }

    #[test]
    fn test_fcall_redis_call() {
        let (engine, storage) = make_engine();
        let code = r#"#!lua name=mylib
redis.register_function("setget", function(keys, args)
    redis.call("SET", keys[1], args[1])
    return redis.call("GET", keys[1])
end)
"#;
        engine.function_load(code, false).unwrap();

        let resp = engine.fcall("setget", vec!["mykey".to_string()], vec!["myvalue".to_string()], storage).unwrap();
        assert_eq!(resp, RespValue::BulkString(Some(Bytes::from("myvalue"))));
    }

    #[test]
    fn test_function_delete() {
        let (engine, _) = make_engine();
        let code = r#"#!lua name=mylib
redis.register_function("hello", function(keys, args)
    return "world"
end)
"#;
        engine.function_load(code, false).unwrap();
        assert!(engine.function_delete("mylib").unwrap());
        assert!(!engine.function_delete("mylib").unwrap());

        let list = engine.function_list(None, false).unwrap();
        assert_eq!(list.len(), 0);
    }

    #[test]
    fn test_function_list() {
        let (engine, _) = make_engine();
        let code = r#"#!lua name=mylib
redis.register_function("hello", function(keys, args)
    return "world"
end, "no-writes")
"#;
        engine.function_load(code, false).unwrap();

        let list = engine.function_list(None, false).unwrap();
        assert_eq!(list.len(), 1);
        assert_eq!(list[0].0, "mylib");
        assert_eq!(list[0].1, "lua");
        assert_eq!(list[0].3.len(), 1);
        assert_eq!(list[0].3[0].0, "hello");
        assert_eq!(list[0].3[0].1, vec!["no-writes"]);
    }

    #[test]
    fn test_function_dump_restore() {
        let (engine, _) = make_engine();
        let code = r#"#!lua name=mylib
redis.register_function("hello", function(keys, args)
    return "world"
end)
"#;
        engine.function_load(code, false).unwrap();

        let dump = engine.function_dump().unwrap();
        assert!(!dump.is_empty());

        // FLUSH 恢复
        engine.function_restore(&dump, "FLUSH").unwrap();
        let list = engine.function_list(None, false).unwrap();
        assert_eq!(list.len(), 1);

        // APPEND 恢复（已有同名库，应跳过）
        engine.function_restore(&dump, "APPEND").unwrap();
        let list = engine.function_list(None, false).unwrap();
        assert_eq!(list.len(), 1);
    }

    #[test]
    fn test_function_flush() {
        let (engine, _) = make_engine();
        let code = r#"#!lua name=mylib
redis.register_function("hello", function(keys, args)
    return "world"
end)
"#;
        engine.function_load(code, false).unwrap();
        engine.function_flush(false).unwrap();

        let list = engine.function_list(None, false).unwrap();
        assert_eq!(list.len(), 0);
    }

    #[test]
    fn test_function_stats() {
        let (engine, _) = make_engine();
        let code = r#"#!lua name=mylib
redis.register_function("f1", function(keys, args) return 1 end)
redis.register_function("f2", function(keys, args) return 2 end)
"#;
        engine.function_load(code, false).unwrap();

        let (libs, funcs) = engine.function_stats().unwrap();
        assert_eq!(libs, 1);
        assert_eq!(funcs, 2);
    }

    #[test]
    fn test_fcall_ro() {
        let (engine, storage) = make_engine();
        let code = r#"#!lua name=mylib
redis.register_function("readonly_fn", function(keys, args)
    return "ro"
end)
"#;
        engine.function_load(code, false).unwrap();

        let resp = engine.fcall_ro("readonly_fn", vec![], vec![], storage).unwrap();
        assert_eq!(resp, RespValue::BulkString(Some(Bytes::from("ro"))));
    }
}
