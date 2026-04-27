//! RESP (REdis Serialization Protocol) 协议解析模块

use bytes::{Buf, Bytes, BytesMut};
use itoa;

use crate::error::{AppError, Result};

/// RESP 数据类型枚举（RESP2 + RESP3）
#[derive(Debug, Clone, PartialEq)]
pub enum RespValue {
    /// 简单字符串，以 + 开头，例如：+OK\r\n
    SimpleString(Bytes),
    /// 错误信息，以 - 开头，例如：-ERR unknown command\r\n
    Error(Bytes),
    /// 整数，以 : 开头，例如：:1000\r\n
    Integer(i64),
    /// 批量字符串，以 $ 开头，例如：$6\r\nfoobar\r\n 或 $-1\r\n（Null）
    BulkString(Option<Bytes>),
    /// 数组，以 * 开头，例如：*2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n 或 *-1\r\n（Null）
    Array(Vec<RespValue>),
    // ----- RESP3 新增类型 -----
    /// Null，以 _ 开头，例如：_\r\n
    Null,
    /// 布尔值，以 # 开头，例如：#t\r\n 或 #f\r\n
    Bool(bool),
    /// 浮点数，以 , 开头，例如：,3.14\r\n
    Double(f64),
    /// Map，以 % 开头，例如：%2\r\n+first\r\n:1\r\n+second\r\n:2\r\n
    Map(Vec<(RespValue, RespValue)>),
    /// Set，以 ~ 开头，例如：~2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n
    Set(Vec<RespValue>),
    /// Push，以 > 开头，例如：>3\r\n+message\r\n+channel\r\n$3\r\nfoo\r\n
    Push { kind: String, data: Vec<RespValue> },
}

/// RESP 协议解析器
#[derive(Debug)]
pub struct RespParser;

impl RespParser {
    /// 创建新的解析器实例
    pub fn new() -> Self {
        Self
    }

    /// 从字节缓冲区中尝试解析出一个 RESP 值
    /// 如果数据不完整，返回 Ok(None)，调用方应继续读取更多数据
    pub fn parse(&self, buf: &mut BytesMut) -> Result<Option<RespValue>> {
        // 缓冲区为空，直接返回 None
        if buf.is_empty() {
            return Ok(None);
        }

        // 先 peek 第一个字节判断数据类型
        let first_byte = buf[0];

        match first_byte {
            b'+' => self.parse_simple_string(buf),
            b'-' => self.parse_error(buf),
            b':' => self.parse_integer(buf),
            b'$' => self.parse_bulk_string(buf),
            b'*' => self.parse_array(buf),
            // RESP3 类型
            b'_' => self.parse_null(buf),
            b'#' => self.parse_bool(buf),
            b',' => self.parse_double(buf),
            b'%' => self.parse_map(buf),
            b'~' => self.parse_set(buf),
            b'>' => self.parse_push(buf),
            // 如果第一个字节不是 RESP 类型标识符，按 inline 命令处理
            // redis-benchmark 等客户端会直接发送 "PING\r\n" 这种格式
            _ => self.parse_inline(buf),
        }
    }

    /// 编码 RESP 值为字节流
    pub fn encode(&self, value: &RespValue) -> Bytes {
        let mut result = Vec::with_capacity(64);
        self.encode_to_vec(value, &mut result);
        Bytes::from(result)
    }

    /// 编码 RESP 值到已有缓冲区（避免重复分配）
    pub fn encode_append(&self, value: &RespValue, out: &mut Vec<u8>) {
        self.encode_to_vec(value, out);
    }

    // ---------- 解析内部方法 ----------

    /// 查找缓冲区中第一个 \r\n 的位置，返回其起始索引
    /// 如果找不到，返回 None，表示数据不完整
    fn find_crlf(&self, buf: &BytesMut, start: usize) -> Option<usize> {
        // 从 start 位置开始搜索 \r\n
        (start..buf.len().saturating_sub(1)).find(|&i| buf[i] == b'\r' && buf[i + 1] == b'\n')
    }

    fn parse_integer_from_bytes(buf: &[u8]) -> std::result::Result<i64, AppError> {
        let mut neg = false;
        let mut val: u64 = 0;
        let mut i = 0;
        if i < buf.len() && buf[i] == b'-' {
            neg = true;
            i += 1;
        }
        if i >= buf.len() {
            return Err(AppError::Protocol("整数解析失败: 空".to_string()));
        }
        while i < buf.len() {
            let b = buf[i];
            if !b.is_ascii_digit() {
                return Err(AppError::Protocol(format!(
                    "整数解析失败: 非法字符 {}",
                    b as char
                )));
            }
            let digit = (b - b'0') as u64;
            val = val
                .checked_mul(10)
                .and_then(|v| v.checked_add(digit))
                .ok_or_else(|| AppError::Protocol("整数解析失败: 溢出".to_string()))?;
            i += 1;
        }
        if neg {
            if val > (i64::MAX as u64) + 1 {
                return Err(AppError::Protocol("整数解析失败: 溢出".to_string()));
            }
            if val == (i64::MAX as u64) + 1 {
                Ok(i64::MIN)
            } else {
                Ok(-(val as i64))
            }
        } else {
            if val > i64::MAX as u64 {
                return Err(AppError::Protocol("整数解析失败: 溢出".to_string()));
            }
            Ok(val as i64)
        }
    }

    /// 解析简单字符串：+OK\r\n
    fn parse_simple_string(&self, buf: &mut BytesMut) -> Result<Option<RespValue>> {
        let end = match self.find_crlf(buf, 1) {
            Some(pos) => pos,
            None => return Ok(None),
        };

        buf.advance(1);
        let content = buf.split_to(end - 1).freeze();
        buf.advance(2);
        Ok(Some(RespValue::SimpleString(content)))
    }

    /// 解析错误：-ERR something\r\n
    fn parse_error(&self, buf: &mut BytesMut) -> Result<Option<RespValue>> {
        let end = match self.find_crlf(buf, 1) {
            Some(pos) => pos,
            None => return Ok(None),
        };

        buf.advance(1);
        let content = buf.split_to(end - 1).freeze();
        buf.advance(2);
        Ok(Some(RespValue::Error(content)))
    }

    /// 解析整数：:1000\r\n
    fn parse_integer(&self, buf: &mut BytesMut) -> Result<Option<RespValue>> {
        let end = match self.find_crlf(buf, 1) {
            Some(pos) => pos,
            None => return Ok(None),
        };

        let num = Self::parse_integer_from_bytes(&buf[1..end])?;
        buf.advance(end + 2);
        Ok(Some(RespValue::Integer(num)))
    }

    /// 解析批量字符串：$6\r\nfoobar\r\n 或 $-1\r\n
    fn parse_bulk_string(&self, buf: &mut BytesMut) -> Result<Option<RespValue>> {
        let len_end = match self.find_crlf(buf, 1) {
            Some(pos) => pos,
            None => return Ok(None),
        };

        let len = Self::parse_integer_from_bytes(&buf[1..len_end])?;

        if len == -1 {
            buf.advance(len_end + 2);
            return Ok(Some(RespValue::BulkString(None)));
        }

        if len < 0 {
            return Err(AppError::Protocol(format!(
                "批量字符串长度不能为负数（除 -1 外）: {}",
                len
            )));
        }

        let len = len as usize;
        let data_start = len_end + 2;
        let total_needed = data_start + len + 2;

        if buf.len() < total_needed {
            return Ok(None);
        }

        buf.advance(data_start);
        let data = buf.split_to(len).freeze();
        buf.advance(2);
        Ok(Some(RespValue::BulkString(Some(data))))
    }

    /// 解析数组：*2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n 或 *-1\r\n
    fn parse_array(&self, buf: &mut BytesMut) -> Result<Option<RespValue>> {
        let len_end = match self.find_crlf(buf, 1) {
            Some(pos) => pos,
            None => return Ok(None),
        };

        let len = Self::parse_integer_from_bytes(&buf[1..len_end])?;

        if len == -1 {
            buf.advance(len_end + 2);
            return Ok(Some(RespValue::Array(vec![])));
        }

        if len < 0 {
            return Err(AppError::Protocol(format!(
                "数组长度不能为负数（除 -1 外）: {}",
                len
            )));
        }

        let len = len as usize;
        let original_buf = buf.clone();
        buf.advance(len_end + 2);

        let mut elements = Vec::with_capacity(len);
        for _ in 0..len {
            match self.parse(buf)? {
                Some(value) => elements.push(value),
                None => {
                    *buf = original_buf;
                    return Ok(None);
                }
            }
        }

        Ok(Some(RespValue::Array(elements)))
    }

    /// 解析 Null: _\r\n
    fn parse_null(&self, buf: &mut BytesMut) -> Result<Option<RespValue>> {
        if buf.len() < 3 {
            return Ok(None);
        }
        if &buf[..3] == b"_\r\n" {
            buf.advance(3);
            Ok(Some(RespValue::Null))
        } else {
            Err(AppError::Protocol("无效的 Null 格式".to_string()))
        }
    }

    /// 解析 Bool: #t\r\n 或 #f\r\n
    fn parse_bool(&self, buf: &mut BytesMut) -> Result<Option<RespValue>> {
        if buf.len() < 4 {
            return Ok(None);
        }
        if &buf[..4] == b"#t\r\n" {
            buf.advance(4);
            Ok(Some(RespValue::Bool(true)))
        } else if buf.len() >= 4 && &buf[..4] == b"#f\r\n" {
            buf.advance(4);
            Ok(Some(RespValue::Bool(false)))
        } else {
            Err(AppError::Protocol("无效的 Bool 格式".to_string()))
        }
    }

    /// 解析 Double: ,1.23\r\n
    fn parse_double(&self, buf: &mut BytesMut) -> Result<Option<RespValue>> {
        let len_end = match self.find_crlf(buf, 1) {
            Some(pos) => pos,
            None => return Ok(None),
        };

        let s = std::str::from_utf8(&buf[1..len_end])
            .map_err(|_| AppError::Protocol("无效的 Double 字符串".to_string()))?;
        let val = s.parse::<f64>()
            .map_err(|_| AppError::Protocol(format!("无法解析 Double: {}", s)))?;

        buf.advance(len_end + 2);
        Ok(Some(RespValue::Double(val)))
    }

    /// 解析 Map: %2\r\n...key...value... (键值对数量 * 2 个元素)
    fn parse_map(&self, buf: &mut BytesMut) -> Result<Option<RespValue>> {
        let len_end = match self.find_crlf(buf, 1) {
            Some(pos) => pos,
            None => return Ok(None),
        };

        let len = Self::parse_integer_from_bytes(&buf[1..len_end])?;
        if len < 0 {
            return Err(AppError::Protocol(format!("Map 长度不能为负数: {}", len)));
        }

        let len = len as usize;
        let original_buf = buf.clone();
        buf.advance(len_end + 2);

        let mut entries = Vec::with_capacity(len);
        for _ in 0..len {
            let key = match self.parse(buf)? {
                Some(v) => v,
                None => {
                    *buf = original_buf;
                    return Ok(None);
                }
            };
            let value = match self.parse(buf)? {
                Some(v) => v,
                None => {
                    *buf = original_buf;
                    return Ok(None);
                }
            };
            entries.push((key, value));
        }

        Ok(Some(RespValue::Map(entries)))
    }

    /// 解析 Set: ~2\r\n...element...
    fn parse_set(&self, buf: &mut BytesMut) -> Result<Option<RespValue>> {
        let len_end = match self.find_crlf(buf, 1) {
            Some(pos) => pos,
            None => return Ok(None),
        };

        let len = Self::parse_integer_from_bytes(&buf[1..len_end])?;
        if len < 0 {
            return Err(AppError::Protocol(format!("Set 长度不能为负数: {}", len)));
        }

        let len = len as usize;
        let original_buf = buf.clone();
        buf.advance(len_end + 2);

        let mut elements = Vec::with_capacity(len);
        for _ in 0..len {
            match self.parse(buf)? {
                Some(value) => elements.push(value),
                None => {
                    *buf = original_buf;
                    return Ok(None);
                }
            }
        }

        Ok(Some(RespValue::Set(elements)))
    }

    /// 解析 Push: >2\r\n...element...
    fn parse_push(&self, buf: &mut BytesMut) -> Result<Option<RespValue>> {
        let len_end = match self.find_crlf(buf, 1) {
            Some(pos) => pos,
            None => return Ok(None),
        };

        let len = Self::parse_integer_from_bytes(&buf[1..len_end])?;
        if len < 0 {
            return Err(AppError::Protocol(format!("Push 长度不能为负数: {}", len)));
        }

        let len = len as usize;
        let original_buf = buf.clone();
        buf.advance(len_end + 2);

        let mut elements = Vec::with_capacity(len);
        for _ in 0..len {
            match self.parse(buf)? {
                Some(value) => elements.push(value),
                None => {
                    *buf = original_buf;
                    return Ok(None);
                }
            }
        }

        // RESP3 Push: 第一个元素是 kind，其余是 data
        let kind = if !elements.is_empty() {
            match &elements[0] {
                RespValue::SimpleString(b) | RespValue::BulkString(Some(b)) => {
                    String::from_utf8_lossy(b).to_string()
                }
                _ => String::new(),
            }
        } else {
            String::new()
        };
        let data = if elements.len() > 1 {
            elements.into_iter().skip(1).collect()
        } else {
            vec![]
        };

        Ok(Some(RespValue::Push { kind, data }))
    }

    /// 解析 inline 命令：以 \r\n 结尾的一行文本，空格分隔参数
    /// 解析后转换为 RespValue::Array(vec![RespValue::BulkString(...), ...])
    /// 用于兼容 redis-benchmark 等直接发送 "PING\r\n" 的客户端
    fn parse_inline(&self, buf: &mut BytesMut) -> Result<Option<RespValue>> {
        // 查找 \r\n
        let end = match self.find_crlf(buf, 0) {
            Some(pos) => pos,
            None => return Ok(None), // 数据不完整，等待更多数据
        };

        // 提取整行文本
        let line = String::from_utf8_lossy(&buf[..end]);
        let parts: Vec<&str> = line.split_whitespace().collect();

        if parts.is_empty() {
            return Err(AppError::Protocol("inline 命令为空".to_string()));
        }

        // 将空格分隔的参数转换为 RESP Array of BulkStrings
        let elements: Vec<RespValue> = parts
            .iter()
            .map(|s| RespValue::BulkString(Some(Bytes::copy_from_slice(s.as_bytes()))))
            .collect();

        buf.advance(end + 2);
        Ok(Some(RespValue::Array(elements)))
    }

    // ---------- 编码内部方法 ----------

    fn encode_to_vec(&self, value: &RespValue, out: &mut Vec<u8>) {
        let mut itoa_buf = itoa::Buffer::new();
        match value {
            RespValue::SimpleString(s) => {
                out.push(b'+');
                out.extend_from_slice(s);
                out.extend_from_slice(b"\r\n");
            }
            RespValue::Error(e) => {
                out.push(b'-');
                out.extend_from_slice(e);
                out.extend_from_slice(b"\r\n");
            }
            RespValue::Integer(i) => {
                out.push(b':');
                out.extend_from_slice(itoa_buf.format(*i).as_bytes());
                out.extend_from_slice(b"\r\n");
            }
            RespValue::BulkString(None) => {
                out.extend_from_slice(b"$-1\r\n");
            }
            RespValue::BulkString(Some(data)) => {
                out.push(b'$');
                out.extend_from_slice(itoa_buf.format(data.len()).as_bytes());
                out.extend_from_slice(b"\r\n");
                out.extend_from_slice(data);
                out.extend_from_slice(b"\r\n");
            }
            RespValue::Array(arr) => {
                out.push(b'*');
                out.extend_from_slice(itoa_buf.format(arr.len()).as_bytes());
                out.extend_from_slice(b"\r\n");
                for item in arr {
                    self.encode_to_vec(item, out);
                }
            }
            // ----- RESP3 编码 -----
            RespValue::Null => {
                out.extend_from_slice(b"_\r\n");
            }
            RespValue::Bool(v) => {
                out.push(b'#');
                out.push(if *v { b't' } else { b'f' });
                out.extend_from_slice(b"\r\n");
            }
            RespValue::Double(v) => {
                out.push(b',');
                let s = format!("{}", v);
                out.extend_from_slice(s.as_bytes());
                out.extend_from_slice(b"\r\n");
            }
            RespValue::Map(entries) => {
                out.push(b'%');
                out.extend_from_slice(itoa_buf.format(entries.len()).as_bytes());
                out.extend_from_slice(b"\r\n");
                for (k, v) in entries {
                    self.encode_to_vec(k, out);
                    self.encode_to_vec(v, out);
                }
            }
            RespValue::Set(entries) => {
                out.push(b'~');
                out.extend_from_slice(itoa_buf.format(entries.len()).as_bytes());
                out.extend_from_slice(b"\r\n");
                for item in entries {
                    self.encode_to_vec(item, out);
                }
            }
            RespValue::Push { kind, data } => {
                out.push(b'>');
                out.extend_from_slice(itoa_buf.format(data.len() + 1).as_bytes());
                out.extend_from_slice(b"\r\n");
                self.encode_to_vec(&RespValue::SimpleString(Bytes::from(kind.clone())), out);
                for item in data {
                    self.encode_to_vec(item, out);
                }
            }
        }
    }
}

impl Default for RespParser {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_encode_simple_string() {
        let parser = RespParser::new();
        let val = RespValue::SimpleString(Bytes::from_static(b"OK"));
        assert_eq!(parser.encode(&val), Bytes::from_static(b"+OK\r\n"));
    }

    #[test]
    fn test_encode_error() {
        let parser = RespParser::new();
        let val = RespValue::Error(Bytes::from_static(b"ERR unknown command"));
        assert_eq!(
            parser.encode(&val),
            Bytes::from_static(b"-ERR unknown command\r\n")
        );
    }

    #[test]
    fn test_encode_integer() {
        let parser = RespParser::new();
        let val = RespValue::Integer(1000);
        assert_eq!(parser.encode(&val), Bytes::from_static(b":1000\r\n"));
    }

    #[test]
    fn test_encode_bulk_string() {
        let parser = RespParser::new();
        let val = RespValue::BulkString(Some(Bytes::from_static(b"foobar")));
        assert_eq!(parser.encode(&val), Bytes::from_static(b"$6\r\nfoobar\r\n"));
    }

    #[test]
    fn test_encode_null_bulk_string() {
        let parser = RespParser::new();
        let val = RespValue::BulkString(None);
        assert_eq!(parser.encode(&val), Bytes::from_static(b"$-1\r\n"));
    }

    #[test]
    fn test_encode_array() {
        let parser = RespParser::new();
        let val = RespValue::Array(vec![
            RespValue::BulkString(Some(Bytes::from_static(b"foo"))),
            RespValue::BulkString(Some(Bytes::from_static(b"bar"))),
        ]);
        assert_eq!(
            parser.encode(&val),
            Bytes::from_static(b"*2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n")
        );
    }

    #[test]
    fn test_parse_simple_string() {
        let parser = RespParser::new();
        let mut buf = BytesMut::from("+OK\r\n");
        assert_eq!(
            parser.parse(&mut buf).unwrap(),
            Some(RespValue::SimpleString(Bytes::from_static(b"OK")))
        );
        assert!(buf.is_empty());
    }

    #[test]
    fn test_parse_bulk_string() {
        let parser = RespParser::new();
        let mut buf = BytesMut::from("$6\r\nfoobar\r\n");
        assert_eq!(
            parser.parse(&mut buf).unwrap(),
            Some(RespValue::BulkString(Some(Bytes::from_static(b"foobar"))))
        );
        assert!(buf.is_empty());
    }

    #[test]
    fn test_parse_array() {
        let parser = RespParser::new();
        let mut buf = BytesMut::from("*2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n");
        assert_eq!(
            parser.parse(&mut buf).unwrap(),
            Some(RespValue::Array(vec![
                RespValue::BulkString(Some(Bytes::from_static(b"foo"))),
                RespValue::BulkString(Some(Bytes::from_static(b"bar"))),
            ]))
        );
        assert!(buf.is_empty());
    }

    #[test]
    fn test_incomplete_data() {
        let parser = RespParser::new();
        let mut buf = BytesMut::from("+OK");
        assert_eq!(parser.parse(&mut buf).unwrap(), None);
        // 补充数据后继续解析
        buf.extend_from_slice(b"\r\n");
        assert_eq!(
            parser.parse(&mut buf).unwrap(),
            Some(RespValue::SimpleString(Bytes::from_static(b"OK")))
        );
    }

    #[test]
    fn test_parse_inline_ping() {
        let parser = RespParser::new();
        let mut buf = BytesMut::from("PING\r\n");
        assert_eq!(
            parser.parse(&mut buf).unwrap(),
            Some(RespValue::Array(vec![RespValue::BulkString(Some(
                Bytes::from_static(b"PING")
            ))]))
        );
        assert!(buf.is_empty());
    }

    #[test]
    fn test_parse_inline_set() {
        let parser = RespParser::new();
        let mut buf = BytesMut::from("SET key value\r\n");
        assert_eq!(
            parser.parse(&mut buf).unwrap(),
            Some(RespValue::Array(vec![
                RespValue::BulkString(Some(Bytes::from_static(b"SET"))),
                RespValue::BulkString(Some(Bytes::from_static(b"key"))),
                RespValue::BulkString(Some(Bytes::from_static(b"value"))),
            ]))
        );
        assert!(buf.is_empty());
    }

    #[test]
    fn test_parse_inline_incomplete() {
        let parser = RespParser::new();
        let mut buf = BytesMut::from("PING");
        assert_eq!(parser.parse(&mut buf).unwrap(), None);
        buf.extend_from_slice(b"\r\n");
        assert_eq!(
            parser.parse(&mut buf).unwrap(),
            Some(RespValue::Array(vec![RespValue::BulkString(Some(
                Bytes::from_static(b"PING")
            ))]))
        );
    }

    // ----- RESP3 编码测试 -----

    #[test]
    fn test_encode_null() {
        let parser = RespParser::new();
        let val = RespValue::Null;
        assert_eq!(parser.encode(&val), Bytes::from_static(b"_\r\n"));
    }

    #[test]
    fn test_encode_bool() {
        let parser = RespParser::new();
        assert_eq!(parser.encode(&RespValue::Bool(true)), Bytes::from_static(b"#t\r\n"));
        assert_eq!(parser.encode(&RespValue::Bool(false)), Bytes::from_static(b"#f\r\n"));
    }

    #[test]
    fn test_encode_double() {
        let parser = RespParser::new();
        let val = RespValue::Double(3.14);
        assert_eq!(parser.encode(&val), Bytes::from_static(b",3.14\r\n"));
    }

    #[test]
    fn test_encode_map() {
        let parser = RespParser::new();
        let val = RespValue::Map(vec![
            (RespValue::SimpleString(Bytes::from_static(b"first")), RespValue::Integer(1)),
            (RespValue::SimpleString(Bytes::from_static(b"second")), RespValue::Integer(2)),
        ]);
        assert_eq!(
            parser.encode(&val),
            Bytes::from_static(b"%2\r\n+first\r\n:1\r\n+second\r\n:2\r\n")
        );
    }

    #[test]
    fn test_encode_set() {
        let parser = RespParser::new();
        let val = RespValue::Set(vec![
            RespValue::BulkString(Some(Bytes::from_static(b"foo"))),
            RespValue::BulkString(Some(Bytes::from_static(b"bar"))),
        ]);
        assert_eq!(
            parser.encode(&val),
            Bytes::from_static(b"~2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n")
        );
    }

    #[test]
    fn test_encode_push() {
        let parser = RespParser::new();
        let val = RespValue::Push {
            kind: "message".to_string(),
            data: vec![
                RespValue::BulkString(Some(Bytes::from_static(b"channel"))),
                RespValue::BulkString(Some(Bytes::from_static(b"payload"))),
            ],
        };
        assert_eq!(
            parser.encode(&val),
            Bytes::from_static(b">3\r\n+message\r\n$7\r\nchannel\r\n$7\r\npayload\r\n")
        );
    }

    // ----- RESP3 解析测试 -----

    #[test]
    fn test_parse_null() {
        let parser = RespParser::new();
        let mut buf = BytesMut::from("_\r\n");
        assert_eq!(parser.parse(&mut buf).unwrap(), Some(RespValue::Null));
        assert!(buf.is_empty());
    }

    #[test]
    fn test_parse_bool() {
        let parser = RespParser::new();
        let mut buf = BytesMut::from("#t\r\n");
        assert_eq!(parser.parse(&mut buf).unwrap(), Some(RespValue::Bool(true)));
        assert!(buf.is_empty());

        let mut buf = BytesMut::from("#f\r\n");
        assert_eq!(parser.parse(&mut buf).unwrap(), Some(RespValue::Bool(false)));
        assert!(buf.is_empty());
    }

    #[test]
    fn test_parse_double() {
        let parser = RespParser::new();
        let mut buf = BytesMut::from(",3.14\r\n");
        assert_eq!(parser.parse(&mut buf).unwrap(), Some(RespValue::Double(3.14)));
        assert!(buf.is_empty());
    }

    #[test]
    fn test_parse_map() {
        let parser = RespParser::new();
        let mut buf = BytesMut::from("%2\r\n+first\r\n:1\r\n+second\r\n:2\r\n");
        assert_eq!(
            parser.parse(&mut buf).unwrap(),
            Some(RespValue::Map(vec![
                (RespValue::SimpleString(Bytes::from_static(b"first")), RespValue::Integer(1)),
                (RespValue::SimpleString(Bytes::from_static(b"second")), RespValue::Integer(2)),
            ]))
        );
        assert!(buf.is_empty());
    }

    #[test]
    fn test_parse_set() {
        let parser = RespParser::new();
        let mut buf = BytesMut::from("~2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n");
        assert_eq!(
            parser.parse(&mut buf).unwrap(),
            Some(RespValue::Set(vec![
                RespValue::BulkString(Some(Bytes::from_static(b"foo"))),
                RespValue::BulkString(Some(Bytes::from_static(b"bar"))),
            ]))
        );
        assert!(buf.is_empty());
    }

    #[test]
    fn test_parse_push() {
        let parser = RespParser::new();
        let mut buf = BytesMut::from(">3\r\n+message\r\n$7\r\nchannel\r\n$7\r\npayload\r\n");
        assert_eq!(
            parser.parse(&mut buf).unwrap(),
            Some(RespValue::Push {
                kind: "message".to_string(),
                data: vec![
                    RespValue::BulkString(Some(Bytes::from_static(b"channel"))),
                    RespValue::BulkString(Some(Bytes::from_static(b"payload"))),
                ],
            })
        );
        assert!(buf.is_empty());
    }
}
