//! 错误类型定义模块，提供统一的 AppError 枚举和 Result 别名
// 错误类型定义，统一处理项目中的各类错误

use thiserror::Error;

/// 应用级别的统一错误类型
#[derive(Error, Debug)]
pub enum AppError {
    /// I/O 错误
    #[error("IO 错误: {0}")]
    Io(#[from] std::io::Error),

    /// RESP 协议解析错误
    #[error("协议解析错误: {0}")]
    Protocol(String),

    /// 命令相关错误
    #[error("命令错误: {0}")]
    Command(String),

    /// 存储引擎错误
    #[error("存储错误: {0}")]
    Storage(String),

    /// Lua 脚本错误
    #[error("Lua 错误: {0}")]
    Lua(String),

    /// 未知错误
    #[error("未知错误: {0}")]
    Unknown(String),
}

impl From<mlua::Error> for AppError {
    fn from(err: mlua::Error) -> Self {
        AppError::Lua(err.to_string())
    }
}

/// 为了方便使用，定义一个通用的 Result 类型
pub type Result<T> = std::result::Result<T, AppError>;
