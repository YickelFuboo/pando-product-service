from fastapi import HTTPException
from typing import Any, Dict, Optional

class BaseException(Exception):
    """基础异常类"""
    def __init__(self, message: str, code: str = None, details: Any = None):
        self.message = message
        self.code = code
        self.details = details
        super().__init__(self.message)

class ValidationError(BaseException):
    """验证错误"""
    def __init__(self, message: str, details: Any = None):
        super().__init__(message, "VALIDATION_ERROR", details)

class NotFoundError(BaseException):
    """资源未找到错误"""
    def __init__(self, message: str, details: Any = None):
        super().__init__(message, "NOT_FOUND", details)

class UnauthorizedError(BaseException):
    """未授权错误"""
    def __init__(self, message: str, details: Any = None):
        super().__init__(message, "UNAUTHORIZED", details)

class ForbiddenError(BaseException):
    """禁止访问错误"""
    def __init__(self, message: str, details: Any = None):
        super().__init__(message, "FORBIDDEN", details)

class InternalServerError(BaseException):
    """内部服务器错误"""
    def __init__(self, message: str, details: Any = None):
        super().__init__(message, "INTERNAL_SERVER_ERROR", details)
