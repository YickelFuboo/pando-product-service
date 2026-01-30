from typing import Dict, Any
from app.utils.logger import logger


class I18nService:
    """国际化消息服务"""
    
    # 消息字典
    MESSAGES = {
        "zh-CN": {
            # 通用消息
            "success": "操作成功",
            "failed": "操作失败",
            "error": "发生错误",
            "not_found": "未找到",
            "unauthorized": "未授权",
            "forbidden": "权限不足",
            "validation_error": "数据验证失败",
            "server_error": "服务器内部错误",
            
            # 知识库相关消息
            "knowledgebase_created": "知识库创建成功",
            "knowledgebase_updated": "知识库更新成功",
        },
        "en-US": {
            # Common messages
            "success": "Operation successful",
            "failed": "Operation failed",
            "error": "An error occurred",
            "not_found": "Not found",
            "unauthorized": "Unauthorized",
            "forbidden": "Forbidden",
            "validation_error": "Validation error",
            "server_error": "Internal server error",
            
            # Knowledge base related messages
            "knowledgebase_created": "Knowledge base created successfully",
            "knowledgebase_updated": "Knowledge base updated successfully",
        }
    }
    
    def __init__(self):
        """初始化国际化服务"""
        pass
    
    def get_message(self, key: str, language: str = "zh-CN", **kwargs) -> str:
        """
        获取国际化消息
        
        Args:
            key: 消息键
            language: 语言代码
            **kwargs: 格式化参数
            
        Returns:
            str: 格式化后的消息
        """
        try:
            # 获取消息字典
            messages = self.MESSAGES.get(language, self.MESSAGES["zh-CN"])
            
            # 获取消息
            message = messages.get(key, key)
            
            # 如果有格式化参数，进行格式化
            if kwargs:
                try:
                    message = message.format(**kwargs)
                except (KeyError, ValueError) as e:
                    logger.warning(f"消息格式化失败: {key}, 语言: {language}, 错误: {e}")
                    # 如果格式化失败，返回原始消息
                    pass
            
            return message
            
        except Exception as e:
            logger.error(f"获取国际化消息失败: {key}, 语言: {language}, 错误: {e}")
            return key
    
    def get_error_message(self, error_type: str, language: str = "zh-CN", **kwargs) -> str:
        """
        获取错误消息
        
        Args:
            error_type: 错误类型
            language: 语言代码
            **kwargs: 格式化参数
            
        Returns:
            str: 错误消息
        """
        return self.get_message(error_type, language, **kwargs)
    
    def get_success_message(self, success_type: str, language: str = "zh-CN", **kwargs) -> str:
        """
        获取成功消息
        
        Args:
            success_type: 成功类型
            language: 语言代码
            **kwargs: 格式化参数
            
        Returns:
            str: 成功消息
        """
        return self.get_message(success_type, language, **kwargs)


# 全局实例
i18n_service = I18nService() 