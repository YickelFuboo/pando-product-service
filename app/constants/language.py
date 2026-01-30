from typing import List

# 支持的语言列表
SUPPORTED_LANGUAGES = [
    {"code": "zh-CN", "name": "简体中文", "is_default": False},
    {"code": "en-US", "name": "English", "is_default": True}
]


def get_supported_languages() -> List[dict]:
    """获取支持的语言列表"""
    return SUPPORTED_LANGUAGES


def is_supported_language(language: str) -> bool:
    """检查是否为支持的语言"""
    return language in [lang["code"] for lang in SUPPORTED_LANGUAGES]


def get_default_language() -> str:
    """获取默认语言"""
    for lang in SUPPORTED_LANGUAGES:
        if lang["is_default"]:
            return lang["code"]
    return "en-US"  # 默认返回英文 