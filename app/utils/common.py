import os
from pathlib import Path
import tomllib

def get_project_meta(package_name: str = "knowledge-service"):
    """从 pyproject.toml 读取项目元数据"""
    toml_path = Path(__file__).parent.parent.parent / "pyproject.toml"
    if not toml_path.exists():
        return {
            "name": "unknown-project",
            "version": "",
            "description": "",
        }
    
    with open(toml_path, "rb") as f:
        data = tomllib.load(f)
    poetry = data.get("tool", {}).get("poetry", {})
    return {
        "name": poetry.get("name", "unknown-project"),
        "version": poetry.get("version", "0.0.0"),
        "description": poetry.get("description", ""),
    }

def get_project_base_directory():
    # 通过查找包含pyproject.toml的目录来确定项目根目录
    current_dir = os.path.dirname(__file__)

    project_root = current_dir
    while project_root != os.path.dirname(project_root):  # 直到到达文件系统根目录
        if os.path.exists(os.path.join(project_root, "pyproject.toml")):
            break
        project_root = os.path.dirname(project_root)

    return project_root

def is_chinese(text: str) -> bool:
    """判断文本是否包含中文字符"""
    for char in text:
        if '\u4e00' <= char <= '\u9fff':
            return True
    return False

def is_english(text: str) -> bool:
    """判断文本是否只包含英文字符"""
    for char in text:
        if not ('a' <= char.lower() <= 'z' or char == ' ' or char == '\n' or char == '\t'):
            return False
    return True
