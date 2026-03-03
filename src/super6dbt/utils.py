"""工具函数模块"""

from typing import Any, Dict, List, Optional
import re


def sanitize_name(name: str) -> str:
    """清理名称，确保符合标识符规范"""
    # 移除特殊字符，替换为下划线
    name = re.sub(r'[^\w\s-]', '', name)
    name = re.sub(r'[-\s]+', '_', name)
    return name.lower().strip('_')


def snake_to_camel(name: str) -> str:
    """蛇形命名转驼峰命名"""
    components = name.split('_')
    return ''.join(x.title() for x in components)


def camel_to_snake(name: str) -> str:
    """驼峰命名转蛇形命名"""
    s1 = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', name)
    return re.sub('([a-z0-9])([A-Z])', r'\1_\2', s1).lower()


def merge_dicts(base: Dict[str, Any], override: Dict[str, Any]) -> Dict[str, Any]:
    """深度合并字典"""
    result = base.copy()
    for key, value in override.items():
        if key in result and isinstance(result[key], dict) and isinstance(value, dict):
            result[key] = merge_dicts(result[key], value)
        else:
            result[key] = value
    return result


def flatten_dict(d: Dict[str, Any], parent_key: str = '', sep: str = '.') -> Dict[str, Any]:
    """扁平化嵌套字典"""
    items = []
    for key, value in d.items():
        new_key = f"{parent_key}{sep}{key}" if parent_key else key
        if isinstance(value, dict):
            items.extend(flatten_dict(value, new_key, sep=sep).items())
        else:
            items.append((new_key, value))
    return dict(items)


def find_matching_key(mapping: Dict[str, Any], search: str, fuzzy: bool = False) -> Optional[str]:
    """在字典中查找匹配的键"""
    if search in mapping:
        return search

    if fuzzy:
        # 模糊匹配
        for key in mapping.keys():
            if search.lower() in key.lower() or key.lower() in search.lower():
                return key

    return None


def validate_ref(ref: str) -> bool:
    """验证dbt引用格式"""
    pattern = r'^ref\([\'"]([^\'"]+)[\'"]\)$'
    return re.match(pattern, ref) is not None


def extract_model_ref(ref: str) -> Optional[str]:
    """从ref中提取模型名称"""
    pattern = r'^ref\([\'"]([^\'"]+)[\'"]\)$'
    match = re.match(pattern, ref)
    return match.group(1) if match else None


def create_position_map(charts: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """创建面板中图表的位置映射"""
    positions = []
    for i, chart in enumerate(charts):
        positions.append({
            "id": chart.get("id", i),
            "size_x": 4,  # 默认宽度占4列
            "size_y": 4,  # 默认高度占4行
            "col": (i % 3) * 4,  # 每行3个图表
            "row": (i // 3) * 4,
        })
    return positions