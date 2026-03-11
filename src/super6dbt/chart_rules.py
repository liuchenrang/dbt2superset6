"""Superset Chart 配置规则

基于 Superset 6.0 API 验证的图表配置规则。
确保 push 功能生成的参数符合官方 API 规范。
"""

# ==================== 通用规则 ====================

# 所有图表类型必须包含的字段
REQUIRED_FIELDS = {
    "datasource": "格式: '{id}__table'",
    "viz_type": "图表类型",
}

# ==================== Table 图表规则 ====================
TABLE_RULES = {
    "viz_type": "table",
    "query_mode": "raw",  # 必须为 raw
    "all_columns": "必须指定，显示的列名数组",
    "adhoc_filters": "必须指定，至少包含时间范围过滤器",
    "server_pagination": True,
    "server_page_length": 10,
    "order_by_cols": [],
    "order_desc": True,
}

# ==================== Pie 图表规则 ====================
PIE_RULES = {
    "viz_type": "pie",  # 注意：不是 echarts_pie
    "metric": "使用单数字符串，不是 metrics 数组",
    "groupby": "维度数组",
    "adhoc_filters": "时间过滤器",
    "row_limit": 100,
    "sort_by_metric": True,
    "orderby": "[[metric_name, False]] 格式",
}

# ==================== Line 图表规则 ====================
LINE_RULES = {
    "viz_type": "echarts_timeseries_line",
    "granularity_sqla": "时间列名",
    # 注意：time_grain_sqla 会导致 PostgreSQL grain 不支持错误，必须移除
    "time_grain_sqla": "必须移除！PostgreSQL 不支持 week grain",
    "x_axis": "时间列名",
    "metrics": "度量数组",
    "groupby": "维度数组",
    "time_range": "No filter",
}

# PostgreSQL 不支持的时间粒度（会导致错误）
UNSUPPORTED_TIME_GRAINS = ["week", "month", "quarter", "year"]

# ==================== Big Number 图表规则 ====================
BIG_NUMBER_RULES = {
    "viz_type": "big_number_total",
    "metric": "使用单数字符串，不是 metrics 数组",
    # 移除不需要的字段
    "remove_fields": ["metrics", "granularity_sqla", "time_grain_sqla", "x_axis", "adhoc_filters", "time_range"],
}

# ==================== query_context 构建规则 ====================

QUERY_CONTEXT_RULES = {
    "table": {
        "columns": "使用 all_columns 字符串数组",
        "filters": "从 adhoc_filters 转换",
    },
    "pie": {
        "columns": "使用 groupby 字符串数组",
        "metrics": "[metric_name] 数组",
        "orderby": "[[metric_name, False]]",
    },
    "line": {
        "time_range": "必须设置为 'No filter'",
        "granularity": "时间列名",
        "columns": "[维度列, 时间列] 字符串数组，不使用 timeGrain 对象",
        "series_columns": "维度数组",
        "extras": "不包含 time_grain_sqla",
        "post_processing": "[pivot, rename, flatten]",
    },
}

# ==================== 验证函数 ====================

def validate_chart_config(chart_type: str, config: dict) -> list:
    """验证图表配置是否符合规则

    Args:
        chart_type: 图表类型 (table, pie, line, big_number 等)
        config: 图表配置 (exposures.yml 中的 chart 配置)

    Returns:
        错误消息列表，空列表表示验证通过
    """
    errors = []

    if chart_type == "table":
        # Table 必须指定 columns
        if not config.get("columns"):
            errors.append("缺少必需的 'columns' 字段，必须指定要显示的列名列表")
        # Table 必须指定 time_column（用于生成 adhoc_filters）
        if not config.get("time_column"):
            errors.append("缺少 'time_column' 字段，建议指定时间列用于过滤器")

    elif chart_type in ("pie", "doughnut"):
        # Pie 必须指定 metrics
        if not config.get("metrics"):
            errors.append("缺少必需的 'metrics' 字段")
        # Pie 必须指定 dimensions (groupby)
        if not config.get("dimensions"):
            errors.append("缺少必需的 'dimensions' 字段")

    elif chart_type == "line":
        # Line 必须指定 metrics
        if not config.get("metrics"):
            errors.append("缺少必需的 'metrics' 字段")
        # Line 必须指定 time_column
        if not config.get("time_column"):
            errors.append("缺少必需的 'time_column' 字段")
        # Line 必须指定 dimensions (groupby)
        if not config.get("dimensions"):
            errors.append("缺少必需的 'dimensions' 字段")

    elif chart_type in ("big_number", "number"):
        # Big Number 必须指定 metrics
        if not config.get("metrics"):
            errors.append("缺少必需的 'metrics' 字段")

    return errors


def validate_chart_params(viz_type: str, params: dict) -> list:
    """验证图表参数是否符合规则

    Args:
        viz_type: 图表类型
        params: 图表参数

    Returns:
        错误消息列表，空列表表示验证通过
    """
    errors = []

    if viz_type == "table":
        if params.get("query_mode") != "raw":
            errors.append("Table 图表必须设置 query_mode='raw'")
        if not params.get("all_columns"):
            errors.append("Table 图表必须指定 all_columns")
        if not params.get("adhoc_filters"):
            errors.append("Table 图表必须指定 adhoc_filters")

    elif viz_type == "pie":
        if params.get("metrics") and not params.get("metric"):
            errors.append("Pie 图表应使用 metric (单数) 而非 metrics (数组)")
        if params.get("viz_type") == "echarts_pie":
            errors.append("Pie 图表 viz_type 应为 'pie' 而非 'echarts_pie'")

    elif viz_type == "echarts_timeseries_line":
        if params.get("time_grain_sqla"):
            errors.append("Line 图表不应包含 time_grain_sqla (会导致 PostgreSQL grain 错误)")
        if not params.get("granularity_sqla") and not params.get("x_axis"):
            errors.append("Line 图表必须指定 granularity_sqla 或 x_axis")

    elif viz_type == "big_number_total":
        if params.get("metrics") and not params.get("metric"):
            errors.append("Big Number 图表应使用 metric (单数)")

    return errors


def fix_chart_params(viz_type: str, params: dict) -> dict:
    """自动修复图表参数

    Args:
        viz_type: 图表类型
        params: 图表参数

    Returns:
        修复后的参数
    """
    fixed_params = params.copy()

    if viz_type == "table":
        fixed_params.setdefault("query_mode", "raw")
        fixed_params.setdefault("server_pagination", True)
        fixed_params.setdefault("server_page_length", 10)

    elif viz_type == "pie":
        # 修复 viz_type
        if fixed_params.get("viz_type") == "echarts_pie":
            fixed_params["viz_type"] = "pie"
        # 转换 metrics 为 metric
        if fixed_params.get("metrics") and not fixed_params.get("metric"):
            fixed_params["metric"] = fixed_params["metrics"][0]
            fixed_params.pop("metrics", None)

    elif viz_type == "echarts_timeseries_line":
        # 移除会导致错误的 time_grain_sqla
        fixed_params.pop("time_grain_sqla", None)
        fixed_params.setdefault("time_range", "No filter")

    elif viz_type == "big_number_total":
        # 转换 metrics 为 metric
        if fixed_params.get("metrics") and not fixed_params.get("metric"):
            fixed_params["metric"] = fixed_params["metrics"][0]
            fixed_params.pop("metrics", None)
        # 移除不需要的字段
        for field in ["granularity_sqla", "time_grain_sqla", "x_axis", "adhoc_filters", "time_range"]:
            fixed_params.pop(field, None)

    return fixed_params