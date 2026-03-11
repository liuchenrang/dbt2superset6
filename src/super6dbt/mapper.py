"""dbt与Superset之间的映射转换

包含：
1. DbtToSuperset: dbt定义转换为Superset配置
2. SupersetToDbt: Superset配置转换为dbt定义
"""

from typing import Any, Dict, List, Optional
from dataclasses import dataclass
import logging
import re

logger = logging.getLogger(__name__)


# ==================== 类型映射 ====================

DBT_TYPE_TO_SUPERSET = {
    "string": "VARCHAR",
    "number": "NUMERIC",
    "date": "TIMESTAMP",
    "time": "TIMESTAMP",
    "datetime": "TIMESTAMP",
    "boolean": "BOOLEAN",
    "integer": "INT",
}

SUPERSET_TYPE_TO_DBT = {
    "VARCHAR": "string",
    "TEXT": "string",
    "NUMERIC": "number",
    "FLOAT": "number",
    "DOUBLE": "number",
    "INT": "integer",
    "INTEGER": "number",  # Lightdash 使用 number 表示数值类型
    "BIGINT": "integer",
    "TIMESTAMP": "datetime",
    "DATETIME": "datetime",
    "DATE": "date",
    "BOOLEAN": "boolean",
}

METRIC_TYPE_TO_SUPERSET_AGG = {
    "count": "COUNT",
    "count_distinct": "COUNT_DISTINCT",
    "sum": "SUM",
    "avg": "AVG",
    "min": "MIN",
    "max": "MAX",
}

SUPERSET_AGG_TO_METRIC_TYPE = {
    "COUNT": "count",
    "COUNT_DISTINCT": "count_distinct",
    "SUM": "sum",
    "AVG": "avg",
    "MIN": "min",
    "MAX": "max",
}

VIZ_TYPE_MAP = {
    "line": "echarts_timeseries_line",
    "bar": "echarts_timeseries_bar",
    "table": "table",
    "number": "big_number_total",
    "big_number": "big_number_total",
    "pie": "pie",
    "doughnut": "pie",
    "area": "echarts_timeseries",
}

# ==================== dbt -> Superset ====================


@dataclass
class DimensionConfig:
    """维度配置（扩展支持计算列）"""
    name: str
    type: str
    description: str = ""
    time_intervals: Optional[List[str]] = None
    expression: str = ""  # 计算列的 SQL 表达式


@dataclass
class ComputedColumn:
    """dbt 计算列定义"""
    name: str
    type: str
    sql: str
    description: str = ""
    verbose_name: str = ""


@dataclass
class MetricConfig:
    """度量配置"""
    name: str
    type: str
    description: str = ""
    sql: Optional[str] = ""


@dataclass
class ColumnMeta:
    """列的元数据配置"""
    name: str
    description: str = ""
    dimensions: Optional[Dict[str, Any]] = None
    metrics: Optional[Dict[str, MetricConfig]] = None


@dataclass
class ModelMeta:
    """模型元数据配置（扩展支持计算列）"""
    name: str
    description: str = ""
    columns: Dict[str, ColumnMeta] = None
    computed_columns: Optional[Dict[str, ComputedColumn]] = None
    metrics: Optional[Dict[str, MetricConfig]] = None  # 表级 metrics

    def __post_init__(self):
        if self.columns is None:
            self.columns = {}
        if self.computed_columns is None:
            self.computed_columns = {}
        if self.metrics is None:
            self.metrics = {}


class DbtToSuperset:
    """dbt定义转换为Superset配置"""

    def __init__(self):
        self.models: Dict[str, ModelMeta] = {}
        self.exposures: Dict[str, Dict[str, Any]] = {}

    def parse_model_meta(self, model_name: str, model_data: Dict[str, Any]) -> ModelMeta:
        """解析模型的meta配置（支持 computed_columns）"""
        meta = ModelMeta(
            name=model_name,
            description=model_data.get("description", ""),
        )

        for column in model_data.get("columns", []):
            col_name = column.get("name")
            col_description = column.get("description", "")
            col_config = column.get("config", {})
            col_meta = col_config.get("meta", {})

            column_meta = ColumnMeta(name=col_name, description=col_description)

            # 解析dimension配置（包含 expression 用于计算列）
            if "dimension" in col_meta:
                dimension = col_meta["dimension"]
                column_meta.dimensions = dimension

            # 解析metrics配置（支持单数 metric 和复数 metrics 两种形式）
            if "metric" in col_meta:
                # 单数形式 metric: {type: sum, ...}
                metric_config = col_meta["metric"]
                column_meta.metrics = {
                    col_name: MetricConfig(
                        name=col_name,
                        type=metric_config.get("type", ""),
                        description=metric_config.get("description", metric_config.get("label", "")),
                        sql=metric_config.get("sql", col_name),
                    )
                }
            elif "metrics" in col_meta:
                # 复数形式 metrics: {metric_name: {...}}
                column_meta.metrics = {}
                for metric_name, metric_config in col_meta["metrics"].items():
                    column_meta.metrics[metric_name] = MetricConfig(
                        name=metric_name,
                        type=metric_config.get("type", ""),
                        description=metric_config.get("description", ""),
                        sql=metric_config.get("sql", ""),
                    )

            meta.columns[col_name] = column_meta

        # 解析 dbt computed_columns 规范
        for computed in model_data.get("computed_columns", []):
            col_name = computed.get("name")
            if col_name:
                meta.computed_columns[col_name] = ComputedColumn(
                    name=col_name,
                    type=computed.get("type", "number"),
                    sql=computed.get("sql", ""),
                    description=computed.get("description", ""),
                    verbose_name=computed.get("verbose_name", col_name),
                )

        # 解析表级 meta.metrics
        model_meta_config = model_data.get("meta", {})
        if "metrics" in model_meta_config:
            for metric_name, metric_config in model_meta_config["metrics"].items():
                meta.metrics[metric_name] = MetricConfig(
                    name=metric_name,
                    type=metric_config.get("type", ""),
                    description=metric_config.get("description", ""),
                    sql=metric_config.get("sql", ""),
                )

        return meta

    def identify_calculated_columns(self, model_meta: ModelMeta) -> List[Dict[str, Any]]:
        """识别计算列（支持多种来源）

        优先级：
        1. dbt computed_columns 规范
        2. Lightdash meta.superset.expression 扩展
        """
        calculated = []
        calculated_names = set()

        # 方式 1: dbt computed_columns 规范
        if model_meta.computed_columns:
            for col_name, computed in model_meta.computed_columns.items():
                calculated.append({
                    "column_name": col_name,
                    "expression": computed.sql,
                    "type": self._infer_type_from_expression(computed.sql),
                    "description": computed.description,
                    "verbose_name": computed.verbose_name or computed.description,
                })
                calculated_names.add(col_name)

        # 方式 2: Lightdash meta.superset 扩展
        for col_name, col_meta in model_meta.columns.items():
            if col_name not in calculated_names and col_meta.dimensions:
                dimension = col_meta.dimensions
                superset_meta = dimension.get("superset", {})
                expression = superset_meta.get("expression")
                if expression:
                    calculated.append({
                        "column_name": col_name,
                        "expression": expression,
                        "type": dimension.get("type", "NUMERIC"),
                        "description": col_meta.description,
                        "verbose_name": col_meta.description,
                    })
                    calculated_names.add(col_name)

        return calculated

    def _infer_type_from_expression(self, expression: str) -> str:
        """从 SQL 表达式推断数据类型"""
        expr_upper = expression.upper()

        if any(op in expr_upper for op in ["COUNT", "COUNT_DISTINCT", "AVG", "SUM", "MIN", "MAX"]):
            return "NUMERIC"
        elif any(op in expr_upper for op in ["DATE", "TIME", "TIMESTAMP", "DATETIME"]):
            return "DATE"
        elif any(op in expr_upper for op in ["EXTRACT"]):
            if "YEAR" in expr_upper:
                return "INTEGER"
            return "NUMERIC"

        # 默认返回 NUMERIC
        return "NUMERIC"

    def parse_exposure(self, exposure_data: Dict[str, Any]) -> Dict[str, Any]:
        """解析exposure配置"""
        return {
            "name": exposure_data.get("name"),
            "type": exposure_data.get("type"),
            "label": exposure_data.get("label"),
            "description": exposure_data.get("description", ""),
            "url": exposure_data.get("url", ""),
            "depends_on": exposure_data.get("depends_on", []),
            "owner": exposure_data.get("owner", {}).get("name"),
            "meta": exposure_data.get("meta", {}),
        }

    def model_to_superset_columns(
        self, model_meta: ModelMeta
    ) -> Dict[str, Any]:
        """将模型元数据转换为Superset列配置"""
        columns = []

        for col_name, col_meta in model_meta.columns.items():
            # 维度
            if col_meta.dimensions:
                columns.append({
                    "column_name": col_name,
                    "type": DBT_TYPE_TO_SUPERSET.get(col_meta.dimensions.get("type", "string"), "VARCHAR"),
                    "verbose_name": col_meta.dimensions.get("label", col_name),
                    "description": col_meta.dimensions.get("description", ""),
                })

            # 度量
            if col_meta.metrics:
                for metric_name, metric in col_meta.metrics.items():
                    agg_type = METRIC_TYPE_TO_SUPERSET_AGG.get(metric.type, "SUM")
                    sql_expr = f"{agg_type}({col_name})"

                    if metric.sql:
                        sql_expr = metric.sql

                    columns.append({
                        "expression_type": "SIMPLE",
                        "sql_expression": sql_expr,
                        "metric_name": metric_name,
                        "verbose_name": metric_name,
                        "description": metric.description,
                        "d3format": ",".format() if metric.type in ("sum", "count") else "",
                    })

        return {"columns": columns}

    def exposure_to_dashboard_config(
        self, exposure: Dict[str, Any], model_metas: Dict[str, ModelMeta]
    ) -> Dict[str, Any]:
        """将exposure转换为Superset面板配置"""
        dashboard_config = {
            "title": exposure.get("label", exposure.get("name")),
            "description": exposure.get("description", ""),
            "charts": [],
        }

        exposure_meta = exposure.get("meta", {}) or {}

        # 从meta中获取charts配置
        charts_config = exposure_meta.get("charts") or []
        for chart_config in charts_config:
            chart = self._create_chart_config(chart_config, model_metas)
            if chart:
                dashboard_config["charts"].append(chart)

        return dashboard_config

    def _create_chart_config(
        self, chart_config: Dict[str, Any], model_metas: Dict[str, ModelMeta]
    ) -> Optional[Dict[str, Any]]:
        """创建单个图表配置"""
        model_name = chart_config.get("model")
        if not model_name or model_name not in model_metas:
            logger.warning(f"模型 {model_name} 未找到")
            return None

        model_meta = model_metas[model_name]

        # 确定可视化类型
        viz_type = VIZ_TYPE_MAP.get(
            chart_config.get("type", "line"),
            "echarts_timeseries_line"
        )

        # 构建图表参数
        params = {
            "metrics": [],
            "groupby": [],
            "time_range": chart_config.get("time_range", "No filter"),
            "viz_type": viz_type,
        }

        # 添加度量
        if "metrics" in chart_config:
            for metric_ref in chart_config["metrics"]:
                if "." in metric_ref:
                    model, metric = metric_ref.split(".", 1)
                    params["metrics"].append(metric)

        # 添加分组维度
        if "dimensions" in chart_config:
            for dim in chart_config["dimensions"]:
                if "." in dim:
                    model, dim_name = dim.split(".", 1)
                    params["groupby"].append(dim_name)

        return {
            "title": chart_config.get("title", chart_config.get("type", "Chart")),
            "description": chart_config.get("description", ""),
            "viz_type": viz_type,
            "params": params,
            "model_name": model_name,
        }


# ==================== Superset -> dbt ====================


class SupersetToDbt:
    """Superset配置转换为dbt定义"""

    def __init__(self):
        self.models: Dict[str, Dict[str, Any]] = {}
        self.exposures: Dict[str, Dict[str, Any]] = {}

    def dashboard_to_exposure(
        self, dashboard: Dict[str, Any], dataset_map: Dict[str, Any]
    ) -> Dict[str, Any]:
        """将Superset面板转换为dbt exposure"""
        charts = dashboard.get("charts", [])
        depends_on = set()

        # 收集依赖的数据集
        for chart in charts:
            datasource_id = chart.get("datasource_id")
            if datasource_id in dataset_map:
                dataset = dataset_map[datasource_id]
                table_name = dataset.get("table_name")
                if table_name:
                    depends_on.add(f"ref('{table_name}')")

        # 构建charts配置
        charts_config = []
        for chart in charts:
            chart_config = self._chart_to_exposure_chart(chart, dataset_map)
            if chart_config:
                charts_config.append(chart_config)

        # 获取标题并确保是字符串
        title = dashboard.get("dashboard_title", dashboard.get("title", ""))
        if isinstance(title, list):
            title = " ".join(str(x) for x in title)

        # name必须是合法的标识符（小写、下划线、不能以数字开头）
        # 首先处理空格为下划线，然后移除特殊字符
        name = title.strip()

        # 如果标题包含非ASCII字符，使用拼音转换或简单处理
        import unicodedata
        # 将中文字符转换为拼音或保留英文
        # 简单处理：只保留ASCII字符，其他用下划线替换
        name_ascii = unicodedata.normalize('NFKD', name).encode('ascii', 'ignore').decode('ascii')
        if name_ascii and len(name_ascii) > len(name) * 0.5:
            # 如果ASCII字符占比超过50%，使用ASCII版本
            name = name_ascii
        else:
            # 否则使用简单的标识符
            name = f"dashboard_{dashboard.get('id', 'untitled')}"

        # 将空格替换为下划线并转为小写
        name = name.replace(" ", "_").replace("-", "_").lower()
        # 移除特殊字符，只保留字母数字下划线
        name = re.sub(r'[^a-z0-9_]', '_', name)
        name = re.sub(r'_+', '_', name).strip('_')
        # 移除开头的数字
        name = re.sub(r'^[0-9]+', '', name)
        # 如果为空，使用默认名称
        if not name:
            name = f"untitled_dashboard_{dashboard.get('id', '')}"

        return {
            "name": name,
            "type": "dashboard",
            "label": title,
            "description": dashboard.get("description_text", dashboard.get("description", "")),
            "url": dashboard.get("slug", ""),
            "depends_on": sorted(list(depends_on)),
            "owner": {
                "name": "admin",
                "email": "admin@example.com",
            },
            "meta": {
                "dashboard_id": dashboard.get("id"),
                "charts": charts_config,
                "superset_url": dashboard.get("permalink", ""),
            },
        }

    def _chart_to_exposure_chart(
        self, chart: Dict[str, Any], dataset_map: Dict[str, Any]
    ) -> Optional[Dict[str, Any]]:
        """将图表转换为exposure中的chart配置"""
        datasource_id = chart.get("datasource_id")
        if not datasource_id or datasource_id not in dataset_map:
            return None

        dataset = dataset_map[datasource_id]
        table_name = dataset.get("table_name")
        if not table_name:
            return None

        viz_type = chart.get("viz_type", "")
        chart_type = None
        for dbt_type, superset_type in VIZ_TYPE_MAP.items():
            if superset_type in viz_type:
                chart_type = dbt_type
                break

        if not chart_type:
            chart_type = "line"

        params = chart.get("params", {})
        metrics = params.get("metrics", [])
        groupby = params.get("groupby", [])

        chart_config = {
            "title": chart.get("slice_name", chart.get("title", "Chart")),
            "type": chart_type,
            "model": table_name,
            "metrics": metrics,
            "dimensions": groupby,
            "time_range": params.get("time_range", "No filter"),
        }

        return chart_config

    def dataset_to_model_meta(
        self, dataset: Dict[str, Any]
    ) -> Dict[str, Any]:
        """将数据集转换为模型的meta配置"""
        table_name = dataset.get("table_name")
        columns_meta = {}

        # 处理列配置
        columns = dataset.get("columns", [])
        for col in columns:
            col_name = col.get("column_name")
            if not col_name:
                continue

            col_type = col.get("type", "")
            dbt_type = SUPERSET_TYPE_TO_DBT.get(col_type, "string")

            meta_key = "dimension"

            # 如果是度量（有expression_type）
            if col.get("expression_type") == "SIMPLE":
                metric_name = col.get("metric_name", col_name)
                meta_key = "metrics"

                # 从sql_expression解析度量类型
                sql_expr = col.get("sql_expression", "")
                metric_type = "sum"
                if "COUNT_DISTINCT" in sql_expr:
                    metric_type = "count_distinct"
                elif "COUNT" in sql_expr:
                    metric_type = "count"
                elif "AVG" in sql_expr:
                    metric_type = "avg"
                elif "MIN" in sql_expr:
                    metric_type = "min"
                elif "MAX" in sql_expr:
                    metric_type = "max"

                columns_meta[col_name] = {
                    "description": col.get("description", ""),
                    "config": {
                        "meta": {
                            "metrics": {
                                metric_name: {
                                    "type": metric_type,
                                    "description": col.get("verbose_name", metric_name),
                                    "sql": sql_expr,
                                }
                            }
                        }
                    }
                }
            else:
                # 维度
                columns_meta[col_name] = {
                    "description": col.get("description", ""),
                    "config": {
                        "meta": {
                            "dimension": {
                                "type": dbt_type,
                                "label": col.get("verbose_name", col_name),
                            }
                        }
                    }
                }

        return {
            "name": table_name,
            "columns": columns_meta,
        }

    def generate_yaml_exposure(self, exposure: Dict[str, Any]) -> str:
        """生成exposure YAML"""
        lines = [
            'exposures:',
            f'  - name: "{exposure["name"]}"',
            f'    type: {exposure["type"]}',
            f'    label: "{exposure["label"]}"',
        ]

        if exposure.get("description"):
            desc = exposure["description"]
            # 处理包含换行符或特殊字符的描述
            if '\n' in desc or '"' in desc or "'" in desc:
                lines.append(f'    description: |')
                for line in desc.split('\n'):
                    lines.append(f'      {line}')
            else:
                lines.append(f'    description: "{desc}"')

        if exposure.get("url"):
            lines.append(f'    url: "{exposure["url"]}"')

        if exposure.get("depends_on"):
            lines.append('    depends_on:')
            for dep in exposure["depends_on"]:
                lines.append(f'      - {dep}')

        if exposure.get("owner"):
            owner = exposure["owner"]
            lines.append('    owner:')
            owner_name = owner.get("name", "")
            if owner_name:
                lines.append(f'      name: "{owner_name}"')
            owner_email = owner.get("email", "")
            if owner_email:
                lines.append(f'      email: "{owner_email}"')

        if exposure.get("meta"):
            lines.append('    meta:')
            meta = exposure["meta"]
            for key, value in meta.items():
                if key == "charts":
                    lines.append('      charts:')
                    for chart in value:
                        chart_title = chart.get("title", "")
                        chart_type = chart.get("type", "line")
                        chart_model = chart.get("model", "")
                        lines.append(f'        - title: "{chart_title}"')
                        lines.append(f'          type: {chart_type}')
                        lines.append(f'          model: "{chart_model}"')

                        metrics = chart.get("metrics", [])
                        if metrics:
                            if len(metrics) == 1:
                                lines.append(f'          metrics: ["{metrics[0]}"]')
                            else:
                                lines.append(f'          metrics:')
                                for m in metrics:
                                    lines.append(f'            - "{m}"')

                        dimensions = chart.get("dimensions", [])
                        if dimensions:
                            if len(dimensions) == 1:
                                lines.append(f'          dimensions: ["{dimensions[0]}"]')
                            else:
                                lines.append(f'          dimensions:')
                                for d in dimensions:
                                    lines.append(f'            - "{d}"')

                        time_range = chart.get("time_range", "")
                        if time_range:
                            lines.append(f'          time_range: "{time_range}"')
                else:
                    # 其他meta字段
                    if isinstance(value, str):
                        lines.append(f'      {key}: "{value}"')
                    elif isinstance(value, bool):
                        lines.append(f'      {key}: {str(value).lower()}')
                    elif isinstance(value, (int, float)):
                        lines.append(f'      {key}: {value}')
                    elif value is None:
                        lines.append(f'      {key}: null')
                    else:
                        lines.append(f'      {key}: {value}')

        return "\n".join(lines)

    def generate_yaml_schema(self, dataset: Dict[str, Any]) -> str:
        """根据数据集生成完整的 schema.yml 文件内容（Lightdash 格式）

        Args:
            dataset: Superset 数据集信息（包含 columns 和 metrics）

        Returns:
            YAML 格式的 schema 内容（Lightdash 格式）
        """
        table_name = dataset.get("table_name", "")
        if not table_name:
            return ""

        # Superset 数据集有两部分：
        # 1. columns - 物理列（用于维度）
        # 2. metrics - 虚拟度量
        columns = dataset.get("columns", [])
        metrics = dataset.get("metrics", [])

        lines = []

        # 生成 x-metric-definitions（Lightdash 格式）
        if metrics:
            lines.append('x-metric-definitions:')

            for i, metric in enumerate(metrics):
                metric_name = metric.get("metric_name", "")
                expression = metric.get("expression", "")
                description = metric.get("description", "")

                if not metric_name or not expression:
                    continue

                # 从 expression 解析度量类型
                metric_type = "sum"
                if "COUNT_DISTINCT" in expression:
                    metric_type = "count_distinct"
                elif "COUNT" in expression and "DISTINCT" not in expression:
                    metric_type = "count"
                elif "AVG" in expression:
                    metric_type = "avg"
                elif "MIN" in expression:
                    metric_type = "min"
                elif "MAX" in expression:
                    metric_type = "max"

                # YAML anchor 引用ID
                anchor_id = f"&id00{i + 1}"

                lines.append(f'  {metric_name}: {anchor_id}')
                lines.append(f'    type: {metric_type}')

                # 处理 SQL 表达式 - 如果包含换行符或特殊字符，使用 YAML 的字面量块样式
                sql_cleaned = expression.strip()
                if '\n' in sql_cleaned or "'" in sql_cleaned or '"' in sql_cleaned:
                    # 使用字面量块样式（|）保留换行和引号
                    lines.append('    sql: |')
                    for line in sql_cleaned.split('\n'):
                        lines.append(f'      {line}')
                else:
                    lines.append(f'    sql: {sql_cleaned}')

                if description:
                    lines.append(f'    description: {description}')

            lines.append('')

        # 生成 models 定义
        lines.append('version: 2')
        lines.append('')
        lines.append('models:')
        lines.append(f"  - name: {table_name}")

        # 使用 Superset 数据集的 description，如果没有则使用默认值
        dataset_description = dataset.get("description", f"从 Superset 数据集 {table_name} 同步")
        if dataset_description:
            lines.append(f'    description: "{dataset_description}"')
        else:
            lines.append(f'    description: "从 Superset 数据集 {table_name} 同步"')

        if columns:
            lines.append('    columns:')

            # 构建列名到度量类型的映射
            col_metrics_map = {}
            for i, metric in enumerate(metrics):
                expression = metric.get("expression", "")
                metric_name = metric.get("metric_name", "")
                anchor_id = f"*id00{i + 1}"

                # 从表达式中提取引用的列名
                # 例如: SUM(total) -> total, COUNT(*) -> None
                import re
                match = re.search(r'\b(\w+)\s*\)$', expression)
                if match:
                    col_name = match.group(1)
                    if col_name not in col_metrics_map:
                        col_metrics_map[col_name] = []
                    col_metrics_map[col_name].append((metric_name, anchor_id))

            for col in columns:
                col_name = col.get("column_name", "")
                if not col_name:
                    continue

                col_type = col.get("type", "")
                col_description = col.get("description", col.get("verbose_name", ""))

                lines.append(f"    - name: {col_name}")
                if col_description:
                    lines.append(f'      description: "{col_description}"')

                lines.append('      config:')
                lines.append('        meta:')

                # 维度配置
                dbt_type = SUPERSET_TYPE_TO_DBT.get(col_type, "string")
                lines.append('          dimension:')
                lines.append(f'            type: {dbt_type}')
                lines.append(f'            label: null')

                # 度量配置（如果该列有相关度量）
                if col_name in col_metrics_map:
                    lines.append('          metrics:')
                    for metric_name, anchor_ref in col_metrics_map[col_name]:
                        lines.append(f'            {metric_name}: {anchor_ref}')

        # 处理计算列（添加到 columns 中，使用 meta.superset 扩展）
        calculated_columns = [c for c in columns if c.get("expression")]
        if calculated_columns:
            if not physical_columns:
                lines.append('    columns:')

            for calc in calculated_columns:
                col_name = calc.get("column_name", "")
                expression = calc.get("expression", "")
                col_type = calc.get("type", "number")
                col_description = calc.get("description", "")

                if not col_name:
                    continue

                lines.append(f"    - name: {col_name}")
                if col_description:
                    lines.append(f'      description: "{col_description}"')

                lines.append('      config:')
                lines.append('        meta:')

                # 维度配置
                dbt_type = SUPERSET_TYPE_TO_DBT.get(col_type, "string")
                lines.append('          dimension:')
                lines.append(f'            type: {dbt_type}')
                lines.append(f'            label: null')

                # Superset 兼容字段（存储 expression）
                lines.append('          superset:')
                lines.append(f'            expression: {expression}')

        return "\n".join(lines)

    def _classify_columns(self, dataset: Dict[str, Any]) -> tuple:
        """分类列：物理列和计算列

        Returns:
            (physical_columns, calculated_columns)
        """
        columns = dataset.get("columns", [])

        physical = []
        calculated = []

        for col in columns:
            if col.get("expression"):
                calculated.append(col)
            else:
                physical.append(col)

        return physical, calculated

    def generate_yaml_schema_with_computed(self, dataset: Dict[str, Any]) -> str:
        """根据数据集生成完整的 schema.yml 文件内容（兼容计算列）

        Args:
            dataset: Superset 数据集信息（包含 columns 和 metrics）

        Returns:
            YAML 格式的 schema 内容（兼容 Lightdash 和 dbt 规范）
        """
        table_name = dataset.get("table_name", "")
        if not table_name:
            return ""

        # 分类列：物理列和计算列
        physical_columns, calculated_columns = self._classify_columns(dataset)
        metrics = dataset.get("metrics", [])

        lines = []

        # 生成 x-metric-definitions（Lightdash 格式）
        if metrics:
            lines.append('x-metric-definitions:')

            for i, metric in enumerate(metrics):
                metric_name = metric.get("metric_name", "")
                expression = metric.get("expression", "")
                description = metric.get("description", "")

                if not metric_name or not expression:
                    continue

                # 从 expression 解析度量类型
                metric_type = "sum"
                if "COUNT_DISTINCT" in expression:
                    metric_type = "count_distinct"
                elif "COUNT" in expression and "DISTINCT" not in expression:
                    metric_type = "count"
                elif "AVG" in expression:
                    metric_type = "avg"
                elif "MIN" in expression:
                    metric_type = "min"
                elif "MAX" in expression:
                    metric_type = "max"

                # YAML anchor 引用ID
                anchor_id = f"&id00{i + 1}"

                lines.append(f'  {metric_name}: {anchor_id}')
                lines.append(f'    type: {metric_type}')

                # 处理 SQL 表达式 - 如果包含换行符或特殊字符，使用 YAML 的字面量块样式
                sql_cleaned = expression.strip()
                if '\n' in sql_cleaned or "'" in sql_cleaned or '"' in sql_cleaned:
                    # 使用字面量块样式（|）保留换行和引号
                    lines.append('    sql: |')
                    for line in sql_cleaned.split('\n'):
                        lines.append(f'      {line}')
                else:
                    lines.append(f'    sql: {sql_cleaned}')

                if description:
                    lines.append(f'    description: {description}')

            lines.append('')

        # 生成 models 定义
        lines.append('version: 2')
        lines.append('')
        lines.append('models:')
        lines.append(f"  - name: {table_name}")

        # 使用 Superset 数据集的 description，如果没有则使用默认值
        dataset_description = dataset.get("description", f"从 Superset 数据集 {table_name} 同步")
        if dataset_description:
            lines.append(f'    description: "{dataset_description}"')
        else:
            lines.append(f'    description: "从 Superset 数据集 {table_name} 同步"')

        # 生成 computed_columns（dbt 规范）
        if calculated_columns:
            lines.append('    computed_columns:')
            for calc in calculated_columns:
                col_name = calc.get("column_name", "")
                expression = calc.get("expression", "")
                col_type = calc.get("type", "number")
                col_description = calc.get("description", "")

                if not col_name:
                    continue

                lines.append(f'      - name: {col_name}')
                if col_description:
                    lines.append(f'        description: "{col_description}"')
                lines.append(f'        type: {col_type.lower() if col_type else "number"}')
                lines.append(f'        sql: {expression}')

        if physical_columns:
            lines.append('    columns:')

            # 构建列名到度量类型的映射
            col_metrics_map = {}
            for i, metric in enumerate(metrics):
                expression = metric.get("expression", "")
                metric_name = metric.get("metric_name", "")
                anchor_ref = f"*id00{i + 1}"

                # 从表达式中提取引用的列名
                import re
                match = re.search(r'\b(\w+)\s*\)$', expression)
                if match:
                    col_name_ref = match.group(1)
                    if col_name_ref not in col_metrics_map:
                        col_metrics_map[col_name_ref] = []
                    col_metrics_map[col_name_ref].append((metric_name, anchor_ref))

            for col in physical_columns:
                col_name = col.get("column_name", "")
                if not col_name:
                    continue

                col_type = col.get("type", "")
                col_description = col.get("description", col.get("verbose_name", ""))

                lines.append(f"    - name: {col_name}")
                if col_description:
                    lines.append(f'      description: "{col_description}"')

                lines.append('      config:')
                lines.append('        meta:')

                # 维度配置
                dbt_type = SUPERSET_TYPE_TO_DBT.get(col_type, "string")
                lines.append('          dimension:')
                lines.append(f'            type: {dbt_type}')
                lines.append(f'            label: null')

                # 度量配置（如果该列有相关度量）
                if col_name in col_metrics_map:
                    lines.append('          metrics:')
                    for metric_name, anchor_ref in col_metrics_map[col_name]:
                        lines.append(f'            {metric_name}: {anchor_ref}')

        # 处理计算列（添加到 columns 中，使用 meta.superset 扩展）
        if calculated_columns:
            # 如果前面没有 columns 块，添加一个
            if not physical_columns:
                lines.append('    columns:')

            for calc in calculated_columns:
                col_name = calc.get("column_name", "")
                expression = calc.get("expression", "")
                col_type = calc.get("type", "number")
                col_description = calc.get("description", "")

                if not col_name:
                    continue

                lines.append(f"    - name: {col_name}")
                if col_description:
                    lines.append(f'      description: "{col_description}"')

                lines.append('      config:')
                lines.append('        meta:')

                # 维度配置
                dbt_type = SUPERSET_TYPE_TO_DBT.get(col_type, "string")
                lines.append('          dimension:')
                lines.append(f'            type: {dbt_type}')
                lines.append(f'            label: null')

                # Superset 兼容字段（存储 expression）
                lines.append('          superset:')
                lines.append(f'            expression: {expression}')

        return "\n".join(lines)