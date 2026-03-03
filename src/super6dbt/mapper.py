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
    "number": "big_number",
    "pie": "echarts_pie",
}

# ==================== dbt -> Superset ====================


@dataclass
class DimensionConfig:
    """维度配置"""
    name: str
    type: str
    description: str = ""
    time_intervals: Optional[List[str]] = None


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
    dimensions: Optional[Dict[str, Any]] = None
    metrics: Optional[Dict[str, MetricConfig]] = None


@dataclass
class ModelMeta:
    """模型元数据配置"""
    name: str
    description: str = ""
    columns: Dict[str, ColumnMeta] = None

    def __post_init__(self):
        if self.columns is None:
            self.columns = {}


class DbtToSuperset:
    """dbt定义转换为Superset配置"""

    def __init__(self):
        self.models: Dict[str, ModelMeta] = {}
        self.exposures: Dict[str, Dict[str, Any]] = {}

    def parse_model_meta(self, model_name: str, model_data: Dict[str, Any]) -> ModelMeta:
        """解析模型的meta配置"""
        meta = ModelMeta(
            name=model_name,
            description=model_data.get("description", ""),
        )

        for column in model_data.get("columns", []):
            col_name = column.get("name")
            col_config = column.get("config", {})
            col_meta = col_config.get("meta", {})

            column_meta = ColumnMeta(name=col_name)

            # 解析dimension配置
            if "dimension" in col_meta:
                dimension = col_meta["dimension"]
                column_meta.dimensions = dimension

            # 解析metrics配置
            if "metrics" in col_meta:
                column_meta.metrics = {}
                for metric_name, metric_config in col_meta["metrics"].items():
                    column_meta.metrics[metric_name] = MetricConfig(
                        name=metric_name,
                        type=metric_config.get("type", ""),
                        description=metric_config.get("description", ""),
                        sql=metric_config.get("sql", ""),
                    )

            meta.columns[col_name] = column_meta

        return meta

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