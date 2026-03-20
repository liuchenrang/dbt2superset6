"""Push功能：从dbt推送配置到Superset"""

from pathlib import Path
from typing import Dict, Any, List, Optional
import yaml
import logging
import json

from .client import SupersetClient
from .mapper import DbtToSuperset
from .config import DbtProjectConfig

logger = logging.getLogger(__name__)


class SupersetPusher:
    """从dbt推送配置到Superset"""

    def __init__(
        self,
        client: SupersetClient,
        dbt_config: DbtProjectConfig,
    ):
        self.client = client
        self.dbt_config = dbt_config
        self.mapper = DbtToSuperset()

    def push(self, exposure_names: List[str] = None, model_names: List[str] = None, schema: str = None) -> None:
        """推送配置

        Args:
            exposure_names: 要推送的exposure名称列表，None表示推送所有
            model_names: 要推送的model名称列表（仅同步数据集），None表示不限制
            schema: 指定数据集的 schema 名称（优先级最高）
        """
        logger.info("开始推送配置到Superset...")

        # 1. 加载所有models的meta信息
        self._load_models(model_names)

        # 2. 同步数据集指标到 Superset
        self._sync_dataset_metrics(model_names, schema)

        # 如果指定了 model_names，只同步数据集，不处理 exposures
        if model_names:
            logger.info(f"仅同步数据集: {', '.join(model_names)}")
            logger.info("推送完成")
            return

        # 3. 加载exposures
        exposures = self._load_exposures()

        if exposure_names:
            exposures = [e for e in exposures if e["name"] in exposure_names]

        logger.info(f"找到 {len(exposures)} 个exposures")

        # 4. 获取当前用户ID
        current_user = self.client.get_current_user()
        if not current_user:
            # 如果无法获取用户信息，尝试从用户列表获取
            users = self.client.get_users()
            if users:
                current_user = users[0]

        if not current_user:
            raise RuntimeError("无法获取当前用户信息")

        owner_id = current_user.get("id")
        logger.info(f"使用用户ID: {owner_id}")

        # 5. 为每个exposure创建或更新面板
        for exposure in exposures:
            self._sync_exposure(exposure, owner_id, schema)

        logger.info("推送完成")

    def _load_models(self, model_names: List[str] = None) -> None:
        """加载所有models的meta配置

        Args:
            model_names: 要加载的模型名称列表，None表示加载所有
        """
        model_paths = self.dbt_config.full_model_paths

        for model_path in model_paths:
            for pattern in ["**/*.yml", "**/*.yaml"]:
                for file_path in model_path.glob(pattern):
                    self._parse_model_file(file_path, model_names)

        # 如果指定了模型名称，只保留指定的模型
        if model_names:
            filtered_models = {
                name: meta for name, meta in self.mapper.models.items()
                if name in model_names
            }
            self.mapper.models = filtered_models

        logger.info(f"加载了 {len(self.mapper.models)} 个模型")

    def _parse_model_file(self, file_path: Path, model_names: List[str] = None) -> None:
        """解析单个model文件

        Args:
            file_path: 文件路径
            model_names: 要加载的模型名称列表，None表示加载所有

        优化：如果指定了 model_names，且文件名不匹配任何模型名，则跳过解析
        """
        # 如果指定了模型名称，先检查文件名是否可能包含指定的模型
        if model_names:
            # 获取文件名（不含路径和扩展名）
            file_stem = file_path.stem
            # 检查文件名是否匹配任何指定的模型名
            file_matches = any(
                model_name == file_stem or model_name.replace("_", "-") == file_stem
                for model_name in model_names
            )
            # 如果不匹配，跳过此文件（但只优化，仍然尝试解析以防万一）
            if not file_matches:
                # 可以在这里跳过，但为了保险起见，仍然解析文件
                # 因为有些情况下文件名可能不完全匹配
                pass

        try:
            with open(file_path, "r", encoding="utf-8") as f:
                data = yaml.safe_load(f)

            if not data:
                return

            models = data.get("models", [])
            for model in models:
                model_name = model.get("name")
                if model_name:
                    # 如果指定了模型名称，只加载指定的模型
                    if model_names and model_name not in model_names:
                        continue
                    model_meta = self.mapper.parse_model_meta(model_name, model)
                    self.mapper.models[model_name] = model_meta

        except Exception as e:
            # 只在可能相关的文件上显示错误
            if model_names is None or any(name in str(file_path) for name in model_names):
                logger.error(f"解析模型文件失败 {file_path}: {e}")

    def _load_exposures(self) -> List[Dict[str, Any]]:
        """加载exposures配置"""
        exposure_paths = self.dbt_config.full_exposure_paths
        exposures = []

        for exposure_path in exposure_paths:
            if not exposure_path.exists():
                continue

            for pattern in ["**/*.yml", "**/*.yaml"]:
                for file_path in exposure_path.glob(pattern):
                    exposures.extend(self._parse_exposure_file(file_path))

        return exposures

    def _parse_exposure_file(self, file_path: Path) -> List[Dict[str, Any]]:
        """解析单个exposure文件"""
        try:
            with open(file_path, "r", encoding="utf-8") as f:
                data = yaml.safe_load(f)

            if not data:
                return []

            return data.get("exposures", [])

        except Exception as e:
            logger.error(f"解析exposure文件失败 {file_path}: {e}")
            return []

    def _sync_exposure(self, exposure: Dict[str, Any], owner_id: int, schema: str = None) -> None:
        """同步单个exposure到Superset

        Args:
            exposure: exposure配置
            owner_id: 面板拥有者ID
            schema: 指定数据集的 schema 名称（优先级最高）
        """
        exposure_name = exposure.get("name")
        exposure_meta = exposure.get("meta", {})

        # 检查是否已存在该面板
        existing_dashboard_id = exposure_meta.get("dashboard_id")
        dashboard_config = self.mapper.exposure_to_dashboard_config(
            exposure, self.mapper.models
        )

        # 获取或创建面板
        if existing_dashboard_id:
            # 更新现有面板
            self._update_dashboard(existing_dashboard_id, dashboard_config)
        else:
            # 创建新面板
            dashboard_id = self._create_dashboard(dashboard_config, owner_id)
            if dashboard_id:
                exposure_meta["dashboard_id"] = dashboard_id
                # 更新exposure文件
                self._update_exposure_file(exposure_name, exposure)

        # 同步图表
        if exposure_meta.get("dashboard_id"):
            self._sync_charts(exposure, exposure_meta["dashboard_id"], schema, owner_id)

    def _create_dashboard(
        self, dashboard_config: Dict[str, Any], owner_id: int
    ) -> Optional[int]:
        """创建面板"""
        result = self.client.create_dashboard(
            title=dashboard_config["title"],
            description=dashboard_config["description"],
            owners=[owner_id],
        )

        if result:
            # API返回格式: {"id": 2, "result": {...}}
            dashboard_id = result.get("id")
            logger.info(f"创建面板: {dashboard_config['title']} (ID: {dashboard_id})")
            return dashboard_id

        return None

    def _update_dashboard(self, dashboard_id: int, dashboard_config: Dict[str, Any]) -> None:
        """更新面板"""
        # Superset 6.0 API update_dashboard 不支持 description 字段
        # 只更新标题
        result = self.client.update_dashboard(
            dashboard_id=dashboard_id,
            title=dashboard_config["title"],
        )

        if result:
            logger.info(f"更新面板: {dashboard_config['title']} (ID: {dashboard_id})")
        else:
            logger.warning(f"更新面板失败: ID {dashboard_id}")

    def _sync_charts(self, exposure: Dict[str, Any], dashboard_id: int, schema: str = None, owner_id: int = None) -> None:
        """同步图表到面板 - 包括创建/更新图表并将其关联到面板

        Args:
            exposure: exposure配置
            dashboard_id: 面板ID
            schema: 指定数据集的 schema 名称（优先级最高）
            owner_id: 图表拥有者ID
        """
        exposure_meta = exposure.get("meta", {})
        charts_config = exposure_meta.get("charts", [])
        existing_charts_map = exposure_meta.get("existing_charts", {})

        # 验证 table 类型的 chart 配置
        validation_errors = self._validate_table_charts(charts_config)
        if validation_errors:
            for error in validation_errors:
                logger.error(f"Chart 配置验证失败: {error}")
            raise ValueError(f"Chart 配置验证失败，共 {len(validation_errors)} 个错误")

        # 获取面板的现有图表
        dashboard = self.client.get_dashboard(dashboard_id)
        if not dashboard:
            logger.warning(f"无法获取面板: {dashboard_id}")
            return

        chart_positions = dashboard.get("position_json") or {}

        # 获取面板上已有的图表ID列表（用于验证图表是否存在）
        dashboard_chart_ids = set()
        if isinstance(chart_positions, dict):
            for key, value in chart_positions.items():
                if isinstance(value, dict) and value.get("type") == "CHART":
                    chart_id = value.get("meta", {}).get("chartId")
                    if chart_id:
                        dashboard_chart_ids.add(chart_id)

        # 收集需要关联的图表信息（包含 position）
        charts_to_add = []

        # 处理每个图表配置
        for chart_config in charts_config:
            chart_title = chart_config.get("title")
            existing_chart_id = existing_charts_map.get(chart_title)

            # 验证 existing_chart_id 是否有效（图表是否真实存在）
            chart_exists = False
            if existing_chart_id:
                # 检查图表 ID 是否在当前面板的图表列表中，或通过 API 验证
                chart_exists = self._verify_chart_exists(existing_chart_id, dashboard_chart_ids)

            # 获取数据集，优先使用指定的 schema
            model_name = chart_config.get("model")
            if schema:
                dataset = self.client.get_or_create_dataset(model_name, schema=schema)
            else:
                dataset = self.client.get_dataset_by_name(model_name)

            if not dataset:
                logger.warning(f"数据集未找到: {model_name}")
                continue

            dataset_id = dataset.get("id")

            # 确定viz_type
            chart_type = chart_config.get("type", "line")
            viz_type = chart_config.get("viz_type")

            if not viz_type:
                # 从类型映射获取
                from .mapper import VIZ_TYPE_MAP
                viz_type = VIZ_TYPE_MAP.get(chart_type, "echarts_timeseries_line")

            # 构建图表参数
            params = self._build_chart_params(chart_config, dataset)

            # 获取 position 配置
            position = chart_config.get("position", {})

            chart_id = None
            if chart_exists and existing_chart_id:
                # 更新现有图表
                update_success = self._update_chart(existing_chart_id, chart_title, params, viz_type, dashboard_id, owner_id)
                if update_success:
                    chart_id = existing_chart_id
                else:
                    # 更新失败，创建新图表
                    logger.warning(f"图表更新失败，尝试创建新图表: {chart_title}")
                    chart_id = self._create_chart(
                        dataset_id, chart_title, params, viz_type, dashboard_id
                    )
                    if chart_id:
                        existing_charts_map[chart_title] = chart_id
            else:
                # 创建新图表
                chart_id = self._create_chart(
                    dataset_id, chart_title, params, viz_type, dashboard_id
                )
                if chart_id:
                    existing_charts_map[chart_title] = chart_id

            if chart_id:
                charts_to_add.append({
                    "id": chart_id,
                    "title": chart_title,
                    "viz_type": viz_type,
                    "position": position,
                })

        # 更新面板，关联所有图表
        if charts_to_add:
            # 传递完整的 exposure_meta 以支持 layout 配置
            self._update_dashboard_charts(dashboard_id, charts_to_add, exposure_meta)

            # 更新exposure文件，保存图表ID映射
            exposure_meta["existing_charts"] = existing_charts_map
            self._update_exposure_file(exposure.get("name"), exposure)

    def _validate_table_charts(self, charts_config: List[Dict[str, Any]]) -> List[str]:
        """验证所有图表配置

        基于 Superset 6.0 API 规则验证图表参数

        Args:
            charts_config: chart 配置列表

        Returns:
            错误消息列表，空列表表示验证通过
        """
        from .chart_rules import validate_chart_config

        errors = []

        for chart_config in charts_config:
            chart_title = chart_config.get("title", "未命名图表")
            chart_type = chart_config.get("type", "line")

            # 使用规则验证
            chart_errors = validate_chart_config(chart_type, chart_config)
            for err in chart_errors:
                errors.append(f"Chart '{chart_title}': {err}")

        return errors

    def _verify_chart_exists(self, chart_id: int, dashboard_chart_ids: set = None) -> bool:
        """验证图表是否存在于 Superset

        Args:
            chart_id: 图表ID
            dashboard_chart_ids: 当前面板已有的图表ID集合（可选）

        Returns:
            图表是否存在
        """
        # 首先检查是否在面板图表列表中
        if dashboard_chart_ids and chart_id in dashboard_chart_ids:
            return True

        # 通过 API 验证图表是否存在
        try:
            result = self.client.get_chart(chart_id)
            return result is not None
        except Exception:
            return False

    def _build_chart_params(self, chart_config: Dict[str, Any], dataset: Dict[str, Any]) -> Dict[str, Any]:
        """构建图表参数 - 使用Superset 6.0的正确格式

        根据 dbt 定义的 model metrics 生成 Superset chart params
        """
        dataset_id = dataset.get("id")
        chart_type = chart_config.get("type", "line")

        # 从 mapper 获取 viz_type 映射
        from .mapper import VIZ_TYPE_MAP
        viz_type = VIZ_TYPE_MAP.get(chart_type, "echarts_timeseries_line")

        # 基础参数
        params = {
            "datasource": f"{dataset_id}__table",
            "viz_type": viz_type,
        }

        # 获取数据集的列信息
        columns_info = self._build_columns_info(dataset)

        # 获取数据集已保存的 metrics
        saved_metrics = {m.get("metric_name"): m for m in dataset.get("metrics", [])}

        # 构建度量 metrics
        metrics = chart_config.get("metrics", [])
        if metrics:
            params["metrics"] = self._build_metrics(metrics, chart_config, columns_info, saved_metrics)

        # 构建分组维度
        dimensions = chart_config.get("dimensions", [])
        if dimensions:
            params["groupby"] = dimensions

        # 处理时间列配置
        time_column = chart_config.get("time_column")
        if time_column:
            params = self._add_time_config(params, time_column, chart_config)

        # 根据图表类型添加特定配置
        params = self._add_chart_type_config(params, chart_type, chart_config)

        # 合并额外的参数
        params.update(chart_config.get("extra_params", {}))

        return params

    def _build_columns_info(self, dataset: Dict[str, Any]) -> Dict[str, Any]:
        """构建列信息映射"""
        columns_info = {}
        for col in dataset.get("columns", []):
            col_name = col.get("column_name")
            if col_name:
                columns_info[col_name] = {
                    "column_name": col_name,
                    "type": col.get("type", "VARCHAR"),
                    "id": col.get("id"),
                    "expressionType": "SIMPLE",
                    "filterable": True,
                    "groupby": True,
                }
        return columns_info

    def _build_metrics(
        self,
        metric_refs: List[str],
        chart_config: Dict[str, Any],
        columns_info: Dict[str, Any],
        saved_metrics: Dict[str, Any] = None,
    ) -> List[Dict[str, Any]]:
        """根据 dbt metrics 定义构建 Superset 度量配置

        Args:
            metric_refs: metric 名称列表
            chart_config: 图表配置
            columns_info: 数据集列信息
            saved_metrics: 数据集已保存的 metrics {metric_name: metric_config}
        """
        if saved_metrics is None:
            saved_metrics = {}

        formatted_metrics = []
        seen_metrics = set()  # 用于去重
        model_name = chart_config.get("model", "")

        for metric_ref in metric_refs:
            metric_name = metric_ref

            # 去重检查：跳过已处理的 metric
            if metric_name in seen_metrics:
                logger.warning(f"跳过重复的 metric: {metric_name}")
                continue
            seen_metrics.add(metric_name)

            # 优先使用数据集已保存的 metric - 直接使用 metric 名称
            if metric_name in saved_metrics:
                # 对于已保存的 metric，直接使用名称字符串
                formatted_metrics.append(metric_name)
                continue

            # 从模型配置获取度量信息
            aggregate = "SUM"
            col_name = None

            if model_name in self.mapper.models:
                model_meta = self.mapper.models[model_name]
                for col_name_in_model, col_meta in model_meta.columns.items():
                    if col_meta.metrics and metric_name in col_meta.metrics:
                        metric_config = col_meta.metrics[metric_name]

                        # 映射聚合类型
                        from .mapper import METRIC_TYPE_TO_SUPERSET_AGG
                        aggregate = METRIC_TYPE_TO_SUPERSET_AGG.get(
                            metric_config.type, "SUM"
                        )

                        # 获取列名
                        if metric_config.sql:
                            col_name = self._extract_column_from_sql(metric_config.sql)
                        else:
                            col_name = col_name_in_model
                        break

            # 如果没有找到列名，尝试推断
            if not col_name:
                col_name = self._infer_column_for_metric(columns_info, metric_name)

            # 构建度量对象
            if col_name and col_name in columns_info:
                formatted_metrics.append({
                    "expressionType": "SIMPLE",
                    "column": columns_info[col_name],
                    "aggregate": aggregate,
                    "label": metric_name,
                })
            elif col_name:
                # 列不在 dataset 中，但仍创建度量（可能需要后续同步）
                formatted_metrics.append({
                    "expressionType": "SIMPLE",
                    "column": {"column_name": col_name},
                    "aggregate": aggregate,
                    "label": metric_name,
                })
            else:
                # 无法推断，使用默认值
                logger.warning(f"无法找到 metric '{metric_name}' 的列定义，使用默认配置")
                formatted_metrics.append(metric_name)

        return formatted_metrics

    def _extract_column_from_sql(self, sql: str) -> Optional[str]:
        """从 SQL 表达式中提取列名

        例如: "SUM(sales)" -> "sales"
              "COUNT(DISTINCT orderid)" -> "orderid"
        """
        import re
        # 匹配函数内的列名
        match = re.search(r'\(\s*(?:DISTINCT\s+)?([a-zA-Z_][a-zA-Z0-9_]*)\s*\)', sql, re.IGNORECASE)
        if match:
            return match.group(1)
        return None

    def _validate_metric_aggregation(self, sql_expr: str) -> bool:
        """验证 SQL 表达式是否包含有效的聚合函数

        Args:
            sql_expr: SQL 表达式

        Returns:
            是否包含有效的聚合函数
        """
        import re
        # 支持的聚合函数列表
        aggregation_functions = [
            'SUM', 'COUNT', 'AVG', 'MIN', 'MAX',
            'COUNT_DISTINCT',  # Superset 特有的去重计数
            'STDDEV', 'STDDEV_SAMP', 'STDDEV_POP',
            'VARIANCE', 'VAR_SAMP', 'VAR_POP',
            'COVAR_POP', 'COVAR_SAMP', 'CORR',
            'PERCENTILE_CONT', 'PERCENTILE_DISC',
            'APPROXIMATE_COUNT_DISTINCT', 'APPROX_DISTINCT',
        ]

        # 构建正则表达式匹配聚合函数
        pattern = r'\b(' + '|'.join(aggregation_functions) + r')\s*\('

        match = re.search(pattern, sql_expr, re.IGNORECASE)
        return match is not None

    def _infer_column_for_metric(self, columns_info: Dict[str, Any], metric_name: str) -> Optional[str]:
        """根据度量名称推断对应的列名"""
        # 常见的后缀模式（按优先级排序）
        suffixes = ["_sum", "_avg", "_count", "_min", "_max", "_total"]

        for suffix in suffixes:
            if metric_name.endswith(suffix):
                potential_col = metric_name[:-len(suffix)]
                if potential_col in columns_info:
                    return potential_col

        # 尝试前缀模式：total_sales -> sales, avg_quantity -> quantity
        prefixes = ["total_", "sum_", "avg_", "count_", "min_", "max_"]
        for prefix in prefixes:
            if metric_name.startswith(prefix):
                potential_col = metric_name[len(prefix):]
                if potential_col in columns_info:
                    return potential_col

        # 使用第一个数值类型的列
        for col_name, col_info in columns_info.items():
            col_type = col_info.get("type", "")
            if any(t in col_type.upper() for t in ["NUMERIC", "INT", "FLOAT", "DOUBLE", "DECIMAL"]):
                return col_name

        return None

    def _add_time_config(
        self,
        params: Dict[str, Any],
        time_column: str,
        chart_config: Dict[str, Any]
    ) -> Dict[str, Any]:
        """添加时间配置"""
        time_grain = chart_config.get("time_grain", "month")

        params["granularity_sqla"] = time_column
        params["time_grain_sqla"] = time_grain
        params["x_axis"] = time_column

        # 添加时间范围过滤器
        time_range = chart_config.get("time_range", "No filter")
        params["adhoc_filters"] = [{
            "clause": "WHERE",
            "comparator": time_range,
            "expressionType": "SIMPLE",
            "operator": "TEMPORAL_RANGE",
            "subject": time_column
        }]

        return params

    def _add_chart_type_config(
        self,
        params: Dict[str, Any],
        chart_type: str,
        chart_config: Dict[str, Any]
    ) -> Dict[str, Any]:
        """根据图表类型添加特定配置"""
        # big_number, big_number_total, number 都是数字卡片类型
        if chart_type in ("big_number", "number", "big_number_total"):
            # 确保移除任何时间相关配置
            params.pop("granularity_sqla", None)
            params.pop("time_grain_sqla", None)
            params.pop("x_axis", None)
            params.pop("adhoc_filters", None)
            params.pop("time_range", None)

            # 设置 big_number_total 特定配置
            metrics = chart_config.get("metrics", [])
            if metrics:
                # big_number_total 使用 metric (字符串) 而不是 metrics (数组)
                params["metric"] = metrics[0]
                # 同时移除 metrics，因为 big_number_total 不使用它
                params.pop("metrics", None)

            # subheader 映射到 subtitle
            extra_label = chart_config.get("extra_label")
            if extra_label:
                params["subtitle"] = extra_label

            # 添加 big_number_total 样式配置
            params.setdefault("header_font_size", 0.4)
            params.setdefault("subtitle_font_size", 0.15)
            params.setdefault("metric_name_font_size", 0.15)
            params.setdefault("y_axis_format", "SMART_NUMBER")
            params.setdefault("time_format", "smart_date")
            params.setdefault("show_trend_line", False)
            params.setdefault("start_y_axis_at_zero", True)

        elif chart_type == "line":
            # 折线图配置 - 补全官方 API 所需字段
            params.setdefault("row_limit", 10000)
            params.setdefault("order_desc", True)
            params.setdefault("truncate_metric", True)
            params.setdefault("show_empty_columns", True)
            params.setdefault("comparison_type", "values")
            params.setdefault("annotation_layers", [])
            params.setdefault("x_axis_sort_asc", True)
            params.setdefault("sort_series_type", "sum")
            params.setdefault("color_scheme", "SUPERSET_DEFAULT")
            params.setdefault("time_shift_color", True)
            params.setdefault("only_total", True)
            params.setdefault("opacity", 0.2)
            params.setdefault("markerSize", 6)
            params.setdefault("show_markers", True)
            params.setdefault("show_legend", True)
            params.setdefault("legendType", "scroll")
            params.setdefault("legendOrientation", "top")
            params.setdefault("line_interpolation", "linear")
            params.setdefault("x_axis_time_format", "smart_date")
            params.setdefault("xAxisLabelInterval", "auto")
            params.setdefault("rich_tooltip", True)
            params.setdefault("showTooltipTotal", True)
            params.setdefault("tooltipTimeFormat", "smart_date")
            params.setdefault("y_axis_format", "SMART_NUMBER")
            params.setdefault("truncateXAxis", True)
            params.setdefault("y_axis_bounds", [None, None])
            params.setdefault("time_range", "No filter")

        elif chart_type == "bar":
            # 柱状图配置
            params.setdefault("show_legend", True)
            params.setdefault("bar_stacked", False)

        elif chart_type == "pie" or chart_type == "doughnut":
            # 饼图/环形图配置
            # 注意：pie 使用 metric (单数字符串)，不是 metrics (数组)
            metrics = chart_config.get("metrics", [])
            if metrics:
                params["metric"] = metrics[0]
                params.pop("metrics", None)  # 移除 metrics 数组

            # 设置 groupby（维度）
            dimensions = chart_config.get("dimensions", [])
            if dimensions:
                params["groupby"] = dimensions

            # 设置时间过滤器
            time_column = chart_config.get("time_column")
            if time_column:
                params["adhoc_filters"] = [{
                    "clause": "WHERE",
                    "subject": time_column,
                    "operator": "TEMPORAL_RANGE",
                    "comparator": "No filter",
                    "expressionType": "SIMPLE"
                }]

            # 饼图特定配置
            params.setdefault("row_limit", 100)
            params.setdefault("sort_by_metric", True)
            params.setdefault("show_legend", True)
            params.setdefault("label_type", "key")
            params.setdefault("color_scheme", "SUPERSET_DEFAULT")
            params.setdefault("show_labels", True)
            params.setdefault("show_labels_threshold", 5)

            # 移除时间配置（饼图不需要）
            params.pop("granularity_sqla", None)
            params.pop("time_grain_sqla", None)
            params.pop("x_axis", None)

        elif chart_type == "table":
            # 表格配置 - Superset 要求 query_mode 必须为 raw
            params["query_mode"] = "raw"

            # 设置 all_columns (必须指定)
            columns = chart_config.get("columns", [])
            if columns:
                params["all_columns"] = columns

            # 设置时间过滤器 (必须指定)
            time_column = chart_config.get("time_column")
            if time_column:
                params["adhoc_filters"] = [{
                    "clause": "WHERE",
                    "subject": time_column,
                    "operator": "TEMPORAL_RANGE",
                    "comparator": "No filter",
                    "expressionType": "SIMPLE"
                }]
            else:
                # 默认时间过滤器 - 使用 "No filter" 表示不过滤
                params["adhoc_filters"] = [{
                    "clause": "WHERE",
                    "subject": "report_date",
                    "operator": "TEMPORAL_RANGE",
                    "comparator": "No filter",
                    "expressionType": "SIMPLE"
                }]

            # 表格特定配置
            params.setdefault("server_pagination", True)
            params.setdefault("server_page_length", 10)
            params.setdefault("order_by_cols", [])
            params.setdefault("order_desc", True)
            params.setdefault("table_timestamp_format", "smart_date")
            params.setdefault("allow_render_html", True)
            params.setdefault("show_cell_bars", True)
            params.setdefault("percent_metrics", [])
            params.setdefault("color_pn", True)
            params.setdefault("include_search", True)
            params.setdefault("table_cell_font_size", 13)

            # 移除时间配置
            params.pop("granularity_sqla", None)
            params.pop("time_grain_sqla", None)
            params.pop("x_axis", None)

        return params

    def _create_chart(
        self,
        dataset_id: int,
        title: str,
        params: Dict[str, Any],
        viz_type: str,
        dashboard_id: int,
    ) -> Optional[int]:
        """创建图表"""
        # 将params序列化为JSON字符串
        params_json = json.dumps(params, ensure_ascii=False)

        result = self.client.create_chart(
            datasource_id=dataset_id,
            viz_type=viz_type,
            title=title,
            params=params_json,
            dashboard_id=dashboard_id,  # 关联到 dashboard
        )

        if result:
            chart_id = result.get("id")
            logger.info(f"创建图表: {title} (ID: {chart_id}) 并关联到面板 {dashboard_id}")
            return chart_id

        return None

    def _update_chart(
        self,
        chart_id: int,
        title: str,
        params: Dict[str, Any],
        viz_type: str,
        dashboard_id: int = None,
        owner_id: int = None,
    ) -> bool:
        """更新图表

        Returns:
            是否更新成功
        """
        # 直接传递 params 字典，让 update_chart 方法处理序列化
        result = self.client.update_chart(
            chart_id=chart_id,
            title=title,
            params=params,
            dashboard_id=dashboard_id,
            owner_id=owner_id,
        )

        if result:
            logger.info(f"更新图表: {title} (ID: {chart_id})")
            return True
        else:
            logger.warning(f"更新图表失败: ID {chart_id}")
            return False

    def _update_dashboard_charts(
        self,
        dashboard_id: int,
        charts_to_add: List[Dict[str, Any]],
        charts_config: List[Dict[str, Any]] = None,
    ) -> None:
        """更新面板关联的图表列表并构建 position_json 布局

        从 exposures.yml 的 meta.layout 配置读取布局结构，生成 Superset 6.0 标准布局

        Args:
            dashboard_id: 面板ID
            charts_to_add: 图表列表，每个包含 id, title, viz_type, position
            charts_config: exposures.yml 中的 charts 配置
        """
        # 获取现有面板信息
        dashboard = self.client.get_dashboard(dashboard_id)
        if not dashboard:
            logger.warning(f"无法获取面板: {dashboard_id}")
            return

        dashboard_title = dashboard.get("dashboard_title", "Dashboard")

        # 构建图表标题到 ID 的映射
        chart_title_to_id = {c.get("title"): c.get("id") for c in charts_to_add if c.get("id")}

        # 尝试从 charts_config 获取 layout 配置
        layout_config = None
        if charts_config:
            # charts_config 就是 exposure meta
            layout_config = charts_config.get("layout") if isinstance(charts_config, dict) else None

        # 如果有 layout 配置，使用 layout 构建 position_json
        if layout_config:
            position_json = self._build_position_json_from_layout(
                layout_config, chart_title_to_id, dashboard_title
            )
        else:
            # 兼容旧版本：使用默认布局
            position_json = self._build_default_position_json(charts_to_add, dashboard_title)

        # 更新面板
        result = self.client.update_dashboard(dashboard_id, positions=position_json)

        if result:
            logger.info(f"图表已关联到面板 ID {dashboard_id}, 图表数量: {len(charts_to_add)}")
        else:
            logger.warning(f"面板布局更新失败，但图表已创建")

    def _build_position_json_from_layout(
        self,
        layout_config: List[Dict[str, Any]],
        chart_title_to_id: Dict[str, int],
        dashboard_title: str
    ) -> Dict[str, Any]:
        """从 exposures.yml 的 layout 配置构建 position_json

        支持的布局组件:
        - header: 标题组件
        - row: 行容器
        - column: 列容器
        - chart: 图表组件
        - markdown: Markdown 文本组件
        - divider: 分隔线组件

        Args:
            layout_config: layout 配置列表
            chart_title_to_id: 图表标题到 ID 的映射
            dashboard_title: Dashboard 标题

        Returns:
            Superset 6.0 标准的 position_json
        """
        import uuid

        position_json = {
            "DASHBOARD_VERSION_KEY": "v2",
            "ROOT_ID": {
                "children": ["GRID_ID"],
                "id": "ROOT_ID",
                "type": "ROOT"
            },
            "GRID_ID": {
                "children": [],
                "id": "GRID_ID",
                "parents": ["ROOT_ID"],
                "type": "GRID"
            }
        }

        grid_children = []

        for item in layout_config:
            item_type = item.get("type")

            if item_type == "header":
                # Header 组件
                position_json["HEADER_ID"] = {
                    "id": "HEADER_ID",
                    "meta": {
                        "text": item.get("text", dashboard_title)
                    },
                    "type": "HEADER"
                }

            elif item_type == "row":
                # Row 组件
                row_id = f"ROW-{uuid.uuid4().hex[:20]}"
                row_children = []
                row_background = item.get("background", "transparent").upper()
                if not row_background.startswith("BACKGROUND_"):
                    row_background = f"BACKGROUND_{row_background}"

                for child in item.get("children", []):
                    child_type = child.get("type")

                    if child_type == "chart":
                        chart_ref = child.get("ref")
                        chart_id = chart_title_to_id.get(chart_ref)

                        if chart_id:
                            component_id = f"CHART-X-{uuid.uuid4().hex[:20]}"
                            chart_uuid = str(uuid.uuid4())

                            width = child.get("width", 4)
                            height = child.get("height", 50)

                            position_json[component_id] = {
                                "children": [],
                                "id": component_id,
                                "meta": {
                                    "chartId": chart_id,
                                    "height": height,
                                    "sliceName": chart_ref,
                                    "uuid": chart_uuid,
                                    "width": width
                                },
                                "parents": ["ROOT_ID", "GRID_ID", row_id],
                                "type": "CHART"
                            }
                            row_children.append(component_id)
                        else:
                            logger.warning(f"图表 '{chart_ref}' 未找到对应的 ID")

                    elif child_type == "markdown":
                        # Markdown 组件
                        md_id = f"MARKDOWN-{uuid.uuid4().hex[:20]}"
                        md_content = child.get("content", "")

                        position_json[md_id] = {
                            "children": [],
                            "id": md_id,
                            "meta": {
                                "code": md_content,
                                "height": child.get("height", 50),
                                "width": child.get("width", 4)
                            },
                            "parents": ["ROOT_ID", "GRID_ID", row_id],
                            "type": "MARKDOWN"
                        }
                        row_children.append(md_id)

                    elif child_type == "divider":
                        # Divider 组件
                        divider_id = f"DIVIDER-{uuid.uuid4().hex[:20]}"

                        position_json[divider_id] = {
                            "children": [],
                            "id": divider_id,
                            "meta": {
                                "height": child.get("height", 10),
                                "width": child.get("width", 12)
                            },
                            "parents": ["ROOT_ID", "GRID_ID", row_id],
                            "type": "DIVIDER"
                        }
                        row_children.append(divider_id)

                    elif child_type == "column":
                        # Column 组件（嵌套布局）
                        col_id = f"COLUMN-{uuid.uuid4().hex[:20]}"
                        col_children = []

                        for col_child in child.get("children", []):
                            if col_child.get("type") == "chart":
                                chart_ref = col_child.get("ref")
                                chart_id = chart_title_to_id.get(chart_ref)

                                if chart_id:
                                    component_id = f"CHART-X-{uuid.uuid4().hex[:20]}"
                                    chart_uuid = str(uuid.uuid4())

                                    position_json[component_id] = {
                                        "children": [],
                                        "id": component_id,
                                        "meta": {
                                            "chartId": chart_id,
                                            "height": col_child.get("height", 50),
                                            "sliceName": chart_ref,
                                            "uuid": chart_uuid,
                                            "width": col_child.get("width", 4)
                                        },
                                        "parents": ["ROOT_ID", "GRID_ID", row_id, col_id],
                                        "type": "CHART"
                                    }
                                    col_children.append(component_id)

                        position_json[col_id] = {
                            "children": col_children,
                            "id": col_id,
                            "meta": {
                                "background": "BACKGROUND_TRANSPARENT",
                                "width": child.get("width", 6)
                            },
                            "parents": ["ROOT_ID", "GRID_ID", row_id],
                            "type": "COLUMN"
                        }
                        row_children.append(col_id)

                position_json[row_id] = {
                    "children": row_children,
                    "id": row_id,
                    "meta": {
                        "background": row_background
                    },
                    "type": "ROW"
                }
                grid_children.append(row_id)

            elif item_type == "divider":
                # 顶层 Divider
                divider_id = f"DIVIDER-{uuid.uuid4().hex[:20]}"

                position_json[divider_id] = {
                    "children": [],
                    "id": divider_id,
                    "meta": {
                        "height": item.get("height", 10),
                        "width": item.get("width", 12)
                    },
                    "parents": ["ROOT_ID", "GRID_ID"],
                    "type": "DIVIDER"
                }
                grid_children.append(divider_id)

            elif item_type == "markdown":
                # 顶层 Markdown
                md_id = f"MARKDOWN-{uuid.uuid4().hex[:20]}"

                position_json[md_id] = {
                    "children": [],
                    "id": md_id,
                    "meta": {
                        "code": item.get("content", ""),
                        "height": item.get("height", 50),
                        "width": item.get("width", 12)
                    },
                    "parents": ["ROOT_ID", "GRID_ID"],
                    "type": "MARKDOWN"
                }
                grid_children.append(md_id)

        # 设置 GRID_ID 的 children
        position_json["GRID_ID"]["children"] = grid_children

        return position_json

    def _build_default_position_json(
        self,
        charts_to_add: List[Dict[str, Any]],
        dashboard_title: str
    ) -> Dict[str, Any]:
        """构建默认的 position_json（兼容旧版本）"""
        import uuid

        row_id = f"ROW-{uuid.uuid4().hex[:20]}"

        position_json = {
            "DASHBOARD_VERSION_KEY": "v2",
            "ROOT_ID": {
                "children": ["GRID_ID"],
                "id": "ROOT_ID",
                "type": "ROOT"
            },
            "GRID_ID": {
                "children": [row_id],
                "id": "GRID_ID",
                "parents": ["ROOT_ID"],
                "type": "GRID"
            },
            "HEADER_ID": {
                "id": "HEADER_ID",
                "meta": {
                    "text": dashboard_title
                },
                "type": "HEADER"
            },
            row_id: {
                "children": [],
                "id": row_id,
                "meta": {
                    "background": "BACKGROUND_TRANSPARENT"
                },
                "type": "ROW"
            }
        }

        for chart in charts_to_add:
            chart_id = chart.get("id")
            chart_title = chart.get("title", "")

            component_id = f"CHART-X-{uuid.uuid4().hex[:20]}"
            chart_uuid = str(uuid.uuid4())

            position_json[component_id] = {
                "children": [],
                "id": component_id,
                "meta": {
                    "chartId": chart_id,
                    "height": 50,
                    "sliceName": chart_title,
                    "uuid": chart_uuid,
                    "width": 4
                },
                "parents": ["ROOT_ID", "GRID_ID", row_id],
                "type": "CHART"
            }
            position_json[row_id]["children"].append(component_id)

        return position_json

    def _update_exposure_file(self, exposure_name: str, exposure: Dict[str, Any]) -> None:
        """更新exposure文件

        支持两种文件结构：
        1. 单独文件: {exposure_name}.yml
        2. 合并文件: exposures.yml 或其他包含多个 exposure 的文件
        """
        exposure_paths = self.dbt_config.full_exposure_paths

        for exposure_path in exposure_paths:
            # 方式1: 尝试单独的 exposure 文件
            file_path = exposure_path / f"{exposure_name}.yml"
            if file_path.exists():
                if self._write_exposure_to_file(file_path, exposure_name, exposure):
                    return

            # 方式2: 遍历目录下所有 yml 文件，查找包含该 exposure 的文件
            for pattern in ["*.yml", "*.yaml"]:
                for yml_file in exposure_path.glob(pattern):
                    if yml_file.is_file():
                        # 跳过已经尝试过的单独文件
                        if yml_file.name == f"{exposure_name}.yml":
                            continue
                        if self._write_exposure_to_file(yml_file, exposure_name, exposure):
                            return

        logger.warning(f"未找到exposure文件: {exposure_name}")

    def _write_exposure_to_file(self, file_path: Path, exposure_name: str, exposure: Dict[str, Any]) -> bool:
        """将 exposure 写入指定文件

        Args:
            file_path: 文件路径
            exposure_name: exposure 名称
            exposure: exposure 数据

        Returns:
            是否成功写入
        """
        try:
            with open(file_path, "r", encoding="utf-8") as f:
                data = yaml.safe_load(f) or {}

            exposures = data.get("exposures", [])

            # 查找并更新对应的 exposure
            found = False
            for i, exp in enumerate(exposures):
                if exp.get("name") == exposure_name:
                    exposures[i] = exposure
                    found = True
                    break

            if not found:
                return False

            data["exposures"] = exposures

            # 写回文件
            with open(file_path, "w", encoding="utf-8") as f:
                yaml.dump(data, f, default_flow_style=False, allow_unicode=True, sort_keys=False)

            logger.info(f"更新exposure文件: {file_path}")
            return True

        except Exception as e:
            logger.error(f"更新exposure文件失败 {file_path}: {e}")
            return False

    def _sync_dataset_metrics(self, model_names: List[str] = None, schema: str = None) -> None:
        """同步 dbt model 中定义的列描述和指标到 Superset 数据集

        根据 dbt model 的 meta.metrics 定义，更新 Superset 数据集的 metrics 字段
        根据 dbt model 的 columns.description，更新 Superset 数据集的 columns 字段
        如果数据集不存在，则自动创建

        Args:
            model_names: 要同步的模型名称列表，None表示同步所有
            schema: 指定数据集的 schema 名称（优先级最高）
        """
        from .mapper import METRIC_TYPE_TO_SUPERSET_AGG

        logger.info("开始同步数据集列描述和指标...")

        for model_name, model_meta in self.mapper.models.items():
            # 如果指定了模型名称，只同步指定的模型
            if model_names and model_name not in model_names:
                continue

            # 获取或创建 Superset 数据集，优先使用指定的 schema
            if schema:
                dataset = self.client.get_or_create_dataset(model_name, schema=schema)
            else:
                dataset = self.client.get_or_create_dataset(model_name)

            if not dataset:
                logger.warning(f"无法获取或创建数据集: {model_name}，跳过指标同步")
                continue

            dataset_id = dataset.get("id")
            existing_metrics = {m.get("metric_name"): m for m in dataset.get("metrics", [])}
            existing_columns = {c.get("column_name"): c for c in dataset.get("columns", [])}

            # 检查数据集是否有列信息
            if not existing_columns:
                logger.warning(
                    f"数据集 {model_name} (ID: {dataset_id}) 没有列信息。"
                    f"可能原因：(1) 数据库表不存在 (2) schema 配置错误 (3) Superset 列同步失败。"
                    f"跳过列描述同步，仅同步 metrics。"
                )

            # 构建新的指标列表
            new_metrics = []
            metrics_to_add = []

            # 保留现有的默认 count 指标（只保留必要字段）
            if "count" in existing_metrics:
                count_m = existing_metrics["count"]
                new_metrics.append({
                    "id": count_m.get("id"),
                    "metric_name": count_m.get("metric_name"),
                    "verbose_name": count_m.get("verbose_name"),
                    "expression": count_m.get("expression"),
                })

            # 从 model meta 中提取指标定义
            for col_name, col_meta in model_meta.columns.items():
                if col_meta.metrics:
                    for metric_name, metric_config in col_meta.metrics.items():
                        # 获取聚合类型
                        agg_func = METRIC_TYPE_TO_SUPERSET_AGG.get(
                            metric_config.type, "SUM"
                        )

                        # 构建 SQL 表达式
                        if metric_config.sql:
                            raw_sql = metric_config.sql.strip()
                            # 检查是否已包含聚合函数
                            if self._validate_metric_aggregation(raw_sql):
                                sql_expr = raw_sql
                            else:
                                # sql 字段只是列名，自动添加聚合函数
                                sql_expr = f"{agg_func}({raw_sql})"
                        elif agg_func == "COUNT_DISTINCT":
                            # COUNT_DISTINCT 需要转换为标准 SQL: COUNT(DISTINCT column)
                            sql_expr = f"COUNT(DISTINCT {col_name})"
                        else:
                            sql_expr = f"{agg_func}({col_name})"

                        # 构建指标对象
                        metric_obj = {
                            "metric_name": metric_name,
                            "verbose_name": metric_config.description or metric_name,
                            "expression": sql_expr,
                            "description": metric_config.description or "",
                        }

                        # 如果指标已存在，保留其 ID
                        if metric_name in existing_metrics:
                            metric_obj["id"] = existing_metrics[metric_name].get("id")
                            metric_obj["uuid"] = existing_metrics[metric_name].get("uuid")

                        metrics_to_add.append(metric_obj)

            # 处理表级 metrics (meta.metrics)
            if model_meta.metrics:
                for metric_name, metric_config in model_meta.metrics.items():
                    # 构建 SQL 表达式
                    if metric_config.sql:
                        raw_sql = metric_config.sql.strip()
                        # 表级 metrics 的 sql 通常已包含聚合函数
                        sql_expr = raw_sql
                    else:
                        logger.warning(f"表级 metric {metric_name} 缺少 sql 表达式，跳过")
                        continue

                    # 构建指标对象
                    metric_obj = {
                        "metric_name": metric_name,
                        "verbose_name": metric_config.description or metric_name,
                        "expression": sql_expr,
                        "description": metric_config.description or "",
                    }

                    # 如果指标已存在，保留其 ID
                    if metric_name in existing_metrics:
                        metric_obj["id"] = existing_metrics[metric_name].get("id")
                        metric_obj["uuid"] = existing_metrics[metric_name].get("uuid")

                    metrics_to_add.append(metric_obj)

            # 更新数据集指标
            if metrics_to_add:
                new_metrics.extend(metrics_to_add)

                # 调用 API 更新数据集指标和列描述
                success = self._update_dataset_with_metrics_and_columns(dataset_id, new_metrics, model_meta, existing_columns)

                if success:
                    logger.info(f"数据集 {model_name} (ID: {dataset_id}) 同步了 {len(metrics_to_add)} 个指标和列描述")
                else:
                    logger.error(f"数据集 {model_name} (ID: {dataset_id}) 同步失败")

    def _update_dataset_metrics(self, dataset_id: int, metrics: List[Dict[str, Any]]) -> bool:
        """更新数据集的指标定义

        Args:
            dataset_id: 数据集 ID
            metrics: 指标列表

        Returns:
            是否更新成功
        """
        import json
        import base64

        # 获取 CSRF token
        csrf_resp = self.client._request("GET", "/api/v1/security/csrf_token/")
        if csrf_resp.status_code == 200:
            csrf_token = csrf_resp.json().get("result")
            if csrf_token:
                self.client.csrf_token = csrf_token

        # 清理指标对象，只保留必要字段，并去重
        clean_metrics = []
        seen_metric_names = set()
        for m in metrics:
            metric_name = m.get("metric_name")
            # 跳过重复的 metric_name
            if metric_name in seen_metric_names:
                logger.warning(f"跳过重复的 metric: {metric_name}")
                continue
            seen_metric_names.add(metric_name)

            clean_m = {
                "metric_name": metric_name,
                "verbose_name": m.get("verbose_name"),
                "expression": m.get("expression"),
            }
            # 可选字段
            if m.get("description"):
                clean_m["description"] = m.get("description")
            if m.get("id"):
                clean_m["id"] = m.get("id")
            if m.get("uuid"):
                clean_m["uuid"] = m.get("uuid")
            clean_metrics.append(clean_m)

        # 构建更新 payload
        payload = {
            "metrics": clean_metrics
        }

        response = self.client._request(
            "PUT",
            f"/api/v1/dataset/{dataset_id}",
            json=payload
        )

        if response.status_code == 200:
            logger.debug(f"更新数据集指标成功: {dataset_id}")
            return True
        else:
            logger.error(f"更新数据集指标失败: {response.status_code} - {response.text[:200]}")
            return False

    def _update_dataset_with_metrics_and_columns(
        self,
        dataset_id: int,
        metrics: List[Dict[str, Any]],
        model_meta: "ModelMeta",
        existing_columns: Dict[str, Dict[str, Any]]
    ) -> bool:
        """更新数据集的指标定义和列描述（支持计算列）

        Args:
            dataset_id: 数据集 ID
            metrics: 指标列表
            model_meta: dbt 模型元数据（包含列描述）
            existing_columns: 现有列信息

        Returns:
            是否更新成功
        """
        import json
        import base64

        # 获取 CSRF token
        csrf_resp = self.client._request("GET", "/api/v1/security/csrf_token/")
        if csrf_resp.status_code == 200:
            csrf_token = csrf_resp.json().get("result")
            if csrf_token:
                self.client.csrf_token = csrf_token

        # 清理指标对象，只保留必要字段，并去重
        clean_metrics = []
        seen_metric_names = set()
        for m in metrics:
            metric_name = m.get("metric_name")
            # 跳过重复的 metric_name
            if metric_name in seen_metric_names:
                logger.warning(f"跳过重复的 metric: {metric_name}")
                continue
            seen_metric_names.add(metric_name)

            clean_m = {
                "metric_name": metric_name,
                "verbose_name": m.get("verbose_name"),
                "expression": m.get("expression"),
            }
            # 可选字段
            if m.get("description"):
                clean_m["description"] = m.get("description")
            if m.get("id"):
                clean_m["id"] = m.get("id")
            if m.get("uuid"):
                clean_m["uuid"] = m.get("uuid")
            clean_metrics.append(clean_m)

        # 识别计算列
        calculated_columns = self.mapper.identify_calculated_columns(model_meta)
        calculated_names = {c["column_name"] for c in calculated_columns}

        # 构建 columns 信息（区分物理列和计算列）
        # 关键修复：只更新已存在的列，避免 422 "columns already exist" 错误
        clean_columns = []
        for col_name, col_meta in model_meta.columns.items():
            # 如果是计算列，则跳过（后续单独处理）
            if col_name in calculated_names:
                continue

            # 关键修复：只处理已存在于数据集中的列，避免尝试创建已存在的列
            if col_name not in existing_columns:
                logger.debug(f"列 '{col_name}' 在数据集中不存在，跳过同步")
                continue

            existing_col = existing_columns[col_name]
            # 跳过已有的计算列（expression 非空）
            if existing_col.get("expression"):
                continue

            # 必须包含 ID，否则 Superset 会尝试创建新列
            col_obj = {
                "id": existing_col.get("id"),
                "column_name": col_name,
            }

            # 保留现有的类型信息
            if "type" in existing_col:
                col_obj["type"] = existing_col["type"]
            if "is_dttm" in existing_col:
                col_obj["is_dttm"] = existing_col["is_dttm"]
            if "filterable" in existing_col:
                col_obj["filterable"] = existing_col["filterable"]

            # 使用列的 description 作为 verbose_name
            if col_meta.description:
                col_obj["description"] = col_meta.description
                col_obj["verbose_name"] = col_meta.description

            # 从 meta.dimension.label 获取（如果存在）
            if col_meta.dimensions and col_meta.dimensions.get("label"):
                col_obj["verbose_name"] = col_meta.dimensions["label"]

            clean_columns.append(col_obj)

        # 添加计算列
        for calc in calculated_columns:
            col_name = calc["column_name"]
            expression = calc["expression"]
            col_type = calc["type"]
            col_description = calc["description"]
            verbose_name = calc["verbose_name"]

            col_obj = {
                "column_name": col_name,
                "expression": expression,
                "type": col_type,
                "description": col_description,
                "verbose_name": verbose_name,
                "is_dttm": False,
                "filterable": True,
                "groupby": True,
                "is_active": True
            }

            # 保留现有 ID（如果存在）
            if col_name in existing_columns:
                existing_col = existing_columns[col_name]
                if existing_col.get("expression"):  # 这是一个现有计算列
                    col_obj["id"] = existing_col.get("id")

            clean_columns.append(col_obj)

        # 构建更新 payload（包含 description、metrics 和 columns）
        payload = {
            "description": model_meta.description,
            "metrics": clean_metrics,
            "columns": clean_columns
        }

        response = self.client._request(
            "PUT",
            f"/api/v1/dataset/{dataset_id}",
            json=payload
        )

        if response.status_code == 200:
            logger.debug(f"更新数据集指标、列描述和计算列成功: {dataset_id}")
            return True
        else:
            logger.error(f"更新数据集失败: {response.status_code} - {response.text[:200]}")
            return False