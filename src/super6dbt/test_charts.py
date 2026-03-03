#!/usr/bin/env python3
"""测试各图表类型的参数生成"""

import sys
sys.path.insert(0, '/Users/chen/IdeaProjects/demo-dbt')

from super6dbt.push import SupersetPusher
from super6dbt.mapper import DbtToSuperset, VIZ_TYPE_MAP
import json


def test_viz_type_map():
    """测试图表类型映射"""
    print("=" * 60)
    print("1. 图表类型映射 (VIZ_TYPE_MAP)")
    print("=" * 60)

    for chart_type, viz_type in VIZ_TYPE_MAP.items():
        print(f"  {chart_type:10} -> {viz_type}")

    print("\n✅ 图表类型映射检查完成\n")


def test_chart_params():
    """测试各图表类型的参数生成"""
    print("=" * 60)
    print("2. 各图表类型参数生成测试")
    print("=" * 60)

    # 模拟 dataset
    mock_dataset = {
        "id": 1,
        "table_name": "orders",
        "columns": [
            {"column_name": "orderdate", "type": "TIMESTAMP", "id": 1},
            {"column_name": "sales", "type": "NUMERIC", "id": 2},
            {"column_name": "quantity", "type": "INT", "id": 3},
            {"column_name": "productid", "type": "VARCHAR", "id": 4},
        ]
    }

    # 模拟 model meta
    mock_model_meta = {
        "orderdate": {
            "dimensions": {"type": "date", "time_intervals": ["DAY", "WEEK", "MONTH", "YEAR"]},
            "metrics": None
        },
        "sales": {
            "dimensions": None,
            "metrics": {
                "total_sales_sum": {"type": "sum", "sql": "sales", "description": "总销售额"},
                "avg_sales": {"type": "avg", "sql": "sales", "description": "平均销售额"}
            }
        },
        "quantity": {
            "dimensions": None,
            "metrics": {
                "total_quantity": {"type": "sum", "sql": "quantity", "description": "总数量"}
            }
        },
        "productid": {
            "dimensions": {"type": "string"},
            "metrics": {
                "product_count": {"type": "count_distinct", "sql": "productid", "description": "产品数量"}
            }
        }
    }

    # 测试配置
    test_cases = [
        {
            "name": "折线图 - 销售额趋势",
            "config": {
                "title": "销售额趋势",
                "type": "line",
                "model": "orders",
                "time_column": "orderdate",
                "time_grain": "month",
                "metrics": ["total_sales_sum"],
                "dimensions": ["orderdate"],
                "time_range": "last 12 months"
            }
        },
        {
            "name": "柱状图 - 产品销量",
            "config": {
                "title": "产品销量",
                "type": "bar",
                "model": "orders",
                "metrics": ["total_quantity"],
                "dimensions": ["productid"],
                "time_range": "No filter"
            }
        },
        {
            "name": "数字卡片 - 总销售额",
            "config": {
                "title": "总销售额",
                "type": "number",
                "model": "orders",
                "metrics": ["total_sales_sum", "total_quantity"],
                "time_range": "last 30 days"
            }
        },
        {
            "name": "饼图 - 产品分布",
            "config": {
                "title": "产品分布",
                "type": "pie",
                "model": "orders",
                "metrics": ["total_sales_sum"],
                "dimensions": ["productid"],
                "time_range": "No filter"
            }
        },
        {
            "name": "表格 - 订单明细",
            "config": {
                "title": "订单明细",
                "type": "table",
                "model": "orders",
                "metrics": ["total_sales_sum", "total_quantity"],
                "dimensions": ["productid", "orderdate"],
                "time_range": "No filter"
            }
        }
    ]

    # 创建模拟的 pusher
    from super6dbt.client import SupersetClient
    from super6dbt.config import DbtProjectConfig

    # 创建一个简单的测试类
    class TestPusher:
        def __init__(self):
            self.mapper = DbtToSuperset()
            # 添加模拟的 model
            from super6dbt.mapper import ColumnMeta, ModelMeta
            model = ModelMeta(name="orders")
            for col_name, col_config in mock_model_meta.items():
                col_meta = ColumnMeta(name=col_name)
                if col_config.get("dimensions"):
                    col_meta.dimensions = col_config["dimensions"]
                if col_config.get("metrics"):
                    col_meta.metrics = {}
                    for m_name, m_config in col_config["metrics"].items():
                        from super6dbt.mapper import MetricConfig
                        col_meta.metrics[m_name] = MetricConfig(
                            name=m_name,
                            type=m_config["type"],
                            description=m_config.get("description", ""),
                            sql=m_config.get("sql", "")
                        )
                model.columns[col_name] = col_meta
            self.mapper.models["orders"] = model

        def _build_columns_info(self, dataset):
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

        def _build_metrics(self, metric_refs, chart_config, columns_info):
            formatted_metrics = []
            model_name = chart_config.get("model", "")

            for metric_ref in metric_refs:
                metric_name = metric_ref
                aggregate = "SUM"
                col_name = None

                if model_name in self.mapper.models:
                    model_meta = self.mapper.models[model_name]
                    for col_name_in_model, col_meta in model_meta.columns.items():
                        if col_meta.metrics and metric_name in col_meta.metrics:
                            metric_config = col_meta.metrics[metric_name]
                            from super6dbt.mapper import METRIC_TYPE_TO_SUPERSET_AGG
                            aggregate = METRIC_TYPE_TO_SUPERSET_AGG.get(metric_config.type, "SUM")
                            if metric_config.sql:
                                col_name = self._extract_column_from_sql(metric_config.sql)
                            else:
                                col_name = col_name_in_model
                            break

                if not col_name:
                    col_name = self._infer_column_for_metric(columns_info, metric_name)

                if col_name and col_name in columns_info:
                    formatted_metrics.append({
                        "expressionType": "SIMPLE",
                        "column": columns_info[col_name],
                        "aggregate": aggregate,
                        "label": metric_name,
                    })

            return formatted_metrics

        def _extract_column_from_sql(self, sql):
            import re
            match = re.search(r'\(\s*(?:DISTINCT\s+)?([a-zA-Z_][a-zA-Z0-9_]*)\s*\)', sql, re.IGNORECASE)
            if match:
                return match.group(1)
            return None

        def _infer_column_for_metric(self, columns_info, metric_name):
            suffixes = ["_sum", "_avg", "_count", "_min", "_max", "_total"]
            for suffix in suffixes:
                if metric_name.endswith(suffix):
                    potential_col = metric_name[:-len(suffix)]
                    if potential_col in columns_info:
                        return potential_col
            for col_name, col_info in columns_info.items():
                col_type = col_info.get("type", "")
                if any(t in col_type.upper() for t in ["NUMERIC", "INT", "FLOAT", "DOUBLE", "DECIMAL"]):
                    return col_name
            return None

        def _add_time_config(self, params, time_column, chart_config):
            time_grain = chart_config.get("time_grain", "month")
            params["granularity_sqla"] = time_column
            params["time_grain_sqla"] = time_grain
            params["x_axis"] = time_column
            time_range = chart_config.get("time_range", "No filter")
            params["adhoc_filters"] = [{
                "clause": "WHERE",
                "comparator": time_range,
                "expressionType": "SIMPLE",
                "operator": "TEMPORAL_RANGE",
                "subject": time_column
            }]
            return params

        def _add_chart_type_config(self, params, chart_type, chart_config):
            if chart_type == "line":
                params.setdefault("show_markers", True)
                params.setdefault("show_legend", True)
                params.setdefault("line_interpolation", "linear")
            elif chart_type == "bar":
                params.setdefault("show_legend", True)
                params.setdefault("bar_stacked", False)
            elif chart_type == "number":
                params.setdefault("subheader", chart_config.get("description", ""))
            elif chart_type == "pie":
                params.setdefault("show_legend", True)
                params.setdefault("label_type", "key")
            elif chart_type == "table":
                params.setdefault("server_pagination", True)
                params.setdefault("order_by_cols", [])
            return params

        def build_chart_params(self, chart_config, dataset):
            dataset_id = dataset.get("id")
            chart_type = chart_config.get("type", "line")
            viz_type = VIZ_TYPE_MAP.get(chart_type, "echarts_timeseries_line")

            params = {
                "datasource": f"{dataset_id}__table",
                "viz_type": viz_type,
                "time_range": chart_config.get("time_range", "No filter"),
                "row_limit": 10000,
                "order_desc": True,
            }

            columns_info = self._build_columns_info(dataset)

            metrics = chart_config.get("metrics", [])
            if metrics:
                params["metrics"] = self._build_metrics(metrics, chart_config, columns_info)

            dimensions = chart_config.get("dimensions", [])
            if dimensions:
                params["groupby"] = dimensions

            time_column = chart_config.get("time_column")
            if time_column:
                params = self._add_time_config(params, time_column, chart_config)

            params = self._add_chart_type_config(params, chart_type, chart_config)
            params.update(chart_config.get("extra_params", {}))

            return params

    pusher = TestPusher()

    # 运行测试
    all_passed = True
    for test in test_cases:
        print(f"\n📋 {test['name']}")
        print("-" * 50)

        try:
            params = pusher.build_chart_params(test["config"], mock_dataset)
            viz_type = params.get("viz_type")

            print(f"  viz_type: {viz_type}")
            print(f"  metrics: {json.dumps(params.get('metrics', []), indent=4, ensure_ascii=False)}")
            print(f"  groupby: {params.get('groupby', [])}")
            print(f"  time_range: {params.get('time_range')}")

            # 验证必要字段
            if not viz_type:
                print("  ❌ 缺少 viz_type")
                all_passed = False
            elif not params.get("metrics"):
                print("  ❌ 缺少 metrics")
                all_passed = False
            else:
                print("  ✅ 参数生成正确")

        except Exception as e:
            print(f"  ❌ 错误: {e}")
            all_passed = False

    return all_passed


def test_superset_viz_types():
    """测试 Superset 6.0 支持的 viz_type"""
    print("\n" + "=" * 60)
    print("3. Superset 6.0 viz_type 兼容性检查")
    print("=" * 60)

    # Superset 6.0 常用的 viz_type
    superset_viz_types = {
        "echarts_timeseries_line": "时间序列折线图",
        "echarts_timeseries_bar": "时间序列柱状图",
        "echarts_pie": "饼图",
        "table": "表格",
        "big_number": "大数字卡片",
        "big_number_total": "总计数字卡片",
        "pivot_table": "透视表",
        "dist_bar": "分布柱状图",
        "area": "面积图",
        "scatter": "散点图",
        "line": "折线图 (旧版)",
        "bar": "柱状图 (旧版)",
        "pie": "饼图 (旧版)",
    }

    print("\n  Superset 6.0 支持的主要 viz_type:")
    for viz_type, desc in superset_viz_types.items():
        in_map = any(v == viz_type for v in VIZ_TYPE_MAP.values())
        status = "✅" if in_map else "⚠️"
        print(f"    {status} {viz_type:30} - {desc}")

    print("\n  当前映射覆盖的 viz_type:")
    for chart_type, viz_type in VIZ_TYPE_MAP.items():
        supported = viz_type in superset_viz_types
        status = "✅" if supported else "❌"
        print(f"    {status} {chart_type:10} -> {viz_type}")


def main():
    print("\n" + "=" * 60)
    print("       super6dbt Chart 参数生成测试")
    print("=" * 60 + "\n")

    test_viz_type_map()
    all_passed = test_chart_params()
    test_superset_viz_types()

    print("\n" + "=" * 60)
    if all_passed:
        print("✅ 所有测试通过!")
    else:
        print("❌ 部分测试失败，请检查上述错误")
    print("=" * 60 + "\n")


if __name__ == "__main__":
    main()