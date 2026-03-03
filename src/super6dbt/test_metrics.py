#!/usr/bin/env python3
"""测试 metrics 构建逻辑"""

import sys
sys.path.insert(0, '/Users/chen/IdeaProjects/demo-dbt')

from super6dbt.push import SupersetPusher
from super6dbt.mapper import DbtToSuperset, VIZ_TYPE_MAP, METRIC_TYPE_TO_SUPERSET_AGG
import json

# 模拟 orders model 的 meta 配置
orders_meta = {
    "name": "orders",
    "columns": {
        "orderdate": {
            "dimensions": {"type": "date", "time_intervals": ["DAY", "WEEK", "MONTH", "YEAR"]},
            "metrics": None
        },
        "productid": {
            "dimensions": {"type": "string"},
            "metrics": {
                "sales_sum": {"type": "count", "sql": None, "description": "订单总销量"}
            }
        },
        "orderid": {
            "dimensions": None,
            "metrics": {
                "total_order_count": {"type": "count_distinct", "sql": "orderid", "description": ""}
            }
        },
        "sales": {
            "dimensions": None,
            "metrics": {
                "total_sales_sum": {"type": "sum", "sql": "sales", "description": "订单总销售额"}
            }
        },
        "year": {
            "dimensions": {"type": "number"},
            "metrics": None
        },
        "quantity": {
            "dimensions": None,
            "metrics": {
                "total_quantity_sum": {"type": "sum", "sql": "quantity", "description": "订单总购买数量"}
            }
        }
    }
}

# 模拟 dataset
mock_dataset = {
    "id": 1,
    "table_name": "orders",
    "columns": [
        {"column_name": "orderdate", "type": "TIMESTAMP", "id": 1},
        {"column_name": "productid", "type": "VARCHAR", "id": 2},
        {"column_name": "orderid", "type": "VARCHAR", "id": 3},
        {"column_name": "sales", "type": "NUMERIC", "id": 4},
        {"column_name": "year", "type": "INT", "id": 5},
        {"column_name": "quantity", "type": "INT", "id": 6},
    ]
}

# 模拟 chart config (来自 orders_dashboard.yml)
chart_configs = [
    {
        "title": "订单总销售额趋势",
        "type": "line",
        "model": "orders",
        "time_column": "orderdate",
        "time_grain": "month",
        "metrics": ["total_sales_sum"],
        "dimensions": ["orderdate"],
        "time_range": "last 12 months"
    },
    {
        "title": "各年销量统计",
        "type": "bar",
        "model": "orders",
        "metrics": ["sales_sum"],
        "dimensions": ["year"],
        "time_range": "No filter"
    },
    {
        "title": "订单概览",
        "type": "number",
        "model": "orders",
        "metrics": ["total_order_count", "total_sales_sum", "total_quantity_sum"],
        "time_range": "last 30 days"
    }
]

print("="*60)
print("测试 _build_metrics 方法")
print("="*60)

# 创建测试 pusher
from super6dbt.mapper import ColumnMeta, MetricConfig, ModelMeta

pusher = SupersetPusher.__new__(SupersetPusher)
pusher.mapper = DbtToSuperset()

# 构建 model meta
model = ModelMeta(name="orders")
for col_name, col_config in orders_meta["columns"].items():
    col_meta = ColumnMeta(name=col_name)
    if col_config.get("dimensions"):
        col_meta.dimensions = col_config["dimensions"]
    if col_config.get("metrics"):
        col_meta.metrics = {}
        for m_name, m_config in col_config["metrics"].items():
            col_meta.metrics[m_name] = MetricConfig(
                name=m_name,
                type=m_config["type"],
                description=m_config.get("description", ""),
                sql=m_config.get("sql")
            )
    model.columns[col_name] = col_meta

pusher.mapper.models["orders"] = model

print(f"\n加载的 model: {list(pusher.mapper.models.keys())}")
print(f"orders columns: {list(model.columns.keys())}")

# 测试每个 chart
for chart_config in chart_configs:
    print(f"\n{'='*60}")
    print(f"Chart: {chart_config['title']}")
    print(f"{'='*60}")

    # 构建 columns_info
    columns_info = {}
    for col in mock_dataset["columns"]:
        columns_info[col["column_name"]] = {
            "column_name": col["column_name"],
            "type": col["type"],
            "id": col["id"],
            "expressionType": "SIMPLE",
            "filterable": True,
            "groupby": True,
        }

    print(f"\n输入 metrics: {chart_config['metrics']}")

    # 调用 _build_metrics
    metrics = pusher._build_metrics(chart_config['metrics'], chart_config, columns_info)

    print(f"\n输出 metrics:")
    print(json.dumps(metrics, indent=2, ensure_ascii=False))

    if not metrics:
        print("\n❌ metrics 为空！")
        # 诊断问题
        print("\n诊断:")
        for metric_ref in chart_config['metrics']:
            print(f"\n  检查 metric: {metric_ref}")
            metric_name = metric_ref
            found = False

            for col_name, col_meta in model.columns.items():
                if col_meta.metrics and metric_name in col_meta.metrics:
                    found = True
                    metric_config = col_meta.metrics[metric_name]
                    print(f"    ✓ 在 column '{col_name}' 中找到")
                    print(f"      type: {metric_config.type}")
                    print(f"      sql: {metric_config.sql}")

                    # 提取列名
                    if metric_config.sql:
                        col = pusher._extract_column_from_sql(metric_config.sql)
                        print(f"      提取的列名: {col}")
                        if col in columns_info:
                            print(f"      ✓ 列存在于 dataset")
                        else:
                            print(f"      ✗ 列不存在于 dataset!")
                    else:
                        print(f"      使用 column 名: {col_name}")
                        if col_name in columns_info:
                            print(f"      ✓ 列存在于 dataset")
                        else:
                            print(f"      ✗ 列不存在于 dataset!")
                    break

            if not found:
                print(f"    ✗ 未在 model 中找到 metric '{metric_name}'")
    else:
        print(f"\n✅ metrics 生成成功，共 {len(metrics)} 个")

print("\n" + "="*60)
print("测试完成")
print("="*60)