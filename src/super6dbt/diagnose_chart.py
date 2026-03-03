#!/usr/bin/env python3
"""诊断 Superset Chart 问题

检查:
1. Chart API 连接
2. Chart 配置格式
3. Datasource 配置
4. Metrics 格式
"""

import os
import sys
import json
import requests
from pathlib import Path
from dataclasses import dataclass

# 添加项目路径
sys.path.insert(0, str(Path(__file__).parent.parent))

from super6dbt.client import SupersetClient


@dataclass
class DiagnosticConfig:
    """诊断配置"""
    base_url: str
    username: str
    password: str
    provider: str = "db"
    verify_ssl: bool = False

    @classmethod
    def from_input(cls) -> "DiagnosticConfig":
        """从用户输入获取配置"""
        print("\n请输入 Superset 连接配置:")
        base_url = input(f"  URL [{os.getenv('SUPERSET_BASE_URL', 'https://superset.qa1.gaia888.com')}]: ").strip()
        if not base_url:
            base_url = os.getenv('SUPERSET_BASE_URL', 'https://superset.qa1.gaia888.com')

        username = input(f"  用户名 [{os.getenv('SUPERSET_USERNAME', 'admin')}]: ").strip()
        if not username:
            username = os.getenv('SUPERSET_USERNAME', 'admin')

        password = input(f"  密码 [{os.getenv('SUPERSET_PASSWORD', 'admin')}]: ").strip()
        if not password:
            password = os.getenv('SUPERSET_PASSWORD', 'admin')

        return cls(base_url=base_url, username=username, password=password)


def diagnose_chart(client: SupersetClient, chart_id: int):
    """诊断单个图表"""
    print(f"\n{'='*60}")
    print(f"诊断 Chart ID: {chart_id}")
    print(f"{'='*60}")

    # 1. 获取图表详情
    print("\n1. 获取图表详情...")
    chart = client.get_chart(chart_id)

    if not chart:
        print(f"   ❌ 无法获取图表 {chart_id}")
        return

    print(f"   ✅ 图表名称: {chart.get('slice_name')}")
    print(f"   - viz_type: {chart.get('viz_type')}")
    print(f"   - datasource_id: {chart.get('datasource_id')}")
    print(f"   - datasource_type: {chart.get('datasource_type')}")

    # 2. 检查 params
    print("\n2. 检查 params 配置...")
    params_str = chart.get('params', '{}')
    try:
        params = json.loads(params_str) if isinstance(params_str, str) else params_str
        print(f"   ✅ params 解析成功")
        print(f"   - datasource: {params.get('datasource')}")
        print(f"   - viz_type: {params.get('viz_type')}")
        print(f"   - metrics: {json.dumps(params.get('metrics', []), indent=4, ensure_ascii=False)}")
        print(f"   - groupby: {params.get('groupby', [])}")
        print(f"   - time_range: {params.get('time_range')}")

        # 检查关键配置
        issues = []

        if not params.get('datasource'):
            issues.append("缺少 datasource 配置")

        if not params.get('viz_type'):
            issues.append("缺少 viz_type 配置")

        if not params.get('metrics'):
            issues.append("缺少 metrics 配置")

        # 检查 metrics 格式
        metrics = params.get('metrics', [])
        for i, metric in enumerate(metrics):
            if isinstance(metric, str):
                issues.append(f"metric[{i}] 是字符串格式，应该使用对象格式")
            elif isinstance(metric, dict):
                if not metric.get('expressionType'):
                    issues.append(f"metric[{i}] 缺少 expressionType")
                if not metric.get('aggregate'):
                    issues.append(f"metric[{i}] 缺少 aggregate")
                if not metric.get('column'):
                    issues.append(f"metric[{i}] 缺少 column")

        if issues:
            print(f"\n   ⚠️ 发现问题:")
            for issue in issues:
                print(f"      - {issue}")
        else:
            print(f"\n   ✅ params 配置检查通过")

    except json.JSONDecodeError as e:
        print(f"   ❌ params JSON 解析失败: {e}")
        return

    # 3. 检查 datasource
    print("\n3. 检查数据集...")
    datasource_id = chart.get('datasource_id')
    if datasource_id:
        dataset = client.get_dataset(datasource_id)
        if dataset:
            print(f"   ✅ 数据集: {dataset.get('table_name')}")
            columns = dataset.get('columns', [])
            print(f"   - 列数量: {len(columns)}")

            # 检查 metrics 中引用的列是否存在
            metric_cols = set()
            for metric in metrics:
                if isinstance(metric, dict) and metric.get('column'):
                    col_info = metric['column']
                    if isinstance(col_info, dict):
                        metric_cols.add(col_info.get('column_name'))

            dataset_cols = {c.get('column_name') for c in columns}
            missing_cols = metric_cols - dataset_cols
            if missing_cols:
                print(f"   ⚠️ Metrics 引用的列不存在于数据集: {missing_cols}")
            else:
                print(f"   ✅ 所有 Metrics 引用的列都存在")
        else:
            print(f"   ❌ 无法获取数据集 {datasource_id}")
    else:
        print(f"   ❌ 图表缺少 datasource_id")

    # 4. 尝试获取图表数据
    print("\n4. 尝试执行图表查询...")
    try:
        # 构建 chart data 请求
        query_data = {
            "datasource": {"id": datasource_id, "type": "table"},
            "queries": [{
                "viz_type": params.get('viz_type'),
                "datasource": f"{datasource_id}__table",
                "metrics": params.get('metrics', []),
                "groupby": params.get('groupby', []),
                "time_range": params.get('time_range', 'No filter'),
                "row_limit": 100,
            }]
        }

        # 添加时间配置
        if params.get('granularity_sqla'):
            query_data["queries"][0]["granularity"] = params.get('granularity_sqla')
        if params.get('time_grain_sqla'):
            query_data["queries"][0]["time_grain_sqla"] = params.get('time_grain_sqla')

        response = client._request("POST", "/api/v1/chart/data", json=query_data)

        if response.status_code == 200:
            result = response.json()
            if result.get('errors'):
                print(f"   ❌ 查询返回错误:")
                for error in result['errors']:
                    print(f"      - {error}")
            else:
                data = result.get('result', [])
                if data and len(data) > 0:
                    row_count = len(data[0].get('data', []))
                    print(f"   ✅ 查询成功，返回 {row_count} 行数据")
                else:
                    print(f"   ⚠️ 查询成功但无数据返回")
        else:
            print(f"   ❌ 查询失败: {response.status_code}")
            print(f"      {response.text[:500]}")
    except Exception as e:
        print(f"   ❌ 查询异常: {e}")


def diagnose_all_charts(client: SupersetClient):
    """诊断所有图表"""
    print("\n" + "="*60)
    print("获取所有图表列表...")
    print("="*60)

    charts = client.get_charts()

    if not charts:
        print("未找到任何图表")
        return

    print(f"找到 {len(charts)} 个图表:\n")

    for chart in charts[:10]:  # 只显示前10个
        chart_id = chart.get('id')
        slice_name = chart.get('slice_name', 'N/A')
        viz_type = chart.get('viz_type', 'N/A')
        print(f"  [{chart_id}] {slice_name} ({viz_type})")

    if len(charts) > 10:
        print(f"  ... 还有 {len(charts) - 10} 个图表")


def main():
    print("\n" + "="*60)
    print("    Superset Chart 诊断工具")
    print("="*60)

    # 加载配置
    config = DiagnosticConfig.from_input()

    print(f"\n连接配置:")
    print(f"  - URL: {config.base_url}")
    print(f"  - User: {config.username}")

    # 创建客户端
    client = SupersetClient(base_url=config.base_url, verify_ssl=config.verify_ssl)
    if not client.login(config.username, config.password, config.provider):
        print("\n❌ 登录失败，请检查用户名和密码")
        return

    print(f"\n✅ 连接成功")

    # 测试连接
    user = client.get_current_user()
    if user:
        print(f"   用户: {user.get('username')}")

    # 获取所有图表
    diagnose_all_charts(client)

    # 诊断特定图表
    chart_ids_input = input("\n输入要诊断的图表ID (多个ID用逗号分隔，回车跳过): ").strip()

    if chart_ids_input:
        chart_ids = [int(x.strip()) for x in chart_ids_input.split(',')]
        for chart_id in chart_ids:
            diagnose_chart(client, chart_id)
    else:
        # 诊断第一个图表
        charts = client.get_charts()
        if charts:
            print("\n默认诊断第一个图表...")
            diagnose_chart(client, charts[0].get('id'))


if __name__ == "__main__":
    main()