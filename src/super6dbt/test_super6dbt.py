"""测试 super6dbt 工具

测试配置：
- Superset URL: https://superset.qa1.gaia888.com/
- Username: admin
- Password: admin
- dbt项目: example
"""

import os
import sys
from pathlib import Path

# 添加项目路径到 Python 路径
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from super6dbt.config import Config, SupersetConfig, DbtProjectConfig
from super6dbt.client import SupersetClient
from super6dbt.pull import SupersetPuller
from super6dbt.push import SupersetPusher
from super6dbt.mapper import DbtToSuperset, SupersetToDbt


# ==================== 测试配置 ====================

SUPERSET_URL = "https://superset.qa1.gaia888.com"
SUPERSET_USERNAME = "admin"
SUPERSET_PASSWORD = "admin"
DBT_PROJECT_DIR = str(project_root / "example")

# ==================== 测试函数 ====================


def test_config():
    """测试配置加载"""
    print("\n" + "="*60)
    print("测试 1: 配置加载")
    print("="*60)

    # 创建测试配置
    superset_config = SupersetConfig(
        base_url=SUPERSET_URL,
        username=SUPERSET_USERNAME,
        password=SUPERSET_PASSWORD,
        verify_ssl=False,
    )

    dbt_config = DbtProjectConfig.from_project_dir(DBT_PROJECT_DIR)

    print(f"✓ Superset URL: {superset_config.base_url}")
    print(f"✓ dbt项目目录: {dbt_config.project_dir}")
    print(f"✓ 模型路径: {dbt_config.model_paths}")
    print(f"✓ Exposure路径: {dbt_config.exposure_paths}")

    return superset_config, dbt_config


def test_client_connection(superset_config):
    """测试Superset客户端连接"""
    print("\n" + "="*60)
    print("测试 2: Superset客户端连接")
    print("="*60)

    client = SupersetClient(
        base_url=superset_config.base_url,
        verify_ssl=superset_config.verify_ssl
    )

    # 登录
    print(f"尝试登录: {superset_config.username}")
    login_success = client.login(
        username=superset_config.username,
        password=superset_config.password,
    )

    if not login_success:
        print("✗ 登录失败")
        return None

    print("✓ 登录成功")

    # 获取当前用户
    user = client.get_current_user()
    if user:
        print(f"✓ 当前用户: {user.get('username')} (ID: {user.get('id')})")

    return client


def test_get_datasets(client):
    """测试获取数据集"""
    print("\n" + "="*60)
    print("测试 3: 获取数据集")
    print("="*60)

    datasets = client.get_datasets()
    print(f"✓ 获取到 {len(datasets)} 个数据集")

    if datasets:
        print("\n前 5 个数据集:")
        for i, dataset in enumerate(datasets[:5], 1):
            print(f"  {i}. {dataset.get('table_name')} (ID: {dataset.get('id')})")

        # 详细展示第一个数据集
        if datasets:
            first_dataset = datasets[0]
            print(f"\n数据集详情: {first_dataset.get('table_name')}")
            print(f"  ID: {first_dataset.get('id')}")
            print(f"  类型: {first_dataset.get('type')}")
            columns = first_dataset.get('columns', [])
            print(f"  列数: {len(columns)}")
            if columns:
                print("  列示例:")
                for col in columns[:3]:
                    print(f"    - {col.get('column_name')}: {col.get('type')}")

    return datasets


def test_get_dashboards(client):
    """测试获取面板"""
    print("\n" + "="*60)
    print("测试 4: 获取面板")
    print("="*60)

    dashboards = client.get_dashboards()
    print(f"✓ 获取到 {len(dashboards)} 个面板")

    if dashboards:
        print("\n面板列表:")
        for i, dashboard in enumerate(dashboards[:10], 1):
            print(f"  {i}. {dashboard.get('dashboard_title', dashboard.get('title'))} (ID: {dashboard.get('id')})")

    return dashboards


def test_get_charts(client, dashboard_ids=None):
    """测试获取图表"""
    print("\n" + "="*60)
    print("测试 5: 获取图表")
    print("="*60)

    if dashboard_ids:
        print(f"获取面板 {dashboard_ids} 的图表...")
        all_charts = []
        for dashboard_id in dashboard_ids:
            charts = client.get_charts(dashboard_id=dashboard_id)
            all_charts.extend(charts)
    else:
        all_charts = client.get_charts()

    print(f"✓ 获取到 {len(all_charts)} 个图表")

    if all_charts:
        print("\n前 5 个图表:")
        for i, chart in enumerate(all_charts[:5], 1):
            print(f"  {i}. {chart.get('slice_name', chart.get('title'))}")
            print(f"     类型: {chart.get('viz_type')}")
            print(f"     数据集ID: {chart.get('datasource_id')}")

    return all_charts


def test_mapper_superset_to_dbt(dataset, dashboard):
    """测试Superset到dbt的映射"""
    print("\n" + "="*60)
    print("测试 6: Superset → dbt 映射")
    print("="*60)

    mapper = SupersetToDbt()

    # 测试数据集转换
    print("转换数据集到model meta...")
    model_meta = mapper.dataset_to_model_meta(dataset)
    print(f"✓ 模型名称: {model_meta.get('name')}")
    print(f"✓ 列数: {len(model_meta.get('columns', {}))}")

    if model_meta.get('columns'):
        print("  列示例:")
        for col_name, col_meta in list(model_meta['columns'].items())[:3]:
            print(f"    - {col_name}")

    # 测试面板转换
    dataset_map = {dataset['id']: dataset}
    print("\n转换面板到exposure...")
    exposure = mapper.dashboard_to_exposure(dashboard, dataset_map)
    print(f"✓ Exposure名称: {exposure.get('name')}")
    print(f"✓ Label: {exposure.get('label')}")
    print(f"✓ 依赖数: {len(exposure.get('depends_on', []))}")

    # 生成YAML
    yaml_content = mapper.generate_yaml_exposure(exposure)
    print("\n生成的YAML (前500字符):")
    print(yaml_content[:500] + "...")

    return model_meta, exposure


def test_mapper_dbt_to_superset(dbt_config):
    """测试dbt到Superset的映射"""
    print("\n" + "="*60)
    print("测试 7: dbt → Superset 映射")
    print("="*60)

    mapper = DbtToSuperset()

    # 解析orders模型的meta配置
    model_path = dbt_config.project_dir / "models" / "light" / "orders.yml"
    if not model_path.exists():
        print(f"✗ 模型文件不存在: {model_path}")
        return None, None

    print(f"读取模型文件: {model_path}")
    import yaml
    with open(model_path, 'r') as f:
        model_data = yaml.safe_load(f)

    if model_data and 'models' in model_data:
        for model in model_data['models']:
            model_meta = mapper.parse_model_meta(model['name'], model)
            mapper.models[model['name']] = model_meta
            print(f"✓ 解析模型: {model['name']}")
            print(f"  描述: {model_meta.description}")
            print(f"  列数: {len(model_meta.columns)}")

            # 展示维度和度量
            dimensions = []
            metrics = []
            for col_name, col_meta in model_meta.columns.items():
                if col_meta.dimensions:
                    dimensions.append(col_name)
                if col_meta.metrics:
                    for metric_name in col_meta.metrics.keys():
                        metrics.append(metric_name)

            print(f"  维度: {dimensions}")
            print(f"  度量: {metrics}")

    # 生成Superset列配置
    if mapper.models:
        model_name = list(mapper.models.keys())[0]
        superset_columns = mapper.model_to_superset_columns(mapper.models[model_name])
        print(f"\n✓ Superset列配置:")
        for col in superset_columns.get('columns', [])[:3]:
            print(f"  - {col}")

    return mapper, model_meta if 'model_meta' in locals() else None


def test_pull(client, dbt_config):
    """测试pull功能"""
    print("\n" + "="*60)
    print("测试 8: Pull 功能")
    print("="*60)

    puller = SupersetPuller(client, dbt_config)

    print("开始拉取所有面板配置...")
    puller.pull()

    print("\n✓ Pull完成")
    print(f"\n生成的exposure文件:")
    exposure_path = dbt_config.project_dir / "models" / "exposures"
    if exposure_path.exists():
        for yml_file in exposure_path.glob("*.yml"):
            print(f"  - {yml_file.name}")


def test_push(client, dbt_config):
    """测试push功能"""
    print("\n" + "="*60)
    print("测试 9: Push 功能")
    print("="*60)

    pusher = SupersetPusher(client, dbt_config)

    # 只推送orders_dashboard
    print("推送 orders_dashboard...")
    pusher.push(exposure_names=["orders_dashboard"])

    print("\n✓ Push完成")


# ==================== 主测试流程 ====================


def main():
    """运行所有测试"""
    print("\n" + "="*60)
    print("super6dbt 工具测试")
    print("="*60)
    print(f"Superset URL: {SUPERSET_URL}")
    print(f"dbt项目目录: {DBT_PROJECT_DIR}")

    # 收集测试结果
    results = {}

    try:
        # 测试1: 配置加载
        superset_config, dbt_config = test_config()
        results['config'] = True

        # 测试2: 客户端连接
        client = test_client_connection(superset_config)
        if client:
            results['connection'] = True
        else:
            print("\n✗ 连接失败，停止测试")
            return

        # 测试3: 获取数据集
        datasets = test_get_datasets(client)
        results['datasets'] = True

        # 测试4: 获取面板
        dashboards = test_get_dashboards(client)
        results['dashboards'] = True

        # 测试5: 获取图表
        if dashboards:
            dashboard_ids = [d['id'] for d in dashboards[:3]]
            test_get_charts(client, dashboard_ids)
            results['charts'] = True

        # 测试6: Superset → dbt 映射
        if datasets and dashboards:
            test_mapper_superset_to_dbt(datasets[0], dashboards[0])
            results['mapper_s2d'] = True

        # 测试7: dbt → Superset 映射
        test_mapper_dbt_to_superset(dbt_config)
        results['mapper_d2s'] = True

        # 测试8: Pull功能
        print("\n" + "="*60)
        print("测试 8: Pull 功能 - 将拉取面板配置到本地exposures")
        print("="*60)
        test_pull(client, dbt_config)
        results['pull'] = True

        # 测试9: Push功能
        print("\n" + "="*60)
        print("测试 9: Push 功能 - 将推送dbt配置到Superset")
        print("="*60)
        test_push(client, dbt_config)
        results['push'] = True

    except Exception as e:
        print(f"\n✗ 测试失败: {e}")
        import traceback
        traceback.print_exc()
        results['error'] = str(e)

    # 输出测试结果摘要
    print("\n" + "="*60)
    print("测试结果摘要")
    print("="*60)
    for test_name, passed in results.items():
        status = "✓ 通过" if passed else "✗ 失败/跳过"
        print(f"  {test_name}: {status}")

    passed_count = sum(1 for v in results.values() if v)
    total_count = len(results)
    print(f"\n总计: {passed_count}/{total_count} 通过")


if __name__ == "__main__":
    main()