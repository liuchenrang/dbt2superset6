# SuperSet 6.0 ↔ dbt 双向同步工具

一个用于在 dbt 和 Apache Superset 6.0 之间双向同步面板配置的工具。

## 功能特性

### Pull (从 Superset 拉取到 dbt)
- 拉取所有或指定的面板配置
- 自动生成 dbt exposures 定义
- 从 Superset 数据集提取列信息，更新 model 的 `meta` 配置
- 支持维度和度量的完整映射

### Push (从 dbt 推送到 Superset)
- 基于 exposures 创建或更新 Superset 面板
- 根据 model 定义的 metrics 和 dimensions 生成图表
- 支持增量同步（仅推送变更）
- 自动维护 exposure 文件中的 ID 映射

## 安装

```bash
# 使用 pip 安装
pip install -e super6dbt

# 或使用 uv
uv pip install -e super6dbt
```

## 配置

### 环境变量方式

```bash
export SUPERSET_BASE_URL="http://localhost:8088"
export SUPERSET_USERNAME="admin"
export SUPERSET_PASSWORD="admin"
export SUPERSET_PROVIDER="db"
export SUPERSET_VERIFY_SSL="false"
```

### 配置文件方式

首先初始化配置：

```bash
super6dbt init
```

这会创建 `super6dbt/config.yml` 文件，编辑内容：

```yaml
superset:
  # Superset基础URL
  base_url: "http://localhost:8088"

  # 登录凭证
  username: "admin"
  password: "admin"
  provider: "db"

  # SSL验证
  verify_ssl: false

  # 数据库名称（可选，用于创建数据集）
  database: "your_database_name"
```

## 使用方法

### 1. 检查连接状态

```bash
super6dbt status
```

### 2. 从 Superset 拉取配置

拉取所有面板：

```bash
super6dbt pull
```

拉取指定面板：

```bash
super6dbt pull --dashboard-ids 1,2,3
```

### 3. 推送配置到 Superset

推送所有 exposures：

```bash
super6dbt push
```

推送指定 exposure：

```bash
super6dbt push --exposure-names my_dashboard
```

仅推送指定 model 的数据集（不创建面板）：

```bash
super6dbt push --model-names orders,products
```

推送指定 model 的数据集，指定 schema：

```bash
super6dbt push --model-names ads_channel_conversion_analysis_full --schema wa_ads
```

## dbt 模型定义

### Model Schema 定义

在 model 的 schema.yml 中定义维度和度量：

```yaml
models:
  - name: 'orders'
    description: '热门日期订单表'
    columns:
      - name: 'orderdate'
        description: '订单日期'
        config:
          meta:
            dimension:
              type: date
              time_intervals: ['DAY', 'WEEK', 'MONTH', 'YEAR']
      - name: 'sales'
        description: '订单销售额'
        config:
          meta:
            metrics:
              total_sales_sum:
                type: sum
                sql: sales
                description: '订单总销售额'
      - name: 'orderid'
        description: '订单ID'
        config:
          meta:
            metrics:
              total_order_count:
                type: count_distinct
```

### Exposure 定义

```yaml
exposures:
  - name: sales_dashboard
    type: dashboard
    label: 销售仪表板
    description: 销售数据分析面板
    url: /sales_dashboard
    depends_on:
      - ref('orders')
      - ref('products')
    owner:
      name: Data Team
      email: data@example.com
    meta:
      dashboard_id: null  # 首次推送后自动填充
      charts:
        - title: 销售趋势
          type: line
          model: orders
          time_column: orderdate
          time_grain: month
          metrics:
            - total_sales_sum
          dimensions:
            - orderdate
          time_range: last 12 months
```

## 支持的度量类型

| dbt 类型 | Superset 聚合 |
|---------|--------------|
| `count` | COUNT |
| `count_distinct` | COUNT_DISTINCT |
| `sum` | SUM |
| `avg` | AVG |
| `min` | MIN |
| `max` | MAX |

## 支持的图表类型

| dbt 类型 | Superset viz_type |
|---------|------------------|
| `line` | echarts_timeseries_line |
| `bar` | echarts_timeseries_bar |
| `table` | table |
| `number` | big_number |
| `pie` | echarts_pie |

## 工作流程

### 初始化工作流

1. 在 Superset 中创建面板和图表
2. 运行 `super6dbt pull` 拉取现有配置
3. 审查生成的 exposures 和 model meta 配置
4. 根据需要进行修改和优化

### 开发工作流

1. 在 dbt model schema 中定义维度和度量
2. 创建/更新 exposure 配置
3. 运行 `super6dbt push` 推送到 Superset
4. 在 Superset 中查看生成的面板和图表

## 项目结构

```
super6dbt/
├── __init__.py       # 包初始化
├── cli.py           # 命令行入口
├── config.py        # 配置管理
├── client.py        # Superset API 客户端
├── mapper.py        # dbt ↔ Superset 映射转换
├── pull.py          # Pull 功能实现
├── push.py          # Push 功能实现
├── utils.py         # 工具函数
├── pyproject.toml   # 项目配置
└── README.md        # 文档
```

## 许可证

MIT# dbt2superset6
