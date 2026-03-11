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
- **支持完整的 Dashboard 布局配置（Header、Row、Column、Markdown、Divider、Tabs）**
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

## Exposure 定义与 Dashboard 布局配置

### 完整示例

```yaml
version: 2
exposures:
  - name: reorder_dashboard
    type: dashboard
    label: 补货时机看板
    description: 采购补货时机智能看板
    url: /superset/dashboard/216/
    depends_on:
      - ref('dws_reorder_kpi_summary')
    owner:
      name: 采购团队
      email: caigou@yehwang.com
    meta:
      dashboard_id: 216

      # ==================== 布局配置 ====================
      layout:
        # Header 组件 (Dashboard 标题)
        - type: header
          text: 补货时机看板

        # Row 1: KPI 指标行
        - type: row
          background: transparent
          children:
            - type: chart
              ref: SPU总数
              width: 2
              height: 50
            - type: chart
              ref: 紧急
              width: 2
              height: 50
            - type: chart
              ref: 即将缺货
              width: 2
              height: 50

        # Row 2: 折线图 + 饼图
        - type: row
          background: transparent
          children:
            - type: chart
              ref: 各类别周销量趋势
              width: 8
              height: 50
            - type: chart
              ref: SPU补货状态分布
              width: 4
              height: 50

        # Row 3: 表格
        - type: row
          background: transparent
          children:
            - type: chart
              ref: SPU补货优先级列表
              width: 12
              height: 50

      # ==================== 图表配置 ====================
      charts:
        - title: SPU总数
          type: big_number
          model: dws_reorder_kpi_summary
          metrics:
            - total_spu_count
          extra_label: 款式总数

        - title: 紧急
          type: big_number
          model: dws_reorder_kpi_summary
          metrics:
            - urgent_spu_count
          extra_label: 立即下单
          color: red

        - title: 各类别周销量趋势
          type: line
          model: dws_reorder_category_trend
          time_column: week_start_date
          time_grain: week
          metrics:
            - sum_quantity
          dimensions:
            - category1_name

        - title: SPU补货优先级列表
          type: table
          model: dws_reorder_spu_analysis
          time_column: report_date
          columns:
            - spu_id
            - category1_name
            - urgency_status

      # 已存在的图表 ID 映射（自动维护）
      existing_charts:
        SPU总数: 2579
        紧急: 2580
```

## Dashboard 布局组件

布局配置位于 `meta.layout` 字段，支持以下组件类型：

### 1. Header（标题组件）

```yaml
- type: header
  text: 补货时机看板
```

### 2. Row（行容器）

水平排列的行容器，可包含多个子组件。

```yaml
- type: row
  background: transparent  # 可选: transparent, primary, secondary
  children:
    - type: chart
      ref: 图表1
      width: 4
      height: 50
    - type: chart
      ref: 图表2
      width: 4
      height: 50
```

**属性：**
| 属性 | 说明 | 默认值 |
|------|------|--------|
| `background` | 背景色：`transparent`、`primary`、`secondary` | `transparent` |
| `children` | 子组件列表 | `[]` |

### 3. Column（列容器）

垂直排列的列容器，用于嵌套布局。

```yaml
- type: row
  children:
    - type: column
      width: 6
      children:
        - type: chart
          ref: 图表1
          width: 6
          height: 50
    - type: column
      width: 6
      children:
        - type: chart
          ref: 图表2
          width: 6
          height: 50
```

### 4. Chart（图表组件）

引用在 `charts` 配置中定义的图表。

```yaml
- type: chart
  ref: SPU总数        # 图表标题，对应 charts[].title
  width: 4            # 宽度（1-12，12为满宽）
  height: 50          # 高度（像素）
```

**属性：**
| 属性 | 说明 | 必填 |
|------|------|------|
| `ref` | 图表引用，对应 `charts[].title` | ✅ |
| `width` | 宽度（1-12） | ✅ |
| `height` | 高度（像素） | ✅ |

### 5. Markdown（文本组件）

显示 Markdown 格式的文本内容。

```yaml
- type: markdown
  content: |
    ## 说明
    这是 **Markdown** 内容
  width: 12
  height: 100
```

### 6. Divider（分隔线组件）

水平分隔线，用于视觉分隔。

```yaml
- type: divider
  width: 12
  height: 10
```

### 7. Tabs（标签页容器）

创建多标签页布局。

```yaml
- type: tabs
  children:
    - type: tab
      text: 概览
      children:
        - type: row
          children:
            - type: chart
              ref: 图表1
    - type: tab
      text: 详情
      children:
        - type: row
          children:
            - type: chart
              ref: 图表2
```

## 宽度计算规则

Dashboard 布局使用 12 列网格系统：

| width 值 | 实际宽度 | 适用场景 |
|----------|---------|---------|
| 1 | 1/12 宽度 | 小型 KPI 卡片 |
| 2 | 1/6 宽度 | Big Number 卡片 |
| 3 | 1/4 宽度 | 中等宽度组件 |
| 4 | 1/3 宽度 | 饼图、小表格 |
| 6 | 1/2 宽度 | 中等图表 |
| 8 | 2/3 宽度 | 折线图、柱状图 |
| 12 | 满宽 | 表格、大图表 |

**建议：**
- Big Number 卡片：width 1-2
- 折线图/柱状图：width 6-8
- 饼图：width 4-6
- 表格：width 12

## 图表类型配置

### Big Number（数字卡片）

```yaml
- title: SPU总数
  type: big_number
  model: dws_reorder_kpi_summary
  metrics:
    - total_spu_count
  extra_label: 款式总数  # 副标题
  color: red             # 可选: red, orange, blue, green
```

### Line（折线图）

```yaml
- title: 销售趋势
  type: line
  model: orders
  time_column: order_date
  time_grain: month
  metrics:
    - total_sales
  dimensions:
    - category
```

### Doughnut（环形图）

```yaml
- title: 状态分布
  type: doughnut
  model: analysis
  metrics:
    - count
  dimensions:
    - status
```

### Table（表格）

```yaml
- title: 明细列表
  type: table
  model: details
  time_column: report_date
  columns:
    - id
    - name
    - status
    - amount
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
| `big_number` / `number` | big_number_total |
| `pie` | echarts_pie |
| `doughnut` | echarts_pie |

## 工作流程

### 初始化工作流

1. 在 Superset 中创建面板和图表
2. 运行 `super6dbt pull` 拉取现有配置
3. 审查生成的 exposures 和 model meta 配置
4. 根据需要进行修改和优化

### 开发工作流

1. 在 dbt model schema 中定义维度和度量
2. 创建 exposure 配置，包含 layout 布局定义
3. 运行 `super6dbt push` 推送到 Superset
4. 在 Superset 中查看生成的面板和图表

## Superset 6.0 position_json 映射

布局配置会被转换为 Superset 6.0 的 `position_json` 结构：

```
ROOT_ID
└── GRID_ID
    ├── HEADER_ID (可选)
    ├── ROW-{uuid}
    │   ├── CHART-X-{uuid}
    │   ├── CHART-X-{uuid}
    │   └── ...
    ├── ROW-{uuid}
    │   └── ...
    └── ...
```

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
├── chart_rules.py   # 图表配置验证规则
├── utils.py         # 工具函数
├── pyproject.toml   # 项目配置
└── README.md        # 文档
```

## 许可证

MIT