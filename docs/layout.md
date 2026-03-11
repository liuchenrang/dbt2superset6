# Dashboard Layout 布局配置文档

## 概述

`dbt2superset6` 支持在 `exposures.yml` 中定义 Dashboard 的完整布局结构，通过 `push` 命令同步到 Superset。

## 布局配置结构

在 `exposures.yml` 的 `meta.layout` 字段中定义布局：

```yaml
meta:
  dashboard_id: 216
  layout:
    # 布局组件列表
    - type: header
      text: Dashboard 标题
    - type: row
      background: transparent
      children:
        - type: chart
          ref: 图表标题
          width: 4
          height: 50
```

## 支持的布局组件

### 1. Header（标题组件）

Dashboard 的标题区域。

```yaml
- type: header
  text: 补货时机看板  # 标题文本
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
- `background`: 背景色，可选值：`transparent`、`primary`、`secondary`，默认 `transparent`
- `children`: 子组件列表

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
  height: 50          # 高度
```

**属性：**
- `ref`: 图表引用，对应 `charts` 配置中的 `title`
- `width`: 宽度（1-12）
- `height`: 高度（像素）

### 5. Markdown（文本组件）

显示 Markdown 格式的文本内容。

```yaml
- type: markdown
  content: |
    ## 标题
    这是 **Markdown** 内容
  width: 12
  height: 100
```

**属性：**
- `content`: Markdown 文本内容
- `width`: 宽度
- `height`: 高度

### 6. Divider（分隔线组件）

水平分隔线，用于视觉分隔。

```yaml
- type: divider
  width: 12   # 宽度
  height: 10  # 高度
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

## 完整示例

```yaml
version: 2
exposures:
- name: reorder_dashboard
  type: dashboard
  label: 补货时机看板
  description: 采购补货时机智能看板
  depends_on:
  - ref('dws_reorder_kpi_summary')
  owner:
    name: 采购团队
    email: caigou@yehwang.com
  meta:
    dashboard_id: 216

    # 布局配置
    layout:
      # 标题
      - type: header
        text: 补货时机看板

      # KPI 指标行
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
          - type: chart
            ref: 计划补货
            width: 2
            height: 50
          - type: chart
            ref: 库存充足
            width: 2
            height: 50
          - type: chart
            ref: SKU总数
            width: 2
            height: 50

      # 图表行
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

      # 表格行
      - type: row
        background: transparent
        children:
          - type: chart
            ref: SPU补货优先级列表
            width: 12
            height: 50

    # 图表配置
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

    # ... 更多图表配置
```

## 宽度计算规则

Dashboard 布局使用 12 列网格系统：

| width 值 | 实际宽度 |
|----------|---------|
| 1 | 1/12 宽度 |
| 2 | 1/6 宽度 |
| 3 | 1/4 宽度 |
| 4 | 1/3 宽度 |
| 6 | 1/2 宽度 |
| 8 | 2/3 宽度 |
| 12 | 满宽 |

**建议：**
- Big Number 卡片：width 1-2
- 折线图/柱状图：width 6-8
- 饼图：width 4-6
- 表格：width 12

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

## 命令使用

### 推送布局到 Superset

```bash
cd dbt2superset6
python -m super6dbt push --exposure reorder_dashboard
```

### 完整推送（数据集 + 图表 + 布局）

```bash
python -m super6dbt push
```

## 注意事项

1. **图表引用**: `chart.ref` 必须与 `charts[].title` 完全匹配
2. **宽度约束**: 同一行内所有子组件的 width 之和应 ≤ 12
3. **兼容性**: 如果未定义 `layout`，将使用默认的单行布局