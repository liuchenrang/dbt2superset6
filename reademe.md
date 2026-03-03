项目介绍
项目流程
    熟悉dbt，lightdash 定义 转换为superset api需要的数据结构
    读取superset api，转换为 dbt，lightdash 的定义
项目目标
    通过dbt管理superset

实现push命令
    推送dbt定义的 数据集，维护数据，指标 ，推送exposures定义的面板集chart
    推送时解析ligthdash语法，及dbt语法。 推送到supperset 项目
实现pull命令
    拉取superset定义的面板转换成exposures，及superset的里定义的数据集传model的定义， model的定义使用lightdash语法
推送时
```yml
metrics: 定义里面的都是指标
              
              sales_sum: //这个作为 metric key
                type: count
                description: '订单总销量' 这个作为superset的指标的label
```

superset项目
- 项目地址 https://superset.qa1.gaia888.com/
- 项目用户名 admin 密码 admin
- 项目api
 - 数据集 api https://superset.apache.org/developer-docs/api/datasets/
    - 指标 纬度
 - chart api https://superset.apache.org/developer-docs/api/charts
 - 面板api https://superset.apache.org/developer-docs/api/dashboards 
dbt 
- https://docs.getdbt.com/docs/build/exposures 
ligthdash
- 指标语法 
  https://docs.lightdash.com/get-started/develop-in-lightdash/how-to-create-metrics