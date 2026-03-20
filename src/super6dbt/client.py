"""Superset 6.0 API客户端 - 修复版

提供与Superset API交互的所有方法
"""

import requests
from typing import Any, Optional, Dict, List
from dataclasses import dataclass, field
import logging
import json
import base64

logger = logging.getLogger(__name__)


def base64url_decode(s: str) -> bytes:
    """Base64 URL 解码"""
    s += '=' * (-len(s) % 4)
    return base64.urlsafe_b64decode(s)


@dataclass
class SupersetClient:
    """Superset API客户端"""
    base_url: str
    access_token: Optional[str] = None
    refresh_token: Optional[str] = None
    verify_ssl: bool = True
    csrf_token: Optional[str] = None
    session: requests.Session = field(default_factory=requests.Session)
    schema_map: Dict[str, str] = field(default_factory=dict)
    database_name: Optional[str] = None  # 指定的数据库名称
    default_schema: Optional[str] = None  # 从 dbt profiles 读取的默认 schema
    # 数据集缓存
    _datasets_cache: Optional[List[Dict[str, Any]]] = field(default=None, init=False)
    _datasets_by_name: Optional[Dict[str, Dict[str, Any]]] = field(default=None, init=False)
    _cache_time: float = 0.0
    cache_ttl: int = 300  # 缓存有效期（秒）

    def login(self, username: str, password: str, provider: str = "db") -> bool:
        """登录获取token"""
        # 1. 访问首页建立session和获取CSRF
        try:
            self.session.get(f"{self.base_url}/", verify=self.verify_ssl)
        except Exception as e:
            logger.warning(f"访问首页失败: {e}")

        # 从 session cookie 获取 CSRF
        session_cookie = self.session.cookies.get("session")
        if session_cookie:
            try:
                parts = session_cookie.split(".")
                if len(parts) >= 2:
                    payload = json.loads(base64url_decode(parts[1]).decode())
                    self.csrf_token = payload.get("csrf_token")
                    logger.debug(f"从session cookie获取CSRF: {self.csrf_token}")
            except Exception as e:
                logger.debug(f"解码session cookie失败: {e}")

        # 2. 登录获取 access token
        payload = {
            "username": username,
            "password": password,
            "provider": provider,
            "refresh": True,
        }

        headers = {"Content-Type": "application/json"}
        if self.csrf_token:
            payload["csrf_token"] = self.csrf_token
            headers["X-CSRFToken"] = self.csrf_token

        response = self.session.post(
            f"{self.base_url}/api/v1/security/login",
            json=payload,
            headers=headers,
            verify=self.verify_ssl,
        )

        if response.status_code == 200:
            data = response.json()
            self.access_token = data.get("access_token")
            self.refresh_token = data.get("refresh_token")

            # 3. 从 JWT access token 中获取 CSRF（用于后续 API 请求）
            try:
                jwt_parts = self.access_token.split(".")
                if len(jwt_parts) >= 2:
                    jwt_payload = json.loads(base64url_decode(jwt_parts[1]).decode())
                    jwt_csrf = jwt_payload.get("csrf")
                    if jwt_csrf:
                        self.csrf_token = jwt_csrf
                        logger.debug(f"从JWT token获取CSRF: {self.csrf_token}")
                        # 重要：设置 CSRF cookie
                        self.session.cookies.set("csrf_token", jwt_csrf, domain=self.base_url.split("//")[1].split("/")[0])
            except Exception as e:
                logger.debug(f"解码JWT token失败: {e}")

            logger.info("登录成功")
            return True
        else:
            logger.error(f"登录失败: {response.status_code} - {response.text}")
            return False

    def _get_headers(self) -> Dict[str, str]:
        """获取请求头"""
        headers = {
            "Content-Type": "application/json",
            "Accept": "application/json",
            "Referer": f"{self.base_url}/",
        }
        if self.access_token:
            headers["Authorization"] = f"Bearer {self.access_token}"
        if self.csrf_token:
            headers["X-CSRFToken"] = self.csrf_token
        return headers

    def _request(self, method: str, endpoint: str, **kwargs) -> requests.Response:
        """发送HTTP请求"""
        url = f"{self.base_url}{endpoint}"
        kwargs.setdefault("headers", {})
        kwargs["headers"].update(self._get_headers())
        kwargs.setdefault("verify", self.verify_ssl)

        response = self.session.request(method, url, **kwargs)

        if response.status_code == 401:
            # Token过期，尝试刷新
            if self._refresh_token():
                kwargs["headers"].update(self._get_headers())
                response = self.session.request(method, url, **kwargs)

        return response

    def _refresh_token(self) -> bool:
        """刷新访问令牌"""
        if not self.refresh_token:
            return False

        url = f"{self.base_url}/api/v1/security/refresh"
        response = self.session.post(
            url,
            json={"refresh_token": self.refresh_token},
            headers=self._get_headers(),
            verify=self.verify_ssl,
        )

        if response.status_code == 200:
            data = response.json()
            self.access_token = data.get("access_token")
            # 更新CSRF token
            try:
                jwt_parts = self.access_token.split(".")
                jwt_payload = json.loads(base64url_decode(jwt_parts[1]).decode())
                new_csrf = jwt_payload.get("csrf")
                if new_csrf:
                    self.csrf_token = new_csrf
                    self.session.cookies.set("csrf_token", new_csrf)
            except Exception:
                pass
            return True
        return False

    def _get_csrf_token(self) -> Optional[str]:
        """获取CSRF token"""
        if not self.access_token:
            return None

        # 尝试从API获取CSRF token
        headers = {
            "Authorization": f"Bearer {self.access_token}",
            "Referer": f"{self.base_url}/",
        }
        response = self._request("GET", "/api/v1/security/csrf_token/")

        if response.status_code == 200:
            csrf_result = response.json().get("result")
            if csrf_result:
                self.csrf_token = csrf_result
                self.session.cookies.set("csrf_token", csrf_result)
                return csrf_result

        return None

    # ==================== Dashboard API ====================

    def get_dashboards(self) -> List[Dict[str, Any]]:
        """获取所有面板"""
        response = self._request("GET", "/api/v1/dashboard/")
        if response.status_code == 200:
            return response.json().get("result", [])
        logger.error(f"获取面板失败: {response.status_code}")
        return []

    def get_dashboard(self, dashboard_id: int) -> Optional[Dict[str, Any]]:
        """获取单个面板详情"""
        response = self._request("GET", f"/api/v1/dashboard/{dashboard_id}")
        if response.status_code == 200:
            return response.json().get("result")
        return None

    def create_dashboard(
        self,
        title: str,
        description: str = "",
        owners: Optional[List[int]] = None,
        roles: Optional[List[int]] = None,
        charts: Optional[List[int]] = None,
    ) -> Optional[Dict[str, Any]]:
        """创建面板"""
        # 获取最新的CSRF token
        self._get_csrf_token()

        # Superset 6.0 使用 dashboard_title 而不是 title
        payload = {
            "dashboard_title": title,
        }

        if owners is not None:
            payload["owners"] = owners
        if roles is not None:
            payload["roles"] = roles
        if charts is not None:
            payload["charts"] = charts

        response = self._request("POST", "/api/v1/dashboard/", json=payload)

        if response.status_code == 201:
            return response.json()
        logger.error(f"创建面板失败: {response.status_code} - {response.text}")
        return None

    def update_dashboard(
        self,
        dashboard_id: int,
        title: Optional[str] = None,
        description: Optional[str] = None,
        charts: Optional[List[int]] = None,
        positions: Optional[Dict[str, Any]] = None,
        json_metadata: Optional[Dict[str, Any]] = None,
    ) -> Optional[Dict[str, Any]]:
        """更新面板"""
        # 获取最新的CSRF token
        self._get_csrf_token()

        payload = {}
        if title is not None:
            payload["dashboard_title"] = title
        if description is not None:
            payload["description"] = description

        # 处理 positions
        if positions is not None:
            position_json = None

            # 如果 positions 已经是完整的 position_json 字典，直接使用
            if isinstance(positions, dict):
                position_json = positions
            elif isinstance(positions, list):
                # 如果是列表格式，构建标准的Superset position_json格式
                position_json = {
                    "DASHBOARD_VERSION_KEY": "v2",
                    "ROOT_ID": {"id": "ROOT", "type": "ROOT", "children": ["TABS-0"]},
                    "TABS-0": {
                        "id": "TABS-0",
                        "type": "TAB",
                        "children": [],
                        "meta": {"text": "Tab 1"}
                    },
                    "CHART-UUID": {},
                }

                # 添加每个图表的位置和ID
                for pos in positions:
                    chart_id = pos.get("id")
                    if chart_id:
                        position_json["TABS-0"]["children"].append(chart_id)
                        position_json["CHART-UUID"][str(chart_id)] = str(chart_id)

                        # 添加图表位置信息
                        position_json[str(chart_id)] = {
                            "id": chart_id,
                            "type": "CHART",
                            "meta": {
                                "chartId": chart_id,
                                "uuid": str(chart_id),
                                "sliceName": f"Chart {chart_id}",
                                "width": pos.get("size_x", 4),
                                "height": pos.get("size_y", 4),
                            },
                            "position": {
                                "x": pos.get("col", 0),
                                "y": pos.get("row", 0),
                                "w": pos.get("size_x", 4),
                                "h": pos.get("size_y", 4),
                            },
                        }

            # position_json 必须是 JSON 字符串
            if position_json:
                payload["position_json"] = json.dumps(position_json)

        if json_metadata is not None:
            payload["json_metadata"] = json.dumps(json_metadata, ensure_ascii=False)

        response = self._request("PUT", f"/api/v1/dashboard/{dashboard_id}", json=payload)

        if response.status_code == 200:
            return response.json()
        logger.error(f"更新面板失败: {response.status_code} - {response.text}")
        return None

    def delete_dashboard(self, dashboard_id: int) -> bool:
        """删除面板"""
        response = self._request("DELETE", f"/api/v1/dashboard/{dashboard_id}")
        return response.status_code == 204

    # ==================== Chart API ====================

    def get_charts(self, dashboard_id: Optional[int] = None) -> List[Dict[str, Any]]:
        """获取图表列表

        GET /api/v1/chart/
        """
        params = {}
        if dashboard_id is not None:
            params["q"] = f'{{"filters":[{{"col":"dashboard_id","opr":"eq","value":{dashboard_id}}}]}}'

        response = self._request("GET", "/api/v1/chart/", params=params)

        if response.status_code == 200:
            return response.json().get("result", [])
        logger.error(f"获取图表列表失败: {response.status_code}")
        return []

    def get_chart(self, chart_id: int) -> Optional[Dict[str, Any]]:
        """获取单个图表详情

        GET /api/v1/chart/{chart_id}
        """
        response = self._request("GET", f"/api/v1/chart/{chart_id}")
        if response.status_code == 200:
            return response.json().get("result")
        logger.error(f"获取图表详情失败: {response.status_code}")
        return None

    def get_chart_data(self, chart_id: int) -> Optional[Dict[str, Any]]:
        """获取图表数据 (用于执行图表查询)

        POST /api/v1/chart/data
        """
        # 先获取图表配置
        chart = self.get_chart(chart_id)
        if not chart:
            return None

        # 构建查询请求
        form_data = {
            "datasource": f"{chart.get('datasource_id')}__table",
            "viz_type": chart.get("viz_type"),
            **chart.get("params", {})
        }

        payload = {
            "datasource": {"id": chart.get("datasource_id"), "type": "table"},
            "queries": [form_data]
        }

        response = self._request("POST", "/api/v1/chart/data", json=payload)

        if response.status_code == 200:
            return response.json()
        logger.error(f"获取图表数据失败: {response.status_code} - {response.text}")
        return None

    def create_chart(
        self,
        datasource_id: int,
        viz_type: str,
        title: str,
        description: str = "",
        params: Optional[Dict[str, Any]] = None,
        dashboard_id: Optional[int] = None,
    ) -> Optional[Dict[str, Any]]:
        """创建图表"""
        # 处理 params
        if params is not None:
            if isinstance(params, str):
                # params 是 JSON 字符串，需要解析
                import json
                params_dict = json.loads(params)
            else:
                params_dict = params

            # 确保 params 中的 viz_type 与传入的 viz_type 一致
            if "viz_type" in params_dict:
                params_dict["viz_type"] = viz_type

            # 将 params 序列化为 JSON 字符串
            import json
            params_str = json.dumps(params_dict, ensure_ascii=False)
        else:
            params_str = None

        # Superset 6.0 使用 slice_name 而不是 title
        payload = {
            "datasource_id": datasource_id,
            "datasource_type": "table",
            "viz_type": viz_type,
            "slice_name": title,  # 使用 slice_name 而不是 title
            "description": description,
        }

        if params_str is not None:
            payload["params"] = params_str

        # 关联到 dashboard
        if dashboard_id is not None:
            payload["dashboards"] = [dashboard_id]

        response = self._request("POST", "/api/v1/chart/", json=payload)

        if response.status_code == 201:
            # API返回格式: {"id": xxx, "result": {...}}
            return response.json()
        logger.error(f"创建图表失败: {response.status_code} - {response.text}")
        return None

    def update_chart(
        self,
        chart_id: int,
        title: Optional[str] = None,
        description: Optional[str] = None,
        params: Optional[Dict[str, Any]] = None,
        dashboard_id: Optional[int] = None,
        owner_id: Optional[int] = None,
    ) -> Optional[Dict[str, Any]]:
        """更新图表

        Args:
            chart_id: 图表ID
            title: 图表标题
            description: 描述
            params: 图表参数字典
            dashboard_id: 关联的面板ID
            owner_id: 所有者ID
        """
        payload = {}
        if title is not None:
            payload["slice_name"] = title  # 使用 slice_name 而不是 title
        if description is not None:
            payload["description"] = description
        if params is not None:
            # 从 params 中提取 viz_type 并同步到图表对象的 viz_type 字段
            if isinstance(params, str):
                # params 是 JSON 字符串，需要解析
                import json
                params_dict = json.loads(params)
            else:
                params_dict = params.copy()

            # 提取 datasource_id
            datasource_str = params_dict.get("datasource", "")
            datasource_id = int(datasource_str.split("__")[0]) if "__" in datasource_str else int(datasource_str) if datasource_str else None

            # 将 viz_type 同步到图表对象层级
            viz_type = params_dict.get("viz_type")
            if viz_type:
                payload["viz_type"] = viz_type

            # 设置顶层必需字段
            payload["datasource_id"] = datasource_id
            payload["datasource_type"] = "table"

            # 设置 dashboards 和 owners
            if dashboard_id:
                payload["dashboards"] = [dashboard_id]
                params_dict["dashboards"] = [dashboard_id]
            if owner_id:
                payload["owners"] = [owner_id]

            # 构建 queries 数组
            # 参考: chart_rules.py 中的 QUERY_CONTEXT_RULES
            query_item = {
                "filters": [],
                "extras": {"having": "", "where": ""},  # 注意: line 类型不应包含 time_grain_sqla
                "applied_time_extras": {},
                "annotation_layers": [],
                "series_limit": 0,
                "group_others_when_limit_reached": False,
                "order_desc": True,
                "url_params": {},
                "custom_params": {},
                "custom_form_data": {},
                "post_processing": []
            }

            # 处理 metrics/metric
            # 规则: Pie 和 Big Number 使用 metric (单数), Line 使用 metrics (数组)
            metric_name = None
            metrics_list = []
            if params_dict.get("metric"):
                metric_name = params_dict.get("metric")
                metrics_list = [metric_name]
                query_item["metrics"] = metrics_list
            elif params_dict.get("metrics"):
                metrics = params_dict.get("metrics")
                query_item["metrics"] = metrics
                metrics_list = metrics if isinstance(metrics, list) else [metrics]
                if metrics:
                    metric_name = metrics[0] if isinstance(metrics[0], str) else metrics[0].get("label") or metrics[0].get("column", {}).get("column_name")

            # ==================== 图表类型特定处理 ====================

            # 处理折线图时间序列 (echarts_timeseries_line)
            # 规则: 必须移除 time_grain_sqla, PostgreSQL 不支持 week grain
            if viz_type == "echarts_timeseries_line":
                time_column = params_dict.get("granularity_sqla") or params_dict.get("x_axis")
                groupby = params_dict.get("groupby", [])

                # 规则: 移除 time_grain_sqla - PostgreSQL 不支持，会导致 "No grain spec for week" 错误
                params_dict.pop("time_grain_sqla", None)

                if time_column:
                    # 规则: 必须设置 time_range
                    query_item["time_range"] = "No filter"

                    # 设置 granularity
                    query_item["granularity"] = time_column

                    # 规则: columns 使用简单字符串数组，不使用 timeGrain 对象
                    # 格式: [维度列, 时间列]
                    columns = []
                    for dim in groupby:
                        columns.append(dim)
                    columns.append(time_column)
                    query_item["columns"] = columns

                    # 规则: 设置 series_columns
                    if groupby:
                        query_item["series_columns"] = groupby

                # 规则: 设置 orderby 格式为 [[metric_name, False]]
                if metric_name:
                    query_item["orderby"] = [[metric_name, False]]

                # 规则: 设置 post_processing (pivot, rename, flatten)
                if groupby and metric_name and time_column:
                    query_item["post_processing"] = [
                        {
                            "operation": "pivot",
                            "options": {
                                "index": [time_column],
                                "columns": groupby,
                                "aggregates": {metric_name: {"operator": "mean"}},
                                "drop_missing_columns": False
                            }
                        },
                        {
                            "operation": "rename",
                            "options": {"columns": {metric_name: None}, "level": 0, "inplace": True}
                        },
                        {"operation": "flatten"}
                    ]

            else:
                # 非折线图的处理
                # 规则: groupby 用于 Pie/Bar 等，all_columns 用于 Table
                if params_dict.get("groupby"):
                    query_item["columns"] = params_dict.get("groupby")

                if params_dict.get("all_columns"):
                    query_item["columns"] = params_dict.get("all_columns")

                # 规则: Pie 图表必须设置 orderby
                if metric_name and viz_type == "pie":
                    query_item["orderby"] = [[metric_name, False]]

            # 处理 adhoc_filters
            if params_dict.get("adhoc_filters"):
                for adhoc_filter in params_dict.get("adhoc_filters"):
                    if adhoc_filter.get("operator") == "TEMPORAL_RANGE":
                        query_item["filters"].append({
                            "col": adhoc_filter.get("subject"),
                            "op": "TEMPORAL_RANGE",
                            "val": adhoc_filter.get("comparator", "No filter")
                        })

            # 处理 row_limit
            query_item["row_limit"] = params_dict.get("row_limit", 10000 if viz_type == "echarts_timeseries_line" else 100)

            # 构建 query_context
            query_context = {
                "datasource": {
                    "id": datasource_id,
                    "type": "table"
                },
                "force": False,
                "queries": [query_item],
                "form_data": params_dict.copy(),
                "result_format": "json",
                "result_type": "full"
            }

            # 将完整的 params 序列化为 JSON 字符串
            import json
            payload["params"] = json.dumps(params_dict, ensure_ascii=False)
            payload["query_context"] = json.dumps(query_context, ensure_ascii=False)

        response = self._request("PUT", f"/api/v1/chart/{chart_id}", json=payload)

        if response.status_code == 200:
            return response.json().get("result")
        logger.error(f"更新图表失败: {response.status_code} - {response.text}")
        return None

    def delete_chart(self, chart_id: int) -> bool:
        """删除图表"""
        response = self._request("DELETE", f"/api/v1/chart/{chart_id}")
        return response.status_code == 204

    # ==================== Dataset API ====================

    def get_databases(self) -> List[Dict[str, Any]]:
        """获取所有数据库连接"""
        response = self._request("GET", "/api/v1/database/")
        if response.status_code == 200:
            return response.json().get("result", [])
        return []

    def get_database_by_name(self, database_name: str) -> Optional[Dict[str, Any]]:
        """根据名称获取数据库连接

        Args:
            database_name: 数据库名称

        Returns:
            数据库信息，未找到返回 None
        """
        databases = self.get_databases()
        for db in databases:
            if db.get("database_name") == database_name or db.get("name") == database_name:
                return db
        return None

    def get_database_tables(self, database_id: int) -> List[Dict[str, Any]]:
        """获取数据库中的所有表

        Args:
            database_id: 数据库ID

        Returns:
            表列表
        """
        response = self._request("GET", f"/api/v1/database/{database_id}/tables/")
        if response.status_code == 200:
            return response.json().get("result", [])
        return []

    def get_table_info(self, database_id: int, schema: str, table_name: str) -> Optional[Dict[str, Any]]:
        """获取表的详细信息

        Args:
            database_id: 数据库ID
            schema: schema名称
            table_name: 表名

        Returns:
            表信息，未找到返回 None
        """
        tables = self.get_database_tables(database_id)
        for table in tables:
            if (table.get("schema") == schema and
                table.get("table_name") == table_name):
                return table
        return None

    def get_database_id(self, database_name: str = None) -> Optional[int]:
        """获取数据库ID

        Args:
            database_name: 数据库名称，如果为None则使用第一个数据库

        Returns:
            数据库ID，未找到返回 None
        """
        if database_name:
            db = self.get_database_by_name(database_name)
            if db:
                return db.get("id")
            logger.error(f"数据库 '{database_name}' 未找到")
            return None
        else:
            db = self.get_first_database()
            if db:
                return db.get("id")
            logger.error("没有找到可用的数据库连接")
            return None

    def _infer_schema_from_table_name(self, table_name: str) -> Optional[str]:
        """根据表名前缀和 schema_map 推断 dbt 的 schema

        Args:
            table_name: 表名

        Returns:
            推断的 schema 名称，无法推断返回 None
        """
        # 优先级 1: 使用从 dbt profiles 读取的 default_schema
        if self.default_schema:
            return self.default_schema

        # 优先级 2: 使用 schema_map 中的配置
        for layer, schema in self.schema_map.items():
            # 检查表名是否以层名称开头（如 "ods_xxx" 对应 layer "ods"）
            if table_name.startswith(f"{layer}_"):
                return schema
            # 检查表名是否以完整层名开头（如 "ods_xxx" 对应 layer "ods"）
            if "_" in table_name:
                first_segment = table_name.split("_")[0]
                if first_segment == layer:
                    return schema

        return None

    def find_table_schema(self, table_name: str, database_id: int) -> Optional[str]:
        """在数据库中查找表所在的 schema

        Args:
            table_name: 表名
            database_id: 数据库ID

        Returns:
            找到的 schema 名称，未找到返回 None
        """
        # 先根据表名前缀推断 schema
        inferred_schema = self._infer_schema_from_table_name(table_name)
        if inferred_schema:
            # 尝试使用推断的 schema
            payload = {
                "table_name": table_name,
                "database": database_id,
                "schema": inferred_schema
            }
            check_resp = self._request("POST", "/api/v1/dataset/", json=payload)
            if check_resp.status_code == 201:
                # 成功创建，返回 schema，并删除这个临时 dataset
                dataset_id = check_resp.json().get("id")
                self._request("DELETE", f"/api/v1/dataset/{dataset_id}")
                return inferred_schema

        # 尝试获取 schemas 列表进行自动发现
        response = self._request("GET", f"/api/v1/database/{database_id}/schemas/")
        if response.status_code == 200:
            schemas = response.json().get("result", [])
            # 按优先级尝试每个 schema
            for schema in schemas:
                # 尝试在该 schema 中创建/获取表
                payload = {
                    "table_name": table_name,
                    "database": database_id,
                    "schema": schema
                }
                check_resp = self._request("POST", "/api/v1/dataset/", json=payload)
                if check_resp.status_code == 201:
                    # 成功创建，返回 schema，并删除这个临时 dataset
                    dataset_id = check_resp.json().get("id")
                    self._request("DELETE", f"/api/v1/dataset/{dataset_id}")
                    return schema
        return None

    def create_dataset(
        self,
        table_name: str,
        database_id: Optional[int] = None,
        schema: Optional[str] = None,
    ) -> Optional[Dict[str, Any]]:
        """创建数据集

        Args:
            table_name: 表名
            database_id: 数据库ID，如果为None则使用配置的数据库名称或第一个数据库
            schema: schema名称，如果为None则自动查找

        Returns:
            创建的数据集信息

        Schema 查找优先级：
        1. 显式指定的 schema 参数（最高优先级）
        2. 根据表名前缀从 schema_map 推断
        3. 常见的默认 schema（public, main, default 等）
        4. 自动遍历数据库所有 schema 尝试查找
        5. 如果所有方式都失败，报错
        """
        # 获取最新的 CSRF token
        self._get_csrf_token()

        # 如果没有指定数据库，使用配置的数据库名称或第一个
        if database_id is None:
            database_id = self.get_database_id(self.database_name)
            if database_id is None:
                return None

        # 如果没有指定 schema，自动查找
        if schema is None:
            # 优先级 2: 根据表名前缀从 schema_map 推断
            inferred_schema = self._infer_schema_from_table_name(table_name)
            if inferred_schema:
                # 先检查该 schema 下是否存在该表
                table_info = self.get_table_info(database_id, inferred_schema, table_name)
                if table_info:
                    # 表存在，使用推断的 schema
                    payload = {
                        "table_name": table_name,
                        "database": database_id,
                        "schema": inferred_schema
                    }
                    response = self._request("POST", "/api/v1/dataset/", json=payload)
                    if response.status_code == 201:
                        result = response.json()
                        logger.info(f"创建数据集成功: {table_name} (schema: {inferred_schema}, ID: {result.get('id')})")
                        return result

                logger.debug(f"推断的 schema '{inferred_schema}' 下表不存在，尝试其他方式")

            # 优先级 3: 尝试常见的默认 schema
            default_schemas = ["public", "main", "default"]
            for s in default_schemas:
                table_info = self.get_table_info(database_id, s, table_name)
                if table_info:
                    payload = {
                        "table_name": table_name,
                        "database": database_id,
                        "schema": s
                    }
                    response = self._request("POST", "/api/v1/dataset/", json=payload)
                    if response.status_code == 201:
                        result = response.json()
                        logger.info(f"创建数据集成功: {table_name} (schema: {s}, ID: {result.get('id')})")
                        return result

            # 优先级 4: 自动遍历数据库所有 schema 尝试查找
            schema = self.find_table_schema(table_name, database_id)
            if schema:
                payload = {
                    "table_name": table_name,
                    "database": database_id,
                    "schema": schema
                }
                response = self._request("POST", "/api/v1/dataset/", json=payload)
                if response.status_code == 201:
                    result = response.json()
                    logger.info(f"创建数据集成功: {table_name} (schema: {schema}, ID: {result.get('id')})")
                    return result

            # 优先级 5: 所有方式都失败，报错
            logger.error(
                f"无法创建数据集 '{table_name}："
                f"1. 指定的 schema 参数: {schema if schema else '未指定'}"
                f"2. 表名前缀推断: {inferred_schema if inferred_schema else '未推断到'}"
                f"3. 常见默认 schema: 未找到表"
                f"4. 数据库所有 schema: 未找到表"
                f"请检查: (1) 表是否存在于数据库中 (2) --schema 参数是否正确"
            )
            return None

        # 使用指定的 schema
        payload = {
            "table_name": table_name,
            "database": database_id,
            "schema": schema,
        }

        response = self._request("POST", "/api/v1/dataset/", json=payload)

        if response.status_code == 201:
            result = response.json()
            logger.info(f"创建数据集成功: {table_name} (ID: {result.get('id')})")
            return result
        else:
            logger.error(f"创建数据集失败: {response.status_code} - {response.text[:200]}")
            return None

    def get_datasets(self, use_cache: bool = True) -> List[Dict[str, Any]]:
        """获取所有数据集（使用多列排序 + ID 遍历补充 + 缓存）

        Args:
            use_cache: 是否使用缓存，默认True。设为False强制刷新。
        """
        import time

        # 检查缓存
        if use_cache and self._datasets_cache is not None:
            if self._cache_time and (time.time() - self._cache_time) < self.cache_ttl:
                logger.debug(f"使用缓存的数据集列表: {len(self._datasets_cache)} 个")
                return self._datasets_cache

        # 使用 ID 去重
        dataset_by_id = {}
        page_size = 100  # 增大分页大小，减少请求次数

        # 第一步：使用多个排序列获取数据集列表
        # Superset API 的排序字段可能为空，导致某些数据集不在结果中
        # 使用多个排序列可以获取更多数据集
        order_columns = ['changed_on_delta_humanized', 'table_name', 'id']

        for order_col in order_columns:
            logger.debug(f"使用 order_column={order_col} 获取数据集")
            page = 1
            while page <= 20:  # 增加页数上限
                q_param = f"(order_column:{order_col},order_direction:asc,page:{page},page_size:{page_size})"
                params = {"q": q_param}
                response = self._request("GET", "/api/v1/dataset/", params=params)

                if response.status_code != 200:
                    break

                data = response.json()
                result = data.get("result", [])

                if not result:
                    break

                # 使用 ID 去重
                new_count = 0
                for ds in result:
                    ds_id = ds.get("id")
                    if ds_id is not None and ds_id not in dataset_by_id:
                        dataset_by_id[ds_id] = ds
                        new_count += 1

                logger.debug(f"  第 {page} 页: {len(result)} 条, 新增 {new_count}, 累计 {len(dataset_by_id)} 个")

                if new_count == 0:
                    break

                page += 1

        # 第二步：如果列表 API 获取的数量少于 count，通过 ID 遍历补充
        response = self._request("GET", "/api/v1/dataset/")
        data = response.json()
        api_count = data.get("count", 0)

        if len(dataset_by_id) < api_count and dataset_by_id:
            missing_count = api_count - len(dataset_by_id)
            logger.info(f"列表 API 获取到 {len(dataset_by_id)} 个，缺少 {missing_count} 个，通过 ID 遍历补充...")
            existing_ids = set(dataset_by_id.keys())

            # 优化：只遍历可能缺失的ID范围
            # 使用更紧凑的搜索范围：从最小ID到最大ID
            search_min = min(existing_ids)
            search_max = max(existing_ids)

            # 批量查询优化：收集缺失的ID，分批查询
            missing_ids = [ds_id for ds_id in range(search_min, search_max + 1) if ds_id not in existing_ids]

            if missing_ids:
                logger.debug(f"需要检查 {len(missing_ids)} 个可能缺失的ID")
                found_count = 0
                for ds_id in missing_ids:
                    ds = self.get_dataset(ds_id)
                    if ds:
                        dataset_by_id[ds_id] = ds
                        found_count += 1
                logger.debug(f"ID遍历补充找到 {found_count} 个数据集")

        # 转换为列表并按 ID 排序
        all_datasets = sorted(dataset_by_id.values(), key=lambda x: x.get("id", 0))

        # 更新缓存
        self._datasets_cache = all_datasets
        self._datasets_by_name = {ds.get("table_name"): ds for ds in all_datasets if ds.get("table_name")}
        self._cache_time = time.time()

        logger.info(f"获取到 {len(all_datasets)} 个去重数据集")
        return all_datasets

    def get_dataset(self, dataset_id: int) -> Optional[Dict[str, Any]]:
        """获取单个数据集详情（包含列信息）"""
        response = self._request("GET", f"/api/v1/dataset/{dataset_id}")
        if response.status_code == 200:
            return response.json().get("result")
        return None

    def fetch_columns_from_database(self, table_name: str, schema: str, database_name: str = None) -> Optional[List[Dict[str, Any]]]:
        """直接从数据库获取表的列信息

        使用 Superset 的 /api/v1/database/{id}/table_metadata/ API 获取列信息。
        该 API 支持 JWT token 认证。

        Args:
            table_name: 表名
            schema: schema 名称
            database_name: 数据库名称（可选，默认使用配置中的数据库）

        Returns:
            列信息列表，失败返回 None
        """
        if not database_name:
            database_name = self.database_name

        if not database_name:
            logger.warning("未配置数据库名称，无法获取列信息")
            return None

        logger.debug(f"从数据库获取列信息: {database_name}.{schema}.{table_name}")

        # 1. 获取数据库 ID
        db_id = self._get_database_id(database_name)
        if not db_id:
            logger.warning(f"未找到数据库: {database_name}")
            return None

        # 2. 调用 table_metadata API 获取列信息
        params = {
            "name": table_name,
            "schema": schema,
        }

        response = self._request("GET", f"/api/v1/database/{db_id}/table_metadata/", params=params)

        if response.status_code == 200:
            data = response.json()
            columns = data.get("columns", [])
            if columns:
                # 转换为标准格式
                result = []
                for col in columns:
                    result.append({
                        "name": col.get("name"),
                        "type": col.get("type"),
                        "longType": col.get("longType"),
                        "comment": col.get("comment"),
                    })
                logger.debug(f"获取到 {len(result)} 列")
                return result

        logger.warning(f"获取列信息失败: {response.status_code}, 响应: {response.text[:200]}")
        return None

    def _get_database_id(self, database_name: str) -> Optional[int]:
        """获取数据库 ID

        Args:
            database_name: 数据库名称

        Returns:
            数据库 ID，未找到返回 None
        """
        params = {
            "q": json.dumps({"filters": [{"col": "database_name", "opr": "eq", "value": database_name}]})
        }
        response = self._request("GET", "/api/v1/database/", params=params)

        if response.status_code == 200:
            results = response.json().get("result", [])
            if results:
                return results[0].get("id")
        return None

    def refresh_dataset(self, dataset_id: int) -> bool:
        """刷新数据集，从数据库同步列信息

        Args:
            dataset_id: 数据集 ID

        Returns:
            是否刷新成功
        """
        # 尝试多种方式刷新数据集
        # 方式1: POST /api/v1/dataset/{id}/refresh
        response = self._request("POST", f"/api/v1/dataset/{dataset_id}/refresh")
        if response.status_code == 200:
            logger.debug(f"刷新数据集成功: {dataset_id}")
            return True

        # 方式2: PUT /api/v1/dataset/{id} 空请求体（部分 Superset 版本）
        if response.status_code == 405:
            logger.debug(f"POST /refresh 返回 405，尝试 PUT 方式...")
            # 获取新的 CSRF token
            csrf_resp = self._request("GET", "/api/v1/security/csrf_token/")
            if csrf_resp.status_code == 200:
                csrf_token = csrf_resp.json().get("result")
                if csrf_token:
                    self.csrf_token = csrf_token
                    logger.debug(f"获取到新的 CSRF token")

            # 先获取当前数据集信息
            current = self.get_dataset(dataset_id)
            if current:
                # 只更新必要字段，触发列刷新
                payload = {
                    "schema": current.get("schema"),
                    "table_name": current.get("table_name"),
                }
                response = self._request("PUT", f"/api/v1/dataset/{dataset_id}", json=payload)
                if response.status_code == 200:
                    logger.debug(f"刷新数据集成功(PUT): {dataset_id}")
                    return True

        # 打印错误详情
        logger.warning(f"刷新数据集失败: {response.status_code}, 响应: {response.text[:500]}")
        return False

    def find_dataset_by_name(self, table_name: str, schema: str = None) -> Optional[Dict[str, Any]]:
        """直接通过名称搜索数据集（不获取全部列表）

        使用 Superset API 的 filters 功能直接搜索，避免获取所有数据集列表。

        Args:
            table_name: 数据集名称（表名）
            schema: 可选的 schema 名称，用于精确匹配

        Returns:
            数据集详情（包含列信息），未找到返回 None
        """
        # 构建 filters
        filters = [{"col": "table_name", "opr": "eq", "value": table_name}]
        if schema:
            filters.append({"col": "schema", "opr": "eq", "value": schema})

        # 使用 filters 直接搜索
        params = {
            "q": f'{{"filters":{json.dumps(filters)}}}'
        }
        logger.debug(f"查找数据集: table_name={table_name}, schema={schema}, filters={filters}")
        response = self._request("GET", "/api/v1/dataset/", params=params)

        if response.status_code == 200:
            data = response.json()
            results = data.get("result", [])
            logger.debug(f"找到 {len(results)} 个匹配的数据集")
            if results:
                # 获取完整的数据集详情（包含列信息）
                dataset_id = results[0].get("id")
                actual_schema = results[0].get("schema")
                logger.debug(f"匹配到数据集 ID: {dataset_id}, schema: {actual_schema}")
                if dataset_id:
                    return self.get_dataset(dataset_id)
        return None

    def get_dataset_by_name(self, name: str, schema: str = None, use_cache: bool = True) -> Optional[Dict[str, Any]]:
        """通过名称获取数据集（包含列信息）

        Args:
            name: 数据集名称（表名）
            schema: 可选的 schema 名称，用于精确匹配
            use_cache: 是否使用缓存
        """
        # 优先使用名称缓存（仅在未指定 schema 时使用缓存）
        if use_cache and schema is None and self._datasets_by_name is not None:
            if name in self._datasets_by_name:
                dataset = self._datasets_by_name[name]
                dataset_id = dataset.get("id")
                if dataset_id:
                    return self.get_dataset(dataset_id)

        # 直接通过名称搜索，避免获取全部列表
        dataset = self.find_dataset_by_name(name, schema=schema)
        if dataset:
            return dataset

        return None

    def get_or_create_dataset(self, table_name: str, schema: str = None) -> Optional[Dict[str, Any]]:
        """获取或创建数据集

        如果数据集存在则返回，不存在则创建

        Args:
            table_name: 表名
            schema: 指定数据集的 schema 名称（优先级最高）

        Returns:
            数据集信息（包含 columns）
        """
        # 先尝试获取（传递 schema 进行精确匹配）
        dataset = self.get_dataset_by_name(table_name, schema=schema)
        if dataset:
            dataset_id = dataset.get("id")
            columns = dataset.get("columns", [])
            actual_schema = dataset.get("schema") or schema

            # 如果数据集存在但没有列信息，尝试从数据库获取
            if not columns and dataset_id and actual_schema:
                logger.info(f"数据集 {table_name} (ID: {dataset_id}) 没有列信息，从数据库获取...")

                # 使用 external_metadata_by_name API 获取列信息
                db_columns = self.fetch_columns_from_database(table_name, actual_schema)
                if db_columns:
                    logger.info(f"从数据库获取到 {len(db_columns)} 列，更新数据集...")
                    # 更新数据集的列信息
                    if self._update_dataset_columns(dataset_id, db_columns):
                        # 重新获取完整信息
                        refreshed_dataset = self.get_dataset(dataset_id)
                        if refreshed_dataset:
                            return refreshed_dataset
                else:
                    logger.warning(f"无法从数据库获取列信息")

            return dataset

        # 不存在则创建
        logger.info(f"数据集 {table_name} 不存在，尝试创建...")
        result = self.create_dataset(table_name, schema=schema)

        # 关键修复：创建后刷新并重新获取完整信息（包含 columns）
        # POST API 返回的结果不包含 columns，需要刷新后通过 GET 获取
        if result:
            dataset_id = result.get("id")
            if dataset_id:
                # 使用 external_metadata_by_name API 获取列信息
                actual_schema = schema or self.default_schema
                if actual_schema:
                    db_columns = self.fetch_columns_from_database(table_name, actual_schema)
                    if db_columns:
                        self._update_dataset_columns(dataset_id, db_columns)
                # 重新获取完整信息
                full_dataset = self.get_dataset(dataset_id)
                if full_dataset:
                    return full_dataset

        return result

    def _update_dataset_columns(self, dataset_id: int, columns: List[Dict[str, Any]]) -> bool:
        """更新数据集的列信息

        Args:
            dataset_id: 数据集 ID
            columns: 列信息列表

        Returns:
            是否更新成功
        """
        # 获取 CSRF token
        csrf_resp = self._request("GET", "/api/v1/security/csrf_token/")
        if csrf_resp.status_code == 200:
            csrf_token = csrf_resp.json().get("result")
            if csrf_token:
                self.csrf_token = csrf_token

        # 转换列格式为 Superset 需要的格式
        formatted_columns = []
        for col in columns:
            formatted_columns.append({
                "column_name": col.get("name"),
                "type": col.get("type"),
                "is_dttm": col.get("is_dttm", False),
                "description": col.get("description", ""),
            })

        payload = {"columns": formatted_columns}
        response = self._request("PUT", f"/api/v1/dataset/{dataset_id}", json=payload)

        if response.status_code == 200:
            logger.debug(f"更新数据集列信息成功: {dataset_id}")
            return True
        else:
            logger.warning(f"更新数据集列信息失败: {response.status_code}, 响应: {response.text[:200]}")
            return False

    # ==================== User API ====================

    def get_users(self) -> List[Dict[str, Any]]:
        """获取所有用户"""
        response = self._request("GET", "/api/v1/security/users/")
        if response.status_code == 200:
            result = response.json()
            if "result" in result:
                return result["result"]
            if isinstance(result, list):
                return result
        return []

    def get_current_user(self) -> Optional[Dict[str, Any]]:
        """获取当前用户信息"""
        response = self._request("GET", "/api/v1/me/")

        if response.status_code == 200:
            data = response.json()

            if "result" in data:
                return data["result"]

            if isinstance(data, dict) and "id" in data:
                return data

            if "user" in data:
                return data["user"]

        users = self.get_users()
        if users:
            return users[0]

        return None

    @classmethod
    def create_from_config(cls, config: "SupersetConfig", schema_map: Dict[str, str] = None, default_schema: str = None) -> "SupersetClient":
        """从配置创建客户端"""
        client = cls(
            base_url=config.base_url,
            verify_ssl=config.verify_ssl,
            schema_map=schema_map or {},
            database_name=getattr(config, 'database', None),
            default_schema=default_schema,
        )
        client.login(config.username, config.password, config.provider)
        return client