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
        positions: Optional[List[Dict[str, Any]]] = None,
    ) -> Optional[Dict[str, Any]]:
        """更新面板"""
        # 获取最新的CSRF token
        self._get_csrf_token()

        payload = {}
        if title is not None:
            payload["dashboard_title"] = title
        if description is not None:
            payload["description"] = description
        # 注意：Superset 6.0 API 不支持直接通过 charts 字段关联图表
        # 图表关联需要在创建图表时通过 dashboards 参数指定
        if positions is not None:
            # 构建标准的Superset position_json格式
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
            payload["position_json"] = json.dumps(position_json)

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
        # Superset 6.0 使用 slice_name 而不是 title
        payload = {
            "datasource_id": datasource_id,
            "datasource_type": "table",
            "viz_type": viz_type,
            "slice_name": title,  # 使用 slice_name 而不是 title
            "description": description,
        }

        if params is not None:
            payload["params"] = params

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
    ) -> Optional[Dict[str, Any]]:
        """更新图表"""
        payload = {}
        if title is not None:
            payload["slice_name"] = title  # 使用 slice_name 而不是 title
        if description is not None:
            payload["description"] = description
        if params is not None:
            payload["params"] = params

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
        # 优先使用 schema_map 中的配置
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

    def get_datasets(self) -> List[Dict[str, Any]]:
        """获取所有数据集（使用多列排序 + ID 遍历补充）"""
        # 使用 ID 去重
        dataset_by_id = {}
        page_size = 25

        # 第一步：使用多个排序列获取数据集列表
        # Superset API 的排序字段可能为空，导致某些数据集不在结果中
        # 使用多个排序列可以获取更多数据集
        order_columns = ['changed_on_delta_humanized', 'table_name']

        for order_col in order_columns:
            logger.debug(f"使用 order_column={order_col} 获取数据集")
            page = 1
            while page <= 10:
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
            logger.info(f"列表 API 获取到 {len(dataset_by_id)} 个，通过 ID 遍历补充...")
            existing_ids = set(dataset_by_id.keys())
            search_min = 1
            search_max = max(existing_ids) + 100

            for ds_id in range(search_min, search_max + 1):
                if ds_id not in existing_ids:
                    ds = self.get_dataset(ds_id)
                    if ds:
                        dataset_by_id[ds_id] = ds

        # 转换为列表并按 ID 排序
        all_datasets = sorted(dataset_by_id.values(), key=lambda x: x.get("id", 0))
        logger.info(f"获取到 {len(all_datasets)} 个去重数据集")
        return all_datasets

    def get_dataset(self, dataset_id: int) -> Optional[Dict[str, Any]]:
        """获取单个数据集详情（包含列信息）"""
        response = self._request("GET", f"/api/v1/dataset/{dataset_id}")
        if response.status_code == 200:
            return response.json().get("result")
        return None

    def get_dataset_by_name(self, name: str) -> Optional[Dict[str, Any]]:
        """通过名称获取数据集（包含列信息）"""
        # 先获取数据集列表
        datasets = self.get_datasets()
        for dataset in datasets:
            if dataset.get("table_name") == name:
                # 获取完整的数据集详情（包含列信息）
                dataset_id = dataset.get("id")
                return self.get_dataset(dataset_id)
        return None

    def get_or_create_dataset(self, table_name: str, schema: str = None) -> Optional[Dict[str, Any]]:
        """获取或创建数据集

        如果数据集存在则返回，不存在则创建

        Args:
            table_name: 表名
            schema: 指定数据集的 schema 名称（优先级最高）

        Returns:
            数据集信息
        """
        # 先尝试获取
        dataset = self.get_dataset_by_name(table_name)
        if dataset:
            return dataset

        # 不存在则创建
        logger.info(f"数据集 {table_name} 不存在，尝试创建...")
        return self.create_dataset(table_name, schema=schema)

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
    def create_from_config(cls, config: "SupersetConfig", schema_map: Dict[str, str] = None) -> "SupersetClient":
        """从配置创建客户端"""
        client = cls(
            base_url=config.base_url,
            verify_ssl=config.verify_ssl,
            schema_map=schema_map or {},
            database_name=getattr(config, 'database', None),
        )
        client.login(config.username, config.password, config.provider)
        return client