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

    def get_first_database(self) -> Optional[Dict[str, Any]]:
        """获取第一个数据库连接"""
        databases = self.get_databases()
        if databases:
            return databases[0]
        return None

    def find_table_schema(self, table_name: str, database_id: int) -> Optional[str]:
        """在数据库中查找表所在的 schema

        Args:
            table_name: 表名
            database_id: 数据库ID

        Returns:
            找到的 schema 名称，未找到返回 None
        """
        # 尝试获取 schemas 列表
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
            database_id: 数据库ID，如果为None则使用第一个数据库
            schema: schema名称，如果为None则自动查找

        Returns:
            创建的数据集信息
        """
        # 获取最新的 CSRF token
        self._get_csrf_token()

        # 如果没有指定数据库，使用第一个
        if database_id is None:
            db = self.get_first_database()
            if not db:
                logger.error("没有找到可用的数据库连接")
                return None
            database_id = db.get("id")

        # 如果没有指定 schema，自动查找
        if schema is None:
            # 先尝试常见的 schema
            common_schemas = ["public", "dev_dbt_demo", "main", "default"]
            for s in common_schemas:
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

            # 常见 schema 都不行，尝试自动发现
            schema = self.find_table_schema(table_name, database_id)
            if not schema:
                logger.error(f"无法找到表 {table_name} 所在的 schema")
                return None

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
        """获取所有数据集"""
        response = self._request("GET", "/api/v1/dataset/")
        if response.status_code == 200:
            return response.json().get("result", [])
        return []

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

    def get_or_create_dataset(self, table_name: str) -> Optional[Dict[str, Any]]:
        """获取或创建数据集

        如果数据集存在则返回，不存在则创建

        Args:
            table_name: 表名

        Returns:
            数据集信息
        """
        # 先尝试获取
        dataset = self.get_dataset_by_name(table_name)
        if dataset:
            return dataset

        # 不存在则创建
        logger.info(f"数据集 {table_name} 不存在，尝试创建...")
        return self.create_dataset(table_name)

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
    def create_from_config(cls, config: "SupersetConfig") -> "SupersetClient":
        """从配置创建客户端"""
        client = cls(base_url=config.base_url, verify_ssl=config.verify_ssl)
        client.login(config.username, config.password, config.provider)
        return client