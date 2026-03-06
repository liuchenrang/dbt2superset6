"""配置管理模块

处理Superset API配置和dbt项目配置
"""

import os
from dataclasses import dataclass, field
from typing import Optional, Dict
from pathlib import Path

import yaml


@dataclass
class SupersetConfig:
    """Superset API配置"""
    base_url: str
    username: str
    password: str
    provider: str = "db"
    verify_ssl: bool = True
    database: Optional[str] = None  # 数据库名称

    @classmethod
    def from_env(cls) -> "SupersetConfig":
        """从环境变量加载配置"""
        return cls(
            base_url=os.getenv("SUPERSET_BASE_URL", "http://localhost:8088"),
            username=os.getenv("SUPERSET_USERNAME", "admin"),
            password=os.getenv("SUPERSET_PASSWORD", "admin"),
            provider=os.getenv("SUPERSET_PROVIDER", "db"),
            verify_ssl=os.getenv("SUPERSET_VERIFY_SSL", "true").lower() == "true",
            database=os.getenv("SUPERSET_DATABASE"),
        )

    @classmethod
    def from_file(cls, config_path: Optional[str] = None) -> "SupersetConfig":
        """从配置文件加载配置"""
        if config_path is None:
            config_path = os.getenv("SUPERSET_CONFIG_PATH", "super6dbt/config.yml")

        with open(config_path, "r") as f:
            config = yaml.safe_load(f)

        superset_config = config.get("superset", {})
        return cls(**superset_config)


@dataclass
class DbtProjectConfig:
    """dbt项目配置"""
    project_dir: Path
    model_paths: list[str]
    exposure_paths: list[str]
    schema_map: Dict[str, str] = field(default_factory=dict)

    @classmethod
    def from_project_dir(cls, project_dir: str) -> "DbtProjectConfig":
        """从dbt项目目录读取配置"""
        project_path = Path(project_dir)
        dbt_project_yml = project_path / "dbt_project.yml"

        if not dbt_project_yml.exists():
            raise FileNotFoundError(f"dbt_project.yml not found in {project_dir}")

        with open(dbt_project_yml, "r") as f:
            dbt_config = yaml.safe_load(f)

        # 从 dbt_project.yml 的 models 配置中读取分层 schema 映射
        schema_map = {}
        models_config = dbt_config.get("models", {})

        # 递归遍历 models 配置，查找 +schema
        def _extract_schema_map(config, prefix=""):
            for key, value in config.items():
                if key.startswith("+"):
                    continue
                full_key = f"{prefix}_{key}" if prefix else key
                if isinstance(value, dict):
                    # 检查是否有 +schema
                    if "+schema" in value:
                        schema_map[full_key] = value["+schema"]
                    # 递归处理嵌套配置
                    _extract_schema_map(value, full_key)

        _extract_schema_map(models_config)

        return cls(
            project_dir=project_path,
            model_paths=dbt_config.get("model-paths", ["models"]),
            exposure_paths=dbt_config.get("exposure-paths", ["models/exposures"]),
            schema_map=schema_map,
        )

    @property
    def full_model_paths(self) -> list[Path]:
        """获取完整的模型路径"""
        return [self.project_dir / path for path in self.model_paths]

    @property
    def full_exposure_paths(self) -> list[Path]:
        """获取完整的exposure路径"""
        return [self.project_dir / path for path in self.exposure_paths]


@dataclass
class Config:
    """完整配置"""
    superset: SupersetConfig
    dbt: DbtProjectConfig

    @classmethod
    def load(cls, dbt_project_dir: str) -> "Config":
        """加载配置

        配置优先级：
        1. 环境变量（最高优先级）
        2. 家目录配置文件 ~/.super6dbt/config.yml
        3. 默认值（最低优先级）
        """
        # 检查家目录配置文件
        home_config_path = Path.home() / ".super6dbt" / "config.yml"

        # 检查是否所有必要的环境变量都设置了
        env_vars_set = all([
            os.getenv("SUPERSET_BASE_URL"),
            os.getenv("SUPERSET_USERNAME"),
            os.getenv("SUPERSET_PASSWORD"),
        ])

        if env_vars_set:
            # 使用环境变量配置
            superset_config = SupersetConfig.from_env()
        elif home_config_path.exists():
            # 使用家目录配置文件
            superset_config = SupersetConfig.from_file(str(home_config_path))
        else:
            # 使用环境变量（会使用默认值）
            superset_config = SupersetConfig.from_env()

        dbt_config = DbtProjectConfig.from_project_dir(dbt_project_dir)

        return cls(superset=superset_config, dbt=dbt_config)