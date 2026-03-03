"""配置管理模块

处理Superset API配置和dbt项目配置
"""

import os
from dataclasses import dataclass
from typing import Optional
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

    @classmethod
    def from_env(cls) -> "SupersetConfig":
        """从环境变量加载配置"""
        return cls(
            base_url=os.getenv("SUPERSET_BASE_URL", "http://localhost:8088"),
            username=os.getenv("SUPERSET_USERNAME", "admin"),
            password=os.getenv("SUPERSET_PASSWORD", "admin"),
            provider=os.getenv("SUPERSET_PROVIDER", "db"),
            verify_ssl=os.getenv("SUPERSET_VERIFY_SSL", "true").lower() == "true",
        )

    @classmethod
    def from_file(cls, config_path: Optional[str] = None) -> "SupersetConfig":
        """从配置文件加载配置"""
        if config_path is None:
            config_path = os.getenv("SUPERSET_CONFIG_PATH", "super6dbt/config.yml")

        with open(config_path, "r") as f:
            config = yaml.safe_load(f)

        return cls(**config["superset"])


@dataclass
class DbtProjectConfig:
    """dbt项目配置"""
    project_dir: Path
    model_paths: list[str]
    exposure_paths: list[str]

    @classmethod
    def from_project_dir(cls, project_dir: str) -> "DbtProjectConfig":
        """从dbt项目目录读取配置"""
        project_path = Path(project_dir)
        dbt_project_yml = project_path / "dbt_project.yml"

        if not dbt_project_yml.exists():
            raise FileNotFoundError(f"dbt_project.yml not found in {project_dir}")

        with open(dbt_project_yml, "r") as f:
            dbt_config = yaml.safe_load(f)

        return cls(
            project_dir=project_path,
            model_paths=dbt_config.get("model-paths", ["models"]),
            exposure_paths=dbt_config.get("exposure-paths", ["models/exposures"]),
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