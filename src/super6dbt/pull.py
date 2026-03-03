"""Pull功能：从Superset拉取配置到dbt"""

from pathlib import Path
from typing import Dict, Any, List
import yaml
import logging

from .client import SupersetClient
from .mapper import SupersetToDbt
from .config import DbtProjectConfig

logger = logging.getLogger(__name__)


class SupersetPuller:
    """从Superset拉取配置到dbt"""

    def __init__(
        self,
        client: SupersetClient,
        dbt_config: DbtProjectConfig,
    ):
        self.client = client
        self.dbt_config = dbt_config
        self.mapper = SupersetToDbt()
        self.dataset_map: Dict[int, Dict[str, Any]] = {}

    def pull(self, dashboard_ids: List[int] = None) -> None:
        """拉取配置

        Args:
            dashboard_ids: 要拉取的面板ID列表，None表示拉取所有
        """
        logger.info("开始从Superset拉取配置...")

        # 1. 获取所有数据集建立映射
        self._load_datasets()

        # 2. 获取面板
        dashboards = self.client.get_dashboards()
        if dashboard_ids:
            dashboards = [d for d in dashboards if d["id"] in dashboard_ids]

        logger.info(f"找到 {len(dashboards)} 个面板")

        # 3. 转换为exposures
        exposures = []
        for dashboard in dashboards:
            exposure = self.mapper.dashboard_to_exposure(dashboard, self.dataset_map)
            exposures.append(exposure)

        # 4. 写入exposures文件
        self._write_exposures(exposures)

        # 5. 更新模型的meta信息
        self._update_model_meta()

        logger.info(f"成功拉取 {len(exposures)} 个exposures")

    def _load_datasets(self) -> None:
        """加载数据集映射"""
        datasets = self.client.get_datasets()
        logger.info(f"加载 {len(datasets)} 个数据集")

        for dataset in datasets:
            self.dataset_map[dataset["id"]] = dataset

    def _write_exposures(self, exposures: List[Dict[str, Any]]) -> None:
        """写入exposures YAML文件"""
        exposure_paths = self.dbt_config.full_exposure_paths

        # 确保目录存在
        for path in exposure_paths:
            path.mkdir(parents=True, exist_ok=True)

        # 写入每个exposure到单独的文件
        for exposure in exposures:
            filename = f"{exposure['name']}.yml"
            file_path = exposure_paths[0] / filename

            yaml_content = self.mapper.generate_yaml_exposure(exposure)

            with open(file_path, "w", encoding="utf-8") as f:
                f.write(yaml_content)

            logger.info(f"写入exposure文件: {file_path}")

    def _update_model_meta(self) -> None:
        """更新模型的meta信息

        从数据集中提取列信息，更新到对应的model schema.yml文件中
        确保初始化现有 models 的 dimension 和 metrics 配置
        """
        model_paths = self.dbt_config.full_model_paths
        schema_files = []

        # 查找所有schema.yml文件
        for model_path in model_paths:
            for pattern in ["**/*.yml", "**/*.yaml"]:
                for file_path in model_path.glob(pattern):
                    # 包含 models 定义的文件
                    if file_path.exists():
                        try:
                            with open(file_path, "r", encoding="utf-8") as f:
                                content = f.read()
                            if content.strip() and "models:" in content:
                                schema_files.append(file_path)
                        except Exception:
                            pass

        logger.info(f"找到 {len(schema_files)} 个schema文件")

        # 为每个数据集生成meta配置
        dataset_meta_map: Dict[str, Dict[str, Any]] = {}

        for dataset in self.dataset_map.values():
            meta = self.mapper.dataset_to_model_meta(dataset)
            table_name = meta["name"]
            dataset_meta_map[table_name] = meta

        # 更新schema文件
        for schema_file in schema_files:
            self._update_schema_file(schema_file, dataset_meta_map)

    def _update_schema_file(
        self, schema_file: Path, dataset_meta_map: Dict[str, Dict[str, Any]]
    ) -> None:
        """更新单个schema文件 - 初始化或更新模型的 meta 配置"""
        try:
            with open(schema_file, "r", encoding="utf-8") as f:
                content = f.read()

            if not content.strip():
                return

            data = yaml.safe_load(content) or {}
            models = data.get("models", [])

            if not models:
                return

            # 更新每个model的meta信息
            updated = False
            for model in models:
                model_name = model.get("name")
                if model_name in dataset_meta_map:
                    dataset_meta = dataset_meta_map[model_name]

                    # 确保columns存在
                    if "columns" not in model:
                        model["columns"] = []

                    # 构建现有列的映射
                    existing_columns = {}
                    for col in model.get("columns", []):
                        col_name = col.get("name")
                        if col_name:
                            existing_columns[col_name] = col

                    # 从数据集meta中获取列配置
                    meta_columns = dataset_meta.get("columns", {})

                    # 处理每个meta列
                    for col_name, col_meta_config in meta_columns.items():
                        col_meta = col_meta_config.get("config", {}).get("meta", {})

                        if col_name in existing_columns:
                            # 更新现有列的meta配置
                            existing_col = existing_columns[col_name]

                            # 确保config和meta存在
                            if "config" not in existing_col:
                                existing_col["config"] = {}
                            if "meta" not in existing_col["config"]:
                                existing_col["config"]["meta"] = {}

                            # 智能合并meta信息 - 保留用户自定义配置
                            current_meta = existing_col["config"]["meta"]

                            # 合并dimension配置
                            if "dimension" in col_meta:
                                if "dimension" not in current_meta:
                                    current_meta["dimension"] = col_meta["dimension"]
                                else:
                                    # 合并dimension属性
                                    for key, value in col_meta["dimension"].items():
                                        if key not in current_meta["dimension"]:
                                            current_meta["dimension"][key] = value

                            # 合并metrics配置
                            if "metrics" in col_meta:
                                if "metrics" not in current_meta:
                                    current_meta["metrics"] = col_meta["metrics"]
                                else:
                                    # 合并每个metric
                                    for metric_name, metric_config in col_meta["metrics"].items():
                                        if metric_name not in current_meta["metrics"]:
                                            current_meta["metrics"][metric_name] = metric_config

                            # 更新description（如果meta中有且当前为空）
                            if col_meta_config.get("description") and not existing_col.get("description"):
                                existing_col["description"] = col_meta_config["description"]
                        else:
                            # 添加新列（仅meta配置）
                            new_col = {
                                "name": col_name,
                                "description": col_meta_config.get("description", ""),
                                "config": {"meta": col_meta},
                            }
                            model["columns"].append(new_col)

                    updated = True

            if updated:
                # 写回文件，保持原有格式
                with open(schema_file, "w", encoding="utf-8") as f:
                    yaml.dump(data, f, default_flow_style=False, allow_unicode=True, sort_keys=False, indent=2)

                logger.info(f"更新schema文件: {schema_file}")

        except Exception as e:
            logger.error(f"更新schema文件失败 {schema_file}: {e}")