"""主CLI入口

提供pull和push两个主要命令
"""

import argparse
import logging
import sys
from pathlib import Path

from .config import Config
from .client import SupersetClient
from .pull import SupersetPuller
from .push import SupersetPusher


def setup_logging(level: str = "INFO") -> None:
    """设置日志"""
    logging.basicConfig(
        level=getattr(logging, level.upper()),
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        handlers=[
            logging.StreamHandler(sys.stdout),
        ],
    )


def cmd_pull(args) -> None:
    """拉取命令"""
    setup_logging(args.log_level)

    logger = logging.getLogger(__name__)

    # 加载配置
    try:
        config = Config.load(args.project_dir)
    except Exception as e:
        logger.error(f"加载配置失败: {e}")
        sys.exit(1)

    # 创建客户端
    try:
        client = SupersetClient.create_from_config(
            config.superset,
            config.dbt.schema_map,
            config.dbt.default_schema
        )
    except Exception as e:
        logger.error(f"创建Superset客户端失败: {e}")
        sys.exit(1)

    # 创建puller
    puller = SupersetPuller(client, config.dbt)

    # 执行拉取
    dashboard_ids = None
    if args.dashboard_ids:
        dashboard_ids = [int(x) for x in args.dashboard_ids.split(",")]

    puller.pull(dashboard_ids=dashboard_ids)


def cmd_push(args) -> None:
    """推送命令"""
    setup_logging(args.log_level)

    logger = logging.getLogger(__name__)

    # 加载配置
    try:
        config = Config.load(args.project_dir)
    except Exception as e:
        logger.error(f"加载配置失败: {e}")
        sys.exit(1)

    # 创建客户端
    try:
        client = SupersetClient.create_from_config(
            config.superset,
            config.dbt.schema_map,
            config.dbt.default_schema
        )
    except Exception as e:
        logger.error(f"创建Superset客户端失败: {e}")
        sys.exit(1)

    # 创建pusher
    pusher = SupersetPusher(client, config.dbt)

    # 执行推送
    exposure_names = None
    if args.exposure_names:
        exposure_names = args.exposure_names.split(",")

    model_names = None
    if args.model_names:
        model_names = args.model_names.split(",")

    schema = getattr(args, "schema", None)

    pusher.push(exposure_names=exposure_names, model_names=model_names, schema=schema)


def cmd_init(args) -> None:
    """初始化命令：创建配置文件"""
    logger = logging.getLogger(__name__)
    setup_logging(args.log_level)

    # 确定配置文件路径
    if args.config_path:
        config_path = Path(args.config_path)
    else:
        # 默认在家目录下创建 ~/.super6dbt/config.yml
        config_path = Path.home() / ".super6dbt" / "config.yml"

    config_path.parent.mkdir(parents=True, exist_ok=True)

    if config_path.exists():
        logger.info(f"配置文件已存在: {config_path}")
        return

    # 生成配置文件
    config_content = """# SuperSet 6.0 API 配置
superset:
  # Superset基础URL
  base_url: "http://localhost:8088"

  # 登录凭证
  username: "admin"
  password: "admin"
  provider: "db"

  # SSL验证
  verify_ssl: false

# dbt项目配置会自动从dbt_project.yml读取
"""

    with open(config_path, "w", encoding="utf-8") as f:
        f.write(config_content)

    logger.info(f"配置文件已创建: {config_path}")
    logger.info("请编辑配置文件并设置正确的Superset连接信息")


def cmd_status(args) -> None:
    """状态命令：检查连接状态"""
    setup_logging(args.log_level)

    logger = logging.getLogger(__name__)

    # 加载配置
    try:
        config = Config.load(args.project_dir)
    except Exception as e:
        logger.error(f"加载配置失败: {e}")
        sys.exit(1)

    logger.info(f"Superset URL: {config.superset.base_url}")
    logger.info(f"dbt项目目录: {config.dbt.project_dir}")

    # 测试连接
    try:
        client = SupersetClient.create_from_config(
            config.superset,
            config.dbt.schema_map,
            config.dbt.default_schema
        )
        logger.info("✓ Superset连接成功")

        # 获取当前用户
        user = client.get_current_user()
        if user:
            logger.info(f"✓ 当前用户: {user.get('username')}")

        # 获取统计信息
        dashboards = client.get_dashboards()
        datasets = client.get_datasets()

        logger.info(f"✓ 面板数量: {len(dashboards)}")
        logger.info(f"✓ 数据集数量: {len(datasets)}")

    except Exception as e:
        logger.error(f"✗ 连接失败: {e}")
        sys.exit(1)


def main() -> None:
    """主入口"""
    parser = argparse.ArgumentParser(
        description="SuperSet 6.0 与 dbt 双向同步工具",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
示例:
  # 初始化配置文件
  super6dbt init

  # 检查连接状态
  super6dbt status

  # 从Superset拉取所有面板配置
  super6dbt pull

  # 从Superset拉取指定面板
  super6dbt pull --dashboard-ids 1,2,3

  # 推送所有exposures到Superset
  super6dbt push

  # 推送指定exposure到Superset
  super6dbt push --exposure-names my_dashboard

  # 仅推送指定model的数据集（不创建面板）
  super6dbt push --model-names orders,products

  # 推送指定model的数据集，指定schema
  super6dbt push --model-names ads_channel_conversion_analysis_full --schema wa_ads

环境变量:
  SUPERSET_BASE_URL     Superset基础URL
  SUPERSET_USERNAME    登录用户名
  SUPERSET_PASSWORD    登录密码
  SUPERSET_PROVIDER    认证方式 (默认: db)
  SUPERSET_VERIFY_SSL  是否验证SSL (默认: true)
  SUPERSET_DATABASE    数据库名称（可选，用于创建数据集）
        """,
    )

    subparsers = parser.add_subparsers(dest="command", help="可用命令")

    # 共同参数
    parser.add_argument(
        "--project-dir",
        type=str,
        default=".",
        help="dbt项目目录路径 (默认: 当前目录)",
    )
    parser.add_argument(
        "--log-level",
        type=str,
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="日志级别 (默认: INFO)",
    )

    # pull命令
    pull_parser = subparsers.add_parser("pull", help="从Superset拉取配置到dbt")
    pull_parser.add_argument(
        "--dashboard-ids",
        type=str,
        help="要拉取的面板ID列表，逗号分隔",
    )

    # push命令
    push_parser = subparsers.add_parser("push", help="从dbt推送配置到Superset")
    push_parser.add_argument(
        "--exposure-names",
        type=str,
        help="要推送的exposure名称列表，逗号分隔",
    )
    push_parser.add_argument(
        "--model-names",
        type=str,
        help="要推送的model名称列表（仅同步数据集），逗号分隔",
    )
    push_parser.add_argument(
        "--schema",
        type=str,
        help="指定数据集的 schema 名称（优先级最高）",
    )

    # init命令
    init_parser = subparsers.add_parser("init", help="初始化配置文件")
    init_parser.add_argument(
        "--config-path",
        type=str,
        help="配置文件路径",
    )

    # status命令
    subparsers.add_parser("status", help="检查连接状态")

    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        sys.exit(1)

    # 执行命令
    if args.command == "pull":
        cmd_pull(args)
    elif args.command == "push":
        cmd_push(args)
    elif args.command == "init":
        cmd_init(args)
    elif args.command == "status":
        cmd_status(args)


if __name__ == "__main__":
    main()