from __future__ import annotations

import argparse
import asyncio
import logging
from pathlib import Path
from typing import Sequence

from .bridge import ModbusToOPCUABridge
from .config import ConfigError, load_config


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Poll Modbus data and publish it through an OPC UA server."
    )
    parser.add_argument("--config", required=True, type=Path, help="Path to JSON config.")
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Python logging level.",
    )
    return parser


def configure_logging(level_name: str) -> None:
    logging.basicConfig(
        level=getattr(logging, level_name.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    configure_logging(args.log_level)

    try:
        config = load_config(args.config)
    except ConfigError as exc:
        logging.error("Configuration error: %s", exc)
        return 2

    bridge = ModbusToOPCUABridge(config)
    try:
        asyncio.run(bridge.run_forever())
    except KeyboardInterrupt:
        return 0
    except Exception:
        logging.exception("Bridge stopped due to a fatal error.")
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

