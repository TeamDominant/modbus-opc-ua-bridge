from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from .models import (
    AppConfig,
    BIT_SOURCES,
    DataType,
    ModbusSettings,
    ModbusSource,
    OPCUASettings,
    PointConfig,
    PollingSettings,
    REGISTER_SOURCES,
    Transport,
)


class ConfigError(ValueError):
    """Raised when configuration is invalid."""


SOURCE_ALIASES = {
    "coil": ModbusSource.COIL,
    "coils": ModbusSource.COIL,
    "discrete_input": ModbusSource.DISCRETE_INPUT,
    "discrete_inputs": ModbusSource.DISCRETE_INPUT,
    "holding_register": ModbusSource.HOLDING_REGISTER,
    "holding_registers": ModbusSource.HOLDING_REGISTER,
    "input_register": ModbusSource.INPUT_REGISTER,
    "input_registers": ModbusSource.INPUT_REGISTER,
}

DATA_TYPE_ALIASES = {
    "bool": DataType.BOOL,
    "boolean": DataType.BOOL,
    "int16": DataType.INT16,
    "uint16": DataType.UINT16,
    "int32": DataType.INT32,
    "uint32": DataType.UINT32,
    "int64": DataType.INT64,
    "uint64": DataType.UINT64,
    "float": DataType.FLOAT32,
    "float32": DataType.FLOAT32,
    "real": DataType.FLOAT32,
    "double": DataType.FLOAT64,
    "float64": DataType.FLOAT64,
}


def load_config(path: str | Path) -> AppConfig:
    config_path = Path(path)
    try:
        raw_data = json.loads(config_path.read_text(encoding="utf-8"))
    except FileNotFoundError as exc:
        raise ConfigError(f"Config file not found: {config_path}") from exc
    except json.JSONDecodeError as exc:
        raise ConfigError(f"Invalid JSON in {config_path}: {exc}") from exc

    if not isinstance(raw_data, dict):
        raise ConfigError("Top-level config must be a JSON object.")

    modbus = parse_modbus_settings(as_dict(raw_data.get("modbus"), "modbus"))
    opcua = parse_opcua_settings(as_dict(raw_data.get("opcua"), "opcua"))
    polling = parse_polling_settings(as_dict(raw_data.get("polling"), "polling"))
    points_raw = raw_data.get("points")
    if not isinstance(points_raw, list) or not points_raw:
        raise ConfigError("'points' must be a non-empty array.")

    points = tuple(parse_point_config(item, index) for index, item in enumerate(points_raw))
    return AppConfig(modbus=modbus, opcua=opcua, polling=polling, points=points)


def parse_modbus_settings(raw: dict[str, Any]) -> ModbusSettings:
    transport_value = as_str(raw.get("transport", "tcp"), "modbus.transport").lower()
    try:
        transport = Transport(transport_value)
    except ValueError as exc:
        raise ConfigError(
            "modbus.transport must be 'tcp' or 'serial'."
        ) from exc

    settings = ModbusSettings(
        transport=transport,
        host=as_str(raw.get("host", "127.0.0.1"), "modbus.host"),
        port=as_int(raw.get("port", 502), "modbus.port"),
        serial_port=as_optional_str(raw.get("serial_port"), "modbus.serial_port"),
        framer=as_str(raw.get("framer", "rtu"), "modbus.framer").lower(),
        baudrate=as_int(raw.get("baudrate", 19200), "modbus.baudrate"),
        bytesize=as_int(raw.get("bytesize", 8), "modbus.bytesize"),
        parity=as_str(raw.get("parity", "N"), "modbus.parity").upper(),
        stopbits=as_int(raw.get("stopbits", 1), "modbus.stopbits"),
        device_id=as_int(raw.get("device_id", 1), "modbus.device_id"),
        timeout=as_float(raw.get("timeout", 3.0), "modbus.timeout"),
        retries=as_int(raw.get("retries", 3), "modbus.retries"),
        reconnect_delay=as_float(
            raw.get("reconnect_delay", 0.1), "modbus.reconnect_delay"
        ),
        reconnect_delay_max=as_float(
            raw.get("reconnect_delay_max", 30.0), "modbus.reconnect_delay_max"
        ),
        name=as_str(raw.get("name", "modbus-bridge"), "modbus.name"),
    )

    if settings.device_id < 0:
        raise ConfigError("modbus.device_id must be >= 0.")
    if settings.timeout <= 0:
        raise ConfigError("modbus.timeout must be > 0.")
    if settings.retries < 0:
        raise ConfigError("modbus.retries must be >= 0.")
    if transport == Transport.TCP:
        if not 1 <= settings.port <= 65535:
            raise ConfigError("modbus.port must be in range 1..65535.")
    else:
        if not settings.serial_port:
            raise ConfigError("modbus.serial_port is required for serial transport.")
        if settings.framer not in {"rtu", "ascii"}:
            raise ConfigError("modbus.framer must be 'rtu' or 'ascii'.")

    return settings


def parse_opcua_settings(raw: dict[str, Any]) -> OPCUASettings:
    endpoint = as_str(raw.get("endpoint"), "opcua.endpoint")
    namespace = as_str(raw.get("namespace"), "opcua.namespace")
    return OPCUASettings(
        endpoint=endpoint,
        namespace=namespace,
        server_name=as_str(
            raw.get("server_name", "Modbus OPC UA Bridge"), "opcua.server_name"
        ),
        root_name=as_str(raw.get("root_name", "ModbusBridge"), "opcua.root_name"),
        tags_folder_name=as_str(
            raw.get("tags_folder_name", "Tags"), "opcua.tags_folder_name"
        ),
        status_folder_name=as_str(
            raw.get("status_folder_name", "Status"), "opcua.status_folder_name"
        ),
    )


def parse_polling_settings(raw: dict[str, Any]) -> PollingSettings:
    settings = PollingSettings(
        interval_ms=as_int(raw.get("interval_ms", 1000), "polling.interval_ms"),
        max_register_batch=as_int(
            raw.get("max_register_batch", 120), "polling.max_register_batch"
        ),
        max_bit_batch=as_int(
            raw.get("max_bit_batch", 2000), "polling.max_bit_batch"
        ),
        max_group_gap=as_int(raw.get("max_group_gap", 0), "polling.max_group_gap"),
    )
    if settings.interval_ms <= 0:
        raise ConfigError("polling.interval_ms must be > 0.")
    if settings.max_register_batch <= 0:
        raise ConfigError("polling.max_register_batch must be > 0.")
    if settings.max_bit_batch <= 0:
        raise ConfigError("polling.max_bit_batch must be > 0.")
    if settings.max_group_gap < 0:
        raise ConfigError("polling.max_group_gap must be >= 0.")
    return settings


def parse_point_config(raw: Any, index: int) -> PointConfig:
    if not isinstance(raw, dict):
        raise ConfigError(f"points[{index}] must be an object.")

    source = parse_source(as_str(raw.get("source"), f"points[{index}].source"))
    data_type = parse_data_type(
        as_str(raw.get("data_type"), f"points[{index}].data_type")
    )
    point = PointConfig(
        name=as_str(raw.get("name"), f"points[{index}].name"),
        folder=as_text(raw.get("folder", ""), f"points[{index}].folder"),
        source=source,
        address=as_int(raw.get("address"), f"points[{index}].address"),
        data_type=data_type,
        node_id=as_optional_str(raw.get("node_id"), f"points[{index}].node_id"),
        device_id=as_optional_int(raw.get("device_id"), f"points[{index}].device_id"),
        description=as_text(
            raw.get("description", ""), f"points[{index}].description"
        ),
        byte_order=parse_order(
            as_str(raw.get("byte_order", "big"), f"points[{index}].byte_order"),
            f"points[{index}].byte_order",
        ),
        word_order=parse_order(
            as_str(raw.get("word_order", "big"), f"points[{index}].word_order"),
            f"points[{index}].word_order",
        ),
        scale=as_float(raw.get("scale", 1.0), f"points[{index}].scale"),
        offset=as_float(raw.get("offset", 0.0), f"points[{index}].offset"),
        bit_index=as_optional_int(raw.get("bit_index"), f"points[{index}].bit_index"),
    )
    validate_point(point, index)
    return point


def validate_point(point: PointConfig, index: int) -> None:
    prefix = f"points[{index}]"
    if point.address < 0:
        raise ConfigError(f"{prefix}.address must be >= 0.")
    if point.device_id is not None and point.device_id < 0:
        raise ConfigError(f"{prefix}.device_id must be >= 0.")
    if point.data_type == DataType.BOOL and (
        point.scale != 1.0 or point.offset != 0.0
    ):
        raise ConfigError(f"{prefix} cannot use scale/offset with bool.")

    if point.source in BIT_SOURCES:
        if point.data_type != DataType.BOOL:
            raise ConfigError(f"{prefix} bit sources support only bool data_type.")
        if point.bit_index is not None:
            raise ConfigError(f"{prefix}.bit_index is valid only for register sources.")
        return

    if point.source not in REGISTER_SOURCES:
        raise ConfigError(f"{prefix}.source is not supported.")

    if point.bit_index is not None:
        if point.data_type != DataType.BOOL:
            raise ConfigError(f"{prefix}.bit_index requires bool data_type.")
        if not 0 <= point.bit_index <= 15:
            raise ConfigError(f"{prefix}.bit_index must be in range 0..15.")


def parse_source(value: str) -> ModbusSource:
    source = SOURCE_ALIASES.get(value.lower())
    if source is None:
        raise ConfigError(f"Unsupported source '{value}'.")
    return source


def parse_data_type(value: str) -> DataType:
    data_type = DATA_TYPE_ALIASES.get(value.lower())
    if data_type is None:
        raise ConfigError(f"Unsupported data_type '{value}'.")
    return data_type


def parse_order(value: str, field_name: str) -> str:
    normalized = value.lower()
    if normalized not in {"big", "little"}:
        raise ConfigError(f"{field_name} must be 'big' or 'little'.")
    return normalized


def as_dict(value: Any, field_name: str) -> dict[str, Any]:
    if value is None:
        return {}
    if not isinstance(value, dict):
        raise ConfigError(f"{field_name} must be an object.")
    return value


def as_str(value: Any, field_name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ConfigError(f"{field_name} must be a non-empty string.")
    return value.strip()


def as_optional_str(value: Any, field_name: str) -> str | None:
    if value is None:
        return None
    return as_str(value, field_name)


def as_text(value: Any, field_name: str) -> str:
    if not isinstance(value, str):
        raise ConfigError(f"{field_name} must be a string.")
    return value.strip()


def as_int(value: Any, field_name: str) -> int:
    if isinstance(value, bool):
        raise ConfigError(f"{field_name} must be an integer.")
    try:
        return int(value)
    except (TypeError, ValueError) as exc:
        raise ConfigError(f"{field_name} must be an integer.") from exc


def as_optional_int(value: Any, field_name: str) -> int | None:
    if value is None:
        return None
    return as_int(value, field_name)


def as_float(value: Any, field_name: str) -> float:
    if isinstance(value, bool):
        raise ConfigError(f"{field_name} must be a number.")
    try:
        return float(value)
    except (TypeError, ValueError) as exc:
        raise ConfigError(f"{field_name} must be a number.") from exc
