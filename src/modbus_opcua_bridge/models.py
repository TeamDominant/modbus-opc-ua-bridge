from __future__ import annotations

from dataclasses import dataclass
from enum import Enum


class Transport(str, Enum):
    TCP = "tcp"
    SERIAL = "serial"


class ModbusSource(str, Enum):
    COIL = "coil"
    DISCRETE_INPUT = "discrete_input"
    HOLDING_REGISTER = "holding_register"
    INPUT_REGISTER = "input_register"


class DataType(str, Enum):
    BOOL = "bool"
    INT16 = "int16"
    UINT16 = "uint16"
    INT32 = "int32"
    UINT32 = "uint32"
    INT64 = "int64"
    UINT64 = "uint64"
    FLOAT32 = "float32"
    FLOAT64 = "float64"


REGISTER_SOURCES = frozenset(
    {ModbusSource.HOLDING_REGISTER, ModbusSource.INPUT_REGISTER}
)
BIT_SOURCES = frozenset({ModbusSource.COIL, ModbusSource.DISCRETE_INPUT})


@dataclass(frozen=True)
class ModbusSettings:
    transport: Transport = Transport.TCP
    host: str = "127.0.0.1"
    port: int = 502
    serial_port: str | None = None
    framer: str = "rtu"
    baudrate: int = 19200
    bytesize: int = 8
    parity: str = "N"
    stopbits: int = 1
    device_id: int = 1
    timeout: float = 3.0
    retries: int = 3
    reconnect_delay: float = 0.1
    reconnect_delay_max: float = 30.0
    name: str = "modbus-bridge"


@dataclass(frozen=True)
class OPCUASettings:
    endpoint: str
    namespace: str
    server_name: str = "Modbus OPC UA Bridge"
    root_name: str = "ModbusBridge"
    tags_folder_name: str = "Tags"
    status_folder_name: str = "Status"


@dataclass(frozen=True)
class PollingSettings:
    interval_ms: int = 1000
    max_register_batch: int = 120
    max_bit_batch: int = 2000
    max_group_gap: int = 0


@dataclass(frozen=True)
class PointConfig:
    name: str
    source: ModbusSource
    address: int
    data_type: DataType
    folder: str = ""
    node_id: str | None = None
    device_id: int | None = None
    description: str = ""
    byte_order: str = "big"
    word_order: str = "big"
    scale: float = 1.0
    offset: float = 0.0
    bit_index: int | None = None

    def resolved_device_id(self, default_device_id: int) -> int:
        return self.device_id if self.device_id is not None else default_device_id


@dataclass(frozen=True)
class AppConfig:
    modbus: ModbusSettings
    opcua: OPCUASettings
    polling: PollingSettings
    points: tuple[PointConfig, ...]

