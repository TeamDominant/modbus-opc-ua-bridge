from __future__ import annotations

import struct
from typing import Sequence

from .models import BIT_SOURCES, DataType, PointConfig


REGISTER_COUNT_BY_TYPE = {
    DataType.BOOL: 1,
    DataType.INT16: 1,
    DataType.UINT16: 1,
    DataType.INT32: 2,
    DataType.UINT32: 2,
    DataType.FLOAT32: 2,
    DataType.INT64: 4,
    DataType.UINT64: 4,
    DataType.FLOAT64: 4,
}

SIGNED_TYPES = {DataType.INT16, DataType.INT32, DataType.INT64}
FLOAT_TYPES = {DataType.FLOAT32, DataType.FLOAT64}


def point_width(point: PointConfig) -> int:
    if point.source in BIT_SOURCES:
        return 1
    if point.bit_index is not None:
        return 1
    return REGISTER_COUNT_BY_TYPE[point.data_type]


def uses_double_output(point: PointConfig) -> bool:
    if point.data_type == DataType.BOOL:
        return False
    return point.scale != 1.0 or point.offset != 0.0 or point.data_type == DataType.FLOAT64


def default_value_for_point(point: PointConfig) -> bool | int | float:
    if point.data_type == DataType.BOOL:
        return False
    if point.data_type in FLOAT_TYPES or uses_double_output(point):
        return 0.0
    return 0


def decode_point(point: PointConfig, payload: Sequence[int | bool]) -> bool | int | float:
    width = point_width(point)
    if len(payload) < width:
        raise ValueError(
            f"Point '{point.name}' expects {width} values, got {len(payload)}."
        )

    if point.source in BIT_SOURCES:
        return bool(payload[0])

    registers = [int(item) for item in payload[:width]]
    if point.bit_index is not None:
        return bool(registers[0] & (1 << point.bit_index))

    if point.data_type == DataType.BOOL:
        return bool(registers[0])

    value_bytes = registers_to_bytes(
        registers, byte_order=point.byte_order, word_order=point.word_order
    )
    if point.data_type in FLOAT_TYPES:
        raw_value = (
            struct.unpack(">f", value_bytes)[0]
            if point.data_type == DataType.FLOAT32
            else struct.unpack(">d", value_bytes)[0]
        )
        return apply_transform(point, raw_value)

    raw_value = int.from_bytes(
        value_bytes, byteorder="big", signed=point.data_type in SIGNED_TYPES
    )
    transformed = apply_transform(point, raw_value)
    if uses_double_output(point):
        return float(transformed)
    return int(transformed)


def apply_transform(point: PointConfig, value: int | float) -> int | float:
    if point.scale == 1.0 and point.offset == 0.0:
        return value
    return float(value) * point.scale + point.offset


def registers_to_bytes(
    registers: Sequence[int], *, byte_order: str, word_order: str
) -> bytes:
    words = [
        int(register).to_bytes(2, byteorder="big", signed=False)
        for register in registers
    ]
    if word_order == "little":
        words.reverse()
    if byte_order == "little":
        words = [word[::-1] for word in words]
    return b"".join(words)

