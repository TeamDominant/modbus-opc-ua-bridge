import math

import pytest

from modbus_opcua_bridge.codec import decode_point, point_width
from modbus_opcua_bridge.models import DataType, ModbusSource, PointConfig


def make_point(**overrides):
    data = {
        "name": "Tag",
        "folder": "",
        "source": ModbusSource.HOLDING_REGISTER,
        "address": 0,
        "data_type": DataType.INT16,
        "byte_order": "big",
        "word_order": "big",
        "scale": 1.0,
        "offset": 0.0,
        "bit_index": None,
    }
    data.update(overrides)
    return PointConfig(**data)


def test_decode_int32_big_endian() -> None:
    point = make_point(data_type=DataType.INT32)
    assert decode_point(point, [0x0001, 0x0002]) == 65538


def test_decode_float32_little_word_order() -> None:
    point = make_point(data_type=DataType.FLOAT32, word_order="little")
    value = decode_point(point, [0x0000, 0x4148])
    assert math.isclose(value, 12.5, rel_tol=1e-6)


def test_decode_register_bit_to_bool() -> None:
    point = make_point(data_type=DataType.BOOL, bit_index=3)
    assert decode_point(point, [0b1000]) is True
    assert decode_point(point, [0b0001]) is False


def test_scaled_integer_becomes_float() -> None:
    point = make_point(data_type=DataType.UINT16, scale=0.1, offset=1.0)
    assert decode_point(point, [20]) == pytest.approx(3.0)


def test_coil_width_is_one() -> None:
    point = make_point(source=ModbusSource.COIL, data_type=DataType.BOOL)
    assert point_width(point) == 1
    assert decode_point(point, [True]) is True

