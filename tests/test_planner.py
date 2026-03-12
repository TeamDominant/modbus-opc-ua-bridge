from modbus_opcua_bridge.bridge import build_read_batches
from modbus_opcua_bridge.models import DataType, ModbusSource, PointConfig, PollingSettings


def make_point(name: str, address: int, **overrides) -> PointConfig:
    data = {
        "name": name,
        "folder": "",
        "source": ModbusSource.HOLDING_REGISTER,
        "address": address,
        "data_type": DataType.UINT16,
        "byte_order": "big",
        "word_order": "big",
        "scale": 1.0,
        "offset": 0.0,
        "bit_index": None,
    }
    data.update(overrides)
    return PointConfig(**data)


def test_build_read_batches_groups_contiguous_registers() -> None:
    points = (
        make_point("A", 0),
        make_point("B", 1),
        make_point("C", 2, data_type=DataType.FLOAT32),
        make_point("D", 10, source=ModbusSource.COIL, data_type=DataType.BOOL),
    )
    polling = PollingSettings(max_group_gap=0, max_register_batch=10, max_bit_batch=10)

    batches = build_read_batches(points, polling, default_device_id=1)

    assert len(batches) == 2
    register_batch = next(
        batch for batch in batches if batch.source == ModbusSource.HOLDING_REGISTER
    )
    assert register_batch.start_address == 0
    assert register_batch.count == 4
    assert [window.offset for window in register_batch.windows] == [0, 1, 2]


def test_build_read_batches_respects_max_batch_size() -> None:
    points = (
        make_point("A", 0, data_type=DataType.FLOAT64),
        make_point("B", 4, data_type=DataType.FLOAT64),
    )
    polling = PollingSettings(max_group_gap=0, max_register_batch=6)

    batches = build_read_batches(points, polling, default_device_id=1)

    assert len(batches) == 2
