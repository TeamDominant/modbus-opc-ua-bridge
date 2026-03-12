import json

import pytest

from modbus_opcua_bridge.config import ConfigError, load_config
from modbus_opcua_bridge.models import DataType, ModbusSource, Transport


def test_load_config_accepts_aliases(tmp_path) -> None:
    config_path = tmp_path / "config.json"
    config_path.write_text(
        json.dumps(
            {
                "modbus": {"transport": "tcp", "host": "127.0.0.1", "port": 502},
                "opcua": {
                    "endpoint": "opc.tcp://0.0.0.0:4840/modbus-opcua/",
                    "namespace": "urn:test:bridge",
                },
                "points": [
                    {
                        "name": "Temp",
                        "source": "holding_registers",
                        "address": 0,
                        "data_type": "float",
                    },
                    {
                        "name": "State",
                        "source": "coils",
                        "address": 1,
                        "data_type": "bool",
                    },
                ],
            }
        ),
        encoding="utf-8",
    )

    config = load_config(config_path)

    assert config.modbus.transport == Transport.TCP
    assert config.points[0].source == ModbusSource.HOLDING_REGISTER
    assert config.points[0].data_type == DataType.FLOAT32
    assert config.points[1].source == ModbusSource.COIL


def test_load_config_rejects_non_bool_coils(tmp_path) -> None:
    config_path = tmp_path / "config.json"
    config_path.write_text(
        json.dumps(
            {
                "opcua": {
                    "endpoint": "opc.tcp://0.0.0.0:4840/modbus-opcua/",
                    "namespace": "urn:test:bridge",
                },
                "points": [
                    {
                        "name": "Invalid",
                        "source": "coil",
                        "address": 0,
                        "data_type": "uint16",
                    }
                ],
            }
        ),
        encoding="utf-8",
    )

    with pytest.raises(ConfigError):
        load_config(config_path)

