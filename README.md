# Modbus -> OPC UA bridge

Python service that polls Modbus registers/coils and exposes them as tags in an embedded OPC UA server.

## What is included

- Modbus TCP and Modbus RTU/ASCII support
- Embedded OPC UA server based on `asyncua`
- JSON configuration for source mapping
- Support for `coil`, `discrete_input`, `holding_register`, `input_register`
- Data types: `bool`, `int16`, `uint16`, `int32`, `uint32`, `int64`, `uint64`, `float32`, `float64`
- Byte order and word order control for multi-register values
- Read batching for contiguous Modbus addresses
- Status nodes in OPC UA: connection state, last poll time, last error, poll counters

## Quick start

Create and activate any virtual environment, then run:

```bash
python -m pip install -e .
python -m modbus_opcua_bridge --config config/example.tcp.json
```

For serial Modbus use `config/example.serial.json`.

## Configuration layout

Top-level sections:

- `modbus`: transport and connection parameters
- `opcua`: OPC UA endpoint and namespace settings
- `polling`: polling interval and batch size settings
- `points`: mapping list from Modbus addresses to OPC UA variables

Example point:

```json
{
  "name": "Temperature",
  "folder": "Line1/Sensors",
  "source": "holding_register",
  "address": 0,
  "data_type": "float32",
  "word_order": "little",
  "byte_order": "big",
  "scale": 0.1
}
```

Notes:

- `source` accepts singular and plural aliases such as `holding_registers`
- `bit_index` can be used only with register sources and `bool`
- if `scale` or `offset` are set for a numeric point, the published OPC UA type becomes `Double`
- if `node_id` is omitted, a stable string NodeId is generated automatically
- current implementation is one-way: Modbus to OPC UA only

## Running

```bash
python -m modbus_opcua_bridge --config config/example.tcp.json --log-level INFO
```

Default OPC UA root:

- `ModbusBridge/Tags/...`
- `ModbusBridge/Status/...`

## Test

```bash
python -m pytest
```
