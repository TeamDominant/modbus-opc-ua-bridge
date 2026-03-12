from __future__ import annotations

import asyncio
import inspect
import logging
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Sequence

from asyncua import Server, ua
from asyncua.common.node import Node
from pymodbus import FramerType
from pymodbus.client import AsyncModbusSerialClient, AsyncModbusTcpClient

from .codec import default_value_for_point, decode_point, point_width, uses_double_output
from .models import (
    AppConfig,
    BIT_SOURCES,
    DataType,
    ModbusSettings,
    ModbusSource,
    OPCUASettings,
    PointConfig,
    PollingSettings,
    Transport,
)


LOGGER = logging.getLogger(__name__)


VARIANT_TYPES = {
    DataType.BOOL: ua.VariantType.Boolean,
    DataType.INT16: ua.VariantType.Int16,
    DataType.UINT16: ua.VariantType.UInt16,
    DataType.INT32: ua.VariantType.Int32,
    DataType.UINT32: ua.VariantType.UInt32,
    DataType.INT64: ua.VariantType.Int64,
    DataType.UINT64: ua.VariantType.UInt64,
    DataType.FLOAT32: ua.VariantType.Float,
    DataType.FLOAT64: ua.VariantType.Double,
}


@dataclass(frozen=True)
class BatchWindow:
    point: PointConfig
    offset: int
    width: int


@dataclass(frozen=True)
class ReadBatch:
    source: ModbusSource
    device_id: int
    start_address: int
    count: int
    windows: tuple[BatchWindow, ...]


@dataclass
class PointBinding:
    node: Node
    variant_type: ua.VariantType


@dataclass
class StatusBindings:
    connected: Node
    last_poll_utc: Node
    last_error: Node
    successful_polls: Node
    failed_polls: Node


def build_read_batches(
    points: Sequence[PointConfig],
    polling: PollingSettings,
    default_device_id: int,
) -> list[ReadBatch]:
    grouped: dict[tuple[int, ModbusSource], list[PointConfig]] = {}
    for point in points:
        key = (point.resolved_device_id(default_device_id), point.source)
        grouped.setdefault(key, []).append(point)

    batches: list[ReadBatch] = []
    for (device_id, source), group_points in grouped.items():
        sorted_points = sorted(group_points, key=lambda item: item.address)
        pending: list[tuple[PointConfig, int]] = []
        batch_start = 0
        batch_end = 0
        max_count = (
            polling.max_bit_batch if source in BIT_SOURCES else polling.max_register_batch
        )

        for point in sorted_points:
            width = point_width(point)
            point_start = point.address
            point_end = point_start + width

            if not pending:
                pending = [(point, width)]
                batch_start = point_start
                batch_end = point_end
                continue

            gap = point_start - batch_end
            candidate_end = max(batch_end, point_end)
            candidate_count = candidate_end - batch_start
            if gap <= polling.max_group_gap and candidate_count <= max_count:
                pending.append((point, width))
                batch_end = candidate_end
                continue

            batches.append(
                materialize_batch(source, device_id, batch_start, batch_end, pending)
            )
            pending = [(point, width)]
            batch_start = point_start
            batch_end = point_end

        if pending:
            batches.append(
                materialize_batch(source, device_id, batch_start, batch_end, pending)
            )

    return sorted(
        batches, key=lambda batch: (batch.device_id, batch.source.value, batch.start_address)
    )


def materialize_batch(
    source: ModbusSource,
    device_id: int,
    batch_start: int,
    batch_end: int,
    pending: Sequence[tuple[PointConfig, int]],
) -> ReadBatch:
    return ReadBatch(
        source=source,
        device_id=device_id,
        start_address=batch_start,
        count=batch_end - batch_start,
        windows=tuple(
            BatchWindow(
                point=point,
                offset=point.address - batch_start,
                width=width,
            )
            for point, width in pending
        ),
    )


class ModbusClientAdapter:
    def __init__(self, settings: ModbusSettings) -> None:
        self._settings = settings
        self._client: Any | None = None

    async def ensure_connected(self) -> bool:
        if self._client is None:
            self._client = self._build_client()
        if bool(getattr(self._client, "connected", False)):
            return True
        return bool(await maybe_await(self._client.connect()))

    async def close(self) -> None:
        if self._client is None:
            return
        await maybe_await(self._client.close())
        self._client = None

    async def reset(self) -> None:
        await self.close()

    async def read_batch(self, batch: ReadBatch) -> list[int | bool]:
        if self._client is None:
            raise RuntimeError("Modbus client is not initialized.")

        reader = {
            ModbusSource.COIL: self._client.read_coils,
            ModbusSource.DISCRETE_INPUT: self._client.read_discrete_inputs,
            ModbusSource.HOLDING_REGISTER: self._client.read_holding_registers,
            ModbusSource.INPUT_REGISTER: self._client.read_input_registers,
        }[batch.source]

        response = await reader(
            batch.start_address, count=batch.count, device_id=batch.device_id
        )
        if response.isError():
            raise RuntimeError(
                f"Modbus error for {batch.source.value} "
                f"address={batch.start_address} count={batch.count}: {response}"
            )

        payload = response.bits if batch.source in BIT_SOURCES else response.registers
        if len(payload) < batch.count:
            raise RuntimeError(
                f"Incomplete response for {batch.source.value} "
                f"address={batch.start_address} count={batch.count}."
            )
        return payload[: batch.count]

    def _build_client(self) -> Any:
        if self._settings.transport == Transport.TCP:
            return AsyncModbusTcpClient(
                self._settings.host,
                port=self._settings.port,
                name=self._settings.name,
                timeout=self._settings.timeout,
                retries=self._settings.retries,
                reconnect_delay=self._settings.reconnect_delay,
                reconnect_delay_max=self._settings.reconnect_delay_max,
            )

        return AsyncModbusSerialClient(
            self._settings.serial_port,
            framer=FramerType(self._settings.framer),
            baudrate=self._settings.baudrate,
            bytesize=self._settings.bytesize,
            parity=self._settings.parity,
            stopbits=self._settings.stopbits,
            name=self._settings.name,
            timeout=self._settings.timeout,
            retries=self._settings.retries,
            reconnect_delay=self._settings.reconnect_delay,
            reconnect_delay_max=self._settings.reconnect_delay_max,
        )


class OPCUAPublisher:
    def __init__(self, settings: OPCUASettings, default_device_id: int) -> None:
        self._settings = settings
        self._default_device_id = default_device_id
        self._server: Server | None = None
        self._namespace_index = 0
        self._folder_cache: dict[tuple[str, ...], Node] = {}
        self._bindings: dict[PointConfig, PointBinding] = {}
        self._status: StatusBindings | None = None

    async def start(self, points: Sequence[PointConfig]) -> None:
        if self._server is not None:
            return

        server = Server()
        await server.init()
        server.set_endpoint(self._settings.endpoint)
        server.set_server_name(self._settings.server_name)
        self._namespace_index = await server.register_namespace(self._settings.namespace)

        root = await server.nodes.objects.add_folder(
            self._namespace_index, self._settings.root_name
        )
        tags_root = await root.add_folder(
            self._namespace_index, self._settings.tags_folder_name
        )
        status_root = await root.add_folder(
            self._namespace_index, self._settings.status_folder_name
        )

        self._folder_cache[tuple()] = tags_root
        self._status = await self._create_status_nodes(status_root)

        for point in points:
            parent = await self._ensure_folder(point.folder)
            variant_type = point_variant_type(point)
            node = await parent.add_variable(
                self._resolve_node_id(point),
                point.name,
                default_value_for_point(point),
                varianttype=variant_type,
            )
            self._bindings[point] = PointBinding(node=node, variant_type=variant_type)

        await server.start()
        self._server = server

    async def stop(self) -> None:
        if self._server is None:
            return
        await self._server.stop()
        self._server = None
        self._folder_cache.clear()
        self._bindings.clear()
        self._status = None

    async def write_point(self, point: PointConfig, value: bool | int | float) -> None:
        binding = self._bindings[point]
        await binding.node.write_value(value, binding.variant_type)

    async def update_status(
        self,
        *,
        connected: bool,
        last_poll_utc: str,
        last_error: str,
        successful_polls: int,
        failed_polls: int,
    ) -> None:
        if self._status is None:
            return
        await self._status.connected.write_value(connected, ua.VariantType.Boolean)
        await self._status.last_poll_utc.write_value(last_poll_utc, ua.VariantType.String)
        await self._status.last_error.write_value(last_error, ua.VariantType.String)
        await self._status.successful_polls.write_value(
            successful_polls, ua.VariantType.UInt64
        )
        await self._status.failed_polls.write_value(failed_polls, ua.VariantType.UInt64)

    async def _create_status_nodes(self, parent: Node) -> StatusBindings:
        return StatusBindings(
            connected=await parent.add_variable(
                self._namespace_index,
                "Connected",
                False,
                varianttype=ua.VariantType.Boolean,
            ),
            last_poll_utc=await parent.add_variable(
                self._namespace_index,
                "LastPollUTC",
                "",
                varianttype=ua.VariantType.String,
            ),
            last_error=await parent.add_variable(
                self._namespace_index,
                "LastError",
                "",
                varianttype=ua.VariantType.String,
            ),
            successful_polls=await parent.add_variable(
                self._namespace_index,
                "SuccessfulPolls",
                0,
                varianttype=ua.VariantType.UInt64,
            ),
            failed_polls=await parent.add_variable(
                self._namespace_index,
                "FailedPolls",
                0,
                varianttype=ua.VariantType.UInt64,
            ),
        )

    async def _ensure_folder(self, folder: str) -> Node:
        segments = tuple(
            segment.strip()
            for segment in re.split(r"[\\/]+", folder)
            if segment and segment.strip()
        )
        parent = self._folder_cache[tuple()]
        current_path: list[str] = []
        for segment in segments:
            current_path.append(segment)
            key = tuple(current_path)
            if key not in self._folder_cache:
                self._folder_cache[key] = await parent.add_folder(
                    self._namespace_index, segment
                )
            parent = self._folder_cache[key]
        return parent

    def _resolve_node_id(self, point: PointConfig) -> str:
        if point.node_id:
            if point.node_id.startswith("ns="):
                return point.node_id
            if point.node_id.startswith(("i=", "g=", "b=", "s=")):
                return f"ns={self._namespace_index};{point.node_id}"
            return f"ns={self._namespace_index};s={point.node_id}"

        folder_segments = [
            segment
            for segment in re.split(r"[\\/]+", point.folder)
            if segment and segment.strip()
        ]
        suffix = (
            f"{point.name}.{point.source.value}.{point.address}."
            f"{point.resolved_device_id(self._default_device_id)}"
        )
        raw_segments = [
            self._settings.root_name,
            self._settings.tags_folder_name,
            *folder_segments,
            suffix,
        ]
        identifier = ".".join(sanitize_identifier(part) for part in raw_segments)
        return f"ns={self._namespace_index};s={identifier}"


class ModbusToOPCUABridge:
    def __init__(self, config: AppConfig) -> None:
        self._config = config
        self._modbus = ModbusClientAdapter(config.modbus)
        self._opcua = OPCUAPublisher(config.opcua, config.modbus.device_id)
        self._batches = build_read_batches(
            config.points, config.polling, config.modbus.device_id
        )
        self._successful_polls = 0
        self._failed_polls = 0

    async def run_forever(self) -> None:
        await self._opcua.start(self._config.points)
        LOGGER.info("OPC UA endpoint: %s", self._config.opcua.endpoint)
        LOGGER.info(
            "Bridge started with %d points in %d Modbus batches.",
            len(self._config.points),
            len(self._batches),
        )
        try:
            while True:
                await self.poll_once()
                await asyncio.sleep(self._config.polling.interval_ms / 1000.0)
        finally:
            await self._modbus.close()
            await self._opcua.stop()

    async def poll_once(self) -> None:
        timestamp = utc_timestamp()
        errors: list[str] = []

        try:
            connected = await self._modbus.ensure_connected()
            if not connected:
                raise RuntimeError("Unable to connect to Modbus endpoint.")

            for batch in self._batches:
                try:
                    payload = await self._modbus.read_batch(batch)
                    await self._publish_batch(batch, payload, errors)
                except Exception as exc:
                    errors.append(str(exc))

            if errors:
                self._failed_polls += 1
                error_text = summarize_errors(errors)
                LOGGER.warning(error_text)
                await self._opcua.update_status(
                    connected=True,
                    last_poll_utc=timestamp,
                    last_error=error_text,
                    successful_polls=self._successful_polls,
                    failed_polls=self._failed_polls,
                )
                return

            self._successful_polls += 1
            await self._opcua.update_status(
                connected=True,
                last_poll_utc=timestamp,
                last_error="",
                successful_polls=self._successful_polls,
                failed_polls=self._failed_polls,
            )
        except Exception as exc:
            self._failed_polls += 1
            LOGGER.warning("Polling cycle failed: %s", exc)
            await self._modbus.reset()
            await self._opcua.update_status(
                connected=False,
                last_poll_utc=timestamp,
                last_error=str(exc),
                successful_polls=self._successful_polls,
                failed_polls=self._failed_polls,
            )

    async def _publish_batch(
        self,
        batch: ReadBatch,
        payload: Sequence[int | bool],
        errors: list[str],
    ) -> None:
        for window in batch.windows:
            try:
                data_slice = payload[window.offset : window.offset + window.width]
                value = decode_point(window.point, data_slice)
                await self._opcua.write_point(window.point, value)
            except Exception as exc:
                errors.append(f"{window.point.name}: {exc}")


def point_variant_type(point: PointConfig) -> ua.VariantType:
    if uses_double_output(point):
        return ua.VariantType.Double
    return VARIANT_TYPES[point.data_type]


async def maybe_await(value: Any) -> Any:
    if inspect.isawaitable(value):
        return await value
    return value


def sanitize_identifier(value: str) -> str:
    return re.sub(r"[^0-9A-Za-z_.-]+", "_", value.strip()) or "node"


def summarize_errors(errors: Sequence[str]) -> str:
    visible = "; ".join(errors[:3])
    extra = len(errors) - 3
    if extra > 0:
        return f"{visible}; +{extra} more"
    return visible


def utc_timestamp() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()
