from __future__ import annotations

import asyncio
import json
import time
import uuid
from dataclasses import dataclass, field

import grpc
import websockets

from proto_loader import load_proto_modules

pb2, pb2_grpc = load_proto_modules()

# Local endpoints for MVP single-machine deployment.
WS_HOST = "127.0.0.1"
WS_PORT = 8765
GRPC_TARGET = "127.0.0.1:50051"


EVENT_TYPE_BY_ACTION = {
    "NEW": "ORDER_SUBMIT_REQUESTED",
    "MODIFY_PRICE": "ORDER_MODIFY_PRICE_REQUESTED",
    "CANCEL": "ORDER_CANCEL_REQUESTED",
}


@dataclass
class OMSState:
    """All runtime state kept in memory for MVP.

    Restarting OMS will clear these structures.
    """

    orders_by_oms_id: dict = field(default_factory=dict)
    oms_to_broker: dict = field(default_factory=dict)
    events_log: list = field(default_factory=list)
    subscribers: set = field(default_factory=set)
    gateway_ready: bool = False


class OMSApp:
    def __init__(self):
        self.state = OMSState()
        # Queue of commands waiting to be sent to gateway stream.
        self.to_gateway_queue: asyncio.Queue = asyncio.Queue()

    def now_ms(self) -> int:
        return int(time.time() * 1000)

    def build_event(
        self,
        *,
        oms_id: str,
        event_source: str,
        event_type: str,
        symbol: str = "",
        side: str = "",
        quantity: int = 0,
        price: float = 0.0,
        gateway: str = "",
        account: str = "",
        broker_order_id: str = "",
        message: str = "",
    ) -> dict:
        """Create one normalized OMS event dict.

        This format is the single event contract sent to strategy.
        """
        return {
            "oms_id": oms_id,
            "event_id": str(uuid.uuid4()),
            "event_source": event_source,
            "event_type": event_type,
            "symbol": symbol,
            "side": side,
            "quantity": quantity,
            "price": price,
            "gateway": gateway,
            "account": account,
            "broker_order_id": broker_order_id,
            "event_time": self.now_ms(),
            "message": message,
        }

    async def publish_event(self, event: dict):
        """Persist event in memory and broadcast to all strategy clients."""
        self.state.events_log.append(event)
        dead_clients = []
        for ws in self.state.subscribers:
            try:
                await ws.send(json.dumps({"kind": "event", "data": event}, ensure_ascii=False))
            except Exception:
                dead_clients.append(ws)

        for ws in dead_clients:
            self.state.subscribers.discard(ws)

    async def publish_ready(self, ws):
        """Send readiness snapshot to one strategy websocket client."""
        msg = {
            "kind": "ready",
            "component": "oms",
            "status": "READY" if self.state.gateway_ready else "DEGRADED",
            "gateway_ready": self.state.gateway_ready,
            "ts_ms": self.now_ms(),
        }
        await ws.send(json.dumps(msg, ensure_ascii=False))

    async def handle_strategy_request(self, payload: dict):
        """Main OMS request handler.

        Flow:
        1) Validate action and required fields.
        2) Emit immediate OMS events (do not wait for gateway).
        3) Enqueue normalized command to gateway stream.
        """
        action = str(payload.get("action", "")).upper()
        if action not in EVENT_TYPE_BY_ACTION:
            event = self.build_event(
                oms_id=payload.get("oms_id", ""),
                event_source="oms",
                event_type="ORDER_REJECTED_BY_OMS",
                message=f"Unsupported action: {action}",
            )
            await self.publish_event(event)
            return

        oms_id = payload.get("oms_id") or str(uuid.uuid4())
        symbol = payload.get("symbol", "")
        side = payload.get("side", "")
        quantity = int(payload.get("quantity", 0) or 0)
        price = float(payload.get("price", 0) or 0)
        gateway = payload.get("gateway", "")
        account = payload.get("account", "")

        # Requirement: emit a new event immediately when OMS receives a request.
        requested_event = self.build_event(
            oms_id=oms_id,
            event_source="strategy",
            event_type=EVENT_TYPE_BY_ACTION[action],
            symbol=symbol,
            side=side,
            quantity=quantity,
            price=price,
            gateway=gateway,
            account=account,
            message="Request received by OMS",
        )
        await self.publish_event(requested_event)

        # Minimal request validation for MVP.
        if action == "NEW":
            if not symbol or side not in {"BUY", "SELL"} or quantity <= 0 or price <= 0:
                event = self.build_event(
                    oms_id=oms_id,
                    event_source="oms",
                    event_type="ORDER_REJECTED_BY_OMS",
                    symbol=symbol,
                    side=side,
                    quantity=quantity,
                    price=price,
                    gateway=gateway,
                    account=account,
                    message="NEW validation failed",
                )
                await self.publish_event(event)
                return
        else:
            if not payload.get("oms_id"):
                event = self.build_event(
                    oms_id=oms_id,
                    event_source="oms",
                    event_type="ORDER_REJECTED_BY_OMS",
                    message=f"{action} requires oms_id",
                )
                await self.publish_event(event)
                return

        accepted_event = self.build_event(
            oms_id=oms_id,
            event_source="oms",
            event_type="ORDER_ACCEPTED_BY_OMS",
            symbol=symbol,
            side=side,
            quantity=quantity,
            price=price,
            gateway=gateway,
            account=account,
            message="OMS accepted request",
        )
        await self.publish_event(accepted_event)

        # For NEW, store the latest order snapshot for later inspection.
        if action == "NEW":
            self.state.orders_by_oms_id[oms_id] = {
                "oms_id": oms_id,
                "symbol": symbol,
                "side": side,
                "quantity": quantity,
                "price": price,
                "gateway": gateway,
                "account": account,
            }

        # Emit routing event before actually sending to gateway queue.
        routing_event = self.build_event(
            oms_id=oms_id,
            event_source="oms",
            event_type="ORDER_ROUTING_TO_GATEWAY",
            symbol=symbol,
            side=side,
            quantity=quantity,
            price=price,
            gateway=gateway,
            account=account,
            message="Command queued to gateway",
        )
        await self.publish_event(routing_event)

        # Convert text action into proto enum before sending to gateway.
        action_map = {
            "NEW": pb2.ACTION_NEW,
            "MODIFY_PRICE": pb2.ACTION_MODIFY_PRICE,
            "CANCEL": pb2.ACTION_CANCEL,
        }
        frame = pb2.GatewayFrame(
            command=pb2.OrderCommand(
                cmd_id=str(uuid.uuid4()),
                action=action_map[action],
                oms_id=oms_id,
                symbol=symbol,
                side=side,
                quantity=quantity,
                price=price,
                gateway=gateway,
                account=account,
            )
        )
        await self.to_gateway_queue.put(frame)

    async def websocket_handler(self, ws):
        """Handle one strategy client lifecycle on WebSocket."""
        self.state.subscribers.add(ws)
        await self.publish_ready(ws)
        try:
            async for raw in ws:
                try:
                    payload = json.loads(raw)
                except json.JSONDecodeError:
                    error_event = self.build_event(
                        oms_id="",
                        event_source="oms",
                        event_type="ORDER_REJECTED_BY_OMS",
                        message="Invalid JSON from strategy",
                    )
                    await self.publish_event(error_event)
                    continue

                await self.handle_strategy_request(payload)
        finally:
            self.state.subscribers.discard(ws)

    async def gateway_request_stream(self):
        """Async generator used by gRPC client stream to send commands."""
        while True:
            frame = await self.to_gateway_queue.get()
            yield frame

    async def consume_gateway_stream(self):
        """Maintain long-lived gRPC stream to gateway with auto-reconnect."""
        while True:
            try:
                async with grpc.aio.insecure_channel(GRPC_TARGET) as channel:
                    stub = pb2_grpc.GatewayBridgeStub(channel)
                    stream = stub.Connect(self.gateway_request_stream())

                    async for frame in stream:
                        if frame.HasField("ready"):
                            # Ready state is forwarded to all strategy clients.
                            self.state.gateway_ready = frame.ready.status.upper() == "READY"
                            # Push latest readiness to all strategy clients.
                            for ws in list(self.state.subscribers):
                                await self.publish_ready(ws)
                            continue

                        if frame.HasField("event"):
                            # Repackage proto event to plain dict for strategy output.
                            evt = frame.event
                            event = {
                                "oms_id": evt.oms_id,
                                "event_id": evt.event_id,
                                "event_source": evt.event_source,
                                "event_type": evt.event_type,
                                "symbol": evt.symbol,
                                "side": evt.side,
                                "quantity": evt.quantity,
                                "price": evt.price,
                                "gateway": evt.gateway,
                                "account": evt.account,
                                "broker_order_id": evt.broker_order_id,
                                "event_time": evt.event_time_ms,
                                "message": evt.message,
                            }

                            if evt.broker_order_id and evt.oms_id:
                                self.state.oms_to_broker[evt.oms_id] = evt.broker_order_id

                            await self.publish_event(event)
            except Exception as exc:
                # On disconnect, mark degraded and notify strategy immediately.
                self.state.gateway_ready = False
                for ws in list(self.state.subscribers):
                    await self.publish_ready(ws)

                err_event = self.build_event(
                    oms_id="",
                    event_source="oms",
                    event_type="ORDER_ROUTING_FAILED",
                    message=f"Gateway stream disconnected: {exc}",
                )
                await self.publish_event(err_event)
                await asyncio.sleep(2)

    async def run(self):
        """Start OMS websocket server and gateway stream task together."""
        gateway_task = asyncio.create_task(self.consume_gateway_stream())
        async with websockets.serve(self.websocket_handler, WS_HOST, WS_PORT):
            print(f"[oms] websocket listening on ws://{WS_HOST}:{WS_PORT}")
            print(f"[oms] connecting to gateway gRPC at {GRPC_TARGET}")
            await asyncio.Future()

        await gateway_task


if __name__ == "__main__":
    app = OMSApp()
    asyncio.run(app.run())
