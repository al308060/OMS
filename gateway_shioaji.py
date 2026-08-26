from __future__ import annotations

import asyncio
import os
import time
import uuid

import grpc
import shioaji as sj
from dotenv import load_dotenv

from proto_loader import load_proto_modules

pb2, pb2_grpc = load_proto_modules()

# Local gRPC bind address for OMS<->Gateway communication.
GRPC_HOST = "127.0.0.1"
GRPC_PORT = 50051


class GatewayBridgeService(pb2_grpc.GatewayBridgeServicer):
    """gRPC bridge between OMS and Shioaji.

    Responsibilities:
    1) Receive normalized commands from OMS.
    2) Execute Shioaji actions.
    3) Convert callbacks/results into normalized events and stream back to OMS.
    """

    def __init__(self):
        self.api: sj.Shioaji | None = None
        self.ready = False
        self.loop: asyncio.AbstractEventLoop | None = None

        # In-memory maps for MVP lifecycle tracking.
        self.trades_by_oms_id: dict = {}
        self.broker_to_oms: dict = {}
        self.custom_to_oms: dict = {}

        # Every OMS stream connection owns one outbound queue.
        self.stream_queues: set[asyncio.Queue] = set()

    def now_ms(self) -> int:
        return int(time.time() * 1000)

    def _get_value(self, container, key, default=None):
        """Read a field from dict-like or attribute-like callback objects."""
        if isinstance(container, dict):
            return container.get(key, default)
        try:
            return container[key]
        except Exception:
            return getattr(container, key, default)

    def _to_dict(self, obj):
        """Best-effort conversion for callback sub-objects."""
        if isinstance(obj, dict):
            return obj
        if hasattr(obj, "items"):
            try:
                return dict(obj.items())
            except Exception:
                pass
        if hasattr(obj, "_asdict"):
            try:
                return obj._asdict()
            except Exception:
                pass
        if hasattr(obj, "__dict__") and isinstance(obj.__dict__, dict):
            return dict(obj.__dict__)
        return {}

    def _new_corr_key(self) -> str:
        """Generate a short key that fits Shioaji custom_field length limit (<=6)."""
        for _ in range(20):
            key = uuid.uuid4().hex[:6]
            if key not in self.custom_to_oms:
                return key
        # Extremely unlikely fallback.
        return uuid.uuid4().hex[:6]

    async def init_shioaji(self):
        """Login and register callback once at startup."""
        self.loop = asyncio.get_running_loop()
        load_dotenv()
        api_key = os.getenv("SJ_API_KEY")
        sec_key = os.getenv("SJ_SEC_KEY")
        if not api_key or not sec_key:
            raise RuntimeError("Missing SJ_API_KEY or SJ_SEC_KEY in environment/.env")

        self.api = sj.Shioaji(simulation=True)
        self.api.login(api_key=api_key, secret_key=sec_key)

        @self.api.on_order
        def order_callback(stat, msg):
            # Shioaji callback is sync; move work into asyncio loop thread-safely.
            if self.loop is not None:
                asyncio.run_coroutine_threadsafe(self.handle_callback(stat, msg), self.loop)

        self.ready = True
        print("[gateway] shioaji login success, callback registered")

    def build_event(
        self,
        *,
        oms_id: str,
        event_type: str,
        symbol: str = "",
        side: str = "",
        quantity: int = 0,
        price: float = 0.0,
        gateway: str = "shioaji_sim",
        account: str = "default_stock_account",
        broker_order_id: str = "",
        message: str = "",
    ):
        # Create normalized event payload for OMS.
        return pb2.OrderEvent(
            event_id=str(uuid.uuid4()),
            oms_id=oms_id,
            event_source="gateway",
            event_type=event_type,
            symbol=symbol,
            side=side,
            quantity=quantity,
            price=price,
            gateway=gateway,
            account=account,
            broker_order_id=broker_order_id,
            event_time_ms=self.now_ms(),
            message=message,
        )

    async def broadcast_event(self, event):
        """Push one event to every connected OMS stream."""
        dead = []
        for queue in self.stream_queues:
            try:
                await queue.put(pb2.GatewayFrame(event=event))
            except Exception:
                dead.append(queue)
        for queue in dead:
            self.stream_queues.discard(queue)

    async def handle_callback(self, stat, msg):
        """Normalize callback payload to MVP event types and publish.

        Callback payload formats may vary by venue/status, so fallback rules are used.
        """
        operation = self._to_dict(self._get_value(msg, "operation", {}))
        order = self._to_dict(self._get_value(msg, "order", {}))
        status = self._to_dict(self._get_value(msg, "status", {}))
        contract = self._to_dict(self._get_value(msg, "contract", {}))

        # Ignore callbacks that carry no parseable fields; they would only add noise.
        if not operation and not order and not status and not contract:
            return

        broker_order_id = order.get("ordno") or order.get("id") or order.get("seqno") or ""
        custom_field = str(order.get("custom_field", "") or "")

        # Prefer deterministic correlation key from custom_field.
        oms_id = ""
        if custom_field:
            oms_id = self.custom_to_oms.get(custom_field, "")

        # Fallback to broker id mapping when custom field is unavailable.
        if not oms_id:
            oms_id = self.broker_to_oms.get(broker_order_id, "")

        # If callback arrives slightly earlier than mapping, wait briefly and retry.
        if broker_order_id and not oms_id:
            for _ in range(6):
                await asyncio.sleep(0.05)
                oms_id = self.broker_to_oms.get(broker_order_id, "")
                if oms_id:
                    break

        event_type = "ORDER_EVENT_UNKNOWN"
        op_type = str(operation.get("op_type", "")).lower()
        op_code_raw = operation.get("op_code")
        op_code = "" if op_code_raw is None else str(op_code_raw)

        # Only treat as failure when op_code is explicitly present and not 00.
        if op_code and op_code != "00":
            if "cancel" in op_type:
                event_type = "ORDER_CANCEL_FAILED"
            elif "update" in op_type or "modify" in op_type:
                event_type = "ORDER_MODIFY_PRICE_FAILED"
            else:
                event_type = "ORDER_SUBMIT_FAILED"
        else:
            if "cancel" in op_type:
                event_type = "ORDER_CANCELED"
            elif "update" in op_type or "modify" in op_type:
                event_type = "ORDER_MODIFY_PRICE_ACKNOWLEDGED"
            elif "new" in op_type:
                event_type = "ORDER_ACKNOWLEDGED"

        # Deal/Filled signals can vary; use text fallback for MVP.
        stat_text = str(stat).lower()
        raw_text = str(msg).lower()
        if "deal" in stat_text or "filled" in raw_text:
            if "part" in raw_text:
                event_type = "ORDER_PARTIALLY_FILLED"
            else:
                event_type = "ORDER_FILLED"

        event = self.build_event(
            oms_id=oms_id,
            event_type=event_type,
            symbol=order.get("code", "") or contract.get("code", ""),
            side=str(order.get("action", "")).upper(),
            quantity=int(order.get("quantity", 0) or 0),
            price=float(order.get("price", 0) or 0),
            broker_order_id=broker_order_id,
            message=f"callback stat={stat}",
        )
        await self.broadcast_event(event)

    async def handle_new_order(self, cmd):
        """Execute NEW order from OMS and store local mappings."""
        api = self.api
        if api is None:
            raise RuntimeError("Shioaji not initialized")

        # Emit submit-sent first so downstream sees request flow in expected order.
        sent_event = self.build_event(
            oms_id=cmd.oms_id,
            event_type="ORDER_SUBMIT_SENT",
            symbol=cmd.symbol,
            side=cmd.side,
            quantity=cmd.quantity,
            price=cmd.price,
            gateway=cmd.gateway,
            account=cmd.account,
            message="place_order request sent from gateway",
        )
        await self.broadcast_event(sent_event)

        contract = api.contracts.get(cmd.symbol)
        if contract is None:
            event = self.build_event(
                oms_id=cmd.oms_id,
                event_type="ORDER_SUBMIT_FAILED",
                symbol=cmd.symbol,
                side=cmd.side,
                quantity=cmd.quantity,
                price=cmd.price,
                gateway=cmd.gateway,
                account=cmd.account,
                message="Contract not found",
            )
            await self.broadcast_event(event)
            return

        action = sj.Action.Buy if cmd.side.upper() == "BUY" else sj.Action.Sell

        # Correlation token for callback -> oms_id lookup.
        # Keep it short and deterministic for this request lifecycle.
        corr_key = self._new_corr_key()
        self.custom_to_oms[corr_key] = cmd.oms_id

        order = sj.StockOrder(
            action=action,
            price=cmd.price,
            quantity=cmd.quantity,
            price_type=sj.StockPriceType.LMT,
            order_type=sj.OrderType.ROD,
            order_lot=sj.StockOrderLot.Common,
            order_cond=sj.StockOrderCond.Cash,
            account=api.stock_account,
            custom_field=corr_key,
        )

        # place_order is sync, so run in worker thread to keep asyncio responsive.
        trade = await asyncio.to_thread(api.place_order, contract, order)
        self.trades_by_oms_id[cmd.oms_id] = trade

        broker_order_id = (
            getattr(trade.order, "ordno", "")
            or getattr(trade.order, "id", "")
            or getattr(trade.order, "seqno", "")
        )

        # Map all available broker ids to reduce callback race/mismatch issues.
        broker_ordno = getattr(trade.order, "ordno", "")
        broker_id = getattr(trade.order, "id", "")
        broker_seqno = getattr(trade.order, "seqno", "")
        for key in [broker_order_id, broker_ordno, broker_id, broker_seqno]:
            if key:
                self.broker_to_oms[key] = cmd.oms_id

        # Do not emit a second submit event here to avoid duplicate semantic overlap.

    async def handle_modify_price(self, cmd):
        """Execute MODIFY_PRICE using in-memory oms_id -> trade mapping."""
        api = self.api
        trade = self.trades_by_oms_id.get(cmd.oms_id)
        if api is None or trade is None:
            event = self.build_event(
                oms_id=cmd.oms_id,
                event_type="ORDER_MODIFY_PRICE_FAILED",
                symbol=cmd.symbol,
                side=cmd.side,
                quantity=cmd.quantity,
                price=cmd.price,
                gateway=cmd.gateway,
                account=cmd.account,
                message="oms_id not found in gateway memory",
            )
            await self.broadcast_event(event)
            return

        # Same semantic as NEW: first indicate command is sent from gateway path.
        sent_event = self.build_event(
            oms_id=cmd.oms_id,
            event_type="ORDER_MODIFY_PRICE_SENT",
            symbol=cmd.symbol,
            side=cmd.side,
            quantity=cmd.quantity,
            price=cmd.price,
            gateway=cmd.gateway,
            account=cmd.account,
            message="update_order request sent from gateway",
        )
        await self.broadcast_event(sent_event)

        await asyncio.to_thread(api.update_order, trade, price=cmd.price)
        # Final status for modify should come from callback event classification.

    async def handle_cancel(self, cmd):
        """Execute CANCEL using in-memory oms_id -> trade mapping."""
        api = self.api
        trade = self.trades_by_oms_id.get(cmd.oms_id)
        if api is None or trade is None:
            event = self.build_event(
                oms_id=cmd.oms_id,
                event_type="ORDER_CANCEL_FAILED",
                symbol=cmd.symbol,
                side=cmd.side,
                quantity=cmd.quantity,
                price=cmd.price,
                gateway=cmd.gateway,
                account=cmd.account,
                message="oms_id not found in gateway memory",
            )
            await self.broadcast_event(event)
            return

        # Same semantic as NEW: first indicate command is sent from gateway path.
        sent_event = self.build_event(
            oms_id=cmd.oms_id,
            event_type="ORDER_CANCEL_SENT",
            symbol=cmd.symbol,
            side=cmd.side,
            quantity=cmd.quantity,
            price=cmd.price,
            gateway=cmd.gateway,
            account=cmd.account,
            message="cancel_order request sent from gateway",
        )
        await self.broadcast_event(sent_event)

        await asyncio.to_thread(api.cancel_order, trade)
        # Final status for cancel should come from callback event classification.

    async def handle_command(self, cmd):
        """Dispatch one incoming OMS command to the correct handler."""
        try:
            if cmd.action == pb2.ACTION_NEW:
                await self.handle_new_order(cmd)
            elif cmd.action == pb2.ACTION_MODIFY_PRICE:
                await self.handle_modify_price(cmd)
            elif cmd.action == pb2.ACTION_CANCEL:
                await self.handle_cancel(cmd)
            else:
                event = self.build_event(
                    oms_id=cmd.oms_id,
                    event_type="ORDER_EVENT_UNKNOWN",
                    message=f"Unknown action code: {cmd.action}",
                )
                await self.broadcast_event(event)
        except Exception as exc:
            event_type = "ORDER_EVENT_UNKNOWN"
            if cmd.action == pb2.ACTION_NEW:
                event_type = "ORDER_SUBMIT_FAILED"
            elif cmd.action == pb2.ACTION_MODIFY_PRICE:
                event_type = "ORDER_MODIFY_PRICE_FAILED"
            elif cmd.action == pb2.ACTION_CANCEL:
                event_type = "ORDER_CANCEL_FAILED"

            event = self.build_event(
                oms_id=cmd.oms_id,
                event_type=event_type,
                symbol=cmd.symbol,
                side=cmd.side,
                quantity=cmd.quantity,
                price=cmd.price,
                gateway=cmd.gateway,
                account=cmd.account,
                message=f"Exception: {exc}",
            )
            await self.broadcast_event(event)

    async def process_incoming(self, request_iterator):
        """Consume inbound gRPC frames from OMS and process commands."""
        async for frame in request_iterator:
            if frame.HasField("command"):
                await self.handle_command(frame.command)

    async def Connect(self, request_iterator, context):
        """Bidirectional stream endpoint for OMS.

        Inbound:  OMS command frames.
        Outbound: gateway ready/event frames.
        """
        queue: asyncio.Queue = asyncio.Queue()
        self.stream_queues.add(queue)

        # Send immediate readiness snapshot when OMS connects.
        ready_frame = pb2.GatewayFrame(
            ready=pb2.ReadyMessage(
                component="gateway_shioaji",
                status="READY" if self.ready else "NOT_READY",
                detail="shioaji login initialized" if self.ready else "not initialized",
                ts_ms=self.now_ms(),
            )
        )
        await queue.put(ready_frame)

        incoming_task = asyncio.create_task(self.process_incoming(request_iterator))

        try:
            while True:
                frame = await queue.get()
                yield frame
        finally:
            incoming_task.cancel()
            self.stream_queues.discard(queue)


async def serve():
    """Bootstrap gateway service and start gRPC server."""
    service = GatewayBridgeService()
    await service.init_shioaji()

    server = grpc.aio.server()
    pb2_grpc.add_GatewayBridgeServicer_to_server(service, server)
    server.add_insecure_port(f"{GRPC_HOST}:{GRPC_PORT}")
    await server.start()

    print(f"[gateway] gRPC listening on {GRPC_HOST}:{GRPC_PORT}")
    await server.wait_for_termination()


if __name__ == "__main__":
    asyncio.run(serve())
