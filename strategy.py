from __future__ import annotations

import asyncio
import json
import uuid

import websockets

# Fixed constants for MVP. These fields are sent in every command,
# but OMS will not use them for routing decisions yet.
DEFAULT_GATEWAY = "shioaji_sim"
DEFAULT_ACCOUNT = "default_stock_account"
WS_URL = "ws://127.0.0.1:8765"


class StrategyClient:
    """CLI strategy client.

    Responsibilities:
    1) Send command payloads to OMS over WebSocket.
    2) Receive and print all real-time events pushed by OMS.
    """

    def __init__(self):
        self.oms_ready = False

    async def receiver(self, ws):
        """Receive push messages from OMS and display them immediately."""
        async for raw in ws:
            try:
                msg = json.loads(raw)
            except json.JSONDecodeError:
                print(f"[strategy] non-json message: {raw}")
                continue

            kind = msg.get("kind")
            if kind == "ready":
                # Strategy only sends commands after OMS confirms gateway is ready.
                self.oms_ready = bool(msg.get("gateway_ready"))
                print(f"[strategy] READY message: {msg}")
            elif kind == "event":
                # OMS pushes every new event; print full event for easy tracing.
                print(f"[strategy] EVENT: {json.dumps(msg['data'], ensure_ascii=False)}")
            else:
                print(f"[strategy] MESSAGE: {json.dumps(msg, ensure_ascii=False)}")

    async def sender(self, ws):
        """Interactive command loop: NEW / MODIFY_PRICE / CANCEL."""
        print("[strategy] command format:")
        print("  new SYMBOL SIDE QTY PRICE")
        print("  modify OMS_ID NEW_PRICE")
        print("  cancel OMS_ID")
        print("  quit")

        while True:
            user_input = await asyncio.to_thread(input, "strategy> ")
            parts = user_input.strip().split()
            if not parts:
                continue

            cmd = parts[0].lower()
            if cmd == "quit":
                print("[strategy] exit requested")
                return

            if not self.oms_ready:
                # Protects from sending commands before OMS<->Gateway is usable.
                print("[strategy] OMS/Gateway not ready yet, please wait for READY message")
                continue

            try:
                payload = self.build_payload(parts)
            except ValueError as exc:
                print(f"[strategy] invalid command: {exc}")
                continue

            await ws.send(json.dumps(payload, ensure_ascii=False))
            print(f"[strategy] command sent: {payload}")

    def build_payload(self, parts: list[str]) -> dict:
        """Convert one CLI command into a normalized OMS request payload."""
        cmd = parts[0].lower()

        if cmd == "new":
            if len(parts) != 5:
                raise ValueError("new requires: new SYMBOL SIDE QTY PRICE")
            symbol, side, qty_text, price_text = parts[1], parts[2], parts[3], parts[4]
            return {
                "request_id": str(uuid.uuid4()),
                "action": "NEW",
                "symbol": symbol,
                "side": side.upper(),
                "quantity": int(qty_text),
                "price": float(price_text),
                "gateway": DEFAULT_GATEWAY,
                "account": DEFAULT_ACCOUNT,
            }

        if cmd == "modify":
            if len(parts) != 3:
                raise ValueError("modify requires: modify OMS_ID NEW_PRICE")
            oms_id, price_text = parts[1], parts[2]
            return {
                "request_id": str(uuid.uuid4()),
                "action": "MODIFY_PRICE",
                "oms_id": oms_id,
                "price": float(price_text),
                "gateway": DEFAULT_GATEWAY,
                "account": DEFAULT_ACCOUNT,
            }

        if cmd == "cancel":
            if len(parts) != 2:
                raise ValueError("cancel requires: cancel OMS_ID")
            oms_id = parts[1]
            return {
                "request_id": str(uuid.uuid4()),
                "action": "CANCEL",
                "oms_id": oms_id,
                "gateway": DEFAULT_GATEWAY,
                "account": DEFAULT_ACCOUNT,
            }

        raise ValueError(f"unknown command: {cmd}")


async def main():
    # Run sender and receiver concurrently on one WebSocket connection.
    client = StrategyClient()
    async with websockets.connect(WS_URL) as ws:
        recv_task = asyncio.create_task(client.receiver(ws))
        send_task = asyncio.create_task(client.sender(ws))

        done, pending = await asyncio.wait(
            [recv_task, send_task],
            return_when=asyncio.FIRST_COMPLETED,
        )

        for task in pending:
            task.cancel()
        for task in done:
            task.result()


if __name__ == "__main__":
    asyncio.run(main())
