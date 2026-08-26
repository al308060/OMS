# Local MVP Run Guide

This project contains 3 local programs:
- `gateway_shioaji.py`
- `oms.py`
- `strategy.py`

All services run on the same machine.

## 1) Install dependencies

```bash
pip install -r requirements.txt
```

## 2) Prepare `.env`

Create a `.env` file in this folder with:

```env
SJ_API_KEY=YOUR_KEY
SJ_SEC_KEY=YOUR_SECRET
```

## 3) Start services (three terminals)

Terminal A:
```bash
python gateway_shioaji.py
```

Terminal B:
```bash
python oms.py
```

Terminal C:
```bash
python strategy.py
```

## 4) Strategy command examples

In strategy terminal:

- New order:
```text
new 2330 BUY 1 1000
```

- Modify price:
```text
modify <OMS_ID> 1001
```

- Cancel order:
```text
cancel <OMS_ID>
```

- Exit:
```text
quit
```

`OMS_ID` is shown in strategy event prints after a new order request.

## 5) Readiness check

- Strategy waits for READY message from OMS.
- OMS marks READY only when gateway stream is connected.

If strategy prints `OMS/Gateway not ready yet`, check gateway/oms logs first.

## 6) Notes for MVP

- State is in Python memory only.
- Restarting a process clears in-memory mappings.
- Event push happens on every new event (strategy request, oms events, gateway callback events).
