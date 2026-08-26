from __future__ import annotations

import importlib
import sys
from pathlib import Path


def load_proto_modules():
    """Compile proto on demand, then import generated modules."""
    base_dir = Path(__file__).resolve().parent
    proto_file = base_dir / "oms_gateway.proto"
    pb2_file = base_dir / "oms_gateway_pb2.py"
    pb2_grpc_file = base_dir / "oms_gateway_pb2_grpc.py"

    if not proto_file.exists():
        raise FileNotFoundError(f"Missing proto file: {proto_file}")

    if not pb2_file.exists() or not pb2_grpc_file.exists():
        try:
            from grpc_tools import protoc
        except ImportError as exc:
            raise RuntimeError(
                "grpcio-tools is required to compile proto. "
                "Please install with: pip install grpcio-tools"
            ) from exc

        result = protoc.main(
            [
                "grpc_tools.protoc",
                f"-I{base_dir}",
                f"--python_out={base_dir}",
                f"--grpc_python_out={base_dir}",
                str(proto_file),
            ]
        )
        if result != 0:
            raise RuntimeError("Failed to compile oms_gateway.proto")

    if str(base_dir) not in sys.path:
        sys.path.insert(0, str(base_dir))

    pb2 = importlib.import_module("oms_gateway_pb2")
    pb2_grpc = importlib.import_module("oms_gateway_pb2_grpc")
    return pb2, pb2_grpc
