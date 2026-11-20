from typing import Any
from uuid import uuid4

import asyncpg
import httpx
from workers.shipments import ShipmentRecovery

from common.constants import TransferDirection, TransferStatus
from common.http import InternalClient


async def test_unavailable_source_does_not_block_other_destination() -> None:
    source_id, destination_id, already_received = uuid4(), uuid4(), uuid4()
    sends = []
    sync_attempts = []

    class Pool:
        async def fetch(self, query: str, *args: Any) -> list[dict[str, Any]]:
            if "FROM transfer_summary" in query:
                assert args == (TransferStatus.CREATED, TransferStatus.SHIPPED)
                return [
                    {"transfer_id": tid, "status": status, "from_dc_id": 1, "to_dc_id": 3}
                    for tid, status in [
                        (source_id, TransferStatus.CREATED),
                        (destination_id, TransferStatus.SHIPPED),
                        (already_received, TransferStatus.SHIPPED),
                    ]
                ]
            if "dc1.transfer_order" in query:
                raise asyncpg.PostgresConnectionError("offline source")
            assert "dc3.transfer_order" in query
            assert args == ([destination_id, already_received],)
            return [
                {
                    "transfer_id": tid,
                    "direction": TransferDirection.INBOUND,
                    "status": status,
                }
                for tid, status in [
                    (destination_id, TransferStatus.CREATED),
                    (already_received, TransferStatus.RECEIVED),
                ]
            ]

        async def execute(self, query: str, *args: Any) -> None:
            assert "SET last_sync_at" in query
            sync_attempts.extend(args)

    async def respond(request: httpx.Request) -> httpx.Response:
        sends.append(request.url.path)
        return httpx.Response(200)

    async with InternalClient("http://dc3", "key", transport=httpx.MockTransport(respond)) as node:
        worker = ShipmentRecovery(Pool(), {3: node})  # type: ignore[arg-type]
        await worker.tick()
        await worker.tick()  # Nam vẫn CREATED dù lần trước đã trả HTTP 200.

    assert sends == [f"/transfers/{destination_id}/sync-status"] * 2
    assert sync_attempts == [destination_id] * 2
