#!/usr/bin/env python3

from pydantic import BaseModel


class IterConfig(BaseModel):
    # TODO add ack_wait time for replay those messages without ack

    subject: str
    nats_server: str = "nats://127.0.0.1:4222"
    stream_name: str = "NATSCALE"
    durable_name: str = "ns_worker_group"
    auto_ack: bool = False
    timeout: float = 30.0
    retry: int = 10


"""
ack_wait example (not tested):
    self._psub = await self._js.pull_subscribe(
        self.subject, 
        durable=self.durable,
        ack_wait=60  # <--- 在这里指定
    )
"""
