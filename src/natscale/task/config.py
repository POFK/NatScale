#!/usr/bin/env python3

from pydantic import BaseModel


class IterConfig(BaseModel):
    subject: str
    nats_server: str = "nats://127.0.0.1:4222"
    stream_name: str = "NATSCALE"
    durable_name: str = "ns_worker_group"
    auto_ack: bool = False
    timeout: float = 30.0
    retry: int = 10
