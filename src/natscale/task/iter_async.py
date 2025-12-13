#!/usr/bin/env python3
# iter.py
import asyncio
import threading
import queue
from loguru import logger
from nats.aio.client import Client as NATS

from natscale.task.config import IterConfig
from natscale.task.msg import parse_msg_json_pydantic


def _ack_handler(ack_coro, loop):
    def handler():
        if loop and loop.is_running():
            asyncio.run_coroutine_threadsafe(ack_coro(), loop)

    return handler


async def stream_get_next(psub, timeout=15):
    while True:
        try:
            msgs = await psub.fetch(1, timeout=timeout)

            for msg in msgs:
                data = msg.data.decode()
                yield (parse_msg_json_pydantic(data), msg.ack)
        except TimeoutError:
            logger.debug(f"get no signal from upstream NATS server in {timeout}s")
            pass
        except Exception as e:
            logger.error(f"Error: {e}")
            await asyncio.sleep(1)  # 出错后稍作等待避免死循环刷屏


class NatsIterator:
    def __init__(self, config: IterConfig, nc=None):
        self.config = config
        # 将队列作为实例变量，而非全局变量
        self._task_queue = queue.Queue(maxsize=1)
        self.loop = None
        self.thread = None
        # 保存最后一个未确认的 ack 协程
        self._last_ack_coro = None
        if nc is None:
            self.nc = NATS()
            self._own_nc = True
        else:
            self.nc = nc
            self._own_nc = False

    @property
    def task_queue(self):
        return self._task_queue

    def _start_background_loop(self, loop):
        asyncio.set_event_loop(loop)
        loop.run_forever()

    async def _run_nats(self, nc):
        try:
            if not nc.is_connected:
                await nc.connect(servers=[self.config.nats_server])

            js = nc.jetstream()

            psub = await js.pull_subscribe(
                self.config.subject, durable=self.config.durable_name
            )

            async for data in stream_get_next(psub, timeout=15):
                self.task_queue.put(data)
                self.task_queue.join()

        except Exception as e:
            logger.error(f"NATS Error: {e}")
        finally:
            if self._own_nc:
                await nc.close()

    def _init_setup(self):
        if self.config.nats_server and not self.loop:
            self.loop = asyncio.new_event_loop()
            self.thread = threading.Thread(
                target=self._start_background_loop, args=(self.loop,), daemon=True
            )
            self.thread.start()
            asyncio.run_coroutine_threadsafe(self._run_nats(self.nc), self.loop)

    def __iter__(self):
        if not self.loop:
            self._init_setup()

        try:
            while True:
                # Auto Ack 逻辑
                if self._last_ack_coro and self.config.auto_ack:
                    if self.loop:
                        asyncio.run_coroutine_threadsafe(
                            self._last_ack_coro(),
                            self.loop,
                        )

                # 阻塞等待，使用配置的 timeout
                task_data, ack_coro = self.task_queue.get(timeout=self.config.timeout)

                self._last_ack_coro = ack_coro

                if self.config.auto_ack:
                    yield task_data
                else:
                    yield (task_data, _ack_handler(ack_coro, self.loop))

                self.task_queue.task_done()

        except queue.Empty:
            return
