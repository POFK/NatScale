#!/usr/bin/env python3

from loguru import logger
import anyio
from anyio.from_thread import start_blocking_portal
import nats
from nats.errors import TimeoutError as NatsTimeoutError

from natscale.task.config import IterConfig
from natscale.task.msg import parse_msg_json_pydantic


async def nats_stream_get_next(psub, timeout: float = 15.0, retry: int = 4):
    retry_time = 0
    while True:
        if retry_time >= retry:
            break

        try:
            msgs = await psub.fetch(1, timeout=timeout)
            for msg in msgs:
                data = msg.data.decode()
                return parse_msg_json_pydantic(data), msg.ack
            retry_time = 0
        except NatsTimeoutError:
            retry_time += 1
            logger.debug(
                f"[retry: {retry_time} / {retry}] get no signal "
                f"from upstream NATS server in {timeout}s"
            )
            pass
        except Exception as e:
            logger.error(f"Error: {e}")
            retry_time += 1
            await anyio.sleep(1)  # 出错后稍作等待避免死循环刷屏
    return None


class NatsIterator:
    def __init__(self, config: IterConfig, nats_connector=None):
        self.config = config

        self.server_url = config.nats_server
        self.subject = config.subject
        self.durable = config.durable_name
        self.timeout = config.timeout
        self.retry = config.retry
        self.auto_ack = config.auto_ack

        # 内部状态
        self._portal = None
        self._ctx_manager = None
        self._nc = nats_connector
        self._external_nc = nats_connector is not None
        self._js = None
        self._psub = None
        self._last_ack_coro = None

        # 启动后台异步环境
        self.start()

    def start(self):
        """
        启动一个后台线程，运行 AnyIO 的 Portal。
        使用 Event 来确保 Portal 准备就绪后主线程才继续。
        """
        if self._portal is not None:
            return

        self._ctx_manager = start_blocking_portal()
        self._portal = self._ctx_manager.__enter__()

        try:
            self._portal.call(self._async_init)
        except Exception as e:
            self.close()
            raise e
        return self

    def close(self):
        if self._portal:
            try:
                self._portal.call(self._async_close)
            except Exception as e:
                logger.error(f"close nats connector error: {e}")
                pass
            finally:
                if self._ctx_manager:
                    self._ctx_manager.__exit__(None, None, None)
                self._portal = None
                self._ctx_manager = None
                logger.debug("NatsJetStreamIterator closed.")

    async def _async_init(self):
        if self._nc is None:
            self._nc = await nats.connect(self.server_url)
        self._js = self._nc.jetstream()
        self._psub = await self._js.pull_subscribe(self.subject, durable=self.durable)

    async def _async_close(self):
        if self._nc and not self._external_nc:
            await self._nc.close()
            logger.debug("Internal NATS connection closed.")

    async def _async_fetch_one(self):
        return await nats_stream_get_next(self._psub, self.timeout, self.retry)

    def _ack_handler(self, ack_coro):
        def wrap():
            return self._portal.call(ack_coro)

        return wrap

    def __iter__(self):
        return self

    def __next__(self):
        try:
            if self._last_ack_coro and self.config.auto_ack:
                self._portal.call(self._last_ack_coro)
            data = self._portal.call(self._async_fetch_one)

            if data is None:
                logger.debug("StopIteration triggered due to no data.")
                self.close()
                raise StopIteration

            task_data, ack_coro = data
            self._last_ack_coro = ack_coro
            if self.auto_ack:
                return task_data
            else:
                return (task_data, self._ack_handler(ack_coro))
        except StopIteration:
            raise
        except Exception:
            self.close()
            raise

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
