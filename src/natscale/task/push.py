#!/usr/bin/env python3

import tqdm
import asyncio
from loguru import logger
from typing import Iterator, Dict, Any, List
from faststream.nats import NatsBroker
from nats.js.errors import APIError


async def push(
    data: Iterator[Dict[str, Any]] | List[Dict[str, Any]],
    broker: NatsBroker,
    stream: str,
    subject: str,
):
    await broker.connect()
    count = 0

    for task_payload in tqdm.tqdm(data, desc="Pushing Tasks", unit="task"):
        try:
            await broker.publish(
                task_payload, subject=subject, stream=stream, timeout=3.0
            )
            count += 1

        except APIError as e:
            # 捕获 NATS JetStream API 错误
            if e.err_code == 10077:  # 10077 是 "maximum messages exceeded" 错误码
                logger.error("❌ 错误: 队列已满！无法接受任务。请稍后重试清空。")
                # 在这里实现你的回退逻辑，比如存入本地数据库或告警
                break
            else:
                logger.error(f"❌ 发生其他 NATS API 错误: {e}")
        except Exception as e:
            logger.error(f"❌ 发送失败: {e}")

    logger.success(f"pushed {count} tasks...")

    await broker.stop()


if __name__ == "__main__":
    broker = NatsBroker("nats://127.0.0.1:4222")
    data = [{"id": i, "data": f"task {i}"} for i in range(10)]
    asyncio.run(push(data, broker, "NATSCALE", "hpc.tasks.dev"))
