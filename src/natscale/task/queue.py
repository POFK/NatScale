#!/usr/bin/env python3

import asyncio
import threading
import queue
from typing import Dict, Any

from loguru import logger
from pydantic import BaseModel, TypeAdapter, ConfigDict
from nats.aio.client import Client as NATS

# 用于桥接的同步队列
# 队列里存放的是 (task_data, ack_coroutine) 的元组
task_queue = queue.Queue(maxsize=1)


class NsIterParam(BaseModel):
    subject: str
    nats_server: str = "nats://127.0.0.1:4222"
    stream_name: str = "NATSCALE"
    durable_name: str = "ns_worker_group"
    auto_ack: bool = False
    timeout: int = 600


msg_adapter = TypeAdapter(Dict[str, Any])


class MsgFlexibleModel(BaseModel):
    model_config = ConfigDict(extra="allow")

    # 固定字段（必须存在）
    id: int


def parse_msg_dict_json(msg: str):
    try:
        data = msg_adapter.validate_json(msg)
    except Exception as e:
        raise ValueError(f"NATSCALE parse error on {msg}: {e}")
    return data


def parse_msg_model_json(msg: str):
    return MsgFlexibleModel.model_validate_json(msg)


async def stream_get_next(psub, timeout=15):
    while True:
        try:
            msgs = await psub.fetch(1, timeout=timeout)

            for msg in msgs:
                data = msg.data.decode()
                yield (parse_msg_model_json(data), msg.ack)

        except TimeoutError:
            logger.warning(f"get no signal from upstream NATS server in {timeout}s")
            pass
        except Exception as e:
            logger.error(f"Error: {e}")
            await asyncio.sleep(1)  # 出错后稍作等待避免死循环刷屏


async def run_nats_jetstream(
    server_url,
    subject,
    stream_name,
    durable_name="ns_worker_group",
    nc_config={},
):
    nc = NATS()
    try:
        await nc.connect(servers=[server_url], **nc_config)
        js = nc.jetstream()

        # 1. 确保 Stream 存在 (通常由管理员创建，这里为了方便自动创建)
        # Stream 就像是消息的硬盘存储区
        try:
            await js.add_stream(name=stream_name, subjects=[subject])
        except Exception:
            pass  # Stream 可能已经存在

        logger.info(f"Connected to NATS JetStream: {server_url}")

        # 2. 创建订阅 (Push Consumer)
        # manual_ack=True 是关键：必须手动确认
        # TODO how to set ack_wait
        # ack_wait=300：如果 300秒(5分钟) 没确认，视为节点挂了，任务重发
        psub = await js.pull_subscribe(
            subject,
            durable=durable_name,  # 记住消费位置
            config=None,  # 可在此配置 ack_wait 等高级参数
        )

        async for data in stream_get_next(psub):
            # 将 (数据, 确认函数) 放入队列传给主线程
            # 这里的 msg.ack() 是一个协程对象，不能直接调，要传回去让 Loop 跑
            task_queue.put(data)

            # 等待主线程取走数据，起流控作用
            # 只有主线程处理完了，队列空了，循环才会继续取下一条
            task_queue.join()

    except Exception as e:
        print(f"NATS Error: {e}")
    finally:
        await nc.close()


def _start_background_loop(loop):
    asyncio.set_event_loop(loop)
    loop.run_forever()


def ack_handler(ack_coro, loop):
    def handler():
        asyncio.run_coroutine_threadsafe(ack_coro(), loop)

    return handler


def get_task_iterator(args: NsIterParam):
    """
    TODO: Only the mode of auto_ack=Flase is worked. When auto_ack=True,
    the iterator will not push ack signal at the last iter.
    """
    if hasattr(args, "nats_server") and args.nats_server:
        print(f"Mode: NATS JetStream from {args.nats_server}")

        # 启动后台线程
        new_loop = asyncio.new_event_loop()
        t = threading.Thread(
            target=_start_background_loop, args=(new_loop,), daemon=True
        )
        t.start()

        # 提交监听任务
        asyncio.run_coroutine_threadsafe(
            run_nats_jetstream(
                args.nats_server,
                args.subject,
                args.stream_name,
                args.durable_name,
            ),
            new_loop,
        )

        last_ack_coro = None

        try:
            while True:
                # 1. 获取新任务 (阻塞等待)
                task_data, ack_coro = task_queue.get(timeout=args.timeout)

                # 2. 如果有上一个任务的 Ack 没发，现在发 (说明上一个循环没报错)
                # TODO make it to two mode: 1. auto sent ack msg 2. user manual sent ack
                # in mode 2, return both data and ack handler
                if last_ack_coro and args.auto_ack:
                    asyncio.run_coroutine_threadsafe(last_ack_coro(), new_loop)

                # 3. 记录当前的 Ack，留给下一次循环发
                last_ack_coro = ack_coro

                if args.auto_ack:
                    yield task_data
                else:
                    yield (task_data, ack_handler(ack_coro, new_loop))

                # 5. 告诉后台队列 "主线程已经取走数据了，你可以去NATS取下一个了"
                # 注意：这只是解除了队列的阻塞，并不代表计算完成。
                # 计算完成的标志是下一次 loop 回到这里执行 step 2。
                task_queue.task_done()
        except queue.Empty:
            logger.warning("Stop the iterator loop because queue is empty.")
        except Exception as e:
            logger.error(f"Error: {e}")

        # 注意：这里有一个边缘情况，就是最后一个任务跑完后，循环结束，
        # last_ack_coro 还没发。
        # 但因为是无限流模式，只要脚本不崩，就在这里循环。
        # 如果脚本正常退出，最好加个 try...finally 块来发送最后一个 ack。
