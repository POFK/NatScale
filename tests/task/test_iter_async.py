#!/usr/bin/env python3

import queue
import pytest
from unittest.mock import MagicMock, AsyncMock, patch

from natscale.task.config import IterConfig
from natscale.task.iter_async import NatsIterator


@pytest.fixture
def mock_ack():
    """创建一个异步的 Mock 对象来模拟 ack()"""
    return AsyncMock()


def make_iterator(cfg):
    with patch.object(NatsIterator, "_init_setup", autospec=True):
        iterator = NatsIterator(cfg)
        # iterator._run_nats = AsyncMock
        mock_loop = MagicMock()
        iterator.loop = mock_loop

    return iterator


def test_manual_ack_mode(mock_ack):
    """测试手动 ACK 模式：yield 出来的数据应该包含 ack handler"""
    cfg = IterConfig(subject="test", auto_ack=False, timeout=1)
    iterator = make_iterator(cfg)

    # 1. 手动往队列里塞入数据 (绕过 NATS)
    fake_data = {"id": 1}
    iterator.task_queue.put((fake_data, mock_ack))

    # 2. 开始迭代
    gen = iterator.__iter__()

    # 3. 获取第一个结果
    result_data, result_handler = next(gen)

    assert result_data == fake_data
    assert callable(result_handler)

    # 验证此时 ack 不应该被调用 (因为是手动模式)
    mock_ack.assert_not_called()

    result_handler()

    mock_ack.assert_called_once()
    # 清理队列状态 (task_done)，否则下一次 put 可能会阻塞
    iterator.task_queue.task_done()


def test_auto_ack_logic():
    """测试自动 ACK 模式：Delayed Ack 机制"""
    cfg = IterConfig(subject="test", auto_ack=True, timeout=1)
    # iterator = NatsIterator(cfg)
    iterator = make_iterator(cfg)
    iterator._task_queue = queue.Queue(maxsize=2)

    tasks = [
        {"data": {"id": 1}, "ack": AsyncMock()},
        {"data": {"id": 2}, "ack": AsyncMock()},
    ]
    for t in tasks:
        iterator.task_queue.put((t["data"], t["ack"]))

    ack = None

    idx = 0
    for i in iterator:
        if ack is not None:
            ack.assert_called_once()
        t = tasks[idx]
        data = t["data"]
        ack = t["ack"]
        assert data == i
        ack.assert_not_called()
        idx += 1
    ack.assert_called_once()


def test_iterator_timeout():
    """测试队列空时的超时退出"""
    cfg = IterConfig(subject="test", timeout=0.1)  # 极短超时
    # iterator = NatsIterator(cfg)
    iterator = make_iterator(cfg)

    # 队列是空的
    # 期待迭代器正常结束 (StopIteration) 而不是一直卡住
    results = list(iterator)
    assert len(results) == 0


@pytest.mark.parametrize("error", [KeyboardInterrupt, ValueError])
def test_auto_ack_when_manual_keyboard_interrupt_skips_ack(error):
    """
    场景：模拟用户按下 Ctrl-C (KeyboardInterrupt)。
    预期：不发送 Ack。
    """
    cfg = IterConfig(subject="test", auto_ack=True, timeout=1)
    # iterator = NatsIterator(cfg)
    iterator = make_iterator(cfg)
    iterator._task_queue = queue.Queue(maxsize=2)

    # 准备两个任务
    ack1 = AsyncMock()
    ack2 = AsyncMock()

    iterator.task_queue.put(({"id": 1}, ack1))
    iterator.task_queue.put(({"id": 2}, ack2))

    with pytest.raises(error):
        for data in iterator:
            if data["id"] == 2:
                raise error()
    ack1.assert_called_once()
    ack2.assert_not_called()
