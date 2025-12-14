#!/usr/bin/env python3

import pytest
import json
from typing import List
from unittest.mock import MagicMock, AsyncMock

from natscale.task.config import IterConfig
from natscale.task.iter import NatsIterator

default_config = dict(subject="test", timeout=0.1, retry=1)


class MockNatsConnector:
    def __init__(self, mock_msg, config):
        self.msg = mock_msg
        self.config = config
        self.sub = MagicMock()
        self.sub.fetch = AsyncMock()
        # self.sub.fetch.side_effect = mock_msg + [Exception("TestStop")]
        # self.sub.fetch.side_effect = mock_msg + [StopIteration]
        self.sub.fetch.side_effect = mock_msg  # + [Exception("TestStop")]

        # 构造一个假的 client 和 js context
        self.nc = MagicMock()
        self.js = MagicMock()
        self.nc.jetstream.return_value = self.js
        self.js.pull_subscribe = AsyncMock(return_value=self.sub)

    @classmethod
    def build_from_msg(cls, data: List[dict], config: IterConfig):
        mock_msg = []
        for i in data:
            msg = MagicMock()
            msg.data = json.dumps(i).encode()
            msg.ack = AsyncMock()
            mock_msg.append([msg])
        return cls(mock_msg, config)

    def msg_ack(self, idx):
        return self.msg[idx][0].ack

    def msg_data(self, idx):
        msg = self.msg[idx][0].data
        return json.loads(msg.decode())

    @property
    def natscale_iter(self):
        return NatsIterator(self.config, nats_connector=self.nc)


@pytest.mark.alpha
def test_manual_ack_mode():
    """测试手动 ACK 模式：yield 出来的数据应该包含 ack handler"""
    cfg = IterConfig(auto_ack=False, **default_config)
    fake_data = {"id": 1}
    conn = MockNatsConnector.build_from_msg(data=[fake_data], config=cfg)
    iterator = conn.natscale_iter
    ack = conn.msg_ack(0)

    # 2. 开始迭代
    gen = iterator.__iter__()

    # 3. 获取第一个结果
    result_data, result_handler = next(gen)

    assert result_data.model_dump() == fake_data
    assert callable(result_handler)

    # 验证此时 ack 不应该被调用 (因为是手动模式)
    ack.assert_not_called()
    result_handler()
    ack.assert_called_once()


@pytest.mark.alpha
def test_auto_ack_logic():
    """测试自动 ACK 模式：Delayed Ack 机制"""
    cfg = IterConfig(auto_ack=True, **default_config)
    tasks = [{"id": 1}, {"id": 2}]
    conn = MockNatsConnector.build_from_msg(data=tasks, config=cfg)
    iterator = conn.natscale_iter

    idx = 0
    for i in iterator:
        if idx > 0:
            conn.msg_ack(idx - 1).assert_called_once()
        if idx >= len(tasks):
            break
        ack = conn.msg_ack(idx)
        ack.assert_not_called()
        assert conn.msg_data(idx) == i.model_dump()
        idx += 1
    conn.msg_ack(len(tasks) - 1).assert_called_once()


@pytest.mark.alpha
def test_iterator_timeout():
    """测试队列空时的超时退出"""
    cfg = IterConfig(auto_ack=False, **default_config)
    tasks = []
    conn = MockNatsConnector.build_from_msg(data=tasks, config=cfg)
    iterator = conn.natscale_iter

    results = list(iterator)
    assert len(results) == 0


@pytest.mark.alpha
@pytest.mark.parametrize("error", [KeyboardInterrupt, ValueError])
def test_auto_ack_when_manual_keyboard_interrupt_skips_ack(error):
    """
    场景：模拟用户按下 Ctrl-C (KeyboardInterrupt)。
    预期：不发送 Ack。
    """
    cfg = IterConfig(auto_ack=True, **default_config)

    tasks = [{"id": 1}, {"id": 2}]
    conn = MockNatsConnector.build_from_msg(data=tasks, config=cfg)
    iterator = conn.natscale_iter

    with pytest.raises(error):
        for data in iterator:
            if data.id == 2:
                raise error()
    conn.msg_ack(0).assert_called_once()
    conn.msg_ack(1).assert_not_called()
