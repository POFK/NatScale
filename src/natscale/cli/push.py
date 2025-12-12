#!/usr/bin/env python3

import asyncio
import typer
import json
from pathlib import Path
from typing import Iterator, Dict, Any
from loguru import logger

from pydantic import BaseModel, ValidationError, ConfigDict
from faststream.nats import NatsBroker

from natscale.task.push import push as ns_push

app = typer.Typer()


class DynamicTaskModel(BaseModel):
    # 配置允许任意额外字段
    model_config = ConfigDict(extra="allow")

    id: int | None = None  # 示例：如果有 id 必须是 int，没有也可以


def load_jsonl_dynamic(file_path: str) -> Iterator[Dict[str, Any]]:
    path = Path(file_path)
    if not path.exists():
        raise FileNotFoundError(f"File not found: {file_path}")

    with open(path, "r", encoding="utf-8") as f:
        for line_num, line in enumerate(f, 1):
            line = line.strip()
            if not line:
                continue

            try:
                # 1. 解析 JSON
                raw_data = json.loads(line)

                # 2. 确保它是字典 (Dict) 而不是列表 (List) 或基础类型
                if not isinstance(raw_data, dict):
                    logger.warning(f"第 {line_num} 行不是 JSON 对象(Dict)，已跳过")
                    continue

                # 3. Pydantic 校验 (即使没有字段定义，也能过滤非法的 key 类型等)
                model = DynamicTaskModel(**raw_data)

                # 4. 转回字典 yield 出去
                yield model.model_dump()

            except json.JSONDecodeError:
                logger.warning(f"第 {line_num} 行 JSON 格式错误，已跳过")
            except ValidationError as e:
                logger.warning(f"第 {line_num} 行 数据校验错误: {e}")
            except Exception as e:
                logger.error(f"第 {line_num} 行 未知错误: {e}")


def parse_from_file(fp: str) -> Iterator[Dict]:
    logger.info(f"parsing the data from {fp}")
    tasks = load_jsonl_dynamic(fp)
    return tasks


@app.command()
def push(
    taskfile: str,
    server: str = typer.Option(
        "nats://127.0.0.1:4222",
        "-s",
        "--server",
        help="The nats server url, default nats://127.0.0.1:4222",
    ),
    subject: str = typer.Option(
        "natscale.tasks.dev",
        "-to",
        "--subject",
        help="The subject that natscale listened to, default natscale.tasks.dev",
    ),
    stream: str = "NATSCALE",
):
    broker = NatsBroker(server)

    tasks = parse_from_file(taskfile)
    asyncio.run(ns_push(tasks, broker, stream, subject))
