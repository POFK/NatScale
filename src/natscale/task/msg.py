#!/usr/bin/env python3

from typing import Dict, Any
from pydantic import BaseModel, TypeAdapter, ConfigDict

msg_adapter = TypeAdapter(Dict[str, Any])


def parse_msg_json_dict(msg: str):
    try:
        data = msg_adapter.validate_json(msg)
    except Exception as e:
        raise ValueError(f"NATSCALE parse error on {msg}: {e}")
    return data


class MsgFlexibleModel(BaseModel):
    model_config = ConfigDict(extra="allow")

    # 固定字段（必须存在）
    id: int


def parse_msg_json_pydantic(msg: str):
    return MsgFlexibleModel.model_validate_json(msg)
