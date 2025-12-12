# test_msg.py
import pytest
from pydantic import ValidationError
from natscale.task.msg import parse_msg_json_pydantic


def test_parse_valid_msg():
    json_str = '{"id": 123, "payload": "data"}'
    model = parse_msg_json_pydantic(json_str)
    assert model.id == 123
    assert model.payload == "data"  # 假设你允许额外字段


def test_parse_missing_id():
    json_str = '{"payload": "data"}'  # 缺少 id
    with pytest.raises(ValidationError):
        parse_msg_json_pydantic(json_str)


def test_parse_invalid_json():
    with pytest.raises(ValueError):  # 假设你的 wrapper 抛出 ValueError
        parse_msg_json_pydantic("not a json")
