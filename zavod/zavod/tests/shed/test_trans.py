from unittest.mock import patch

from zavod.context import Context
from zavod.extract.llm import TextPromptResponse
from zavod.shed.trans import (
    CHINESE_NAME_TRANSLIT_MODEL,
    NAME_TRANSLIT_MODEL,
    apply_translit_full_name,
    apply_translit_names,
)
from zavod.util import LangText


def test_apply_translit_full_name_selects_model(vcontext: Context) -> None:
    for code in ("chi", "zho"):
        response = TextPromptResponse(content='{"eng": "Cai Qi"}', cache_key="key")
        with patch("zavod.shed.trans.run_text_prompt", return_value=response) as prompt:
            entity = vcontext.make("Person")
            entity.id = f"{code}-person"
            apply_translit_full_name(vcontext, entity, LangText("蔡奇", code))
        assert prompt.call_args.kwargs["model"] == CHINESE_NAME_TRANSLIT_MODEL
        statement = next(s for s in entity.statements if s.prop == "name")
        assert statement.value == "Cai Qi"
        assert statement.origin == CHINESE_NAME_TRANSLIT_MODEL

    response = TextPromptResponse(content='{"eng": "Hor Namhong"}', cache_key="key")
    with patch("zavod.shed.trans.run_text_prompt", return_value=response) as prompt:
        entity = vcontext.make("Person")
        entity.id = "km-person"
        apply_translit_full_name(vcontext, entity, LangText("ហោ ណាំហុង", "khm"))
    assert prompt.call_args.kwargs["model"] == NAME_TRANSLIT_MODEL
    statement = next(s for s in entity.statements if s.prop == "name")
    assert statement.origin == NAME_TRANSLIT_MODEL


def test_apply_translit_full_name_explicit_model(vcontext: Context) -> None:
    response = TextPromptResponse(content='{"eng": "Cai Qi"}', cache_key="key")
    with patch("zavod.shed.trans.run_text_prompt", return_value=response) as prompt:
        entity = vcontext.make("Person")
        entity.id = "zh-person"
        apply_translit_full_name(
            vcontext, entity, LangText("蔡奇", "chi"), model="gpt-4o"
        )
    assert prompt.call_args.kwargs["model"] == "gpt-4o"


def test_apply_translit_names_selects_model(vcontext: Context) -> None:
    responses = [
        TextPromptResponse(content='{"eng": "Davit"}', cache_key="first"),
        TextPromptResponse(content='{"eng": "Ovashvili"}', cache_key="last"),
    ]
    with patch("zavod.shed.trans.run_text_prompt", side_effect=responses) as prompt:
        entity = vcontext.make("Person")
        entity.id = "ka-person"
        apply_translit_names(
            vcontext,
            entity,
            input_code="kat",
            first_name="დავით",
            last_name="ოვაშვილი",
        )
    models = [call.kwargs["model"] for call in prompt.call_args_list]
    assert models == [NAME_TRANSLIT_MODEL, NAME_TRANSLIT_MODEL]
    assert entity.get("firstName") == ["Davit"]
    assert entity.get("lastName") == ["Ovashvili"]
