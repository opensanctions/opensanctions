from time import sleep
from typing import List

# isort: skip to prevent reordering
from zavod import settings  # isort: skip
import dspy  # isort: skip


class CleanNames(dspy.Signature):
    """Names categorised and cleaned from non-name characters."""

    text: str = dspy.InputField()
    full_names: List[str] = dspy.OutputField(
        desc="A list of the names of this entity, potentially in various languages and transliterations."
    )
    aliases: list[str] = dspy.OutputField(
        desc="A list of alternative names or nicknames for this entity."
    )
    weak_aliases: list[dict[str, str]] = dspy.OutputField(
        desc="A list of names identified as weak aliases in the text."
    )


def make_module():
    lm = dspy.LM("openai/gpt-4o-mini", api_key=settings.OPENAI_API_KEY)
    dspy.configure(lm=lm)
    return dspy.Predict(CleanNames)


def do_thing():
    module = make_module()
    names = [
        "EFERMEROV / YEFEMEROV Yulia Alexandrova",
        "Vladimir Putin (Pyutin)",
        "Jeffrey Blankard (Bobo)",  # (nickname/weak alias)
        "BABIC Josip geb. 06.02.1979 in Deutschland; GOJKOVIC Dragan geb. 08.09.1977 in Berane/Montenegro; GREPO David geb. 02.03.1983; KAROLY Ferenc geb. 31.03.1975; und weitere",
        "Mme Joséphine Solange ANABA MBARGA;Anaba Mbarga joséphine solange",
        "JILL O&#039;CONNOR",
    ]
    for text in names:
        response = module(text=text)
        print()
        print("Input:", text)
        print("full_names:", response.full_names)
        print("aliases:", response.aliases)
        print("weak_aliases:", response.weak_aliases)


if __name__ == "__main__":
    do_thing()
