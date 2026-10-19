"""Tests for the vocabulary shared by the funes agents."""

from zavod.shed.funes.agents import Brief, render_brief


_BRIEF = Brief(
    people_sought="Board members",
    subject_label="Organization",
    subject="Example Foundation",
)


def test_render_brief() -> None:
    brief = "People sought: Board members\nOrganization: Example Foundation"
    assert render_brief(_BRIEF) == brief
