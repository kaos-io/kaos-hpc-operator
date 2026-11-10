import pytest
from packaging.version import Version

from kaos_engine.version import pep440_from_tag


@pytest.mark.parametrize("tag,want", [
    ("v0.4.7", "0.4.7"),
    ("0.4.7", "0.4.7"),
    ("v0.4.7-dev-20261110-123456-abc1234", "0.4.7.post1.dev20261110123456+abc1234"),
])
def test_should_convert_the_build_tag_to_pep440(tag, want):
    assert pep440_from_tag(tag) == want
    Version(want)


def test_should_sort_a_dev_build_after_its_tag_and_before_the_next():
    dev = Version(pep440_from_tag("v0.4.7-dev-20261110-123456-abc1234"))
    assert Version("0.4.7") < dev < Version("0.4.8.dev1") < Version("0.4.8")


@pytest.mark.parametrize("tag", ["", "latest", "v1.2", "v1.2.3-rc1", "v1.2.3-dev-2026-1"])
def test_should_reject_a_tag_in_neither_form(tag):
    with pytest.raises(ValueError):
        pep440_from_tag(tag)
