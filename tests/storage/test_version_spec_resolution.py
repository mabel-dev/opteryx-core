"""`OpteryxConnector._resolve_version_spec` - one resolver for every version.

`CREATE TAG ... AS OF VERSION x` and `ROLLBACK TO VERSION x` both name a version
with the same words, and a read names it with `VERSION AS OF x`. If any of them
resolved a word differently the same statement would mean two things depending
on where it was written, so they share this.

The four spellings cannot collide: `current` and `previous` are names the catalog
refuses to let a tag take, so a bare word is a tag name or it is nothing.
"""

import os
import sys
from types import SimpleNamespace

sys.path.insert(0, os.path.abspath(os.getcwd()))

import pytest

from opteryx.connectors.opteryx_connector import OpteryxConnector

# Tag resolution needs an opteryx-catalog new enough to have snapshot tags at
# all. Skipped rather than faked when it is not: a fake exception class here
# would pass against a catalog whose tag API this code cannot actually call.
_HAS_TAGS = True
try:  # noqa: SIM105
    from opteryx_catalog.exceptions import TagNotFound  # noqa: F401
except ImportError:  # pragma: no cover - depends on the installed catalog
    _HAS_TAGS = False

needs_tags = pytest.mark.skipif(
    not _HAS_TAGS, reason="installed opteryx-catalog predates snapshot tags"
)


class _FakeDataset:
    def __init__(self, head=None, previous=None):
        self._head = head
        self._previous = previous

    def snapshot(self, snapshot_id=None):
        return self._head

    def previous_user_snapshot(self):
        return self._previous


class _FakeCatalog:
    def __init__(self, tags=None):
        self._tags = tags or {}

    def resolve_tag(self, dataset, name):
        from opteryx_catalog.exceptions import TagNotFound

        if name not in self._tags:
            raise TagNotFound(f"Tag not found: {name}")
        return self._tags[name]


def _resolve(spec, *, head=None, previous=None, tags=None, allow_tag=True):
    connector = object.__new__(OpteryxConnector)
    return connector._resolve_version_spec(
        _FakeCatalog(tags),
        _FakeDataset(head=head, previous=previous),
        "coll.reports",
        "space.coll.reports",
        spec,
        allow_tag=allow_tag,
    )


_HEAD = SimpleNamespace(snapshot_id=200)
_PREVIOUS = SimpleNamespace(snapshot_id=100)


# --- the four spellings --------------------------------------------------


def test_a_snapshot_id_resolves_to_itself():
    assert _resolve("12345", head=_HEAD) == 12345


def test_current_resolves_to_the_head():
    assert _resolve("current", head=_HEAD) == 200


def test_an_omitted_spec_means_current():
    assert _resolve(None, head=_HEAD) == 200


def test_previous_resolves_to_the_previous_version_of_the_data():
    """Not the head's parent: the catalog walks past commits that changed no rows."""
    assert _resolve("previous", head=_HEAD, previous=_PREVIOUS) == 100


@needs_tags
def test_a_bare_word_resolves_as_a_tag():
    assert _resolve("month_end", head=_HEAD, tags={"month_end": 150}) == 150


def test_the_spelling_is_case_insensitive():
    assert _resolve("CURRENT", head=_HEAD) == 200
    assert _resolve("Previous", head=_HEAD, previous=_PREVIOUS) == 100


# --- refusals ------------------------------------------------------------


def test_a_dataset_with_no_commits_has_no_version_to_name():
    with pytest.raises(ValueError) as err:
        _resolve("current", head=None)

    assert "no data has been committed" in str(err.value)


def test_previous_on_the_earliest_version_is_refused():
    with pytest.raises(ValueError) as err:
        _resolve("previous", head=_HEAD, previous=None)

    assert "No previous version" in str(err.value)


@needs_tags
def test_an_unknown_tag_is_refused_without_naming_the_tags_that_exist():
    """Somebody who cannot see a dataset's tags must not learn them from a
    failed guess - the same rule the read path's tag resolution follows."""
    with pytest.raises(ValueError) as err:
        _resolve("nonexistent", head=_HEAD, tags={"month_end": 150})

    assert "nonexistent" in str(err.value)
    assert "month_end" not in str(err.value)


def test_create_tag_does_not_accept_a_tag_as_its_version():
    """A tag whose version is another tag is a copy that silently stops
    tracking it, so the word is refused rather than resolved."""
    with pytest.raises(ValueError) as err:
        _resolve("month_end", head=_HEAD, tags={"month_end": 150}, allow_tag=False)

    assert "CURRENT" in str(err.value)
