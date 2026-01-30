"""NueFS manifest models (nue.yaml)."""

from itertools import groupby
from os.path import commonpath
from typing import Annotated, Literal

import pydantic
from pydantic import BeforeValidator, PlainSerializer
from sheaves.sheaf import Sheaf
from sheaves.typing import LocalPath, Pathspec, Resource


def _parse_local_path(v: str | LocalPath) -> LocalPath:
    if isinstance(v, LocalPath):
        return v
    return LocalPath(v)


AnnotatedLocalPath = Annotated[
    LocalPath,
    BeforeValidator(_parse_local_path),
    PlainSerializer(str, when_used="always"),
]


class MountEntry(pydantic.BaseModel):
    """A single mount entry in the manifest."""

    model_config = pydantic.ConfigDict(extra="forbid", arbitrary_types_allowed=True)

    source: Resource
    target: AnnotatedLocalPath = LocalPath(".")
    exclude: Pathspec = pydantic.Field(default_factory=Pathspec)
    include: Pathspec = pydantic.Field(default_factory=Pathspec)


class Manifest(Sheaf, app_name="nue"):
    """NueFS manifest (nue.yaml)."""

    apiVersion: Literal["nue/v1"] = "nue/v1"
    mounts: list[MountEntry] = pydantic.Field(default_factory=list)


def minimal_cover_prefixes(paths: list[str]) -> set[str]:
    """Find minimal directory prefixes that cover all paths."""
    if not paths:
        return set()

    return {
        commonpath(list(g))
        for _, g in groupby(sorted(set(paths)), key=lambda p: p.split("/")[0])
    }
