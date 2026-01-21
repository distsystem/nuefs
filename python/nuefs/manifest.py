"""NueFS manifest and lock file models."""

import pathlib
import typing

import pydantic


class MountEntry(pydantic.BaseModel):
    """Single mount entry in nue.yaml."""

    target: pathlib.Path
    source: pathlib.Path
    exclude: list[str] = pydantic.Field(default_factory=list)
    include: list[str] = pydantic.Field(default_factory=list)

    @pydantic.model_validator(mode="after")
    def check_include_exclude_mutual(self) -> typing.Self:
        if self.include and self.exclude:
            msg = "Cannot specify both 'include' and 'exclude'"
            raise ValueError(msg)
        return self


class NueManifest(pydantic.BaseModel):
    """Schema for nue.yaml - human-editable manifest."""

    apiVersion: typing.Literal["nue/v1"] = "nue/v1"
    mounts: list[MountEntry]


class LockMapping(pydantic.BaseModel):
    """Single file mapping in nue.lock (fully resolved)."""

    target: pathlib.Path
    source: pathlib.Path


class NueLock(pydantic.BaseModel):
    """Schema for nue.lock - machine-generated lock file."""

    apiVersion: typing.Literal["nue/v1"] = "nue/v1"
    generated_at: pydantic.AwareDatetime
    manifest_hash: str
    mappings: list[LockMapping]
