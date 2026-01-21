import pathlib
import typing

import pydantic
from sheaves.resource import Resource


class MountEntry(pydantic.BaseModel):
    target: pathlib.Path
    source: Resource
    exclude: list[str] = pydantic.Field(default_factory=list)
    include: list[str] = pydantic.Field(default_factory=list)

    @pydantic.model_validator(mode="after")
    def check_include_exclude_mutual(self) -> typing.Self:
        if self.include and self.exclude:
            msg = "Cannot specify both 'include' and 'exclude'"
            raise ValueError(msg)
        return self


class NueManifest(pydantic.BaseModel):
    apiVersion: typing.Literal["nue/v1"] = "nue/v1"
    mounts: list[MountEntry]


class LockMapping(pydantic.BaseModel):
    target: pathlib.Path
    source: pathlib.Path


class NueLock(pydantic.BaseModel):
    apiVersion: typing.Literal["nue/v1"] = "nue/v1"
    mappings: list[LockMapping]
