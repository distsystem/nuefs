"""NueFS lockfile models (nue.lock).

The lockfile is a compiled, machine-oriented snapshot of the resolved union view.
It is intended to be generated from `nue.yaml` + the current filesystem state.
"""

import pathlib
import sys
import typing

import pydantic


class LockMeta(pydantic.BaseModel):
    """Metadata about how/when the lockfile was produced."""

    model_config = pydantic.ConfigDict(extra="forbid")

    generated_at: int
    nuefs_version: str | None = None
    python: str = sys.version.split()[0]
    platform: str = sys.platform


class LockMapping(pydantic.BaseModel):
    """A single mapping used to build the lockfile."""

    model_config = pydantic.ConfigDict(extra="forbid")

    target: str
    source: str
    source_type: typing.Literal["dir", "file"] | None = None
    include_git: bool = False

    @pydantic.field_validator("target")
    @classmethod
    def _validate_target(cls, v: str) -> str:
        v = v.strip()
        if v == "":
            raise ValueError("target cannot be empty")
        if pathlib.PurePosixPath(v).is_absolute():
            raise ValueError("target must be a relative path")
        if "\\" in v:
            raise ValueError("target must use '/' separators")
        return v


class LockEntry(pydantic.BaseModel):
    """A single resolved manifest entry: virtual path -> backend path."""

    model_config = pydantic.ConfigDict(extra="forbid")

    virtual_path: str
    backend_path: str
    is_dir: bool

    @pydantic.field_validator("virtual_path")
    @classmethod
    def _validate_virtual_path(cls, v: str) -> str:
        v = v.strip()
        if v == "":
            raise ValueError("virtual_path cannot be empty")
        if v in {".", "./"}:
            raise ValueError("virtual_path must not be '.'")
        if pathlib.PurePosixPath(v).is_absolute():
            raise ValueError("virtual_path must be a relative path")
        if "\\" in v:
            raise ValueError("virtual_path must use '/' separators")
        return v

    @pydantic.field_validator("backend_path")
    @classmethod
    def _validate_backend_path(cls, v: str) -> str:
        v = v.strip()
        if v == "":
            raise ValueError("backend_path cannot be empty")
        if not pathlib.PurePath(v).is_absolute():
            raise ValueError("backend_path must be an absolute path")
        return v


class LockFile(pydantic.BaseModel):
    """Compiled lockfile for a NueFS mount."""

    model_config = pydantic.ConfigDict(extra="forbid")

    apiVersion: typing.Literal["nue/lock/v1"] = "nue/lock/v1"
    root: str
    meta: LockMeta

    manifest_path: str = "nue.yaml"
    manifest_sha256: str | None = None

    mappings: list[LockMapping] = pydantic.Field(default_factory=list)
    entries: list[LockEntry] = pydantic.Field(default_factory=list)

    @pydantic.field_validator("root")
    @classmethod
    def _validate_root(cls, v: str) -> str:
        v = v.strip()
        if v == "":
            raise ValueError("root cannot be empty")
        # Allow relative roots (e.g. '.') for portability.
        if "\\" in v:
            raise ValueError("root must use '/' separators")
        return v

    @pydantic.field_validator("manifest_sha256")
    @classmethod
    def _validate_sha256(cls, v: str | None) -> str | None:
        if v is None:
            return None
        v = v.strip().lower()
        if len(v) != 64:
            raise ValueError("manifest_sha256 must be 64 hex chars")
        if any(c not in "0123456789abcdef" for c in v):
            raise ValueError("manifest_sha256 must be lowercase hex")
        return v
