"""Error type shared by the manifest parser and the check engine.

A ``CheckError`` always names the file(s) that could not be used.  The Flask
layer deliberately renders only this error and never any partial ticket
result, which keeps the "no partial conclusions" rule.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class PathConflict:
    """A path that makes the offline migration unsafe to package."""

    path: str
    detail: str


class CheckError(Exception):
    def __init__(
        self,
        message: str,
        *,
        files: list[str] | None = None,
        conflicts: list[PathConflict] | None = None,
    ):
        super().__init__(message)
        self.message = message
        self.files = list(files or [])
        self.conflicts = list(conflicts or [])

    def __str__(self) -> str:  # pragma: no cover - trivial
        if self.files:
            return f"{self.message}（文件：{', '.join(self.files)}）"
        return self.message
