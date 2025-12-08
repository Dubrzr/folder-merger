"""Data models for folder merger."""

from dataclasses import dataclass, asdict
from enum import Enum
from typing import Optional


class ConflictResolution(Enum):
    """Types of conflict resolution strategies."""
    PREFER_RECENT = "prefer_recent"
    PREFER_OLDEST = "prefer_oldest"
    PENDING = "pending"


@dataclass
class FileInfo:
    """Information about a file including metadata and hash."""
    relative_path: str
    absolute_path: str
    hash: str
    size: int
    modified_time: float

    def to_dict(self) -> dict:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict) -> "FileInfo":
        return cls(**data)


@dataclass
class ConflictRecord:
    """Record of a file conflict and its resolution."""
    relative_path: str
    file1_info: dict
    file2_info: dict
    resolution: str
    chosen_source: str
    resolved_at: Optional[str] = None

    def to_dict(self) -> dict:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict) -> "ConflictRecord":
        return cls(**data)