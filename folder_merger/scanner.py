"""Folder scanning functionality."""

import os
import sys
from pathlib import Path
from typing import Optional

import xxhash
from tqdm import tqdm

from .models import FileInfo


def _long_path(path: Path) -> str:
    """Convert path to long path format on Windows to handle paths > 260 chars."""
    path_str = str(path.resolve())
    if os.name == 'nt' and not path_str.startswith('\\\\?\\'):
        return '\\\\?\\' + path_str
    return path_str


class ScanError:
    """Record of a file that failed to scan."""

    def __init__(self, relative_path: str, absolute_path: str, error: str):
        self.relative_path = relative_path
        self.absolute_path = absolute_path
        self.error = error


def compute_file_hash(file_path: Path, chunk_size: int = 65536) -> str:
    """Compute hash of a file using xxhash (fast hashing algorithm)."""
    hasher = xxhash.xxh64()
    # Use long path format on Windows for paths > 260 chars
    with open(_long_path(file_path), 'rb') as f:
        while chunk := f.read(chunk_size):
            hasher.update(chunk)
    return hasher.hexdigest()


def get_file_info(base_path: Path, relative_path: str) -> FileInfo:
    """Get file information including hash and metadata."""
    abs_path = base_path / relative_path
    # Use long path format on Windows for paths > 260 chars
    long_path = _long_path(abs_path)
    stat = os.stat(long_path)
    file_hash = compute_file_hash(abs_path)

    return FileInfo(
        relative_path=relative_path,
        absolute_path=str(abs_path),
        hash=file_hash,
        size=stat.st_size,
        modified_time=stat.st_mtime
    )


def scan_folder(
    folder_path: Path,
    desc: str = "Scanning",
    on_error: str = "skip"
) -> tuple[dict[str, FileInfo], list[ScanError]]:
    """
    Scan a folder and return a dictionary of relative paths to FileInfo.

    Args:
        folder_path: Path to the folder to scan
        desc: Description for the progress bar
        on_error: How to handle errors - "skip" to continue, "fail" to raise

    Returns:
        Tuple of (files dict, list of scan errors)
    """
    files = {}
    errors = []
    all_files = []

    # First, collect all file paths
    for root, _, filenames in os.walk(folder_path):
        for filename in filenames:
            abs_path = Path(root) / filename
            rel_path = abs_path.relative_to(folder_path)
            # Use POSIX-style paths for cross-platform consistency
            all_files.append(rel_path.as_posix())

    # Then process with progress bar
    with tqdm(all_files, desc=desc, unit="file") as pbar:
        for rel_path in pbar:
            try:
                file_info = get_file_info(folder_path, rel_path)
                files[rel_path] = file_info
            except (OSError, IOError, PermissionError) as e:
                abs_path = folder_path / rel_path
                error = ScanError(rel_path, str(abs_path), str(e))
                errors.append(error)

                if on_error == "fail":
                    # Print errors so far before raising
                    print(f"\nError scanning file: {rel_path}", file=sys.stderr)
                    print(f"  {e}", file=sys.stderr)
                    raise

    return files, errors