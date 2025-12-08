"""Folder scanning functionality."""

import os
from pathlib import Path

import xxhash
from tqdm import tqdm

from .models import FileInfo


def compute_file_hash(file_path: Path, chunk_size: int = 65536) -> str:
    """Compute hash of a file using xxhash (fast hashing algorithm)."""
    hasher = xxhash.xxh64()
    with open(file_path, 'rb') as f:
        while chunk := f.read(chunk_size):
            hasher.update(chunk)
    return hasher.hexdigest()


def get_file_info(base_path: Path, relative_path: str) -> FileInfo:
    """Get file information including hash and metadata."""
    abs_path = base_path / relative_path
    stat = abs_path.stat()
    file_hash = compute_file_hash(abs_path)

    return FileInfo(
        relative_path=relative_path,
        absolute_path=str(abs_path),
        hash=file_hash,
        size=stat.st_size,
        modified_time=stat.st_mtime
    )


def scan_folder(folder_path: Path, desc: str = "Scanning") -> dict[str, FileInfo]:
    """Scan a folder and return a dictionary of relative paths to FileInfo."""
    files = {}
    all_files = []

    # First, collect all file paths
    for root, _, filenames in os.walk(folder_path):
        for filename in filenames:
            abs_path = Path(root) / filename
            rel_path = abs_path.relative_to(folder_path)
            all_files.append(str(rel_path))

    # Then process with progress bar
    with tqdm(all_files, desc=desc, unit="file") as pbar:
        for rel_path in pbar:
            file_info = get_file_info(folder_path, rel_path)
            files[rel_path] = file_info

    return files