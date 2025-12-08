#!/usr/bin/env python3
"""
Folder Merger - A CLI tool to merge two folders into a third destination folder.

Features:
- Full join merge (keeps all files from both sources)
- Conflict resolution via CLI prompts
- Checkpoint/resume for large folders
- Progress visualization
- Fast file comparison using xxhash
- Detailed conflict logging
"""

import argparse
import json
import os
import shutil
import subprocess
import sys
import platform
from dataclasses import dataclass, asdict
from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import Optional

import xxhash

from tqdm import tqdm


class ConflictResolution(Enum):
    PREFER_RECENT = "prefer_recent"
    PREFER_OLDEST = "prefer_oldest"
    PENDING = "pending"


@dataclass
class FileInfo:
    relative_path: str
    absolute_path: str
    hash: str
    size: int
    modified_time: float

    def to_dict(self):
        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict):
        return cls(**data)


@dataclass
class ConflictRecord:
    relative_path: str
    file1_info: dict
    file2_info: dict
    resolution: str
    chosen_source: str
    resolved_at: Optional[str] = None

    def to_dict(self):
        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict):
        return cls(**data)


class ConflictLogger:
    """Handles logging of conflicts and their resolutions."""

    def __init__(self, log_path: Path):
        self.log_path = log_path
        self.conflicts: list[ConflictRecord] = []
        self._load_existing()

    def _load_existing(self):
        if self.log_path.exists():
            try:
                with open(self.log_path, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    self.conflicts = [ConflictRecord.from_dict(c) for c in data]
            except (json.JSONDecodeError, KeyError):
                self.conflicts = []

    def log_conflict(self, record: ConflictRecord):
        self.conflicts.append(record)
        self._save()

    def _save(self):
        with open(self.log_path, 'w', encoding='utf-8') as f:
            json.dump([c.to_dict() for c in self.conflicts], f, indent=2)

    def get_previous_resolution(self, relative_path: str) -> Optional[ConflictRecord]:
        for conflict in self.conflicts:
            if conflict.relative_path == relative_path and conflict.resolution != "pending":
                return conflict
        return None


class CheckpointManager:
    """Manages checkpoint state for resumable operations."""

    def __init__(self, checkpoint_path: Path):
        self.checkpoint_path = checkpoint_path
        self.state = {
            "processed_files": [],
            "pending_conflicts": [],
            "phase": "scanning",
            "folder1_scanned": False,
            "folder2_scanned": False,
            "folder1_files": {},
            "folder2_files": {},
        }
        self._load()

    def _load(self):
        if self.checkpoint_path.exists():
            try:
                with open(self.checkpoint_path, 'r', encoding='utf-8') as f:
                    saved_state = json.load(f)
                    self.state.update(saved_state)
            except (json.JSONDecodeError, KeyError):
                pass

    def save(self):
        with open(self.checkpoint_path, 'w', encoding='utf-8') as f:
            json.dump(self.state, f, indent=2)

    def mark_file_processed(self, relative_path: str):
        if relative_path not in self.state["processed_files"]:
            self.state["processed_files"].append(relative_path)
            self.save()

    def is_file_processed(self, relative_path: str) -> bool:
        return relative_path in self.state["processed_files"]

    def set_phase(self, phase: str):
        self.state["phase"] = phase
        self.save()

    def clear(self):
        if self.checkpoint_path.exists():
            self.checkpoint_path.unlink()


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


def open_file_in_viewer(file_path: str):
    """Open a file using the system default application."""
    system = platform.system()
    try:
        if system == "Windows":
            os.startfile(file_path)
        elif system == "Darwin":  # macOS
            subprocess.run(["open", file_path], check=True)
        else:  # Linux and others
            subprocess.run(["xdg-open", file_path], check=True)
    except Exception as e:
        print(f"Could not open file: {e}")
        print(f"Please manually open: {file_path}")


def format_timestamp(timestamp: float) -> str:
    """Format a Unix timestamp as a human-readable string."""
    return datetime.fromtimestamp(timestamp).strftime("%Y-%m-%d %H:%M:%S")


def format_size(size: int) -> str:
    """Format file size in human-readable form."""
    for unit in ['B', 'KB', 'MB', 'GB']:
        if size < 1024:
            return f"{size:.1f} {unit}"
        size /= 1024
    return f"{size:.1f} TB"


def resolve_conflict(
    file1: FileInfo,
    file2: FileInfo,
    conflict_logger: ConflictLogger
) -> tuple[FileInfo, str]:
    """
    Prompt user to resolve a file conflict.
    Returns the chosen FileInfo and the resolution type.
    """
    # Check for previous resolution
    prev = conflict_logger.get_previous_resolution(file1.relative_path)
    if prev and prev.resolution != "pending":
        print(f"\nUsing previous resolution for: {file1.relative_path}")
        if prev.chosen_source == "folder1":
            return file1, prev.resolution
        else:
            return file2, prev.resolution

    print("\n" + "=" * 60)
    print(f"CONFLICT DETECTED: {file1.relative_path}")
    print("=" * 60)

    print("\nFolder 1 version:")
    print(f"  Path: {file1.absolute_path}")
    print(f"  Modified: {format_timestamp(file1.modified_time)}")
    print(f"  Size: {format_size(file1.size)}")
    print(f"  Hash: {file1.hash[:16]}...")

    print("\nFolder 2 version:")
    print(f"  Path: {file2.absolute_path}")
    print(f"  Modified: {format_timestamp(file2.modified_time)}")
    print(f"  Size: {format_size(file2.size)}")
    print(f"  Hash: {file2.hash[:16]}...")

    # Determine which is more recent
    if file1.modified_time > file2.modified_time:
        recent_label = "Folder 1"
        oldest_label = "Folder 2"
    elif file2.modified_time > file1.modified_time:
        recent_label = "Folder 2"
        oldest_label = "Folder 1"
    else:
        recent_label = "Same time"
        oldest_label = "Same time"

    print(f"\nMore recent: {recent_label}")

    while True:
        print("\nOptions:")
        print("  1: Prefer more recent file")
        print("  2: Prefer oldest file")
        print("  3: Open both files to inspect")

        choice = input("\nEnter your choice (1-3): ").strip()

        if choice == "1":
            if file1.modified_time >= file2.modified_time:
                chosen, source = file1, "folder1"
            else:
                chosen, source = file2, "folder2"
            resolution = ConflictResolution.PREFER_RECENT.value
            break

        elif choice == "2":
            if file1.modified_time <= file2.modified_time:
                chosen, source = file1, "folder1"
            else:
                chosen, source = file2, "folder2"
            resolution = ConflictResolution.PREFER_OLDEST.value
            break

        elif choice == "3":
            print("\nOpening both files...")
            open_file_in_viewer(file1.absolute_path)
            open_file_in_viewer(file2.absolute_path)
            print("Files opened. Please inspect them.")
            # Loop back to ask again
            continue
        else:
            print("Invalid choice. Please enter 1, 2, or 3.")

    # Log the conflict resolution
    record = ConflictRecord(
        relative_path=file1.relative_path,
        file1_info=file1.to_dict(),
        file2_info=file2.to_dict(),
        resolution=resolution,
        chosen_source=source,
        resolved_at=datetime.now().isoformat()
    )
    conflict_logger.log_conflict(record)

    print(f"Resolved: Using {source} version")
    return chosen, resolution


def copy_file(src: Path, dst: Path):
    """Copy a file, creating parent directories if needed."""
    dst.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(src, dst)


def merge_folders(
    folder1: Path,
    folder2: Path,
    output: Path,
    checkpoint_mgr: CheckpointManager,
    conflict_logger: ConflictLogger
):
    """
    Merge two folders into an output folder.
    Performs a full join - all files from both folders are included.
    """
    print("\nUsing xxhash64 for file hashing")
    print(f"Checkpoint file: {checkpoint_mgr.checkpoint_path}")
    print(f"Conflict log: {conflict_logger.log_path}")

    # Phase 1: Scan folders (with checkpoint support)
    if not checkpoint_mgr.state["folder1_scanned"]:
        print(f"\nScanning folder 1: {folder1}")
        files1 = scan_folder(folder1, "Scanning folder 1")
        checkpoint_mgr.state["folder1_files"] = {k: v.to_dict() for k, v in files1.items()}
        checkpoint_mgr.state["folder1_scanned"] = True
        checkpoint_mgr.save()
    else:
        print("\nFolder 1 already scanned, loading from checkpoint...")
        files1 = {k: FileInfo.from_dict(v) for k, v in checkpoint_mgr.state["folder1_files"].items()}

    if not checkpoint_mgr.state["folder2_scanned"]:
        print(f"\nScanning folder 2: {folder2}")
        files2 = scan_folder(folder2, "Scanning folder 2")
        checkpoint_mgr.state["folder2_files"] = {k: v.to_dict() for k, v in files2.items()}
        checkpoint_mgr.state["folder2_scanned"] = True
        checkpoint_mgr.save()
    else:
        print("\nFolder 2 already scanned, loading from checkpoint...")
        files2 = {k: FileInfo.from_dict(v) for k, v in checkpoint_mgr.state["folder2_files"].items()}

    # Phase 2: Determine merge operations
    checkpoint_mgr.set_phase("merging")

    all_paths = set(files1.keys()) | set(files2.keys())
    only_in_1 = set(files1.keys()) - set(files2.keys())
    only_in_2 = set(files2.keys()) - set(files1.keys())
    in_both = set(files1.keys()) & set(files2.keys())

    print(f"\n--- Merge Summary ---")
    print(f"Files only in folder 1: {len(only_in_1)}")
    print(f"Files only in folder 2: {len(only_in_2)}")
    print(f"Files in both folders: {len(in_both)}")
    print(f"Total unique paths: {len(all_paths)}")

    # Count conflicts (same path, different hash)
    conflicts = []
    identical = []
    for path in in_both:
        if files1[path].hash != files2[path].hash:
            conflicts.append(path)
        else:
            identical.append(path)

    print(f"  - Identical files: {len(identical)}")
    print(f"  - Conflicting files: {len(conflicts)}")
    print("-" * 20)

    # Create output directory
    output.mkdir(parents=True, exist_ok=True)

    # Phase 3: Copy files
    total_operations = len(all_paths)
    completed = len(checkpoint_mgr.state["processed_files"])

    print(f"\nStarting merge... ({completed}/{total_operations} already processed)")

    with tqdm(total=total_operations, initial=completed, desc="Merging", unit="file") as pbar:
        # Files only in folder 1
        for path in only_in_1:
            if checkpoint_mgr.is_file_processed(path):
                continue
            src = folder1 / path
            dst = output / path
            copy_file(src, dst)
            checkpoint_mgr.mark_file_processed(path)
            pbar.update(1)

        # Files only in folder 2
        for path in only_in_2:
            if checkpoint_mgr.is_file_processed(path):
                continue
            src = folder2 / path
            dst = output / path
            copy_file(src, dst)
            checkpoint_mgr.mark_file_processed(path)
            pbar.update(1)

        # Identical files (just copy from folder 1)
        for path in identical:
            if checkpoint_mgr.is_file_processed(path):
                continue
            src = folder1 / path
            dst = output / path
            copy_file(src, dst)
            checkpoint_mgr.mark_file_processed(path)
            pbar.update(1)

        # Conflicting files - need user input
        for path in conflicts:
            if checkpoint_mgr.is_file_processed(path):
                continue

            chosen, _ = resolve_conflict(files1[path], files2[path], conflict_logger)
            src = Path(chosen.absolute_path)
            dst = output / path
            copy_file(src, dst)
            checkpoint_mgr.mark_file_processed(path)
            pbar.update(1)

    # Cleanup checkpoint on successful completion
    checkpoint_mgr.clear()

    print("\n" + "=" * 60)
    print("MERGE COMPLETE!")
    print("=" * 60)
    print(f"Output folder: {output}")
    print(f"Total files merged: {len(all_paths)}")
    print(f"Conflicts resolved: {len(conflicts)}")
    print(f"Conflict log saved to: {conflict_logger.log_path}")


def main():
    parser = argparse.ArgumentParser(
        description="Merge two folders into a third destination folder.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s /path/to/folder1 /path/to/folder2 /path/to/output
  %(prog)s --checkpoint ./merge.checkpoint folder1 folder2 merged_output
        """
    )

    parser.add_argument("folder1", type=Path, help="First source folder")
    parser.add_argument("folder2", type=Path, help="Second source folder")
    parser.add_argument("output", type=Path, help="Output folder for merged content")

    parser.add_argument(
        "--checkpoint", "-c",
        type=Path,
        default=Path("merge_checkpoint.json"),
        help="Path to checkpoint file for resuming (default: merge_checkpoint.json)"
    )

    parser.add_argument(
        "--log", "-l",
        type=Path,
        default=Path("conflict_log.json"),
        help="Path to conflict log file (default: conflict_log.json)"
    )

    parser.add_argument(
        "--reset",
        action="store_true",
        help="Reset checkpoint and start fresh"
    )

    args = parser.parse_args()

    # Validate input folders
    if not args.folder1.exists():
        print(f"Error: Folder 1 does not exist: {args.folder1}")
        sys.exit(1)
    if not args.folder2.exists():
        print(f"Error: Folder 2 does not exist: {args.folder2}")
        sys.exit(1)
    if not args.folder1.is_dir():
        print(f"Error: Folder 1 is not a directory: {args.folder1}")
        sys.exit(1)
    if not args.folder2.is_dir():
        print(f"Error: Folder 2 is not a directory: {args.folder2}")
        sys.exit(1)

    # Check if output folder exists and has content
    if args.output.exists() and any(args.output.iterdir()):
        print(f"Warning: Output folder already exists and is not empty: {args.output}")
        response = input("Continue anyway? (y/N): ").strip().lower()
        if response != 'y':
            print("Aborted.")
            sys.exit(0)

    # Initialize managers
    checkpoint_mgr = CheckpointManager(args.checkpoint)
    conflict_logger = ConflictLogger(args.log)

    if args.reset:
        print("Resetting checkpoint...")
        checkpoint_mgr.clear()
        checkpoint_mgr = CheckpointManager(args.checkpoint)

    print("=" * 60)
    print("FOLDER MERGER")
    print("=" * 60)
    print(f"Folder 1: {args.folder1.absolute()}")
    print(f"Folder 2: {args.folder2.absolute()}")
    print(f"Output:   {args.output.absolute()}")

    try:
        merge_folders(
            args.folder1.absolute(),
            args.folder2.absolute(),
            args.output.absolute(),
            checkpoint_mgr,
            conflict_logger
        )
    except KeyboardInterrupt:
        print("\n\nInterrupted! Progress has been saved.")
        print(f"To resume, run the same command again.")
        print(f"To start fresh, use --reset flag.")
        sys.exit(1)


if __name__ == "__main__":
    main()