"""Core merge logic."""

import os
import platform
import shutil
import subprocess
from datetime import datetime
from pathlib import Path

from tqdm import tqdm

from .db import CheckpointDB
from .models import ConflictRecord, ConflictResolution, FileInfo
from .scanner import scan_folder


def open_file_in_viewer(file_path: str) -> None:
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


def copy_file(src: Path, dst: Path) -> None:
    """Copy a file, creating parent directories if needed."""
    dst.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(src, dst)


def resolve_conflict(
    file1: FileInfo,
    file2: FileInfo,
    db: CheckpointDB,
    conflict_index: int,
    total_conflicts: int
) -> tuple[FileInfo, str]:
    """
    Prompt user to resolve a file conflict.
    Returns the chosen FileInfo and the resolution type.
    """
    # Check for previous resolution
    prev = db.get_previous_resolution(file1.relative_path)
    if prev and prev.resolution != "pending":
        print(f"\n[{conflict_index}/{total_conflicts}] Using previous resolution for: {file1.relative_path}")
        if prev.chosen_source == "folder1":
            return file1, prev.resolution
        else:
            return file2, prev.resolution

    print("\n" + "=" * 60)
    print(f"CONFLICT [{conflict_index}/{total_conflicts}]: {file1.relative_path}")
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
    elif file2.modified_time > file1.modified_time:
        recent_label = "Folder 2"
    else:
        recent_label = "Same time"

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
    db.log_conflict(record)

    print(f"Resolved: Using {source} version")
    return chosen, resolution


def merge_folders(
    folder1: Path,
    folder2: Path,
    output: Path,
    db: CheckpointDB
) -> None:
    """
    Merge two folders into an output folder.
    Performs a full join - all files from both folders are included.

    The merge is done in distinct phases to ensure conflict resolution
    never blocks scanning or copying of non-conflicting files:

    Phase 1: Scan both folders (no user interaction)
    Phase 2: Copy all non-conflicting files (no user interaction)
    Phase 3: Resolve and copy conflicting files (user interaction)
    """
    print("\nUsing xxhash64 for file hashing")
    print(f"Database file: {db.db_path}")

    # =========================================================================
    # PHASE 1: Scan folders (no user interaction)
    # =========================================================================
    print("\n" + "=" * 60)
    print("PHASE 1: Scanning folders")
    print("=" * 60)

    if not db.is_folder_scanned(1):
        print(f"\nScanning folder 1: {folder1}")
        files1 = scan_folder(folder1, "Scanning folder 1")
        db.save_scanned_files_batch(1, files1)
        db.mark_folder_scanned(1)
    else:
        print("\nFolder 1 already scanned, loading from checkpoint...")
        files1 = db.get_scanned_files(1)
        print(f"  Loaded {len(files1)} files")

    if not db.is_folder_scanned(2):
        print(f"\nScanning folder 2: {folder2}")
        files2 = scan_folder(folder2, "Scanning folder 2")
        db.save_scanned_files_batch(2, files2)
        db.mark_folder_scanned(2)
    else:
        print("\nFolder 2 already scanned, loading from checkpoint...")
        files2 = db.get_scanned_files(2)
        print(f"  Loaded {len(files2)} files")

    # Analyze files
    all_paths = set(files1.keys()) | set(files2.keys())
    only_in_1 = set(files1.keys()) - set(files2.keys())
    only_in_2 = set(files2.keys()) - set(files1.keys())
    in_both = set(files1.keys()) & set(files2.keys())

    # Separate identical from conflicting
    conflicts = []
    identical = []
    for path in in_both:
        if files1[path].hash != files2[path].hash:
            conflicts.append(path)
        else:
            identical.append(path)

    print(f"\n--- Scan Summary ---")
    print(f"Files only in folder 1: {len(only_in_1)}")
    print(f"Files only in folder 2: {len(only_in_2)}")
    print(f"Identical files: {len(identical)}")
    print(f"Conflicting files: {len(conflicts)}")
    print(f"Total unique paths: {len(all_paths)}")
    print("-" * 20)

    # Create output directory
    output.mkdir(parents=True, exist_ok=True)

    # =========================================================================
    # PHASE 2: Copy non-conflicting files (no user interaction)
    # =========================================================================
    db.set_phase("copying")

    non_conflicting = list(only_in_1) + list(only_in_2) + identical
    non_conflicting_to_process = [p for p in non_conflicting if not db.is_file_processed(p)]

    if non_conflicting_to_process or not conflicts:
        print("\n" + "=" * 60)
        print("PHASE 2: Copying non-conflicting files")
        print("=" * 60)

    if non_conflicting_to_process:
        already_copied = len(non_conflicting) - len(non_conflicting_to_process)
        print(f"\n{already_copied} files already copied, {len(non_conflicting_to_process)} remaining")

        with tqdm(total=len(non_conflicting), initial=already_copied,
                  desc="Copying", unit="file") as pbar:
            # Files only in folder 1
            for path in only_in_1:
                if db.is_file_processed(path):
                    continue
                src = folder1 / path
                dst = output / path
                copy_file(src, dst)
                db.mark_file_processed(path)
                pbar.update(1)

            # Files only in folder 2
            for path in only_in_2:
                if db.is_file_processed(path):
                    continue
                src = folder2 / path
                dst = output / path
                copy_file(src, dst)
                db.mark_file_processed(path)
                pbar.update(1)

            # Identical files (copy from folder 1)
            for path in identical:
                if db.is_file_processed(path):
                    continue
                src = folder1 / path
                dst = output / path
                copy_file(src, dst)
                db.mark_file_processed(path)
                pbar.update(1)

        print(f"\nAll {len(non_conflicting)} non-conflicting files copied.")
    elif non_conflicting:
        print(f"\nAll {len(non_conflicting)} non-conflicting files already copied.")

    # =========================================================================
    # PHASE 3: Resolve conflicts (user interaction)
    # =========================================================================
    if conflicts:
        db.set_phase("resolving_conflicts")

        conflicts_to_process = [p for p in conflicts if not db.is_file_processed(p)]
        already_resolved = len(conflicts) - len(conflicts_to_process)

        print("\n" + "=" * 60)
        print("PHASE 3: Resolving conflicts")
        print("=" * 60)

        if conflicts_to_process:
            print(f"\n{already_resolved} conflicts already resolved, {len(conflicts_to_process)} remaining")
            print("You will now be prompted to resolve each conflict.\n")

            for i, path in enumerate(conflicts_to_process, start=already_resolved + 1):
                chosen, _ = resolve_conflict(
                    files1[path],
                    files2[path],
                    db,
                    conflict_index=i,
                    total_conflicts=len(conflicts)
                )
                src = Path(chosen.absolute_path)
                dst = output / path
                copy_file(src, dst)
                db.mark_file_processed(path)

            print(f"\nAll {len(conflicts)} conflicts resolved.")
        else:
            print(f"\nAll {len(conflicts)} conflicts already resolved.")

    # =========================================================================
    # Complete
    # =========================================================================
    db.clear()

    print("\n" + "=" * 60)
    print("MERGE COMPLETE!")
    print("=" * 60)
    print(f"Output folder: {output}")
    print(f"Total files merged: {len(all_paths)}")
    print(f"  - Non-conflicting: {len(non_conflicting)}")
    print(f"  - Conflicts resolved: {len(conflicts)}")