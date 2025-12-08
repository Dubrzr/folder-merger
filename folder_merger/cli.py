"""Command-line interface for folder merger."""

import argparse
import sys
from pathlib import Path

from .db import CheckpointDB
from .merger import merge_folders


def parse_args() -> argparse.Namespace:
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(
        description="Merge two folders into a third destination folder.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s /path/to/folder1 /path/to/folder2 /path/to/output
  %(prog)s --db ./merge.db folder1 folder2 merged_output
        """
    )

    parser.add_argument("folder1", type=Path, help="First source folder")
    parser.add_argument("folder2", type=Path, help="Second source folder")
    parser.add_argument("output", type=Path, help="Output folder for merged content")

    parser.add_argument(
        "--db", "-d",
        type=Path,
        default=Path("merge_checkpoint.db"),
        help="Path to SQLite database for checkpoints and conflict log (default: merge_checkpoint.db)"
    )

    parser.add_argument(
        "--reset",
        action="store_true",
        help="Reset checkpoint and start fresh"
    )

    return parser.parse_args()


def validate_args(args: argparse.Namespace) -> None:
    """Validate command-line arguments."""
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


def confirm_output_overwrite(output: Path) -> bool:
    """Prompt user to confirm if output folder is not empty."""
    if output.exists() and any(output.iterdir()):
        print(f"Warning: Output folder already exists and is not empty: {output}")
        response = input("Continue anyway? (y/N): ").strip().lower()
        return response == 'y'
    return True


def main() -> None:
    """Main entry point."""
    args = parse_args()
    validate_args(args)

    if not confirm_output_overwrite(args.output):
        print("Aborted.")
        sys.exit(0)

    # Handle reset
    if args.reset and args.db.exists():
        print("Resetting checkpoint...")
        args.db.unlink()

    # Initialize database
    db = CheckpointDB(args.db)

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
            db
        )
    except KeyboardInterrupt:
        print("\n\nInterrupted! Progress has been saved.")
        print("To resume, run the same command again.")
        print("To start fresh, use --reset flag.")
        db.close()
        sys.exit(1)