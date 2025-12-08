#!/usr/bin/env python3
"""
Folder Merger - A CLI tool to merge two folders into a third destination folder.

This is a convenience launcher script. The main code is in the folder_merger package.

Usage:
    python folder_merger.py folder1 folder2 output
    python -m folder_merger folder1 folder2 output
"""

from folder_merger.cli import main

if __name__ == "__main__":
    main()