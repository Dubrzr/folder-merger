"""
Folder Merger - A CLI tool to merge two folders into a third destination folder.

Features:
- Full join merge (keeps all files from both sources)
- Conflict resolution via CLI prompts
- Checkpoint/resume for large folders (SQLite-backed for durability)
- Progress visualization
- Fast file comparison using xxhash
- Detailed conflict logging
"""

__version__ = "1.0.0"