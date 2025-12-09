# Folder Merger

[![Tests](https://github.com/dubrzr/folder-merger/actions/workflows/tests.yml/badge.svg)](https://github.com/dubrzr/folder-merger/actions/workflows/tests.yml)
[![codecov](https://codecov.io/gh/dubrzr/folder-merger/branch/master/graph/badge.svg)](https://codecov.io/gh/dubrzr/folder-merger)
[![Python 3.10+](https://img.shields.io/badge/python-3.10+-blue.svg)](https://www.python.org/downloads/)

Outil CLI pour fusionner deux dossiers dans un dossier de destination.

## Fonctionnalites

- Fusion complete (conserve tous les fichiers des deux sources)
- Resolution de conflits interactive
- Checkpoint/resume pour les gros dossiers (SQLite)
- Barre de progression
- Comparaison rapide via xxhash

## Installation

```bash
pip install -r requirements.txt
```

## Utilisation

```bash
python folder_merger.py <folder1> <folder2> <output>
```

Ou via le module :

```bash
python -m folder_merger <folder1> <folder2> <output>
```

### Options

| Option | Description |
|--------|-------------|
| `--db`, `-d` | Chemin vers la base SQLite (defaut: `merge_checkpoint.db`) |
| `--reset` | Recommencer depuis le debut |

### Exemples

```bash
# Fusion simple
python folder_merger.py /path/to/folder1 /path/to/folder2 /path/to/output

# Avec base de donnees personnalisee
python folder_merger.py --db ./merge.db folder1 folder2 merged_output

# Recommencer une fusion
python folder_merger.py --reset folder1 folder2 output
```

## Resume apres interruption

Si le processus est interrompu (Ctrl+C), relancez simplement la meme commande pour reprendre.
