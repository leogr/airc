#!/usr/bin/env python3
"""Check repository-local Markdown links without traversing generated trees."""
import pathlib
import re
import sys

root = pathlib.Path(__file__).resolve().parent.parent
documents = [root / "README.md", root / "AGENTS.md"]
documents += list((root / "docs").rglob("*.md"))
documents += list((root / "skills").rglob("*.md"))
errors = []
for document in documents:
    for link in re.findall(r"\[[^\]]*\]\(([^)]+)\)", document.read_text()):
        if "://" in link or link.startswith("#"):
            continue
        target = link.split("#", 1)[0]
        if not (document.parent / target).exists():
            errors.append(f"{document.relative_to(root)}: missing {target}")
if errors:
    sys.exit("\n".join(errors))
print(f"Local links valid in {len(documents)} documents")
