#!/usr/bin/env python3
"""Check the repository's Markdown links and compact KB evidence structure.

Uses the KB's inline links, ATX headings, explicit source anchors and numbered
source entries. Also accepts ordinary reference-style links. It deliberately
does not fetch upstream URLs or certify the truth of prose.
"""
from pathlib import Path
import re
import subprocess
import sys
from urllib.parse import unquote, urlsplit

INLINE_LINK = re.compile(r'\[[^\]\n]*\]\(([^\s)]+)(?:\s+"[^"]*")?\)')
REFERENCE = re.compile(r'^\[([^\]]+)\]:\s*(\S+)', re.MULTILINE)
CITATION = re.compile(r'\[(\d+)\]\(#source-(\d+)\)')
SOURCE = re.compile(r'^(\d+)\. <a id="source-(\d+)"></a> (.+)$', re.MULTILINE)
LINE_ANCHOR = re.compile(r'L([1-9]\d*)(?:-L([1-9]\d*))?')


def prose(text):
    """Exclude fenced examples from link/heading/citation interpretation."""
    out, fence = [], None
    for line in text.splitlines():
        marker = re.match(r'^\s{0,3}(`{3,}|~{3,})', line)
        if marker:
            token = marker[1]
            if fence is None:
                fence = token
            elif token[0] == fence[0] and len(token) >= len(fence):
                fence = None
            out.append('')
        else:
            out.append(line if fence is None else '')
    return '\n'.join(out)


def links(text):
    text = prose(text)
    result = INLINE_LINK.findall(text)
    definitions = dict(REFERENCE.findall(text))
    result.extend(definitions.values())
    for label in re.findall(r'\[[^\]\n]+\]\[([^\]\n]+)\]', text):
        if label not in definitions:
            result.append('#undefined-reference-' + label)
    return [target.strip('<>') for target in result]


def anchors(text):
    text = prose(text)
    found = set(re.findall(r'<a\s+id="([^"]+)"\s*></a>', text))
    seen = {}
    for heading in re.findall(r'^#{1,6}\s+(.+?)(?:\s+#+)?$', text, re.MULTILINE):
        heading = re.sub(r'\[([^]]+)\]\([^)]*\)', r'\1', heading)
        heading = re.sub(r'<[^>]*>', '', heading)
        slug = re.sub(r'[^\w\- ]', '', heading.lower()).replace(' ', '-')
        count = seen.get(slug, 0)
        seen[slug] = count + 1
        found.add(slug if count == 0 else f'{slug}-{count}')
    return found


def local_target(root, page, target):
    parsed = urlsplit(target)
    if parsed.scheme or parsed.netloc:
        return None
    path = unquote(parsed.path)
    resolved = (root / path.lstrip('/') if path.startswith('/') else page.parent / path).resolve()
    return resolved, unquote(parsed.fragment)


def check(root, pages):
    root = root.resolve()
    errors = []
    texts = {p.resolve(): p.read_text() for p in pages if p.is_file()}
    def kind(page):
        for folder in ('specs', 'digests'):
            if page.is_relative_to(root / 'docs' / folder):
                return folder
        return None

    kb = sorted(p for p in texts if kind(p))
    index = root / 'docs/README.md'

    def fail(page, message):
        errors.append(f'{page.relative_to(root)}: {message}')

    for page, raw in texts.items():
        for target in links(raw):
            resolved = local_target(root, page, target)
            if resolved is None:
                continue
            dest, fragment = resolved
            if not dest.is_relative_to(root):
                fail(page, f'link escapes repository: {target}')
                continue
            if not dest.exists():
                fail(page, f'missing link target: {target}')
                continue
            if fragment and dest.is_file():
                if dest.suffix == '.md':
                    if fragment not in anchors(texts.get(dest, dest.read_text())):
                        fail(page, f'missing Markdown anchor: {target}')
                else:
                    match = LINE_ANCHOR.fullmatch(fragment)
                    if not match:
                        fail(page, f'unsupported source anchor: {target}')
                    else:
                        start, end = int(match[1]), int(match[2] or match[1])
                        if not start <= end <= len(dest.read_text().splitlines()):
                            fail(page, f'invalid source line span: {target}')

    if not index.is_file():
        errors.append('docs/README.md: missing KB index')
    indexed = set()
    for target in links(texts.get(index, '')):
        dest = local_target(root, index, target)
        if dest:
            indexed.add(dest[0])
    for folder in ('specs', 'digests'):
        if not any(kind(p) == folder for p in kb):
            errors.append(f'docs/{folder}: no KB pages')

    edges = {p: set() for p in kb}
    for page in kb:
        text = prose(texts[page])
        if page not in indexed:
            fail(page, 'not linked from KB index')
        if text.count('\n## Sources\n') != 1:
            fail(page, 'expected one Sources section')
            continue
        body, sources = text.split('\n## Sources\n')
        entries = SOURCE.findall(sources)
        numbers = [int(n) for n, _, _ in entries]
        if not entries or numbers != list(range(1, len(entries) + 1)):
            fail(page, 'source entries must be consecutively numbered from 1')
        for n, anchor, entry in entries:
            if n != anchor or len(links(entry)) != 1:
                fail(page, f'source {n} needs a matching anchor and one link')
            for target in links(entry):
                dest = local_target(root, page, target)
                if dest and dest[0].suffix != '.md' and not LINE_ANCHOR.fullmatch(dest[1]):
                    fail(page, f'source {n} needs a code line span')
        cites = CITATION.findall(body)
        if any(n != anchor for n, anchor in cites):
            fail(page, 'citation number does not match its anchor')
        if {int(n) for n, _ in cites} != set(numbers):
            fail(page, 'citations and source entries must match (no missing/unused sources)')
        if '[FACT]' not in body and '[FACT,' not in body:
            fail(page, 'missing epistemic fact tags')
        if kind(page) == 'digests':
            if not re.search(r'^Verification: `[0-9a-f]{40}`', body, re.MULTILINE):
                fail(page, 'missing verification revision')
            if re.search(r'\[(?:DERIVED|INFERENCE|ASSUMPTION)\]', body):
                fail(page, 'digest evidence must be factual; keep deductions in specs')
        for target in links(text):
            dest = local_target(root, page, target)
            if not dest or dest[0] == page:
                continue
            if dest[0] in edges:
                edges[page].add(dest[0])
            if dest[0].suffix == '.md' and kind(dest[0]) != 'digests':
                fail(page, f'evidence must point to a digest or source, not {target}')

    visited, active = set(), set()

    def visit(page):
        if page in active:
            fail(page, 'circular evidence links')
            return
        if page in visited:
            return
        active.add(page)
        for target in sorted(edges[page]):
            visit(target)
        active.remove(page)
        visited.add(page)

    for page in kb:
        visit(page)
    return errors


def main():
    root = Path(__file__).resolve().parent.parent
    # Include new docs, exclude ignored build/research trees, tolerate deletions.
    listed = subprocess.check_output(
        ['git', 'ls-files', '-z', '--cached', '--others', '--exclude-standard', '--', '*.md'],
        cwd=root, text=True)
    pages = sorted({root / name for name in listed.split('\0') if name})
    errors = check(root, pages)
    for error in errors:
        print(error, file=sys.stderr)
    if errors:
        return 1
    print(f'Docs: {sum(p.is_file() for p in pages)} Markdown files checked')
    return 0


if __name__ == '__main__':
    sys.exit(main())
