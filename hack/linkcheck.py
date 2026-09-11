#!/usr/bin/env python3
"""Verify that every internal Markdown link and anchor in this repo resolves.

The design set is heavily cross-referenced and internal consistency is a stated
deliverable, so a broken anchor is treated as a build failure
(docs/test-plan.md section 10). External http(s) links are not fetched: a CI job
that fails because someone else's site is down teaches people to ignore CI.

Usage:
    python3 hack/linkcheck.py [root]

Exits 0 when every internal link resolves, 1 otherwise.
"""

from __future__ import annotations

import re
import sys
from pathlib import Path

# Inline links: [text](target). Excludes image embeds, which are checked the
# same way but reported distinctly.
LINK_RE = re.compile(r"(?<!!)\[(?P<text>(?:[^\[\]]|\[[^\]]*\])*)\]\((?P<target>[^)\s]+)(?:\s+\"[^\"]*\")?\)")
IMAGE_RE = re.compile(r"!\[(?P<text>[^\]]*)\]\((?P<target>[^)\s]+)(?:\s+\"[^\"]*\")?\)")
HEADING_RE = re.compile(r"^(?P<hashes>#{1,6})\s+(?P<title>.*?)\s*#*\s*$")
FENCE_RE = re.compile(r"^\s*(?P<fence>```+|~~~+)")
# Explicit anchors, e.g. <a id="foo"></a> or <a name="foo">.
ANCHOR_RE = re.compile(r"<a\s+(?:id|name)=[\"'](?P<id>[^\"']+)[\"']")

SKIP_DIRS = {".git", "node_modules", "vendor", "bin", ".venv"}


def slugify(title: str) -> str:
    """Reproduce GitHub's heading-to-anchor transformation."""
    text = title

    # Strip inline markup that GitHub resolves before slugging.
    text = re.sub(r"`([^`]*)`", r"\1", text)
    text = re.sub(r"!\[[^\]]*\]\([^)]*\)", "", text)
    text = re.sub(r"\[([^\]]*)\]\([^)]*\)", r"\1", text)
    text = re.sub(r"(\*\*|__)(.*?)\1", r"\2", text)
    text = re.sub(r"(\*|_)(.*?)\1", r"\2", text)
    text = re.sub(r"~~(.*?)~~", r"\1", text)
    text = re.sub(r"<[^>]+>", "", text)

    text = text.strip().lower()
    # Drop everything that is neither word character, space, nor hyphen.
    text = re.sub(r"[^\w\s-]", "", text, flags=re.UNICODE)
    # Each space becomes one hyphen. Runs are *not* collapsed: GitHub maps them
    # one for one, so a heading like "Step 3 — Stability gates" loses the em
    # dash and keeps both surrounding spaces, yielding "step-3--stability-gates"
    # with a double hyphen. Collapsing here would reject every such anchor.
    return re.sub(r"\s", "-", text)


def strip_code_fences(lines: list[str]) -> list[tuple[int, str]]:
    """Return (line_number, text) pairs outside fenced code blocks.

    Headings and links inside a fenced block are examples, not references. The
    config schema in requirements.md is a large YAML block full of '#' lines,
    and treating those as headings would invent dozens of phantom anchors.
    """
    out: list[tuple[int, str]] = []
    fence: str | None = None

    for number, line in enumerate(lines, start=1):
        match = FENCE_RE.match(line)
        if match:
            marker = match.group("fence")
            if fence is None:
                fence = marker
                continue
            if marker[0] == fence[0] and len(marker) >= len(fence):
                fence = None
                continue
        if fence is None:
            out.append((number, line))
    return out


def collect_anchors(path: Path) -> set[str]:
    """Every fragment that can be linked to within one file."""
    lines = path.read_text(encoding="utf-8").splitlines()
    anchors: set[str] = set()
    seen: dict[str, int] = {}

    for _, line in strip_code_fences(lines):
        for match in ANCHOR_RE.finditer(line):
            anchors.add(match.group("id"))

        heading = HEADING_RE.match(line)
        if not heading:
            continue

        slug = slugify(heading.group("title"))
        if not slug:
            continue

        # GitHub disambiguates repeated headings with -1, -2, ...
        count = seen.get(slug, 0)
        anchors.add(slug if count == 0 else f"{slug}-{count}")
        seen[slug] = count + 1

    return anchors


def markdown_files(root: Path) -> list[Path]:
    return sorted(
        p
        for p in root.rglob("*.md")
        if not any(part in SKIP_DIRS for part in p.relative_to(root).parts)
    )


def main(argv: list[str]) -> int:
    root = Path(argv[1]).resolve() if len(argv) > 1 else Path(__file__).resolve().parent.parent
    files = markdown_files(root)
    if not files:
        print(f"no markdown files found under {root}", file=sys.stderr)
        return 1

    anchors = {path: collect_anchors(path) for path in files}
    problems: list[str] = []
    checked = 0

    for path in files:
        lines = path.read_text(encoding="utf-8").splitlines()

        for number, line in strip_code_fences(lines):
            for match in list(LINK_RE.finditer(line)) + list(IMAGE_RE.finditer(line)):
                target = match.group("target").strip()
                if not target or target.startswith(("http://", "https://", "mailto:", "tel:")):
                    continue

                checked += 1
                where = f"{path.relative_to(root)}:{number}"

                file_part, _, fragment = target.partition("#")

                if file_part:
                    resolved = (path.parent / file_part).resolve()
                    if not resolved.exists():
                        problems.append(f"{where}: target does not exist: {target}")
                        continue
                    if fragment and resolved.suffix.lower() != ".md":
                        continue  # a fragment into a non-markdown file
                else:
                    resolved = path

                if not fragment:
                    continue

                available = anchors.get(resolved)
                if available is None:
                    available = collect_anchors(resolved)
                    anchors[resolved] = available

                if fragment not in available:
                    problems.append(
                        f"{where}: anchor #{fragment} not found in "
                        f"{resolved.relative_to(root) if root in resolved.parents or resolved == root else resolved}"
                    )

    for problem in problems:
        print(problem)

    print(f"checked {checked} internal link(s) across {len(files)} file(s): {len(problems)} problem(s)")
    return 1 if problems else 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
