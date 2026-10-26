import posixpath
import re
import sys
from pathlib import Path

PAGES = [
    ("README.md", "", "DZDCache"),
    ("docs/GETTING_STARTED.md", "getting-started", "DZDCache: getting started"),
    ("docs/CONFIGURATION.md", "configuration", "DZDCache configuration"),
    ("docs/BENCHMARK.md", "benchmark", "DZDCache benchmark"),
    ("CHANGELOG.md", "changelog", "DZDCache changelog"),
]
CHARTS = "docs/benchmark/"
LINK = re.compile(r"\]\(([^)\s]+)\)")


def relative(source, target):
    up = "../" if source else ""
    return (up + (target + "/" if target else "")) or "./"


def rewrite(text, source_file, slug, repository, published):
    base = posixpath.dirname(source_file)

    def link(match):
        target = match.group(1)
        if target.startswith("#") or re.match(r"^[a-z][a-z0-9+.-]*:", target):
            return match.group(0)
        path, _, fragment = target.partition("#")
        resolved = posixpath.normpath(posixpath.join(base, path))
        anchor = "#" + fragment if fragment else ""
        if resolved in published:
            return "](" + relative(slug, published[resolved]) + anchor + ")"
        if resolved.startswith(CHARTS) and "benchmark" in published.values():
            return "](" + ("" if slug == "benchmark" else relative(slug, "benchmark")) + resolved[len(CHARTS):] + ")"
        return "](https://github.com/" + repository + "/blob/main/" + resolved + anchor + ")"

    return LINK.sub(link, text)


def build(root, repository):
    published = {source: slug for source, slug, _ in PAGES if (root / source).exists()}
    pages = {}
    for source, slug, title in PAGES:
        if source not in published:
            continue
        text = rewrite((root / source).read_text(encoding="utf-8"), source, slug, repository, published)
        pages[slug] = "---\nlayout: default\ntitle: " + title + "\nsource: " + source + "\n---\n{% raw %}\n" + text + "\n{% endraw %}\n"
    return pages


def main(arguments):
    root, output, repository = Path(arguments[0]), Path(arguments[1]), arguments[2]
    for slug, page in build(root, repository).items():
        target = output / slug / "index.md" if slug else output / "index.md"
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(page, encoding="utf-8")


if __name__ == "__main__":
    main(sys.argv[1:])
