"""Assembles the website's sources from the repository's own docs, so the site can't drift from them.

Called by scripts/site.sh, which runs mdBook after it:

- the book's sources, in BOOK/src: the guide's chapters (docs/guide/*.md),
  with a contents page made from the guide's own index, and the architecture overview
  (docs/ARCHITECTURE.md). A link to a file outside the book (WIRE.md, say)
  goes to it on GitHub, at REF.
- the landing page, OUT/index.html: site/index.html with the version, and the counter example
  (crates/turbofan/examples/counter.rs) and the command line that runs it, filled in, and
  the benchmark's figures, from the results in the guide's performance chapter.

Then, with --finish, after mdBook has built the book into OUT/guide:

- each page's description, which mdBook leaves empty, for search results.
- for language models: each page's markdown beside its HTML (guide/first-session.md), with the
  examples' hidden lines shown so they're whole programs; OUT/llms.txt, an index of them; and
  OUT/llms-full.txt, the whole book in one file.

Usage: python3 site/build.py BOOK OUT REF
       python3 site/build.py --finish BOOK OUT REF
"""

import html
import posixpath
import re
import shutil
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
REPO = "https://github.com/jrogers/turbofan"
SITE = "https://jrogers.github.io/turbofan"


def main(book: Path, out: Path, ref: str) -> None:
    src = book / "src"
    src.mkdir(parents=True)
    shutil.copy(ROOT / "site" / "book.toml", book / "book.toml")
    # The logo as the book's favicon, in place of mdBook's own.
    (book / "theme").mkdir()
    shutil.copy(ROOT / "docs" / "assets" / "logo.svg", book / "theme" / "favicon.svg")
    shutil.copy(ROOT / "docs" / "assets" / "logo-512.png", book / "theme" / "favicon.png")
    shutil.copy(ROOT / "site" / "theme.css", book / "theme" / "turbofan.css")

    guide = ROOT / "docs" / "guide"
    chapters = []
    # The guide's index is the book's first page, index.html: mdBook would render a README.md
    # there but leave links to it pointing at README.html.
    names = {p.name: ("index.md" if p.name == "README.md" else p.name) for p in guide.glob("*.md")}
    for path in sorted(guide.glob("*.md")):
        text = relink(path.read_text().replace(f"]({REPO}/blob/main/docs/ARCHITECTURE.md)", "](architecture.md)"), "docs/guide", set(names) | {"architecture.md"}, ref).replace("](README.md)", "](index.md)")
        (src / names[path.name]).write_text(text)
        chapters.append(names[path.name])
    architecture = (ROOT / "docs" / "ARCHITECTURE.md").read_text()
    architecture = architecture.replace("](guide/README.md)", "](index.md)")
    (src / "architecture.md").write_text(relink(architecture, "docs", {"index.md"}, ref))
    (src / "SUMMARY.md").write_text(summary((guide / "README.md").read_text(), chapters))

    out.mkdir(parents=True, exist_ok=True)
    shutil.copy(ROOT / "docs" / "assets" / "logo.svg", out / "logo.svg")
    page = (ROOT / "site" / "index.html").read_text()
    page = page.replace("{{version}}", version())
    page = page.replace("{{counter}}", highlight(counter()))
    page = page.replace("{{serve}}", html.escape(serve(), quote=False))
    for name, value in bench().items():
        page = page.replace(f"{{{{bench.{name}}}}}", html.escape(value, quote=False))
    assert "{{" not in page, "a placeholder in site/index.html has nothing to fill it"
    (out / "index.html").write_text(page)


def summary(index: str, chapters: list[str]) -> str:
    """The book's contents, in the order the guide's index lists its chapters."""
    lines = ["# Summary", "", "[The Turbofan guide](index.md)", ""]
    for title, target, _ in listed(index):
        assert target in chapters, f"the guide's index lists {target}, which isn't a chapter"
        lines.append(f"- [{title}]({target})")
    lines += ["", "---", "", "[Architecture](architecture.md)", ""]
    return "\n".join(lines)


def listed(index: str) -> list[tuple[str, str, str]]:
    """The chapters the guide's index lists, in order: each one's title, file and what it covers."""
    targets = dict(re.findall(r"^\[([\w-]+)\]: (\S+\.md)$", index, re.M))
    items = re.findall(r"^\d+\. \[([^\]]+)\]\[([\w-]+)\]: (.+?)(?=\n\d+\. |\n\n)", index, re.M | re.S)
    return [(title, targets[ref], " ".join(about.split())) for title, ref, about in items]


def finish(book: Path, out: Path, ref: str) -> None:
    """Fills in what mdBook leaves out of the built book: page descriptions and the markdown."""
    src, guide = book / "src", out / "guide"
    chapters = listed((src / "index.md").read_text())
    pages = [("The Turbofan guide", "index.md", first_paragraph((src / "index.md").read_text()))]
    pages += [(title, target, f"{title} in Turbofan, a Raft-replicated state machine framework in Rust: {about}") for title, target, about in chapters]
    pages.append(("Architecture", "architecture.md", "The architecture of Turbofan, a Raft-replicated state machine framework in Rust: how its crates and layers fit together."))
    names = {target for _, target, _ in pages}

    full = []
    for _, target, description in pages:
        page = guide / target.replace(".md", ".html")
        text = page.read_text()
        empty = '<meta name="description" content="">'
        assert empty in text, f"mdBook's page head changed: update site/build.py's finish for {page}"
        page.write_text(text.replace(empty, f'<meta name="description" content="{html.escape(description)}">'))
        markdown = unhide((src / target).read_text())
        (guide / target).write_text(markdown)
        # One file has no directory to resolve the chapters' links to each other against.
        absolute = lambda m: f"{m.group(1)}{SITE}/guide/{m.group(2)}" if m.group(2) in names else m.group(0)
        full.append(re.sub(r"(\]\(|^\[[^\]]+\]: )([\w-]+\.md)", absolute, markdown, flags=re.M))

    (out / "llms.txt").write_text(llms(chapters, ref))
    (out / "llms-full.txt").write_text(llms(chapters, ref) + "\n---\n\n" + "\n\n---\n\n".join(full))


def llms(chapters: list[tuple[str, str, str]], ref: str) -> str:
    """The site's index for language models, as llmstxt.org lays it out: what Turbofan is, its
    status, and links to each chapter's markdown."""
    landing = (ROOT / "site" / "index.html").read_text()
    about = html.unescape(re.search(r'<meta name="description" content="([^"]+)">', landing).group(1))
    readme = (ROOT / "README.md").read_text()
    warning = re.search(r"^> \[!WARNING\]\n((?:> .*\n)+)", readme, re.M)
    assert warning, "README.md's warning moved: update site/build.py's llms"
    what = re.search(r"^\*\*Turbofan\*\* .*?(?=\n\n)", readme, re.M | re.S)
    assert what, "README.md no longer says what Turbofan is: update site/build.py's llms"
    status = " ".join(line[2:] for line in warning.group(1).splitlines()).replace("/blob/main/", f"/blob/{ref}/")
    lines = [
        "# Turbofan", "", f"> {about}", "", " ".join(what.group(0).replace("**", "").split()), "", status, "",
        "The guide's examples are compiled and run as tests. The markdown below shows their hidden",
        "lines, so each is a whole program.", "",
        "## Guide", "",
        f"- [The Turbofan guide]({SITE}/guide/index.md): what the guide covers, chapter by chapter",
    ]
    lines += [f"- [{title}]({SITE}/guide/{target}): {about}" for title, target, about in chapters]
    lines += [
        "", "## Reference", "",
        f"- [Architecture]({SITE}/guide/architecture.md): how its crates and layers fit together",
        "- [API reference](https://docs.rs/turbofan): every public type, on docs.rs",
        f"- [Wire protocol]({REPO}/blob/{ref}/docs/WIRE.md): how the protocol may change, with old and new nodes side by side",
        f"- [Changelog]({REPO}/blob/{ref}/CHANGELOG.md)",
        "", "## Optional", "",
        f"- [The whole guide in one file]({SITE}/llms-full.txt)",
        "",
    ]
    return "\n".join(lines)


def first_paragraph(markdown: str) -> str:
    """A page's first paragraph after its heading, as plain text."""
    paragraph = next(block for block in markdown.split("\n\n") if block and not block.startswith("#"))
    text = re.sub(r"\[([^\]]+)\](?:\([^)]*\)|\[[^\]]*\])", r"\1", " ".join(paragraph.split()))
    return text.replace("`", "")


def unhide(markdown: str) -> str:
    """Shows the lines rustdoc and mdBook hide in Rust examples (`# use ...`), so each is whole."""
    out, fenced, rust = [], False, False
    for line in markdown.split("\n"):
        if line.startswith("```"):
            fenced = not fenced
            rust = fenced and line[3:].split(",")[0] == "rust"
        elif rust and (line == "#" or line.startswith("# ")):
            line = line[2:]
        elif rust and line.startswith("##"):
            line = line[1:]
        out.append(line)
    return "\n".join(out)


def relink(text: str, directory: str, in_book: set[str], ref: str) -> str:
    """Points relative links to files the book doesn't have at them on GitHub, at `ref`."""

    def github(target: str) -> str:
        path, _, anchor = target.partition("#")
        if path in in_book or "://" in path or path.startswith(("#", "mailto:")) or not path:
            return target
        url = f"{REPO}/blob/{ref}/{posixpath.normpath(posixpath.join(directory, path))}"
        return f"{url}#{anchor}" if anchor else url

    text = re.sub(r"\]\(([^)\s]+)\)", lambda m: f"]({github(m.group(1))})", text)
    return re.sub(r"^(\[[^\]]+\]): (\S+)$", lambda m: f"{m.group(1)}: {github(m.group(2))}", text, flags=re.M)


def version() -> str:
    manifest = (ROOT / "Cargo.toml").read_text()
    return re.search(r'^version = "([^"]+)"', manifest, re.M).group(1)


def counter() -> str:
    """The counter service, from examples/counter.rs: its state, its Service and its main."""
    example = (ROOT / "crates" / "turbofan" / "examples" / "counter.rs").read_text()
    return example[example.index("#[derive(Debug, Default)]") :]


def serve() -> str:
    """The command line that starts the counter's first node, from examples/counter.rs's docs."""
    example = (ROOT / "crates" / "turbofan" / "examples" / "counter.rs").read_text()
    match = re.search(r"^//! ```sh\n((?://!.*\n)+?)//! ```", example, re.M)
    assert match, "examples/counter.rs no longer shows its command line: update site/build.py's serve"
    return "".join(line[4:] + "\n" for line in match.group(1).splitlines())


def bench() -> dict[str, str]:
    """The landing page's figures, from the guide's performance chapter: a cluster of machines of
    its own, on Linux, its full table introduced by "On AWS <machine>: ", which has a row per run
    and a column per figure."""
    chapter = (ROOT / "docs" / "guide" / "performance.md").read_text()
    _, found, results = chapter.partition("\n### On Linux servers\n")
    assert found, "the performance chapter has no 'On Linux servers' section: update site/build.py's bench"
    machine = re.search(r"^On AWS (.+?): ", results, re.M)
    assert machine, "the Linux section has no full table introduced by 'On AWS <machine>: '"
    # The table after that line.
    first = re.search(r"^\|.*?(?=\n[^|]|\Z)", results[machine.end():], re.M | re.S)
    assert first, "the Linux section's machine has no table"
    rows = [[cell.strip() for cell in line.strip("|").split("|")] for line in first.group(0).splitlines()]
    header, table = rows[0], {row[0]: dict(zip(rows[0], row)) for row in rows[2:]}

    def cell(run: str, column: str) -> str:
        assert column in header and run in table, f"the performance chapter's table has no {column} for {run}"
        return table[run][column]

    return {
        "machine": machine.group(1),
        # Synced, the default; then without, for comparison.
        "throughput": cell("throughput, fsync", "msgs/s"),
        "throughput.p99": cell("throughput, fsync", "p99"),
        "latency": cell("latency, fsync", "p50"),
        "latency.p99": cell("latency, fsync", "p99"),
        "unsynced": cell("throughput", "msgs/s"),
    }


RUST_KEYWORDS = {
    "as", "async", "await", "break", "const", "continue", "crate", "else", "enum", "fn", "for", "if",
    "impl", "in", "let", "loop", "match", "mod", "move", "mut", "pub", "ref", "return", "self", "Self",
    "static", "struct", "trait", "type", "use", "where", "while",
}


def highlight(code: str) -> str:
    """Rust as HTML with spans for comments, strings, keywords, types and macros: enough for the
    landing page's one example, without a highlighting library."""
    token = re.compile(r'(?P<comment>//[^\n]*)|(?P<string>"(?:\\.|[^"\\])*")|(?P<word>[A-Za-z_][A-Za-z0-9_]*!?)|(?P<other>.)', re.S)
    out = []
    for m in token.finditer(code):
        text = html.escape(m.group(0), quote=False)
        if m.group("comment"):
            out.append(f'<span class="c">{text}</span>')
        elif m.group("string"):
            out.append(f'<span class="s">{text}</span>')
        elif m.group("word"):
            word = m.group("word")
            if word.endswith("!"):
                out.append(f'<span class="m">{text}</span>')
            elif word in RUST_KEYWORDS:
                out.append(f'<span class="k">{text}</span>')
            elif word[0].isupper():
                out.append(f'<span class="t">{text}</span>')
            else:
                out.append(text)
        else:
            out.append(text)
    return "".join(out)


if __name__ == "__main__":
    if sys.argv[1] == "--finish":
        finish(Path(sys.argv[2]), Path(sys.argv[3]), sys.argv[4])
    else:
        main(Path(sys.argv[1]), Path(sys.argv[2]), sys.argv[3])
