from __future__ import annotations

from pathlib import Path

from huldra.atom import parse_arxiv_atom


def test_atom_parser_normalizes_arxiv_paper_metadata() -> None:
    parsed = parse_arxiv_atom(Path("tests/fixtures/arxiv_sample_feed.xml").read_text(encoding="utf-8"))
    paper = parsed.papers[0]
    assert parsed.total_results == 1
    assert paper.arxiv_id == "2604.27001v2"
    assert paper.version == 2
    assert paper.canonical_url == "https://arxiv.org/abs/2604.27001v2"
    assert paper.title == "Pool Paper"
    assert paper.abstract == "Abstract text."
    assert paper.authors == ["Ada Lovelace", "Grace Hopper"]
    assert paper.primary_category == "cs.AI"
    assert paper.categories == ["cs.AI", "cs.LG"]
    assert paper.comment == "12 pages"
    assert paper.journal_ref == "Journal Demo"
    assert paper.doi == "10.1234/demo"
    assert set(paper.raw_atom) == {"entry_id", "alternate_url", "pdf_url"}


def test_atom_parser_handles_old_style_ids() -> None:
    feed = """<?xml version="1.0"?>
    <feed xmlns="http://www.w3.org/2005/Atom">
      <entry>
        <id>http://arxiv.org/abs/math/0309136v1</id>
        <updated>2003-09-12T00:00:00Z</updated>
        <published>2003-09-12T00:00:00Z</published>
        <title>Old Style</title>
        <summary>Text.</summary>
        <author><name>Author One</name></author>
      </entry>
    </feed>"""
    paper = parse_arxiv_atom(feed).papers[0]
    assert paper.arxiv_id == "math/0309136v1"
    assert paper.version == 1
