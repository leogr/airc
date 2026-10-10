"""Negative controls for KB links and evidence direction, without a Git repo."""
import importlib.util
from pathlib import Path
import tempfile
import unittest

spec = importlib.util.spec_from_file_location('check_docs', Path(__file__).with_name('check-docs.py'))
checker = importlib.util.module_from_spec(spec)
spec.loader.exec_module(checker)


class DocumentationChecks(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.write('source.rs', '// Rust fixture\nfn example() {}\n')
        self.write('docs/README.md', '# KB\n[spec](specs/topic.md) [digest](digests/topic.md)\n')
        self.write('docs/specs/topic.md', '# Topic\n\n[FACT] Contract. [1](#source-1)\n\n'
                   '## Sources\n\n1. <a id="source-1"></a> [Evidence](../digests/topic.md).\n')
        self.write('docs/digests/topic.md', '# Evidence\n\nVerification: `' + 'a' * 40 + '`\n\n'
                   '[FACT] Implementation. [1](#source-1)\n\n## Sources\n\n'
                   '1. <a id="source-1"></a> [Code](../../source.rs#L1-L2) — `example`.\n')

    def write(self, name, text):
        p = self.root / name
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text(text)

    def change(self, name, before, after):
        p = self.root / name
        p.write_text(p.read_text().replace(before, after))

    def errors(self):
        return '\n'.join(checker.check(self.root, list(self.root.rglob('*.md'))))

    def test_valid_graph_and_fenced_examples(self):
        self.write('README.md', '# Usage\n```md\n[example](missing.md)\n```\n')
        self.assertEqual(self.errors(), '')

    def test_missing_file_and_anchor(self):
        self.write('README.md', '[bad](absent.md) [bad anchor](docs/README.md#absent)\n')
        errors = self.errors()
        self.assertIn('missing link target', errors)
        self.assertIn('missing Markdown anchor', errors)

    def test_duplicate_heading_anchor(self):
        self.write('README.md', '# Same\n# Same\n[second](#same-1)\n')
        self.assertEqual(self.errors(), '')

    def test_source_span_must_exist_and_be_ordered(self):
        for span in ('L1-L20', 'L2-L1', 'L0'):
            with self.subTest(span=span):
                self.change('docs/digests/topic.md', '#L1-L2', '#' + span)
                self.assertIn('source', self.errors())
                self.change('docs/digests/topic.md', '#' + span, '#L1-L2')

    def test_upward_evidence_rejected_after_normalization(self):
        self.change('docs/digests/topic.md', '../../source.rs#L1-L2', '../digests/../specs/topic.md')
        self.assertIn('evidence must point', self.errors())

    def test_citation_mismatch_and_unindexed_page(self):
        self.change('docs/specs/topic.md', '[1](#source-1)', '[2](#source-1)')
        self.change('docs/README.md', '[digest](digests/topic.md)', '')
        errors = self.errors()
        self.assertIn('citation number', errors)
        self.assertIn('citations and source entries', errors)
        self.assertIn('not linked from KB index', errors)

    def test_revision_and_epistemic_constraints(self):
        self.change('docs/digests/topic.md', 'a' * 40, 'HEAD')
        self.change('docs/digests/topic.md', '[FACT]', '[DERIVED]')
        errors = self.errors()
        self.assertIn('missing verification', errors)
        self.assertIn('digest evidence must be factual', errors)

    def test_no_repository_escape(self):
        self.write('README.md', '[outside](../outside.md)\n')
        self.assertIn('link escapes repository', self.errors())

    def test_digest_cycle(self):
        self.write('docs/digests/second.md', (self.root / 'docs/digests/topic.md').read_text())
        self.change('docs/digests/topic.md', '../../source.rs#L1-L2', 'second.md')
        self.change('docs/digests/second.md', '../../source.rs#L1-L2', 'topic.md')
        self.assertIn('circular evidence', self.errors())

    def test_nested_page_is_checked_for_index_coverage(self):
        self.write('docs/digests/nested/extra.md', '# Unindexed\n')
        self.assertIn('not linked from KB index', self.errors())


if __name__ == '__main__':
    unittest.main()
