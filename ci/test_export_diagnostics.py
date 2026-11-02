"""Pure local-file/mock controls: no Docker, processes, build, network or VM."""
import importlib.util
import json
import os
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch

SPEC = importlib.util.spec_from_file_location('diagnostics', Path(__file__).with_name('export-diagnostics.py'))
m = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(m)
BASE = Path(__file__).resolve().parent / 'testdata' / 'diagnostics'
PROFILE = BASE / 'OpenShell' / 'diagnostics-profile.json'


def environment(repository='leogr/OpenShell'):
    return {'GITHUB_ACTIONS': 'true', 'GITHUB_REPOSITORY': repository,
            'SOURCE_SUBJECT': m.SUBJECTS[repository], 'BUILD_ARCH': 'amd64',
            'BUILD_DEFINITION': 'a' * 40, 'GITHUB_SHA': 'a' * 40,
            'GITHUB_RUN_ID': '123', 'GITHUB_RUN_ATTEMPT': '1'}


def owned_output(root, repository='leogr/OpenShell'):
    output = root / 'release'
    output.mkdir(mode=0o700)
    (output / 'BUILD-OWNER.json').write_text(json.dumps(m.expected_owner(environment(repository), repository)))
    return output


def file_at(root, name, raw):
    p = root / name
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_bytes(raw)
    return p


class DiagnosticsTests(unittest.TestCase):
    def collect(self, root, output, outcome='failure', profile=PROFILE, env=None):
        return m.collect(profile, output, root / 'diagnostics', root / 'native-build.exit',
                         outcome, environment() if env is None else env)

    def test_setup_failure_without_output_is_explicit_metadata_only(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp).resolve()
            (root / 'native-build.exit').write_text('1\n')
            report = self.collect(root, root / 'release')
            self.assertEqual(report['output_status'], 'missing_output_preflight_or_setup_failure')
            self.assertEqual(report['actual_exit_file']['actual_exit'], 1)
            self.assertEqual(report['actual_exit_file']['mutation_outcome'], 'not_inferred')
            self.assertFalse(report['owner_verified'])
            self.assertEqual(report['copied_files'], 0)
            self.assertTrue((root / 'diagnostics/DIAGNOSTICS.json').is_file())

    def test_unowned_preexisting_output_is_never_adopted(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp).resolve()
            output = root / 'release'
            output.mkdir()
            (output / 'INCOMPLETE.json').write_bytes(b'not owned')
            report = self.collect(root, output)
            self.assertEqual(report['output_status'], 'missing_or_unmatched_owned_marker')
            self.assertEqual(report['copied_files'], 0)
            self.assertFalse((output / 'BUILD-OWNER.json').exists())

    def test_owner_wrong_run_repository_or_arch_is_refused(self):
        for field, value in [('run_id', '999'), ('repository', 'leogr/other'), ('arch', 'arm64')]:
            with self.subTest(field=field), tempfile.TemporaryDirectory() as temp:
                root = Path(temp).resolve()
                output = owned_output(root)
                owner = json.loads((output / 'BUILD-OWNER.json').read_text())
                owner[field] = value
                (output / 'BUILD-OWNER.json').write_text(json.dumps(owner))
                report = self.collect(root, output)
                self.assertFalse(report['owner_verified'])
                self.assertEqual(report['copied_files'], 0)

    def test_nonzero_and_lost_reply_logs_preserve_original_bytes_without_inference(self):
        for known, exit_code in [(True, 7), (False, 124)]:
            with self.subTest(known=known), tempfile.TemporaryDirectory() as temp:
                root = Path(temp).resolve()
                output = owned_output(root)
                (root / 'native-build.exit').write_text(str(exit_code) + '\n')
                result = json.dumps({'result_known': known, 'exit_code': 7 if known else None}).encode()
                name = 'evidence/commands/001-start-attach-1.result.json'
                file_at(output, name, result)
                file_at(output, 'build-1.cid', b'f' * 64 + b'\n')
                file_at(output, 'evidence/build-1/openshell-sandbox-build.log', b'partial compiler error\n')
                report = self.collect(root, output)
                self.assertEqual((root / 'diagnostics/captured' / name).read_bytes(), result)
                self.assertEqual(report['actual_exit_file']['actual_exit'], exit_code)
                self.assertFalse(report['positive_build_proof'])
                self.assertFalse(report['complete'])
                self.assertEqual(report['mutation_outcome'], 'not_inferred')
                self.assertTrue(any(x['status'] == 'missing' for x in report['files']))

    def test_prefix_truncation_is_bounded_hashed_and_explicit(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp).resolve()
            output = owned_output(root)
            data = b'x' * (m.PER_FILE + 123)
            name = 'evidence/build-1/openshell-sandbox-build.log'
            file_at(output, name, data)
            report = self.collect(root, output)
            record, = [x for x in report['files'] if x['path'] == name]
            self.assertTrue(record['truncated'])
            self.assertEqual(record['original_size'], len(data))
            self.assertEqual(record['captured_size'], m.PER_FILE)
            self.assertEqual(record['captured_prefix_sha256'], m.digest(data[:m.PER_FILE]))
            self.assertEqual((root / 'diagnostics' / record['captured_path']).stat().st_size, m.PER_FILE)

    def test_symlink_fifo_and_hardlink_are_refused_without_reading_targets(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp).resolve()
            output = owned_output(root)
            secret = root / 'outside'
            secret.write_bytes(b'never collect this')
            (output / 'build-1.cid').symlink_to(secret)
            os.mkfifo(output / 'build-2.cid')
            os.link(secret, output / 'INCOMPLETE.json')
            report = self.collect(root, output)
            for name in ('build-1.cid', 'build-2.cid', 'INCOMPLETE.json'):
                entry, = [x for x in report['files'] if x['path'] == name]
                self.assertTrue(entry['status'].startswith('refused'))
                self.assertNotIn('captured_path', entry)
            self.assertEqual(secret.read_bytes(), b'never collect this')

    def test_work_source_cache_keyring_and_success_publication_are_untouched(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp).resolve()
            output = owned_output(root)
            excluded = ['work-1/cargo/token', 'source/Cargo.lock', 'target/private',
                        'evidence/public-keyring/state', 'publication/CI-MANIFEST.json',
                        'publication/openshell-sandbox-amd64.tar.gz']
            before = {name: file_at(output, name, name.encode()).read_bytes() for name in excluded}
            report = self.collect(root, output, outcome='success')
            for name, raw in before.items():
                self.assertEqual((output / name).read_bytes(), raw)
                self.assertFalse((root / 'diagnostics/captured' / name).exists())
            self.assertFalse(report['positive_build_proof'])
            self.assertEqual(report['build_step_outcome'], 'success')

    def test_profile_widening_is_refused_even_in_same_repository(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp).resolve()
            output = owned_output(root)
            profile = json.loads(PROFILE.read_text())
            profile['files'].append('source/private')
            altered = root / 'profile.json'
            altered.write_text(json.dumps(profile))
            report = self.collect(root, output, profile=altered)
            self.assertEqual(report['output_status'], 'profile_or_expected_identity_invalid')
            self.assertEqual(report['copied_files'], 0)

    def test_unknown_action_outcome_and_missing_exit_remain_explicit(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp).resolve()
            report = self.collect(root, root / 'release', outcome='unknown')
            self.assertEqual(report['build_step_outcome'], 'unknown')
            self.assertEqual(report['actual_exit_file']['status'], 'missing')
            self.assertEqual(report['mutation_outcome'], 'not_inferred')

    def test_changed_file_is_marked_unstable(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp).resolve()
            output = owned_output(root)
            path = file_at(output, 'INCOMPLETE.json', b'first')
            inode = path.stat().st_ino
            original_read = m.os.read
            changed = False
            def changing_read(fd, size):
                nonlocal changed
                raw = original_read(fd, size)
                if not changed and os.fstat(fd).st_ino == inode:
                    changed = True
                    path.write_bytes(b'different-size')
                return raw
            with patch.object(m.os, 'read', side_effect=changing_read):
                report = self.collect(root, output)
            entry, = [x for x in report['files'] if x['path'] == 'INCOMPLETE.json']
            self.assertEqual(entry['status'], 'unstable')
            self.assertFalse(entry['unchanged'])
            self.assertFalse(report['complete'])

    def test_budgets_apply_before_copy_growth(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp).resolve()
            output = owned_output(root)
            for number in range(1, 80):
                file_at(output, f'evidence/commands/{number:03d}-image-pull.stdout', b'x' * 1024)
            # Smaller test ceilings retain identical fixed-budget algorithm.
            with patch.object(m, 'TOTAL', 16384), patch.object(m, 'MANIFEST_RESERVE', 8192), \
                 patch.object(m, 'PER_FILE', 1024), patch.object(m, 'MAX_FILES', 12):
                report = self.collect(root, output)
            files = [p for p in (root / 'diagnostics').rglob('*') if p.is_file()]
            self.assertLessEqual(len(files), 12)
            self.assertLessEqual(report['captured_bytes'], 8192)
            self.assertTrue(report['scan_limited'])

    @staticmethod
    def model_writer_uids(ownership):
        """Model a nonroot CI UID and container writers without chown/privilege."""
        real_fstat = m.os.fstat
        def modeled_fstat(fd):
            value = real_fstat(fd)
            fields = {name: getattr(value, name) for name in dir(value) if name.startswith('st_')}
            fields['st_uid'] = ownership.get(value.st_ino, 1001)
            return SimpleNamespace(**fields)
        return modeled_fstat

    def test_verified_ci_root_captures_readable_root_written_inner_log(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp).resolve()
            output = owned_output(root)
            name = 'evidence/build-1/openshell-sandbox-build.log'
            log = file_at(output, name, b'actual root-written inner compiler log\n')
            owners = {log.stat().st_ino: 0}
            with patch.object(m.os, 'geteuid', return_value=1001), \
                 patch.object(m.os, 'fstat', side_effect=self.model_writer_uids(owners)):
                report = self.collect(root, output)
            self.assertTrue(report['owner_verified'])
            entry, = [x for x in report['files'] if x['path'] == name]
            self.assertEqual(entry['status'], 'captured')
            self.assertEqual((root / 'diagnostics' / entry['captured_path']).read_bytes(), log.read_bytes())
            self.assertFalse(report['positive_build_proof'])

    def test_foreign_uid_profile_data_is_still_refused(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp).resolve()
            output = owned_output(root)
            name = 'evidence/build-1/openshell-sandbox-build.log'
            log = file_at(output, name, b'foreign writer')
            owners = {log.stat().st_ino: 2002}
            with patch.object(m.os, 'geteuid', return_value=1001), \
                 patch.object(m.os, 'fstat', side_effect=self.model_writer_uids(owners)):
                report = self.collect(root, output)
            self.assertTrue(report['owner_verified'])
            entry, = [x for x in report['files'] if x['path'] == name]
            self.assertTrue(entry['status'].startswith('refused'))
            self.assertNotIn('captured_path', entry)

    def test_actual_collect_both_profiles_accept_only_inner_file_root_writer(self):
        cases = [('leogr/OpenShell', 'OpenShell', 'evidence/build-1/openshell-sandbox-build.log'),
                 ('leogr/airc', 'airc', 'build-1/build/build.stderr')]
        for repository, label, name in cases:
            with self.subTest(repository=repository), tempfile.TemporaryDirectory() as temp:
                root = Path(temp).resolve()
                output = owned_output(root, repository)
                log = file_at(output, name, b'root inner compiler output')
                with patch.object(m.os, 'geteuid', return_value=1001), \
                     patch.object(m.os, 'fstat', side_effect=self.model_writer_uids({log.stat().st_ino: 0})):
                    report = self.collect(root, output, profile=BASE / label / 'diagnostics-profile.json',
                                          env=environment(repository))
                entry, = [x for x in report['files'] if x['path'] == name]
                self.assertEqual(entry['status'], 'captured')
                self.assertEqual((root / 'diagnostics' / entry['captured_path']).read_bytes(), log.read_bytes())

    def test_actual_collect_root_owned_host_receipts_and_flat_airc_results_are_refused(self):
        cases = [('leogr/OpenShell', 'OpenShell', name) for name in
                 ('evidence/commands/001-create-1.request.json',
                  'evidence/commands/001-create-1.result.json', 'INCOMPLETE.json',
                  'build-1.cid', 'evidence/worker.json')]
        cases += [('leogr/airc', 'airc', name) for name in
                  ('build-1/create-result.json', 'build-1/start-result.json', 'FAILED.json',
                   'evidence/docker/admission.json', 'build-1/airc.elf.stdout',
                   'build-1/airc.version.stdout', 'build-1/container.log')]
        for repository, label, name in cases:
            with self.subTest(repository=repository, name=name), tempfile.TemporaryDirectory() as temp:
                root = Path(temp).resolve()
                output = owned_output(root, repository)
                record = file_at(output, name, b'host output with unexpected writer')
                with patch.object(m.os, 'geteuid', return_value=1001), \
                     patch.object(m.os, 'fstat', side_effect=self.model_writer_uids({record.stat().st_ino: 0})):
                    report = self.collect(root, output, profile=BASE / label / 'diagnostics-profile.json',
                                          env=environment(repository))
                self.assertTrue(report['owner_verified'])
                entry, = [x for x in report['files'] if x['path'] == name]
                self.assertTrue(entry['status'].startswith('refused'))
                self.assertNotIn('captured_path', entry)

    def test_root_owned_host_ancestors_are_refused_even_for_eligible_inner_leaf(self):
        for repository, label, name in [('leogr/OpenShell', 'OpenShell', 'evidence/build-1/openshell-sandbox-build.log'),
                                         ('leogr/airc', 'airc', 'build-1/build/build.stderr')]:
            with self.subTest(repository=repository), tempfile.TemporaryDirectory() as temp:
                root = Path(temp).resolve()
                output = owned_output(root, repository)
                log = file_at(output, name, b'root inner compiler output')
                owners = {log.stat().st_ino: 0, log.parent.stat().st_ino: 0}
                with patch.object(m.os, 'geteuid', return_value=1001), \
                     patch.object(m.os, 'fstat', side_effect=self.model_writer_uids(owners)):
                    report = self.collect(root, output, profile=BASE / label / 'diagnostics-profile.json',
                                          env=environment(repository))
                self.assertTrue(report['owner_verified'])
                self.assertFalse((root / 'diagnostics/captured' / name).exists())
                self.assertIn(str(Path(name).parent), report['missing_or_refused_directories'])

    def test_writer_selector_requires_pinned_profile_membership_and_repo_scope(self):
        shell = m.load_profile(PROFILE)
        airc = m.load_profile(BASE / 'airc' / 'diagnostics-profile.json')
        self.assertTrue(m.container_written_file(shell, 'evidence/build-1/rustc.txt'))
        self.assertTrue(m.container_written_file(airc, 'build-2/build/rustc.txt'))
        for name in ('evidence/build-1/unknown.log', 'evidence/commands/001-create-1.result.json',
                     'build-1/build/rustc.txt', 'INCOMPLETE.json'):
            self.assertFalse(m.container_written_file(shell, name))
        for name in ('build-1/airc.elf.stdout', 'build-1/container.log',
                     'build-1/build/unknown.log', 'evidence/docker/admission.json'):
            self.assertFalse(m.container_written_file(airc, name))
        widened = dict(shell, files=shell['files'] + ['evidence/build-1/unknown.log'])
        self.assertFalse(m.container_written_file(widened, 'evidence/build-1/rustc.txt'))

    def test_root_written_marker_or_top_root_cannot_admit_collection(self):
        for boundary in ('BUILD-OWNER.json', '.'):
            with self.subTest(boundary=boundary), tempfile.TemporaryDirectory() as temp:
                root = Path(temp).resolve()
                output = owned_output(root)
                file_at(output, 'evidence/build-1/openshell-sandbox-build.log', b'not admitted')
                owners = {(output / boundary).stat().st_ino: 0}
                with patch.object(m.os, 'geteuid', return_value=1001), \
                     patch.object(m.os, 'fstat', side_effect=self.model_writer_uids(owners)):
                    report = self.collect(root, output)
                self.assertFalse(report['owner_verified'])
                self.assertEqual(report['copied_files'], 0)
                self.assertFalse((root / 'diagnostics/captured').exists())

    def test_default_reader_remains_strict_without_explicit_profile_capture_flag(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp).resolve()
            log = file_at(root, 'inner.log', b'root writer')
            descriptor = os.open(root, os.O_RDONLY | os.O_DIRECTORY)
            try:
                with patch.object(m.os, 'geteuid', return_value=1001), \
                     patch.object(m.os, 'fstat', side_effect=self.model_writer_uids({log.stat().st_ino: 0})):
                    raw, record = m.read_prefix(descriptor, 'inner.log', 1024)
                self.assertIsNone(raw)
                self.assertTrue(record['status'].startswith('refused'))
            finally:
                os.close(descriptor)

    def test_frozen_both_repository_profiles_are_accepted(self):
        self.assertEqual(m.load_profile(PROFILE)['repository'], 'leogr/OpenShell')
        self.assertEqual(m.load_profile(BASE / 'airc/diagnostics-profile.json')['repository'], 'leogr/airc')


if __name__ == '__main__':
    unittest.main()
