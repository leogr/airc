"""Pure original-release attachment contracts; no GitHub, Docker or Rust run."""
import copy
import importlib.util
import json
import os
from pathlib import Path
import tempfile
import unittest
from unittest import mock

SPEC = importlib.util.spec_from_file_location('attach_test', Path(__file__).with_name('attach-release.py'))
attach = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(attach)
pub = attach.pub
S, W = 'b' * 40, 'a' * 40


class FakeAPI:
    def __init__(self, root):
        self.root = Path(root)
        self.repository = 'leogr/airc'
        self.release = {'id': 17, 'tag_name': 'v0.1.0', 'draft': False, 'prerelease': False,
                        'published_at': '2026-11-03T00:00:00Z'}
        self.source = S
        self.rows, self.data, self.posts, self.reads = [], {}, [], []
        self.known_effects = []
        self.lost_reply = False
        self.corrupt_download = False
        self.move_after_upload = False

    def existing(self, name, data):
        identity = 100 + len(self.rows)
        self.rows.append({'id': identity, 'name': name, 'size': len(data), 'state': 'uploaded',
                          'digest': 'sha256:' + pub.sha(data)})
        self.data[identity] = data

    def call(self, endpoint, method='GET', **kwargs):
        if method == 'POST':
            # There is deliberately no implementation for creating tags,
            # releases, PATCH, DELETE, or overwrite of an existing name.
            assert endpoint.startswith('https://uploads.github.com/repos/leogr/airc/releases/17/assets?name=')
            name = endpoint.split('?name=', 1)[1]
            assert not any(row['name'] == name for row in self.rows)
            self.posts.append(name)
            self.existing(name, Path(kwargs['upload']).read_bytes())
            row = copy.deepcopy(self.rows[-1])
            if self.lost_reply:
                raise pub.Uncertain('fixture lost upload acknowledgement')
            kwargs['acknowledgement'](row)
            if self.move_after_upload:
                self.source = 'c' * 40
            return row
        assert method == 'GET'
        self.reads.append(endpoint)
        if '/releases/assets/' in endpoint:
            identity = int(endpoint.rsplit('/', 1)[1])
            data = b'corrupt' if self.corrupt_download else self.data[identity]
            Path(kwargs['download']).write_bytes(data)
            return None
        if '/git/ref/tags/' in endpoint:
            return {'ref': 'refs/tags/v0.1.0', 'object': {'type': 'commit', 'sha': self.source}}
        if '/releases/tags/' in endpoint or endpoint.endswith('/releases/17'):
            return copy.deepcopy(self.release)
        raise AssertionError(endpoint)

    def list(self, endpoint, *args):
        assert endpoint == 'repos/leogr/airc/releases/17/assets'
        return copy.deepcopy(self.rows)


class AttachmentTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.api = FakeAPI(self.root)
        self.event = {'id': 17, 'tag': 'v0.1.0', 'source': S, 'definition': W,
                      'workflow': 'leogr/airc/.github/workflows/native-release.yml@refs/tags/v0.1.0'}
        self.assets = {}
        for name, data in [('airc-linux-amd64.tar.gz', b'fixture AMD64'),
                           ('airc-linux-arm64.tar.gz', b'fixture ARM64')]:
            path = self.root / name
            path.write_bytes(data)
            self.assets[name] = path

    def test_fresh_attachment_preserves_original_stable_release_and_unrelated_assets(self):
        self.api.existing('README.txt', b'owner asset')
        before = copy.deepcopy(self.api.release)
        result = attach.attach_existing(self.api, self.event, self.assets)
        self.assertEqual(set(self.api.posts), set(self.assets))
        self.assertEqual(self.api.release, before)
        self.assertEqual(self.api.data[100], b'owner asset')
        self.assertEqual(result['release_id'], 17)
        self.assertFalse(result['reconciled_existing_complete'])
        self.assertFalse(result['historical_mutation_outcome_inferred'])
        self.assertEqual(result['consumer_qualification'], 'NOT RUN')

    def test_complete_matching_assets_reconcile_read_only_without_historical_inference(self):
        for name, path in self.assets.items():
            self.api.existing(name, path.read_bytes())
        result = attach.attach_existing(self.api, self.event, self.assets)
        self.assertTrue(result['reconciled_existing_complete'])
        self.assertFalse(result['effects'])
        self.assertFalse(result['historical_mutation_outcome_inferred'])
        self.assertEqual(self.api.posts, [])

    def test_partial_conflicting_and_duplicate_sets_refuse_before_mutation(self):
        for mode in ('partial', 'bytes', 'size', 'duplicate', 'starter'):
            with self.subTest(mode=mode):
                api = FakeAPI(self.root)
                for name, path in self.assets.items():
                    api.existing(name, path.read_bytes())
                if mode == 'partial':
                    api.rows.pop()
                elif mode == 'bytes':
                    api.data[100] = b'wrong bytes!!'
                    api.rows[0]['digest'] = None
                elif mode == 'size':
                    api.rows[0]['size'] += 1
                elif mode == 'duplicate':
                    api.rows.append(copy.deepcopy(api.rows[0]))
                else:
                    api.rows[0]['state'] = 'starter'
                with self.assertRaises(pub.Refusal):
                    attach.attach_existing(api, self.event, self.assets)
                self.assertEqual(api.posts, [])

    def test_changed_source_or_release_identity_refuses_without_creation(self):
        for mode in ('tag', 'id', 'draft', 'tag_name'):
            with self.subTest(mode=mode):
                api = FakeAPI(self.root)
                if mode == 'tag':
                    api.source = 'd' * 40
                elif mode == 'id':
                    api.release['id'] = 99
                elif mode == 'draft':
                    api.release['draft'] = True
                else:
                    api.release['tag_name'] = 'foreign'
                with self.assertRaises(pub.Refusal):
                    attach.attach_existing(api, self.event, self.assets)
                self.assertEqual(api.posts, [])

    def test_lost_upload_reply_is_uncertain_and_never_replayed(self):
        self.api.lost_reply = True
        with self.assertRaises(pub.Uncertain):
            attach.attach_existing(self.api, self.event, self.assets)
        self.assertEqual(len(self.api.posts), 1)

    def test_verified_ack_then_bad_download_is_known_partial_not_fake_success(self):
        self.api.corrupt_download = True
        with self.assertRaises(pub.PartialPublication) as caught:
            attach.attach_existing(self.api, self.event, self.assets)
        self.assertEqual(len(caught.exception.known_effects), 1)
        self.assertEqual(len(self.api.posts), 1)

    def test_tag_change_after_ack_stops_before_next_upload(self):
        self.api.move_after_upload = True
        with self.assertRaises(pub.PartialPublication) as caught:
            attach.attach_existing(self.api, self.event, self.assets)
        self.assertEqual(len(caught.exception.known_effects), 1)
        self.assertEqual(len(self.api.posts), 1)

    def test_final_duplicate_or_incomplete_ids_refuse(self):
        for rows in [
            [{'name': 'x', 'id': 1, 'state': 'uploaded'}] * 2,
            [{'name': 'x', 'id': 1, 'state': 'starter'}],
            [{'name': 'x', 'id': True, 'state': 'uploaded'}],
            [{'name': 'x', 'id': 1, 'state': 'uploaded'}, {'name': 'y', 'id': 1, 'state': 'uploaded'}],
        ]:
            with self.subTest(rows=rows), self.assertRaises(pub.Refusal):
                attach.managed_ids(rows, {'x', 'y'})

    def test_annotated_tag_is_peeled_with_a_bound(self):
        sha = 'c' * 40
        with mock.patch.object(self.api, 'call', side_effect=[
            {'ref': 'refs/tags/v0.1.0', 'object': {'type': 'tag', 'sha': sha}},
            {'sha': sha, 'object': {'type': 'commit', 'sha': S}},
        ]):
            self.assertEqual(attach.tag_commit(self.api, 'v0.1.0'), S)
        with mock.patch.object(self.api, 'call', return_value={
            'ref': 'refs/tags/v0.1.0', 'sha': sha, 'object': {'type': 'tag', 'sha': sha},
        }), self.assertRaises(pub.Refusal):
            attach.tag_commit(self.api, 'v0.1.0')

    def test_event_source_and_workflow_are_distinct_or_equal_without_forging_either(self):
        for definition in (W, S):
            path = self.root / ('event-' + definition + '.json')
            path.write_text(json.dumps({'action': 'published', 'repository': {'full_name': 'leogr/airc'},
                                        'release': {'id': 17, 'tag_name': 'v0.1.0', 'draft': False}}))
            env = {'GITHUB_EVENT_NAME': 'release', 'GITHUB_EVENT_PATH': str(path),
                   'GITHUB_REPOSITORY': 'leogr/airc', 'GITHUB_SHA': S, 'SOURCE_SUBJECT': S,
                   'BUILD_DEFINITION': definition, 'GITHUB_WORKFLOW_SHA': definition,
                   'GITHUB_REF': 'refs/tags/v0.1.0', 'GITHUB_WORKFLOW_REF': self.event['workflow']}
            actual = attach.native.release_event(env)
            self.assertEqual(actual['source'], S)
            self.assertEqual(actual['definition'], definition)
            for key, value in [('SOURCE_SUBJECT', W), ('GITHUB_WORKFLOW_SHA', 'f' * 40),
                               ('GITHUB_REF', 'refs/heads/release-ci'), ('GITHUB_REPOSITORY', 'other/airc')]:
                with self.subTest(key=key), self.assertRaises(RuntimeError):
                    attach.native.release_event({**env, key: value})

    def test_outside_ci_and_unbounded_run_identity_start_no_local_or_remote_effect(self):
        env = {'RELEASE_MODE': 'attach', 'GH_TOKEN': 'fixture-token', 'RUNNER_TEMP': str(self.root)}
        for run in ('1/../outside', '', '0', '1' * 21):
            with self.subTest(run=run), self.assertRaises(pub.Refusal):
                attach.main([S, 'v0.1.0', 'inputs', 'source'], {**env, 'GITHUB_RUN_ID': run})
        self.assertEqual(set(p.name for p in self.root.iterdir()), set(self.assets))

    def test_malformed_original_event_refuses_before_attachment(self):
        path = self.root / 'event.json'
        env = {'GITHUB_EVENT_NAME': 'release', 'GITHUB_REPOSITORY': 'leogr/airc',
               'GITHUB_EVENT_PATH': str(path)}
        for raw in ('[]', '{"release":[],"repository":{}}',
                    '{"action":"published","action":"published"}',
                    '{"action":"published","repository":{"full_name":"leogr/airc"},'
                    '"release":{"id":17,"draft":false,"tag_name":7}}'):
            path.write_text(raw)
            with self.subTest(raw=raw), self.assertRaises(RuntimeError):
                attach.native.release_event(env)
        self.assertEqual(self.api.posts, [])

    def test_actual_run_uses_source_head_and_requires_owner_and_both_successful_jobs(self):
        env = {'GITHUB_ACTIONS': 'true', 'GITHUB_REPOSITORY': 'leogr/airc',
               'GITHUB_EVENT_NAME': 'release', 'GITHUB_RUN_ATTEMPT': '1',
               'GITHUB_JOB': 'attach', 'GITHUB_RUN_ID': '7'}
        run = {'repository': {'full_name': 'leogr/airc', 'owner': {'login': 'leogr', 'id': 42}},
               'actor': {'id': 42}, 'triggering_actor': {'id': 42}, 'event': 'release',
               'head_sha': S, 'run_attempt': 1, 'status': 'in_progress',
               'path': '.github/workflows/native-release.yml'}
        jobs = [{'name': 'build (' + arch + ')', 'status': 'completed', 'conclusion': 'success',
                 'run_id': 7, 'head_sha': S} for arch in ('amd64', 'arm64')]
        jobs.append({'name': 'attach', 'status': 'in_progress'})
        real_call, real_list = self.api.call, self.api.list

        def check(current_run, current_jobs, corrupt_code=False):
            def call(endpoint, *args, **kwargs):
                if endpoint.endswith('/actions/runs/7'):
                    return current_run
                return real_call(endpoint, *args, **kwargs)

            def listing(endpoint, *args):
                if endpoint.endswith('/attempts/1/jobs'):
                    return current_jobs
                return real_list(endpoint, *args)

            def content(api, name, commit):
                self.assertEqual(commit, W)
                if corrupt_code:
                    return b'foreign W bytes'
                return (Path(__file__).parent.parent / name).read_bytes()

            with mock.patch.object(self.api, 'call', side_effect=call), \
                 mock.patch.object(self.api, 'list', side_effect=listing), \
                 mock.patch.object(pub, 'api_content', side_effect=content):
                attach.actual_context(env, self.api, self.event)

        check(run, jobs)
        for control in ('workflow-instead-of-source-head', 'foreign-owner', 'fork', 'failed-arm', 'bad-W-code'):
            changed, changed_jobs = copy.deepcopy(run), copy.deepcopy(jobs)
            if control == 'workflow-instead-of-source-head':
                changed['head_sha'] = W
            elif control == 'foreign-owner':
                changed['actor']['id'] = 99
            elif control == 'fork':
                changed['repository']['full_name'] = 'foreign/airc'
            elif control == 'failed-arm':
                changed_jobs[1]['conclusion'] = 'failure'
            with self.subTest(control=control), self.assertRaises(pub.Refusal):
                check(changed, changed_jobs, control == 'bad-W-code')
        self.assertEqual(self.api.posts, [])

    def test_event_manifest_can_bind_equal_W_S_but_manual_validator_cannot_adopt_it(self):
        policy = dict(pub.REPOS['leogr/airc'])
        policy.update(source=S, tree='c' * 40, archive=pub.sha(b'fixture archive'), lock='d' * 64)
        definition = {'commit': S, 'repository': 'leogr/airc', 'workflow': self.event['workflow'],
                      'run_id': '7', 'run_attempt': '1'}
        manifest = {'schema': 1, 'repository': 'leogr/airc', 'architecture': 'amd64',
                    'source_subject': {'commit': S, 'tree': policy['tree'],
                                       'archive_sha256': policy['archive'], 'lock_sha256': policy['lock']},
                    'workflow_definition': definition, 'build_proof': 'build-proof-amd64.json',
                    'artifacts': {'airc-linux-amd64.tar.gz': pub.sha(b'fixture archive')}}
        files = mock.Mock()
        files.json.return_value = manifest
        files.read.return_value = b'fixture archive'
        pub.common_manifest(files, 'leogr/airc', 'amd64', definition,
                            source_policy=policy, event_workflow=definition)
        with self.assertRaises(pub.Refusal):
            pub.common_manifest(files, 'leogr/airc', 'amd64')
        wrong = {**definition, 'commit': W}
        with self.assertRaises(pub.Refusal):
            pub.common_manifest(files, 'leogr/airc', 'amd64', wrong,
                                source_policy=policy, event_workflow=wrong)

    def test_both_actual_producer_recipes_are_bound_to_independent_workflow_bytes(self):
        plan = {'matrix': {arch: {'files': mock.Mock()} for arch in pub.ARCHES}}
        for entry in plan['matrix'].values():
            entry['files'].read.side_effect = lambda name: (attach.HERE / Path(name).name).read_bytes()
        attach.bind_producer_recipe(plan)
        plan['matrix']['arm64']['files'].read.side_effect = lambda _: b'foreign self-described recipe'
        with self.assertRaises(pub.Refusal):
            attach.bind_producer_recipe(plan)
        self.assertEqual(self.api.posts, [])

    def test_workflow_retains_dispatch_and_published_event_without_create_loop(self):
        text = (Path(__file__).parent.parent / '.github/workflows/native-release.yml').read_text()
        self.assertIn('types: [published]', text)
        self.assertIn('workflow_dispatch:', text)
        self.assertIn('github.workflow_sha', text)
        self.assertIn("github.event_name == 'workflow_dispatch' && inputs.publish", text)
        self.assertIn('ci/attach-release.py', text)
        # Uploading assets does not publish/edit another release. Manual
        # creation uses GITHUB_TOKEN; GitHub suppresses its release trigger.
        self.assertNotIn('gh release create', text)

class RefusalReportTests(unittest.TestCase):
    """A refused attachment states its controlled reason, never only a fixed line."""

    def run_script(self, args, env):
        import subprocess
        import sys
        return subprocess.run([sys.executable, str(Path(__file__).with_name('attach-release.py')), *args],
                              env=env, capture_output=True, text=True, timeout=60)

    def test_refusal_prints_its_reason(self):
        result = self.run_script(['only-one-argument'], {'PATH': os.environ.get('PATH', '')})
        self.assertEqual(result.returncode, 1)
        self.assertIn('AIRC release attachment refused', result.stderr)
        report = json.loads(result.stderr.strip().splitlines()[-1])
        self.assertEqual(report['scope'], 'release attachment')
        self.assertEqual(report['status'], 'BLOCKED')
        self.assertIn('usage: attach-release.py', report['reason'])
        self.assertEqual(report['automatic_retry'], False)

    def test_non_refusal_errors_report_only_their_type(self):
        with tempfile.TemporaryDirectory() as temporary:
            env = {'PATH': os.environ.get('PATH', ''), 'RELEASE_MODE': 'attach', 'GH_TOKEN': 'secret-token-value',
                   'RUNNER_TEMP': temporary, 'GITHUB_ACTIONS': 'true', 'GITHUB_RUN_ATTEMPT': '1',
                   'GITHUB_JOB': 'attach', 'GITHUB_RUN_ID': '1', 'GITHUB_EVENT_PATH': str(Path(temporary) / 'absent.json')}
            result = self.run_script(['a', 'b', 'c', 'd'], env)
        self.assertEqual(result.returncode, 1)
        self.assertNotIn('secret-token-value', result.stderr)
        report = json.loads(result.stderr.strip().splitlines()[-1])
        self.assertEqual(report['status'], 'BLOCKED')
        self.assertTrue(report['reason'])

if __name__ == '__main__':
    unittest.main()

