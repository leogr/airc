"""Pure/mocked publication contracts. No gh/network/build/container is run."""
import copy
import hashlib
import importlib.util
import io
import json
import os
from pathlib import Path
from types import SimpleNamespace
import tempfile
import unittest
from unittest import mock


SPEC = importlib.util.spec_from_file_location('publisher', Path(__file__).with_name('publish-release.py'))
pub = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(pub)
REPO = 'leogr/airc'
POLICY = pub.REPOS[REPO]
W = 'a' * 40


def definition():
    return {'commit': W, 'repository': REPO, 'workflow': REPO + '/.github/workflows/release.yml@refs/heads/release-ci',
            'run_id': '7', 'run_attempt': '1'}


class MemoryFiles:
    def __init__(self):
        self.data, self.objects = {}, {}

    def read(self, path, expected=None, limit=pub.MAX_FILE):
        pub.need(path in self.data, 'referenced evidence missing: ' + path)
        value = self.data[path]
        pub.need(len(value) <= limit and (expected is None or pub.sha(value) == expected), 'fixture evidence mismatch')
        return value

    def json(self, path, expected=None):
        return copy.deepcopy(self.objects[path])


def airc_fixture():
    """Use final producer's object-shaped inspection and real referenced paths."""
    files = MemoryFiles()
    files.data.update({'evidence/recipe/release-native.py': b'reviewed-python',
                       'evidence/recipe/release-native.sh': b'reviewed-shell', 'source.tar.gz': b'source',
                       'image-index.raw.json': b'index', 'image-manifest.raw.json': b'manifest',
                       'image-config.raw.json': b'config'})
    recipe = pub.sha(b'reviewed-pythonreviewed-shell')
    image_id = 'sha256:' + 'b' * 64
    files.objects['image.json'] = {'Id': image_id, 'Os': 'linux', 'Architecture': 'amd64',
                                  'RepoDigests': ['docker.io/library/rust@' + POLICY['images']['amd64']]}
    archive = 'airc-linux-amd64.tar.gz'
    files.data[archive] = b'actual-paired-archive'
    proof = {'schema': 1, 'repository': REPO, 'source_commit': POLICY['source'], 'source_tree': POLICY['tree'],
             'source_archive_sha256': POLICY['archive'], 'cargo_lock_sha256': POLICY['lock'], 'architecture': 'amd64',
             'build_definition': {'commit': W, 'tree': 'c' * 40, 'recipe_sha256': recipe},
             'github_run_id': '7', 'github_run_attempt': '1', 'github_workflow_ref': definition()['workflow'],
             'producer_build_complete': True, 'publication_channel': 'candidate-prerelease', 'consumer_qualification': 'not_run',
             'runner': {'system': 'Linux', 'machine': 'x86_64'},
             'toolchain': {'image_index': POLICY['index'], 'image_digest': POLICY['images']['amd64'],
                           'image_config': image_id, 'target': 'x86_64-unknown-linux-musl'},
             'reproducible': True, 'commands': {}, 'builds': [], 'payloads': []}
    def command(key, argv, stdout_path, stdout):
        stderr_path = stdout_path.removesuffix('.stdout') + '.stderr'
        files.data[stdout_path], files.data[stderr_path] = stdout, b''
        proof['commands'][key] = {'argv': argv, 'exit_code': 0,
            'stdout': {'path': stdout_path, 'sha256': pub.sha(stdout)},
            'stderr': {'path': stderr_path, 'sha256': pub.sha(b'')}}
    command('host-system', ['uname', '-s'], 'evidence/host/system.stdout', b'Linux\n')
    command('host-machine', ['uname', '-m'], 'evidence/host/machine.stdout', b'x86_64\n')
    endpoint, output_path, docker_path = 'unix:///run/docker.sock', '/worker/release', '/var/lib/docker'
    bound = ['docker', '--host', endpoint]
    socket_format = '{"path":"%n","mode_hex":"%f","device":%d,"inode":%i,"uid":%u,"gid":%g}'
    socket = {'path': '/run/docker.sock', 'mode_hex': 'c1b0', 'device': 1, 'inode': 22, 'uid': 0, 'gid': 0}
    daemon = {'ID': 'native-local-daemon', 'OSType': 'linux', 'Architecture': 'x86_64',
              'NCPU': 2, 'MemTotal': 8 * 1024**3, 'DockerRootDir': docker_path, 'ServerVersion': 'pinned-daemon-version'}
    for label, argv, raw in (
        ('context-show', ['docker', 'context', 'show'], b'default\n'),
        ('context-inspect', ['docker', 'context', 'inspect', 'default'], pub.canonical([
            {'Name': 'default', 'Endpoints': {'docker': {'Host': endpoint}}}])),
        ('socket-stat', ['stat', '-Lc', socket_format, '/run/docker.sock'], pub.canonical(socket)),
        ('daemon-info', bound + ['info', '--format', '{' + ','.join('"' + key + '":{{json .' + key + '}}' for key in
            ('ID', 'OSType', 'Architecture', 'NCPU', 'MemTotal', 'DockerRootDir', 'ServerVersion')) + '}'], pub.canonical(daemon)),
        ('output-filesystem', ['stat', '-f', '-c', '%T', output_path], b'ext4\n'),
        ('docker-filesystem', ['stat', '-f', '-c', '%T', docker_path], b'ext4\n')):
        command('docker-' + label, argv, 'evidence/docker/' + label + '.stdout', raw)
    admission = {'schema': 1, 'selection': {'method': 'default-context', 'context': 'default',
        'endpoint': endpoint, 'selected_endpoint': endpoint, 'context_set': False, 'host_set': False, 'host_ignored': False},
        'daemon': {'id': daemon['ID'], 'os': 'linux', 'architecture': 'amd64', 'cpus': 2,
                   'mem_total': daemon['MemTotal'], 'reported_docker_root_dir': docker_path, 'docker_root_dir': docker_path},
        'storage': {name: {'path': path, 'filesystem': 'ext4', 'free_bytes': 24 * 1024**3}
                    for name, path in [('output', output_path), ('docker', docker_path)]},
        'socket': socket, 'bound_argv': bound, 'docker_env_scrubbed': True,
        'floors': {'admission_disk_gib': 20, 'before_pull_gib': 16, 'build_gib': 14, 'ram_total_gib': 4, 'ram_available_gib': 2},
        'monitoring': {'path': 'evidence/docker/resources.jsonl', 'paths': [output_path, docker_path], 'minimum_disk_gib': 14}}
    files.data['evidence/docker/admission.json'] = pub.canonical(admission)
    proof['docker_admission'] = {'path': 'evidence/docker/admission.json', 'sha256': pub.sha(files.data['evidence/docker/admission.json'])}
    files.data['evidence/docker/resources.jsonl'] = b''.join(pub.canonical({
        'disk_floor_gib': floor, 'disks': [{'path': path, 'free_bytes': 24 * 1024**3} for path in (output_path, docker_path)],
        'mem_total': 8 * 1024**3, 'mem_available': 4 * 1024**3}) for floor in (20, 16, 14, 14))
    for number in (1, 2):
        prefix, cid = 'build-' + str(number), str(number) * 64
        files.objects[prefix + '/create-result.json'] = {'container_id': cid, 'name': 'owned'}
        files.objects[prefix + '/create-intent.json'] = {'argv': bound + ['create', '--name', 'owned']}
        files.objects[prefix + '/start-intent.json'] = {'container': 'owned', 'action': 'start --attach', 'endpoint': endpoint, 'replay': False}
        files.objects[prefix + '/start-result.json'] = {'exit_code': 0}
        files.objects[prefix + '/inspect-completed.json'] = {'Id': cid, 'Image': image_id,
            'State': {'Status': 'exited', 'ExitCode': 0, 'OOMKilled': False}}
        files.data[prefix + '/build/rustc.txt'] = b'release: 1.99.0\nhost: x86_64-unknown-linux-musl\n'
        files.data[prefix + '/' + archive] = files.data[archive]
        proof['builds'].append({'iteration': number, 'archive_sha256': pub.sha(files.data[archive])})
        for label, argv in (('upstream-check', ['make', 'check']), ('build', ['cargo', 'build', '--locked', '--release', '--bins']),
                            ('release-tests', ['cargo', 'test', '--locked', '--release', '--all-targets'])):
            out = (b'\n'.join(('test ' + name + ' ... ok').encode() for name in pub.BEHAVIOR_TESTS)
                   + b'\ntest result: ok. 7 passed; 0 failed; 0 ignored\n') if label == 'release-tests' else b'actual command output\n'
            files.data[prefix + '/build/' + label + '.status'] = b'0\n'
            command(str(number) + '-' + label, argv, prefix + '/build/' + label + '.stdout', out)
        for binary in POLICY['bins']:
            for label in ('elf', 'version', 'help'):
                out = (binary + ' 0.1.0\n').encode() if label == 'version' else b'nonempty native evidence\n'
                command(str(number) + '-' + binary + '-' + label, ['public-binary', '--' + label],
                        prefix + '/' + binary + '.' + label + '.stdout', out)
    files.objects['build-proof-amd64.json'] = proof
    manifest = {'workflow_definition': definition(), 'build_proof': 'build-proof-amd64.json',
                'recipe': {'sha256': recipe}, 'artifacts': {archive: pub.sha(files.data[archive])}}
    return files, manifest, proof


class InputContracts(unittest.TestCase):
    def test_actual_airc_shape_and_paths_are_supported_without_consumer_cycle(self):
        files, manifest, _ = airc_fixture()
        def oci(raw, expected):
            if raw == b'manifest':
                return {'config': {'digest': files.objects['image.json']['Id']}}
            if raw == b'config':
                return {'architecture': 'amd64', 'os': 'linux'}
            return {'manifests': []}
        with mock.patch.object(pub, 'source_archive', return_value={'Cargo.toml': (b'[package]\nversion="0.1.0"\n', b'100644')}), mock.patch.object(pub, 'oci_raw', side_effect=oci), \
             mock.patch.object(pub, 'oci_binding'), mock.patch.object(pub, 'payload_archive', return_value={}):
            result = pub.validate_airc(files, manifest, 'amd64')
        self.assertTrue(result['producer_build_complete'])
        self.assertEqual(result['consumer_qualification'], 'not_run')

    def test_airc_list_inspection_missing_raw_receipt_or_failed_actual_build_blocks(self):
        for control in ('list', 'missing-log', 'nonzero', 'different-pair'):
            files, manifest, proof = airc_fixture()
            if control == 'list':
                files.objects['build-1/inspect-completed.json'] = [files.objects['build-1/inspect-completed.json']]
            elif control == 'missing-log':
                files.data.pop('build-1/build/build.stdout')
            elif control == 'nonzero':
                files.data['build-1/build/build.status'] = b'101\n'
            else:
                files.data['build-2/airc-linux-amd64.tar.gz'] = b'different actual pair'
            def oci(raw, expected):
                return ({'config': {'digest': files.objects['image.json']['Id']}} if raw == b'manifest' else
                        {'architecture': 'amd64', 'os': 'linux'} if raw == b'config' else {})
            with self.subTest(control=control), mock.patch.object(pub, 'source_archive', return_value={'Cargo.toml': (b'[package]\nversion="0.1.0"\n', b'100644')}), \
                 mock.patch.object(pub, 'oci_raw', side_effect=oci), mock.patch.object(pub, 'oci_binding'), \
                 mock.patch.object(pub, 'payload_archive', return_value={}):
                with self.assertRaises(pub.Refusal):
                    pub.validate_airc(files, manifest, 'amd64')

    def test_fixed_subject_tree_and_only_two_repositories(self):
        self.assertEqual(POLICY['tree'], '11b1a7aa80b6ce101ec849bb831a201f7ea1a0f0')
        for repo, source, tag in (('other/airc', POLICY['source'], 'candidate'),
                                  (REPO, W, 'candidate'), (REPO, POLICY['source'], 'latest'),
                                  (REPO, POLICY['source'], '../escape')):
            with self.subTest(repo=repo, tag=tag), self.assertRaises(pub.Refusal):
                pub.validate_inputs(repo, source, tag, '/never-read')

    def test_checksum_only_command_or_changed_evidence_is_not_success(self):
        files = MemoryFiles()
        files.data['actual.stdout'], files.data['actual.stderr'] = b'actual', b''
        record = {'argv': ['cargo', 'build'], 'exit_code': 101,
                  'stdout': {'path': 'actual.stdout', 'sha256': pub.sha(b'actual')},
                  'stderr': {'path': 'actual.stderr', 'sha256': pub.sha(b'')}}
        with self.assertRaises(pub.Refusal):
            pub.command_record(files, record)
        record['exit_code'] = 0
        record['stdout']['sha256'] = '0' * 64
        with self.assertRaises(pub.Refusal):
            pub.command_record(files, record)

    def test_files_refuse_workspace_alias_and_post_validation_changes(self):
        with tempfile.TemporaryDirectory(prefix='publisher-pure-data-') as temporary:
            root = Path(temporary)
            (root / 'public.log').write_bytes(b'original')
            files = pub.Files(root)
            self.assertEqual(files.read('public.log'), b'original')
            (root / 'public.log').write_bytes(b'changed')
            with self.assertRaises(pub.Refusal):
                files.read('public.log')
            (root / 'target').mkdir()
            (root / 'target/compiled').write_bytes(b'not a publication export')
            with self.assertRaises(pub.Refusal):
                pub.Files(root)

    def test_default_plan_has_no_api_or_write_authority_and_attempt_two_rejected(self):
        plan = {'repository': REPO, 'source': POLICY['source'], 'tree': POLICY['tree'], 'tag': 'candidate',
                'workflow_definition': definition(), 'producer_build_complete': True,
                'publication_channel': 'candidate-prerelease', 'consumer_qualification': 'NOT RUN'}
        factory = mock.Mock(side_effect=AssertionError('PLAN must never connect'))
        with mock.patch.object(pub, 'validate_inputs', return_value=plan), mock.patch('sys.stdout', new=io.StringIO()) as output:
            self.assertEqual(pub.main([POLICY['source'], 'candidate', '/public-inputs'], {'RELEASE_REPOSITORY': REPO}, factory), 0)
            self.assertEqual(json.loads(output.getvalue())['effects'], False)
        factory.assert_not_called()
        with mock.patch.object(pub, 'validate_inputs') as validate, self.assertRaises(pub.Refusal):
            pub.main(['subject', 'tag', 'inputs'], {'GITHUB_RUN_ATTEMPT': '2'})
        validate.assert_not_called()

    def test_workflow_requires_actual_literal_environment_not_boolean_or_pending_absence(self):
        good = b'jobs:\n  publish:\n    if: inputs.publish\n    environment: upstream-release\n    steps: []\n'
        pub.workflow_environment(good)
        for raw in (good.replace(b'upstream-release', b'${{ inputs.environment }}'), good.replace(b'    environment: upstream-release\n', b'')):
            with self.assertRaises(pub.Refusal):
                pub.workflow_environment(raw)


class NativeAdmissionControls(unittest.TestCase):
    def test_actual_local_context_daemon_socket_storage_and_monitors_join(self):
        files, _, proof = airc_fixture()
        admission = pub.validate_airc_admission(files, proof, 'amd64')
        self.assertEqual(admission['bound_argv'], ['docker', '--host', 'unix:///run/docker.sock'])

    def test_remote_context_wrong_daemon_low_docker_storage_or_missing_monitor_blocks(self):
        for control in ('remote-context', 'remote-env', 'wrong-daemon', 'low-docker', 'monitor-low', 'monitor-missing'):
            files, _, proof = airc_fixture()
            admission = json.loads(files.data['evidence/docker/admission.json'])
            if control == 'remote-context':
                admission['selection']['selected_endpoint'] = 'ssh://remote-worker'
            elif control == 'remote-env':
                admission['selection'].update(method='DOCKER_HOST', context=None, host_set=True,
                                              endpoint='tcp://remote:2375', selected_endpoint='tcp://remote:2375')
            elif control == 'wrong-daemon':
                path = 'evidence/docker/daemon-info.stdout'
                raw = json.loads(files.data[path])
                raw['Architecture'] = 'aarch64'
                files.data[path] = pub.canonical(raw)
                proof['commands']['docker-daemon-info']['stdout']['sha256'] = pub.sha(files.data[path])
            elif control == 'low-docker':
                admission['storage']['docker']['free_bytes'] = 2 * 1024**3
            elif control == 'monitor-low':
                rows = [json.loads(row) for row in files.data['evidence/docker/resources.jsonl'].splitlines()]
                rows[-1]['disks'][1]['free_bytes'] = 1024**3
                files.data['evidence/docker/resources.jsonl'] = b''.join(pub.canonical(row) for row in rows)
            else:
                files.data.pop('evidence/docker/resources.jsonl')
            files.data['evidence/docker/admission.json'] = pub.canonical(admission)
            proof['docker_admission']['sha256'] = pub.sha(files.data['evidence/docker/admission.json'])
            with self.subTest(control=control), self.assertRaises(pub.Refusal):
                pub.validate_airc_admission(files, proof, 'amd64')

    def test_context_overrides_ignored_host_only_with_raw_selected_context(self):
        files, _, proof = airc_fixture()
        admission = json.loads(files.data['evidence/docker/admission.json'])
        admission['selection'].update(method='DOCKER_CONTEXT', context_set=True, host_set=True, host_ignored=True)
        files.data['evidence/docker/admission.json'] = pub.canonical(admission)
        proof['docker_admission']['sha256'] = pub.sha(files.data['evidence/docker/admission.json'])
        pub.validate_airc_admission(files, proof, 'amd64')
        files.data.pop('evidence/docker/context-inspect.stdout')
        with self.assertRaises(pub.Refusal):
            pub.validate_airc_admission(files, proof, 'amd64')


class ReleaseAPI:
    def __init__(self, root, bad_download=False, existing=False, lose_create=False):
        self.root, self.repository, self.events = Path(root), REPO, []
        self.bad_download, self.existing, self.lose_create = bad_download, existing, lose_create
        self.created_ref, self.assets = False, {}

    def list(self, path, field=None):
        return ([{'id': row['id'], 'name': name} for name, row in self.assets.items()] if path.endswith('/assets') else [])

    def call(self, path, method='GET', payload=None, upload=None, download=None, expected=(200,), allow404=False, acknowledgement=None):
        value = self.response(path, method, payload, upload, download, expected, allow404)
        if method != 'GET':
            try:
                acknowledgement(value)
            except (pub.Refusal, TypeError, KeyError, AttributeError):
                raise pub.Uncertain('mock transport succeeded but exact ACK unproved') from None
        return value

    def response(self, path, method='GET', payload=None, upload=None, download=None, expected=(200,), allow404=False):
        self.events.append((method, path))
        if download is not None:
            identifier = int(path.rsplit('/', 1)[1])
            row = next(value for value in self.assets.values() if value['id'] == identifier)
            Path(download).write_bytes(b'wrong bytes' if self.bad_download else row['data'])
            return {'size': Path(download).stat().st_size}
        if '/git/ref/tags/' in path:
            return {'object': {'sha': POLICY['source'], 'type': 'commit'}} if self.created_ref or self.existing else None
        if path.endswith('/git/refs'):
            self.created_ref = True
            return {'ref': 'refs/tags/candidate', 'object': {'sha': POLICY['source'], 'type': 'commit'}}
        if path.startswith('https://uploads.github.com/'):
            name = path.split('?name=', 1)[1]
            data = Path(upload).read_bytes()
            self.assets[name] = {'id': len(self.assets) + 10, 'data': data}
            return {'id': self.assets[name]['id'], 'name': name, 'state': 'uploaded', 'size': len(data), 'digest': 'sha256:' + pub.sha(data)}
        if method == 'PATCH':
            return {'id': 1, 'tag_name': 'candidate', 'draft': False, 'prerelease': True, 'published_at': '2026-01-01T00:00:00Z'}
        if method == 'POST' and path.endswith('/releases'):
            if self.lose_create:
                raise pub.Uncertain('reply lost')
            return {'id': 1, 'tag_name': 'candidate', 'target_commitish': POLICY['source'], 'draft': True, 'prerelease': True}
        raise AssertionError('unexpected fake request')


class PublicationSequencing(unittest.TestCase):
    def plan(self):
        return {'repository': REPO, 'tag': 'candidate', 'source': POLICY['source']}

    def test_new_tag_draft_exact_download_verified_asset_then_candidate_publish(self):
        with tempfile.TemporaryDirectory(prefix='publisher-api-mock-') as temporary:
            path = Path(temporary) / 'public.tar.gz'
            path.write_bytes(b'public verified bytes')
            api = ReleaseAPI(temporary)
            result = pub.new_release(self.plan(), {'public.tar.gz': path}, api)
            self.assertEqual(result['channel'], 'candidate-prerelease')
            self.assertEqual(result['consumer_qualification'], 'NOT RUN')
            self.assertEqual(api.events[-1][0], 'PATCH')
            self.assertIn('/releases/assets/', api.events[-3][1])

    def test_download_mismatch_existing_tag_or_lost_draft_reply_never_publish_or_replay(self):
        for control in ('bad-download', 'existing', 'lost'):
            with self.subTest(control=control), tempfile.TemporaryDirectory(prefix='publisher-api-mock-') as temporary:
                path = Path(temporary) / 'public.tar.gz'
                path.write_bytes(b'public bytes')
                api = ReleaseAPI(temporary, control == 'bad-download', control == 'existing', control == 'lost')
                with self.assertRaises(pub.Refusal):
                    pub.new_release(self.plan(), {'public.tar.gz': path}, api)
                self.assertFalse(any(method == 'PATCH' for method, _ in api.events))
                self.assertLessEqual(sum(method == 'POST' and path.endswith('/releases') for method, path in api.events), 1)

    def test_intent_is_one_use_fsynced_file_and_never_overwritten(self):
        with tempfile.TemporaryDirectory(prefix='publisher-intent-pure-') as temporary:
            path = Path(temporary) / 'intent.json'
            pub.fsync_json(path, {'result_known': False, 'automatic_retry': False})
            with self.assertRaises(FileExistsError):
                pub.fsync_json(path, {'result_known': True})
            self.assertFalse(json.loads(path.read_text())['result_known'])

    def test_private_token_is_env_only_and_intent_precedes_even_failed_client_start(self):
        with tempfile.TemporaryDirectory(prefix='publisher-transport-mock-') as temporary:
            token = 'PRIVATE_TOKEN_SENTINEL_DO_NOT_LOG'
            with mock.patch.object(pub.shutil, 'which', return_value='/usr/bin/gh'):
                api = pub.GitHub(REPO, {'GH_TOKEN': token, 'PATH': '/usr/bin', 'GH_DEBUG': 'api'}, Path(temporary))
            def start(argv, **kwargs):
                intent = Path(temporary) / 'operation-001.intent.json'
                self.assertTrue(intent.is_file())
                self.assertFalse(json.loads(intent.read_text())['result_known'])
                self.assertNotIn(token, json.dumps(argv))
                self.assertEqual(kwargs['env']['GH_TOKEN'], token)
                self.assertNotIn('GH_DEBUG', kwargs['env'])
                raise OSError('mock client could not start')
            with mock.patch.object(pub.subprocess, 'Popen', side_effect=start) as launch:
                with self.assertRaises(pub.Uncertain):
                    api.call('repos/' + REPO + '/git/refs', 'POST', {'ref': 'refs/tags/new', 'sha': POLICY['source']}, expected=(201,), acknowledgement=lambda _: None)
            self.assertEqual(launch.call_count, 1)
            self.assertFalse((Path(temporary) / 'operation-001.complete.json').exists())
            self.assertNotIn(token, (Path(temporary) / 'operation-001.intent.json').read_text())

    def test_unknown_or_foreign_github_endpoint_never_starts_network_client(self):
        with tempfile.TemporaryDirectory(prefix='publisher-transport-mock-') as temporary:
            with mock.patch.object(pub.shutil, 'which', return_value='/usr/bin/gh'):
                api = pub.GitHub(REPO, {'GH_TOKEN': 'private', 'PATH': '/usr/bin'}, Path(temporary))
            with mock.patch.object(pub.subprocess, 'Popen') as launch, self.assertRaises(pub.Refusal):
                api.call('https://untrusted.invalid/release', 'POST', {})
            launch.assert_not_called()


class MutationAcknowledgementControls(unittest.TestCase):
    def test_each_http_success_with_missing_exact_ack_is_uncertain_and_stops_suffix(self):
        for stage in ('tag', 'draft', 'upload', 'publish'):
            with self.subTest(stage=stage), tempfile.TemporaryDirectory(prefix='publisher-ack-mock-') as temporary:
                class MalformedAPI(ReleaseAPI):
                    def response(self, path, method='GET', payload=None, upload=None, download=None, expected=(200,), allow404=False):
                        value = super().response(path, method, payload, upload, download, expected, allow404)
                        actual = ('tag' if path.endswith('/git/refs') else
                                  'draft' if method == 'POST' and path.endswith('/releases') else
                                  'upload' if path.startswith('https://uploads.github.com/') else
                                  'publish' if method == 'PATCH' else None)
                        return {} if actual == stage else value
                path = Path(temporary) / 'public.tar.gz'
                path.write_bytes(b'public bytes')
                api = MalformedAPI(temporary)
                with self.assertRaises(pub.Uncertain) as raised:
                    pub.new_release({'repository': REPO, 'tag': 'candidate', 'source': POLICY['source']}, {'public.tar.gz': path}, api)
                self.assertEqual(len(raised.exception.known_effects), {'tag': 0, 'draft': 1, 'upload': 2, 'publish': 3}[stage])
                self.assertEqual(api.events[-1][0], 'PATCH' if stage == 'publish' else 'POST')
                self.assertEqual(sum(method == 'PATCH' for method, _ in api.events), int(stage == 'publish'))
                self.assertEqual(sum(method == 'POST' and endpoint.endswith('/git/refs') for method, endpoint in api.events), 1)

    def test_post_upload_read_failure_retains_known_upload_as_partial_not_uncertain(self):
        with tempfile.TemporaryDirectory(prefix='publisher-partial-mock-') as temporary:
            class FailedDownload(ReleaseAPI):
                def response(self, path, method='GET', payload=None, upload=None, download=None, expected=(200,), allow404=False):
                    if download is not None:
                        self.events.append((method, path))
                        raise pub.Refusal('mock read unavailable')
                    return super().response(path, method, payload, upload, download, expected, allow404)
            path = Path(temporary) / 'public.tar.gz'
            path.write_bytes(b'public bytes')
            api = FailedDownload(temporary)
            with self.assertRaises(pub.PartialPublication) as raised:
                pub.new_release({'repository': REPO, 'tag': 'candidate', 'source': POLICY['source']}, {'public.tar.gz': path}, api)
            self.assertNotIsInstance(raised.exception, pub.Uncertain)
            self.assertEqual([effect['kind'] for effect in raised.exception.known_effects], ['new-tag', 'new-draft', 'uploaded-asset'])
            self.assertNotIn('no publication effect authorized', str(raised.exception))
            self.assertFalse(any(method == 'PATCH' for method, _ in api.events))

    def test_real_transport_receipt_is_not_semantic_completion_without_exact_ack(self):
        # Only OS pipes feed a mocked Popen. No executable or network is started.
        class Process:
            def __init__(self, raw):
                self.stdout = self.pipe(raw)
                self.stderr = self.pipe(b'')
            @staticmethod
            def pipe(raw):
                read_fd, write_fd = os.pipe()
                os.write(write_fd, raw)
                os.close(write_fd)
                return os.fdopen(read_fd, 'rb', buffering=0)
            def poll(self):
                return 0
        for body in (b'{}', b'{malformed-json'):
            with self.subTest(body=body), tempfile.TemporaryDirectory(prefix='publisher-transport-ack-') as temporary:
                with mock.patch.object(pub.shutil, 'which', return_value='/usr/bin/gh'):
                    api = pub.GitHub(REPO, {'GH_TOKEN': 'private-token', 'PATH': '/usr/bin'}, temporary)
                def exact_ack(value):
                    pub.need(value.get('ref') == 'refs/tags/candidate', 'exact tag ACK absent')
                process = Process(b'HTTP/2.0 201 Created\r\ncontent-type: application/json\r\n\r\n' + body)
                with mock.patch.object(pub.subprocess, 'Popen', return_value=process) as launch:
                    with self.assertRaises(pub.Uncertain):
                        api.call('repos/' + REPO + '/git/refs', 'POST', {'ref': 'refs/tags/candidate', 'sha': POLICY['source']},
                                 expected=(201,), acknowledgement=exact_ack)
                self.assertEqual(launch.call_count, 1)
                root = Path(temporary)
                self.assertFalse(json.loads((root / 'operation-001.intent.json').read_text())['result_known'])
                self.assertFalse((root / 'operation-001.complete.json').exists())
                self.assertEqual(api.known_effects, [])
                transport = json.loads((root / 'operation-001.transport.json').read_text())
                self.assertFalse(transport['result_known'])
                self.assertEqual(transport['receipt_kind'], 'http-transport-only')

    def test_real_http_201_and_200_semantic_failures_never_complete_the_failed_effect(self):
        class Process:
            def __init__(self, raw):
                streams = []
                for value in (raw, b''):
                    r, w = os.pipe()
                    os.write(w, value)
                    os.close(w)
                    streams.append(os.fdopen(r, 'rb', buffering=0))
                self.stdout, self.stderr = streams
            def poll(self):
                return 0
        for stage in ('tag', 'draft', 'upload', 'publish'):
            with self.subTest(stage=stage), tempfile.TemporaryDirectory(prefix='publisher-http-sequence-') as temporary:
                root, data = Path(temporary), b'public bytes'
                path = root / 'asset.tar.gz'
                path.write_bytes(data)
                with mock.patch.object(pub.shutil, 'which', return_value='/usr/bin/gh'):
                    api = pub.GitHub(REPO, {'GH_TOKEN': 'private-token', 'PATH': '/usr/bin'}, root)
                ref = {'ref': 'refs/tags/candidate', 'object': {'sha': POLICY['source'], 'type': 'commit'}}
                draft = {'id': 1, 'tag_name': 'candidate', 'target_commitish': POLICY['source'], 'draft': True, 'prerelease': True}
                asset = {'id': 9, 'name': path.name, 'state': 'uploaded', 'size': len(data), 'digest': 'sha256:' + pub.sha(data)}
                calls = []
                def start(argv, **kwargs):
                    endpoint, method = argv[-1], argv[argv.index('--method') + 1]
                    calls.append((method, endpoint))
                    if method == 'GET' and '/git/ref/tags/' in endpoint:
                        status, value = (200, ref) if any(m == 'POST' and e.endswith('/git/refs') for m, e in calls) else (404, {})
                    elif method == 'GET' and '/releases/assets/' in endpoint:
                        return Process(data)
                    elif method == 'GET' and '/releases/1/assets?' in endpoint:
                        status, value = 200, [asset]
                    elif method == 'GET':
                        status, value = 200, []
                    else:
                        actual = ('tag' if endpoint.endswith('/git/refs') else 'upload' if endpoint.startswith('https://uploads.github.com/')
                                  else 'publish' if method == 'PATCH' else 'draft')
                        status = 200 if method == 'PATCH' else 201
                        value = {} if actual == stage else {'tag': ref, 'draft': draft, 'upload': asset}[actual]
                    return Process(('HTTP/2.0 ' + str(status) + ' mock\r\n\r\n').encode() + json.dumps(value).encode())
                with mock.patch.object(pub.subprocess, 'Popen', side_effect=start):
                    with self.assertRaises(pub.Uncertain) as raised:
                        pub.new_release({'repository': REPO, 'tag': 'candidate', 'source': POLICY['source']}, {path.name: path}, api)
                completes = sorted(root.glob('operation-*.complete.json'))
                intents = sorted(root.glob('operation-*.intent.json'))
                self.assertEqual(len(completes), {'tag': 0, 'draft': 1, 'upload': 2, 'publish': 3}[stage])
                self.assertEqual(len(intents), len(completes) + 1)
                self.assertEqual(len(raised.exception.known_effects), len(completes))
                failed = intents[-1].with_suffix('').with_suffix('.transport.json')
                self.assertFalse(json.loads(failed.read_text())['result_known'])
                self.assertTrue(all(json.loads(p.read_text())['result_known'] for p in completes))
                self.assertEqual(sum(method == 'PATCH' for method, _ in calls), int(stage == 'publish'))

    def test_known_transport_ack_survives_later_read_failure(self):
        class Process:
            def __init__(self, raw):
                r, w = os.pipe()
                os.write(w, raw)
                os.close(w)
                self.stdout = os.fdopen(r, 'rb', buffering=0)
                r, w = os.pipe()
                os.close(w)
                self.stderr = os.fdopen(r, 'rb', buffering=0)
            def poll(self):
                return 0
        with tempfile.TemporaryDirectory(prefix='publisher-known-ack-') as temporary:
            with mock.patch.object(pub.shutil, 'which', return_value='/usr/bin/gh'):
                api = pub.GitHub(REPO, {'GH_TOKEN': 'private-token', 'PATH': '/usr/bin'}, temporary)
            ack = {'ref': 'refs/tags/candidate'}
            process = Process(b'HTTP/2.0 201 Created\r\n\r\n' + json.dumps(ack).encode())
            with mock.patch.object(pub.subprocess, 'Popen', return_value=process):
                api.call('repos/' + REPO + '/git/refs', 'POST', ack, expected=(201,),
                         acknowledgement=lambda value: pub.need(value == ack, 'ACK mismatch'))
            with mock.patch.object(pub.subprocess, 'Popen', side_effect=OSError('mock read startup failure')):
                with self.assertRaises(pub.PartialPublication) as raised:
                    api.call('repos/' + REPO + '/releases/assets/5', download=Path(temporary) / 'download')
            self.assertNotIsInstance(raised.exception, pub.Uncertain)
            complete = json.loads((Path(temporary) / 'operation-001.complete.json').read_text())
            self.assertTrue(complete['result_known'])
            self.assertEqual(complete['receipt_kind'], 'exact-operation-acknowledgement')
            self.assertEqual(len(raised.exception.known_effects), 1)


class MainPostPublicationReportingControls(unittest.TestCase):
    def check_failure(self, phase):
        # Exercise real main/new_release/transport/ACK receipts with mocked gh
        # pipes. Only admission/input setup is mocked; no network client runs.
        class Process:
            def __init__(self, raw):
                streams = []
                for value in (raw, b''):
                    read_fd, write_fd = os.pipe()
                    os.write(write_fd, value)
                    os.close(write_fd)
                    streams.append(os.fdopen(read_fd, 'rb', buffering=0))
                self.stdout, self.stderr = streams
            def poll(self):
                return 0
        with tempfile.TemporaryDirectory(prefix='publisher-main-report-') as temporary:
            parent = Path(temporary)
            root = parent / 'publication-airc-7'
            path, data = parent / 'asset.tar.gz', b'public release bytes'
            path.write_bytes(data)
            plan = {'repository': REPO, 'source': POLICY['source'], 'tree': POLICY['tree'], 'tag': 'candidate',
                    'workflow_definition': definition(), 'producer_build_complete': True,
                    'publication_channel': 'candidate-prerelease', 'consumer_qualification': 'NOT RUN'}
            env = {'RELEASE_REPOSITORY': REPO, 'RELEASE_MODE': 'apply', 'RUNNER_TEMP': temporary,
                   'GH_TOKEN': 'private-token', 'PATH': '/usr/bin'}
            ref = {'ref': 'refs/tags/candidate', 'object': {'sha': POLICY['source'], 'type': 'commit'}}
            draft = {'id': 1, 'tag_name': 'candidate', 'target_commitish': POLICY['source'], 'draft': True, 'prerelease': True}
            asset = {'id': 9, 'name': path.name, 'state': 'uploaded', 'size': len(data), 'digest': 'sha256:' + pub.sha(data)}
            calls = []
            def start(argv, **kwargs):
                endpoint, method = argv[-1], argv[argv.index('--method') + 1]
                calls.append((method, endpoint))
                if method == 'GET' and '/git/ref/tags/' in endpoint:
                    status, value = (200, ref) if any(m == 'POST' and e.endswith('/git/refs') for m, e in calls) else (404, {})
                elif method == 'GET' and '/releases/assets/' in endpoint:
                    return Process(data)
                elif method == 'GET' and '/releases/1/assets?' in endpoint:
                    status, value = 200, [asset]
                elif method == 'GET':
                    status, value = 200, []
                elif method == 'PATCH':
                    status, value = 200, {'id': 1, 'tag_name': 'candidate', 'draft': False,
                                          'prerelease': True, 'published_at': '2026-11-02T08:00:00Z'}
                else:
                    status = 201
                    value = ref if endpoint.endswith('/git/refs') else asset if endpoint.startswith('https://uploads.github.com/') else draft
                return Process(('HTTP/2.0 ' + str(status) + ' mock\r\n\r\n').encode() + json.dumps(value).encode())
            original_fsync = pub.fsync_json
            injected = []
            def fsync(path, value):
                is_summary = Path(path).name == 'publication-complete.json'
                is_final_ack = str(path).endswith('.complete.json') and value.get('method') == 'PATCH'
                if (phase == 'summary' and is_summary) or (phase == 'operation-ack' and is_final_ack):
                    injected.append(str(path))
                    raise OSError('private local detail must not be reported')
                return original_fsync(path, value)
            output = mock.Mock() if phase == 'stdout' else io.StringIO()
            if phase == 'stdout':
                output.write.side_effect = OSError('private stdout detail must not be reported')
            expected_error = pub.Uncertain if phase == 'operation-ack' else pub.KnownPublishedLocalReportingFailure
            with mock.patch.object(pub, 'validate_inputs', return_value=plan), \
                 mock.patch.object(pub, 'protected_context'), mock.patch.object(pub, 'same_run_archives'), \
                 mock.patch.object(pub, 'publication_assets', return_value={path.name: path}), \
                 mock.patch.object(pub.shutil, 'which', return_value='/usr/bin/gh'), \
                 mock.patch.object(pub.subprocess, 'Popen', side_effect=start), \
                 mock.patch.object(pub, 'fsync_json', side_effect=fsync), \
                 mock.patch.object(pub, 'new_release', wraps=pub.new_release) as release, \
                 mock.patch('sys.stdout', new=output):
                with self.assertRaises(expected_error) as raised:
                    pub.main([POLICY['source'], 'candidate', '/mocked-same-run-inputs'], env)
            self.assertEqual(release.call_count, 1)
            self.assertEqual(sum(method == 'PATCH' for method, _ in calls), 1)
            self.assertEqual(calls[-1], ('PATCH', 'repos/' + REPO + '/releases/1'))
            report = pub.error_report(raised.exception)
            self.assertNotIn('private local detail', json.dumps(report))
            self.assertNotIn('private stdout detail', json.dumps(report))
            final_intents = [p for p in root.glob('operation-*.intent.json') if json.loads(p.read_text())['method'] == 'PATCH']
            self.assertEqual(len(final_intents), 1)
            final_complete = final_intents[0].with_suffix('').with_suffix('.complete.json')
            if phase == 'operation-ack':
                self.assertEqual(report['status'], 'UNCERTAIN')
                self.assertNotIn('result_known', report)
                self.assertFalse(final_complete.exists())
                self.assertFalse((root / 'publication-complete.json').exists())
                self.assertEqual(len(injected), 1)
            else:
                self.assertEqual(report['status'], 'PUBLISHED_LOCAL_REPORTING_FAILURE')
                self.assertTrue(report['result_known'])
                self.assertFalse(report['local_reporting_complete'])
                self.assertEqual(report['publication']['release_id'], 1)
                self.assertEqual(report['publication']['tag'], 'candidate')
                self.assertEqual(report['publication']['assets'], {path.name: 9})
                self.assertEqual(report['receipt_directory'], str(root))
                final_ack = json.loads(final_complete.read_text())
                self.assertTrue(final_ack['result_known'])
                self.assertEqual(final_ack['receipt_kind'], 'exact-operation-acknowledgement')
                self.assertEqual(report['known_effects'][-1]['method'], 'PATCH')
                self.assertEqual(report['known_effects'][-1]['receipt'], final_complete.name)
                self.assertEqual(len(report['known_effects']), 4)
                self.assertFalse(report['automatic_retry'])
                self.assertEqual((root / 'publication-complete.json').exists(), phase == 'stdout')
                self.assertEqual(len(injected), int(phase == 'summary'))

    def test_main_aggregate_summary_fsync_failure_preserves_known_publication_and_original_ack(self):
        self.check_failure('summary')

    def test_main_stdout_failure_preserves_known_publication_and_original_ack(self):
        self.check_failure('stdout')

    def test_final_operation_ack_persistence_failure_remains_uncertain(self):
        self.check_failure('operation-ack')


if __name__ == '__main__':
    unittest.main()
