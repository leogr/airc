#!/usr/bin/env python3
"""Protected candidate release publisher. Default PLAN performs local reads only.

Usage: publish-release.sh SOURCE_SHA NEW_RELEASE_TAG SAME_RUN_INPUT_DIR
RELEASE_REPOSITORY is exactly leogr/OpenShell or leogr/airc. RELEASE_MODE=apply
is accepted only in the first protected owner-approved workflow_dispatch run.
CI definition W is distinct from the fixed Rust subject. This is publication
of candidate producer bytes, never yoloctl consumer/runtime qualification.

Every mutation consumes a durable local one-use intent before gh starts. Any
lost/failed result stops without retry/adoption/deletion. Cross-run uncertainty
requires owner inspection and a new explicit protected dispatch; absence of a
tag is never proof that an earlier request failed. Receipts must be retained
with an always-run artifact upload, including on runner/job failure.
"""
import ast
import base64
import gzip
import hashlib
import io
import json
import os
from pathlib import Path, PurePosixPath
import re
import selectors
import shutil
import stat
import struct
import subprocess
import sys
import tarfile
import time
import tomllib
import urllib.parse
import zipfile


ARCHES = ('amd64', 'arm64')
MAX_FILE = 256 * 1024**2
MAX_JSON = 4 * 1024**2
MAX_TOTAL = 8 * 1024**3
MAX_FILES = 12000
MAX_RELEASE_FILE = 2 * 1024**3
SHA = re.compile(r'[0-9a-f]{64}')
COMMIT = re.compile(r'[0-9a-f]{40}')
REPOS = {
    'leogr/OpenShell': {
        'slug': 'OpenShell', 'source': 'e8605009bed50a05136efe106a0c1e2f283ee771',
        'tree': '9112ab178200f6751290ac2c2a20150e31c8708b',
        'archive': 'de56446970c4ae421fd95dc5b32ae6a46a08cf8f71348d5fa30fb7f6afa9850a',
        'lock': '845a694d70974d6eb22f402952daa0ff35d2cbaa3bc25117c39230daa3855776',
        'rust': '1.95.0', 'index': 'sha256:064dfc925d68d1a63f4fd2871bd7dc6e6ea56692989a487185855d62885d90aa',
        'images': {'amd64': 'sha256:2805e96db5234c9cfaf7ecb50f488693dab84d28ad30b6290cc1b707a18bf775',
                   'arm64': 'sha256:7027e56e68dfd2c7bc66383bda559e1e1efb72301f55e4252925f6fa637d979d'},
        'bins': ('openshell-sandbox', 'openshell-supervisor'),
    },
    'leogr/airc': {
        'slug': 'airc', 'source': 'b76ba355863cc3eb51900a313d7647d9190eedfa',
        'tree': '11b1a7aa80b6ce101ec849bb831a201f7ea1a0f0',
        'archive': 'ed1df502a58411cd9574024ebcd1964c02d9a7fef14330b4f227c96957dcc21c',
        'lock': '3cb565afe685053c6f848ebde87138eaadc18c7be31458887d065eed782c3b05',
        'rust': '1.99.0', 'index': 'sha256:c3a5ad77ff2e5ec99fffaf62cb518e61a9dcbd9e1d7526d281a380db0be3bb3b',
        'images': {'amd64': 'sha256:e4693cd4f8e853454d5a576f6ef38166d0fa55161576d5883468a58267d1507c',
                   'arm64': 'sha256:ba511babd167c72f478edbfa2461f5499a7e9639e9793705e8b8090382731198'},
        'bins': ('airc', 'aircd', 'airc-codex'),
    },
}
INNER = '745b2ea7a524e7b11b309fab8ec0cb385d6ae72124dd4fd36ba62d9111a4f337'
FINGERPRINT = 'E7EAD79CBD60C07541CE0CDF5826A20627574B83'
REGO = 'f60349dcfe8f42ff6c6957fd860cbfc1aef536c6f22740025c6771745f637c38'
BEHAVIOR_TESTS = ('tls_requires_trust_and_checks_hostname', 'proxy_tls_requires_trust_and_checks_hostname',
    'history_survives_restart_and_reports_lost_references',
    'schema_one_migrates_without_losing_state_and_newer_versions_are_refused',
    'unix_mode_serves_irc_and_artifacts_without_tcp',
    'jsonl_v2_assembles_multiline_and_reports_correlated_receipts',
    'https_artifact_requires_trust_and_checks_hostname')


class Refusal(RuntimeError):
    pass


class Uncertain(Refusal):
    def __init__(self, reason, known_effects=()):
        super().__init__(reason)
        self.known_effects = list(known_effects)


class PartialPublication(Refusal):
    """Earlier exact ACKs remain known; later verification did not qualify publication."""
    def __init__(self, reason, known_effects):
        super().__init__(reason)
        self.known_effects = list(known_effects)


class KnownPublishedLocalReportingFailure(Refusal):
    """The final exact remote ACK is durable; only later local reporting failed."""
    def __init__(self, result, known_effects, receipt_directory, phase, failure_type):
        super().__init__('Publication acknowledged and known; local ' + phase + ' failed (' + failure_type
                         + '); retain original operation receipts; no replay')
        self.publication = dict(result)
        self.known_effects = list(known_effects)
        self.receipt_directory = str(receipt_directory)
        self.phase = phase


def need(ok, reason):
    if not ok:
        raise Refusal(reason)


def sha(raw):
    return hashlib.sha256(raw).hexdigest()


def canonical(value):
    return (json.dumps(value, sort_keys=True, separators=(',', ':'), allow_nan=False) + '\n').encode()


def decode(raw):
    need(isinstance(raw, bytes) and len(raw) <= MAX_JSON, 'bounded JSON evidence required')
    def pairs(items):
        result = {}
        for key, value in items:
            need(key not in result, 'duplicate JSON member')
            result[key] = value
        return result
    try:
        return json.loads(raw, object_pairs_hook=pairs,
                          parse_constant=lambda _: (_ for _ in ()).throw(ValueError('nonfinite')))
    except (ValueError, UnicodeError, RecursionError) as error:
        raise Refusal('invalid JSON evidence') from error


def relative(name):
    need(isinstance(name, str) and name and len(name.encode()) <= 4096 and '\\' not in name
         and not name.startswith('/') and str(PurePosixPath(name)) == name
         and '..' not in PurePosixPath(name).parts and not any(ord(c) < 32 for c in name), 'unsafe relative evidence path')
    return name


def read_regular(path, limit=MAX_FILE):
    """No-follow leaf and ancestor traversal; never read/list an ancestor."""
    path = Path(path).absolute()
    flags = getattr(os, 'O_PATH', os.O_RDONLY) | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC
    parent = os.open('/', flags)
    try:
        for part in path.parts[1:-1]:
            child = os.open(part, flags, dir_fd=parent)
            os.close(parent)
            parent = child
        fd = os.open(path.name, os.O_RDONLY | os.O_NONBLOCK | os.O_NOFOLLOW | os.O_CLOEXEC, dir_fd=parent)
        try:
            before = os.fstat(fd)
            need(stat.S_ISREG(before.st_mode) and before.st_nlink == 1 and 0 <= before.st_size <= limit,
                 'evidence is not a bounded regular single-link file')
            raw = bytearray()
            while len(raw) <= limit:
                block = os.read(fd, min(1024**2, limit + 1 - len(raw)))
                if not block:
                    break
                raw.extend(block)
            after = os.fstat(fd)
            identity = lambda s: (s.st_dev, s.st_ino, s.st_size, s.st_mtime_ns, s.st_ctime_ns)
            need(identity(before) == identity(after) and len(raw) == before.st_size, 'evidence changed while reading')
            return bytes(raw)
        finally:
            os.close(fd)
    finally:
        os.close(parent)


def digest_regular(path, limit=MAX_RELEASE_FILE):
    """Stream large owned downloads/evidence assets without a giant byte copy."""
    fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_CLOEXEC)
    try:
        before = os.fstat(fd)
        need(stat.S_ISREG(before.st_mode) and before.st_nlink == 1 and before.st_size <= limit, 'bounded regular release file required')
        digest, length = hashlib.sha256(), 0
        while True:
            block = os.read(fd, 1024**2)
            if not block:
                break
            length += len(block)
            need(length <= limit, 'release file size bound exceeded')
            digest.update(block)
        after = os.fstat(fd)
        identity = lambda s: (s.st_dev, s.st_ino, s.st_size, s.st_mtime_ns, s.st_ctime_ns)
        need(identity(before) == identity(after) and length == before.st_size, 'release file changed during hashing')
        return digest.hexdigest()
    finally:
        os.close(fd)


class Files:
    def __init__(self, root):
        self.root = Path(root).absolute()
        need(self.root.is_dir() and not any(p.is_symlink() for p in (self.root, *self.root.parents)), 'unaliased input directory required')
        self.inventory = {}
        total = 0
        for parent, dirs, files in os.walk(self.root, followlinks=False):
            for name in dirs:
                item = Path(parent) / name
                need(not item.is_symlink(), 'input directory symlink refused')
            for name in files:
                item = Path(parent) / name
                key = relative(str(item.relative_to(self.root)))
                need(not set(PurePosixPath(key).parts) & {'target', 'cargo', 'cache', 'source', '.git', 'public-keyring'},
                     'source/build/cache workspace exported instead of explicit publication closure')
                metadata = item.lstat()
                need(stat.S_ISREG(metadata.st_mode) and metadata.st_nlink == 1 and metadata.st_size <= MAX_FILE,
                     'nonregular/oversized publication input')
                total += metadata.st_size
                need(total <= MAX_TOTAL and len(self.inventory) < MAX_FILES, 'publication input budget exceeded')
                self.inventory[key] = {'size': metadata.st_size}
        need(self.inventory, 'empty publication input')

    def read(self, name, expected=None, limit=MAX_FILE):
        name = relative(name)
        need(name in self.inventory, 'referenced evidence missing: ' + name)
        raw = read_regular(self.root / name, limit)
        digest = sha(raw)
        need(self.inventory[name].get('sha256', digest) == digest, 'publication input changed after validation: ' + name)
        need(expected is None or isinstance(expected, str) and SHA.fullmatch(expected) and digest == expected,
             'evidence hash mismatch: ' + name)
        self.inventory[name].update(sha256=digest, size=len(raw))
        return raw

    def json(self, name, expected=None):
        return decode(self.read(name, expected, MAX_JSON))


def git_object(kind, raw):
    return hashlib.sha1(kind.encode() + b' ' + str(len(raw)).encode() + b'\0' + raw).digest()


def source_archive(raw, policy):
    need(sha(raw) == policy['archive'], 'fixed source archive mismatch')
    files, tree, roots, total = {}, {}, set(), 0
    with tarfile.open(fileobj=io.BytesIO(raw), mode='r:gz') as archive:
        for member in archive:
            name = relative(member.name.rstrip('/'))
            parts = PurePosixPath(name).parts
            roots.add(parts[0])
            if len(parts) == 1 or member.isdir():
                continue
            name = '/'.join(parts[1:])
            need(name not in files and (member.isfile() or member.issym()) and not member.issparse(), 'unsafe/duplicate source member')
            need(member.size <= 64 * 1024**2, 'source member exceeds bound')
            if member.issym():
                need(not member.linkname.startswith('/'), 'absolute source symlink')
                depth = len(parts) - 2
                for part in PurePosixPath(member.linkname).parts:
                    depth += -1 if part == '..' else 0 if part == '.' else 1
                    need(depth >= 0, 'escaping source symlink')
                data, mode = member.linkname.encode(), b'120000'
            else:
                data = archive.extractfile(member).read()
                mode = b'100755' if member.mode & 0o111 else b'100644'
            total += len(data)
            need(total <= 512 * 1024**2 and len(files) < MAX_FILES, 'source archive budget exceeded')
            files[name] = (data, mode)
            node = tree
            for part in parts[1:-1]:
                node = node.setdefault(part, {})
                need(isinstance(node, dict), 'conflicting source paths')
            need(parts[-1] not in node, 'conflicting source paths')
            node[parts[-1]] = (mode, git_object('blob', data))
    def digest(node):
        entries = []
        for name, child in node.items():
            directory = isinstance(child, dict)
            mode, obj = (b'40000', digest(child)) if directory else child
            entries.append((name.encode() + (b'/' if directory else b''), mode + b' ' + name.encode() + b'\0' + obj))
        return git_object('tree', b''.join(value for _, value in sorted(entries)))
    need(len(roots) == 1 and digest(tree).hex() == policy['tree'], 'source archive Git tree mismatch')
    need('Cargo.lock' in files and sha(files['Cargo.lock'][0]) == policy['lock'], 'source lock mismatch')
    return files


def static_elf(raw, arch):
    need(len(raw) >= 64 and raw[:6] == b'\x7fELF\x02\x01', 'ELF64 little-endian required')
    need(struct.unpack_from('<H', raw, 18)[0] == {'amd64': 62, 'arm64': 183}[arch], 'ELF architecture mismatch')
    offset = struct.unpack_from('<Q', raw, 32)[0]
    size, count = struct.unpack_from('<HH', raw, 54)
    need(size >= 56 and count > 0 and count <= 4096 and offset + size * count <= len(raw), 'invalid ELF program headers')
    for index in range(count):
        at = offset + index * size
        kind = struct.unpack_from('<I', raw, at)[0]
        need(kind != 3, 'ELF INTERP forbidden')
        if kind == 2:
            start, length = struct.unpack_from('<Q', raw, at + 8)[0], struct.unpack_from('<Q', raw, at + 32)[0]
            need(length % 16 == 0 and start + length <= len(raw), 'invalid ELF dynamic table')
            need(all(struct.unpack_from('<q', raw, pos)[0] != 1 for pos in range(start, start + length, 16)), 'ELF NEEDED forbidden')


def payload_archive(raw, arch, repo, source_files, policy=None):
    need(raw[:2] == b'\x1f\x8b' and len(raw) >= 10 and raw[4:8] == b'\0'*4, 'deterministic gzip mtime0 required')
    members, total = {}, 0
    with tarfile.open(fileobj=io.BytesIO(raw), mode='r:gz') as archive:
        for member in archive:
            name = relative(member.name)
            need(member.isfile() and not member.issparse() and name not in members and not member.pax_headers,
                 'release archive requires distinct regular members only')
            need(member.uid == member.gid == member.mtime == 0 and not member.uname and not member.gname,
                 'release tar identity/time metadata differs')
            need(member.size <= MAX_FILE and member.mode in (0o555, 0o644, 0o755), 'invalid payload mode/size')
            data = archive.extractfile(member).read()
            total += len(data)
            need(total <= 512 * 1024**2 and len(members) < MAX_FILES, 'release tar budget exceeded')
            members[name] = {'sha256': sha(data), 'size': len(data), 'mode': member.mode, 'data': data}
    bins = REPOS[repo]['bins']
    if repo == 'leogr/OpenShell':
        need(len(members) == 1 and next(iter(members)) in bins, 'OpenShell archive requires one named binary')
        for value in members.values():
            need(value['mode'] == 0o555, 'OpenShell binary mode must be0555')
            static_elf(value['data'], arch)
    else:
        expected = {name for name in source_files if name == 'LICENSE' or name.startswith(('skills/airc/', 'integrations/claude-plugin/'))}
        need('LICENSE' in expected and 'skills/airc/SKILL.md' in expected and any(x.startswith('integrations/claude-plugin/') for x in expected), 'source payload incomplete')
        need(set(members) == expected | {'bin/' + b for b in bins} | {'RELEASE-IDENTITY.json'}, 'AIRC archive payload inventory differs')
        for name in expected:
            data, mode = source_files[name]
            need(members[name]['data'] == data and members[name]['mode'] == (0o755 if mode == b'100755' else 0o644), 'source-linked skill/plugin/license payload differs')
        for binary in bins:
            need(members['bin/' + binary]['mode'] == 0o755, 'AIRC binary mode must be0755')
            static_elf(members['bin/' + binary]['data'], arch)
        identity = decode(members['RELEASE-IDENTITY.json']['data'])
        policy = REPOS[repo] if policy is None else policy
        need(identity == {'repository': repo, 'source_commit': policy['source'], 'source_tree': policy['tree'],
                          'cargo_lock_sha256': policy['lock'], 'architecture': arch, 'libc': 'static-musl'}, 'AIRC payload release identity mismatch')
    return {name: {k: value[k] for k in ('sha256', 'size', 'mode')} for name, value in members.items()}


def text(files, name):
    try:
        return files.read(name, limit=MAX_FILE).decode('utf-8')
    except UnicodeError as error:
        raise Refusal('text evidence is not UTF-8: ' + name) from error


def zero(files, name):
    need(files.read(name, limit=16) == b'0\n', 'actual numeric successful exit receipt missing: ' + name)


def raw_command(files, proof, name):
    keys = [key for key in proof['evidence_sha256'] if re.fullmatch(r'evidence/commands/[0-9]{3}-' + re.escape(name) + r'\.request\.json', key)]
    need(len(keys) == 1, 'exact actual command request/result absent: ' + name)
    prefix = keys[0][:-len('.request.json')]
    request, result = files.json(prefix + '.request.json'), files.json(prefix + '.result.json')
    need(isinstance(request.get('argv'), list) and request['argv'] and all(isinstance(x, str) for x in request['argv']), 'public command argv missing')
    need(result.get('result_known') is True and type(result.get('exit_code')) is int and result['exit_code'] == 0,
         'command failed/unknown: ' + name)
    return request['argv'], files.read(prefix + '.stdout'), files.read(prefix + '.stderr')


def oci_raw(raw, expected):
    for data in (raw, raw.removesuffix(b'\n')):
        if 'sha256:' + sha(data) == expected:
            return decode(data)
    raise Refusal('raw OCI bytes do not match pinned digest')


def oci_binding(index, manifest, image, policy, arch):
    matches = [m for m in index.get('manifests', []) if m.get('platform', {}).get('os') == 'linux'
               and m.get('platform', {}).get('architecture') == arch]
    need(len(matches) == 1 and matches[0]['digest'] == policy['images'][arch], 'OCI index/native image binding mismatch')
    need(image['Id'] == manifest['config']['digest'] and image['Os'] == 'linux' and image['Architecture'] == arch
         and any(x.endswith('@' + policy['images'][arch]) for x in image['RepoDigests']), 'actual inspected native image differs')


def common_manifest(files, repo, arch, workflow=None, *, source_policy=None, event_workflow=None):
    policy = REPOS[repo] if source_policy is None else source_policy
    manifest = files.json('CI-MANIFEST.json')
    need(manifest.get('schema') == 1 and manifest.get('repository') == repo and manifest.get('architecture') == arch, 'CI manifest identity mismatch')
    need(manifest.get('source_subject') == {'commit': policy['source'], 'tree': policy['tree'],
        'archive_sha256': policy['archive'], 'lock_sha256': policy['lock']}, 'actual Rust subject/tree/lock/archive mismatch')
    definition = manifest['workflow_definition']
    event = event_workflow is not None
    need(not event or repo == 'leogr/airc', 'release-event workflow is AIRC only')
    need(definition.get('repository') == repo and COMMIT.fullmatch(definition.get('commit', ''))
         and (event or definition['commit'] != policy['source']) and str(definition.get('run_attempt')) == '1'
         and re.fullmatch('[1-9][0-9]*', str(definition.get('run_id', '')))
         and (definition == event_workflow if event else
              re.fullmatch(re.escape(repo) + r'/\.github/workflows/[A-Za-z0-9_.-]+\.ya?ml@refs/heads/release-ci', definition.get('workflow', ''))),
         'workflow W/run/first-attempt/protected definition identity missing')
    need(workflow is None or definition == workflow, 'architecture outputs are not from exactly same run/workflow W')
    need(manifest.get('build_proof') == 'build-proof-' + arch + '.json', 'wrong architecture build proof')
    for name, expected in manifest['artifacts'].items():
        need('/' not in relative(name), 'release asset must have a plain name')
        files.read(name, expected)
    return manifest


def validate_openshell(files, manifest, arch):
    repo, policy = 'leogr/OpenShell', REPOS['leogr/OpenShell']
    proof = files.json(manifest['build_proof'])
    need(proof.get('schema') == 1 and proof.get('architecture') == arch and proof.get('source_commit') == policy['source']
         and proof.get('source_tree') == policy['tree'] and proof.get('recipe_inner_sha256') == INNER,
         'OpenShell build proof source/recipe mismatch')
    need(proof.get('native') is True and proof.get('reproducible') is True, 'native/reproduction claim absent')
    for name, expected in proof['evidence_sha256'].items():
        files.read(name, expected)
    source = source_archive(files.read('evidence/reviewed-inputs/source.tar.gz'), policy)
    source_proof = files.json('source-proof.json')
    need(all(source_proof.get(key) == value for key, value in {'schema': 1, 'commit': policy['source'], 'tree': policy['tree'],
        'archive_sha256': policy['archive'], 'lock_sha256': policy['lock'], 'signature_fingerprint': FINGERPRINT}.items()), 'OpenShell source proof identity mismatch')
    need(all(source_proof.get(key) is True for key in ('signature_verified', 'archive_tree_verified', 'source_checkout_verified')),
         'source proof declaration contradicts required source verification')
    _, _, signature = raw_command(files, proof, 'verify-source-signature')
    need(re.search(rb'\[GNUPG:\] VALIDSIG ' + FINGERPRINT.encode() + rb' ', signature), 'actual successful source signature verification absent')
    _, commit, _ = raw_command(files, proof, 'source-commit-object')
    need(git_object('commit', commit).hex() == policy['source'] and commit.startswith(('tree ' + policy['tree'] + '\n').encode()), 'signed source commit/tree bytes mismatch')
    worker = files.json('evidence/worker.json')
    need(worker['uname'][0] == 'Linux' and worker['uname'][4] == {'amd64': 'x86_64', 'arm64': 'aarch64'}[arch], 'actual native worker uname mismatch')
    recipe = manifest['recipe']
    need(sha(files.read('evidence/inner-release.sh')) == recipe['inner_sha256'] == INNER, 'reviewed inner recipe differs')
    files.read('evidence/recipe/release_native.py', recipe['outer_sha256'])
    files.read('evidence/recipe/release-native.sh', recipe['launcher_sha256'])
    files.read('evidence/reviewed-inputs/INPUTS.json', recipe['inputs_sha256'])
    _, raw_index, _ = raw_command(files, proof, 'registry-index')
    _, raw_manifest, _ = raw_command(files, proof, 'registry-selected-manifest')
    _, raw_image, _ = raw_command(files, proof, 'image-inspect')
    image = decode(raw_image)
    need(isinstance(image, list) and len(image) == 1, 'ambiguous local image inspection')
    oci_binding(oci_raw(raw_index, policy['index']), oci_raw(raw_manifest, policy['images'][arch]), image[0], policy, arch)
    need(proof['image']['index_digest'] == policy['index'] and proof['image']['selected_manifest_digest'] == policy['images'][arch]
         and proof['image']['config_digest'] == proof['image']['local_image_id'] == image[0]['Id'], 'build proof image identity differs')
    builds = proof['builds']
    need(len(builds) == 2 and [b.get('number') for b in builds] == [1, 2]
         and builds[0]['container_id'] != builds[1]['container_id'], 'two distinct clean build attempts required')
    expected_artifacts = {*(b + '-' + arch + '.tar.gz' for b in policy['bins']), 'sandbox-policy.rego', 'source-proof.json', manifest['build_proof']}
    need(set(manifest['artifacts']) == expected_artifacts, 'OpenShell publication asset inventory differs')
    for build in builds:
        number, prefix = build['number'], build['evidence']
        need(prefix == 'evidence/build-' + str(number) and build['container_exit'] == build['cleanup_exit'] == 0, 'build/cleanup success receipt missing')
        need(set(build['artifacts']) == {*policy['bins'], *(b + '-' + arch + '.tar.gz' for b in policy['bins']), 'sandbox-policy.rego'}, 'build pair inventory incomplete')
        for name, expected in build['artifacts'].items():
            files.read(prefix + '/' + name, expected)
        argv, _, _ = raw_command(files, proof, 'start-attach-' + str(number))
        need(argv == ['docker', 'start', '--attach', build['container_id']], 'actual build command/CID mismatch')
        _, completed, _ = raw_command(files, proof, 'inspect-completed-' + str(number))
        state = decode(completed)
        need(len(state) == 1 and state[0]['Id'] == build['container_id'] and state[0]['State']['Status'] == 'exited'
             and state[0]['State']['ExitCode'] == 0 and not state[0]['State'].get('OOMKilled'), 'actual completed container result absent')
        raw_command(files, proof, 'remove-owned-' + str(number))
        _, inventory, _ = raw_command(files, proof, 'inventory-after-remove-' + str(number))
        need(build['container_id'].encode() not in inventory.splitlines(), 'owned container removal not observed')
        need(files.read(prefix + '/build-complete.txt') == b'complete\n', 'inner build incomplete')
        rustc = text(files, prefix + '/rustc.txt')
        triple = {'amd64': 'x86_64', 'arm64': 'aarch64'}[arch] + '-unknown-linux-musl'
        need('release: 1.95.0\n' in rustc and 'host: ' + triple + '\n' in rustc, 'actual compiler/target mismatch')
        for tool in ('rustc', 'cargo', 'cc', 'cc-target', 'readelf-version', 'packages-unsorted'):
            zero(files, prefix + '/' + tool + '.txt.exit')
        for binary in policy['bins']:
            for suffix in ('build', 'elf-header.txt', 'elf-programs.txt', 'elf-dynamic.txt', 'version.txt', 'help.txt'):
                zero(files, prefix + '/' + binary + '-' + suffix + '.exit')
            need(text(files, prefix + '/' + binary + '-version.txt') == binary + ' 0.1.6\n', 'actual binary version differs')
            flag = '--unmediated-network' if binary.endswith('sandbox') else '--workspace-read-only'
            need(flag in text(files, prefix + '/' + binary + '-help.txt'), 'actual required CLI interface absent')
            archive_name = binary + '-' + arch + '.tar.gz'
            archive_raw = files.read(prefix + '/' + archive_name, build['artifacts'][archive_name])
            payload = payload_archive(archive_raw, arch, repo, source)
            need(set(payload) == {binary} and payload[binary]['sha256'] == sha(files.read(prefix + '/' + binary)), 'archive/binary pair differs')
            need(sha(archive_raw) == manifest['artifacts'][archive_name], 'published/reproduced archive differs')
        need(sha(files.read(prefix + '/sandbox-policy.rego')) == REGO == manifest['artifacts']['sandbox-policy.rego'], 'matching Rego missing')
    need(builds[0]['artifacts'] == builds[1]['artifacts'], 'actual two-build output hashes differ')
    return proof


def command_record(files, record):
    need(type(record.get('exit_code')) is int and record['exit_code'] == 0
         and isinstance(record.get('argv'), list) and record['argv'], 'actual command nonzero/unknown')
    return tuple(files.read(record[s]['path'], record[s]['sha256']) for s in ('stdout', 'stderr'))


def validate_airc_admission(files, proof, arch):
    """Validate producer-exported local/native Docker admission, never probe it."""
    descriptor = proof['docker_admission']
    need(descriptor['path'] == 'evidence/docker/admission.json', 'AIRC Docker admission path differs')
    admission = decode(files.read(descriptor['path'], descriptor['sha256'], MAX_JSON))
    need(admission.get('schema') == 1 and admission.get('docker_env_scrubbed') is True,
         'AIRC native Docker admission/scrubbed binding missing')
    selection, daemon, storage = admission['selection'], admission['daemon'], admission['storage']
    endpoint = selection['endpoint']
    def local_socket(value):
        return (isinstance(value, str) and value.startswith('unix:///') and len(value) <= 256
                and all(not c.isspace() and ord(c) >= 32 for c in value)
                and not any(c in value for c in ('?', '#', '%', '\\'))
                and str(PurePosixPath(value[7:])) == value[7:] and '..' not in PurePosixPath(value[7:]).parts)
    need(local_socket(endpoint) and local_socket(selection['selected_endpoint']), 'remote/nonlocal Docker endpoint refused')
    bound = ['docker', '--host', endpoint]
    need(admission['bound_argv'] == bound, 'AIRC admitted Docker binding differs')
    method, context = selection['method'], selection['context']
    for key in ('context_set', 'host_set', 'host_ignored'):
        need(type(selection[key]) is bool, 'actual Docker selector presence metadata missing')
    need(selection['host_ignored'] == (selection['context_set'] and selection['host_set']), 'Docker selector precedence differs')
    def command(label, argv):
        record = proof['commands']['docker-' + label]
        need(record['argv'] == argv and record['stdout']['path'] == 'evidence/docker/' + label + '.stdout'
             and record['stderr']['path'] == 'evidence/docker/' + label + '.stderr', 'Docker admission raw command/path mismatch: ' + label)
        return command_record(files, record)[0]
    if method in ('DOCKER_CONTEXT', 'default-context'):
        need(isinstance(context, str) and re.fullmatch(r'[A-Za-z0-9_.-]{1,128}', context), 'Docker context identity missing')
        need(selection['context_set'] == (method == 'DOCKER_CONTEXT'), 'Docker effective context precedence mismatch')
        if method == 'default-context':
            need(not selection['host_set'] and command('context-show', ['docker', 'context', 'show']).decode().strip() == context,
                 'default context selection unavailable')
        contexts = decode(command('context-inspect', ['docker', 'context', 'inspect', context]))
        need(isinstance(contexts, list) and len(contexts) == 1 and contexts[0]['Name'] == context
             and contexts[0]['Endpoints']['docker']['Host'] == selection['selected_endpoint'], 'raw selected context endpoint differs')
    else:
        need(method == 'DOCKER_HOST' and context is None and selection['host_set'] and not selection['context_set']
             and not selection['host_ignored'], 'unsupported/effective Docker environment selection differs')
    socket_path = endpoint[7:]
    socket_format = '{"path":"%n","mode_hex":"%f","device":%d,"inode":%i,"uid":%u,"gid":%g}'
    socket = decode(command('socket-stat', ['stat', '-Lc', socket_format, socket_path]))
    need(socket == admission['socket'] and socket['path'] == socket_path
         and stat.S_ISSOCK(int(socket['mode_hex'], 16))
         and all(type(socket[key]) is int and socket[key] >= 0 for key in ('device', 'inode', 'uid', 'gid')),
         'raw admitted local Docker socket identity missing')
    info_format = '{' + ','.join('"' + key + '":{{json .' + key + '}}' for key in
                                ('ID', 'OSType', 'Architecture', 'NCPU', 'MemTotal', 'DockerRootDir', 'ServerVersion')) + '}'
    info = decode(command('daemon-info', bound + ['info', '--format', info_format]))
    aliases = {'x86_64': 'amd64', 'amd64': 'amd64', 'aarch64': 'arm64', 'arm64': 'arm64'}
    need(info['OSType'] == 'linux' and aliases.get(info['Architecture']) == arch and daemon['os'] == 'linux'
         and daemon['architecture'] == arch and isinstance(info['ID'], str) and bool(info['ID']) and daemon['id'] == info['ID']
         and type(info['NCPU']) is int and info['NCPU'] >= 2 and daemon['cpus'] == info['NCPU']
         and type(info['MemTotal']) is int and info['MemTotal'] >= 4 * 1024**3 and daemon['mem_total'] == info['MemTotal'],
         'actual Docker daemon is not admitted native Linux/resources')
    # Canonicalization occurred on the admitted native producer, whose exact
    # recipe is bound to W. The publishing worker must not resolve its paths.
    need(info['DockerRootDir'] == daemon['reported_docker_root_dir']
         and isinstance(info['DockerRootDir'], str) and info['DockerRootDir'].startswith('/')
         and daemon['docker_root_dir'] == storage['docker']['path'], 'raw/reported/canonical Docker backing root differs')
    native_filesystems = {'ext2/ext3', 'ext2/ext3/ext4', 'ext4', 'xfs', 'btrfs', 'zfs'}
    paths = []
    for role in ('output', 'docker'):
        row = storage[role]
        path = row['path']
        need(isinstance(path, str) and path.startswith('/') and str(PurePosixPath(path)) == path
             and '..' not in PurePosixPath(path).parts, 'Docker backing path not canonical absolute')
        need(command(role + '-filesystem', ['stat', '-f', '-c', '%T', path]).decode().strip() == row['filesystem']
             and row['filesystem'] in native_filesystems and type(row['free_bytes']) is int and row['free_bytes'] >= 20 * 1024**3,
             'actual output/Docker backing filesystem/headroom not admitted')
        paths.append(path)
    need(admission['floors'] == {'admission_disk_gib': 20, 'before_pull_gib': 16, 'build_gib': 14,
                                'ram_total_gib': 4, 'ram_available_gib': 2}, 'AIRC native admission floors changed')
    need(admission['monitoring'] == {'path': 'evidence/docker/resources.jsonl', 'paths': paths, 'minimum_disk_gib': 14},
         'Docker backing monitor path/floor differs')
    lines = files.read('evidence/docker/resources.jsonl', limit=2 * 1024**2).splitlines()
    need(3 <= len(lines) <= 20000 and all(lines), 'bounded Docker/output resource monitoring absent')
    floors = []
    for line in lines:
        sample = decode(line)
        floor, disks = sample['disk_floor_gib'], sample['disks']
        need(type(floor) is int and floor in (14, 16, 20) and isinstance(disks, list) and len(disks) == 2
             and [row['path'] for row in disks] == paths, 'actual monitored backing paths/floor changed')
        need(all(type(row['free_bytes']) is int and row['free_bytes'] >= floor * 1024**3 for row in disks)
             and type(sample['mem_total']) is int and sample['mem_total'] >= 4 * 1024**3
             and type(sample['mem_available']) is int and sample['mem_available'] >= 2 * 1024**3,
             'Docker/output disk or memory floor violated during producer run')
        floors.append(floor)
    need(floors[0] == 20 and 16 in floors and 14 in floors and floors.index(16) < floors.index(14),
         'admission/before-pull/single-build monitoring sequence missing')
    return admission


def validate_airc(files, manifest, arch, source_policy=None, event=False):
    repo = 'leogr/airc'
    policy = REPOS[repo] if source_policy is None else source_policy
    proof = files.json(manifest['build_proof'])
    for key, value in {'schema': 1, 'repository': repo, 'source_commit': policy['source'], 'source_tree': policy['tree'],
                       'source_archive_sha256': policy['archive'], 'cargo_lock_sha256': policy['lock'], 'architecture': arch}.items():
        need(proof.get(key) == value, 'AIRC build proof identity mismatch: ' + key)
    channel = 'published-event-release' if event else 'candidate-prerelease'
    need(proof.get('producer_build_complete') is True and proof.get('publication_channel') == channel
         and proof.get('consumer_qualification') == 'not_run', 'producer versus consumer qualification boundary missing')
    definition = manifest['workflow_definition']
    need(proof['build_definition']['commit'] == definition['commit'] and proof['github_run_id'] == definition['run_id']
         and str(proof['github_run_attempt']) == '1' and proof['github_workflow_ref'] == definition['workflow'], 'AIRC same-run metadata differs')
    recipe_py, recipe_sh = files.read('evidence/recipe/release-native.py'), files.read('evidence/recipe/release-native.sh')
    need(sha(recipe_py + recipe_sh) == manifest['recipe']['sha256'] == proof['build_definition']['recipe_sha256'], 'AIRC actual recipe differs')
    source = source_archive(files.read('source.tar.gz'), policy)
    version = tomllib.loads(source['Cargo.toml'][0].decode())['package']['version']
    need(isinstance(version, str) and len(version) <= 128
         and re.fullmatch(r'[0-9]+\.[0-9]+\.[0-9]+(?:[-+][0-9A-Za-z.+-]+)?', version),
         'bounded literal source package version required')
    need(proof['runner']['system'] == 'Linux' and proof['runner']['machine'] == {'amd64': 'x86_64', 'arm64': 'aarch64'}[arch], 'actual native host mismatch')
    admission = validate_airc_admission(files, proof, arch)
    image = files.json('image.json')
    manifest_image = oci_raw(files.read('image-manifest.raw.json'), policy['images'][arch])
    oci_binding(oci_raw(files.read('image-index.raw.json'), policy['index']), manifest_image, image, policy, arch)
    config = oci_raw(files.read('image-config.raw.json'), manifest_image['config']['digest'])
    need(config.get('architecture') == arch and config.get('os') == 'linux', 'actual OCI config platform differs')
    need(proof['toolchain']['image_index'] == policy['index'] and proof['toolchain']['image_digest'] == policy['images'][arch]
         and proof['toolchain']['image_config'] == image['Id'], 'AIRC compiler image differs')
    triple = {'amd64': 'x86_64', 'arm64': 'aarch64'}[arch] + '-unknown-linux-musl'
    need(proof['toolchain']['target'] == triple, 'AIRC target does not match native architecture')
    for label, expected in (('host-system', b'Linux'), ('host-machine', {'amd64': b'x86_64', 'arm64': b'aarch64'}[arch])):
        out, _ = command_record(files, proof['commands'][label])
        need(out.strip() == expected, 'actual host uname receipt differs')
    need(proof.get('reproducible') is False and proof.get('reproducibility_check') == 'not_run'
         and [b['iteration'] for b in proof['builds']] == [1],
         'one AIRC build with explicit untested reproducibility required')
    archive_name = 'airc-linux-' + arch + '.tar.gz'
    need(set(manifest['artifacts']) == {archive_name}, 'AIRC archive inventory mismatch')
    for iteration in (1,):
        prefix = 'build-' + str(iteration)
        created = files.json(prefix + '/create-result.json')
        cid = created['container_id']
        need(re.fullmatch('[0-9a-f]{64}', cid), 'actual AIRC create result missing')
        create_argv = files.json(prefix + '/create-intent.json')['argv']
        start = files.json(prefix + '/start-intent.json')
        need(create_argv[:4] == admission['bound_argv'] + ['create']
             and '--name' in create_argv and create_argv[create_argv.index('--name') + 1] == created['name']
             and start == {'container': created['name'], 'action': 'start --attach',
                           'endpoint': admission['selection']['endpoint'], 'replay': False}, 'actual create/start not bound to admitted local daemon')
        need(files.json(prefix + '/start-result.json')['exit_code'] == 0, 'actual AIRC start failed')
        state = files.json(prefix + '/inspect-completed.json')
        need(isinstance(state, dict) and state['Id'] == cid and state['Image'] == image['Id'] and state['State']['Status'] == 'exited'
             and state['State']['ExitCode'] == 0 and not state['State'].get('OOMKilled'), 'actual AIRC container completion absent')
        rustc = text(files, prefix + '/build/rustc.txt')
        need('release: 1.99.0\n' in rustc and 'host: ' + proof['toolchain']['target'] + '\n' in rustc, 'actual AIRC compiler mismatch')
        for label, expected in (('upstream-check', ['make', 'check']), ('build', ['cargo', 'build', '--locked', '--release', '--bins']),
                                ('release-tests', ['cargo', 'test', '--locked', '--release', '--all-targets'])):
            record = proof['commands'][str(iteration) + '-' + label]
            need(record['argv'] == expected, 'AIRC actual qualification command differs')
            out, _ = command_record(files, record)
            zero(files, prefix + '/build/' + label + '.status')
            if label == 'release-tests':
                for name in BEHAVIOR_TESTS:
                    need(re.search(rb'^test ' + name.encode() + rb' \.\.\. ok$', out, re.M), 'actual behavioral test did not pass: ' + name)
                need(b'test result: ok.' in out and b'test result: FAILED.' not in out, 'release unit harness failed/incomplete')
        for binary in policy['bins']:
            for label in ('elf', 'version', 'help'):
                out, _ = command_record(files, proof['commands'][str(iteration) + '-' + binary + '-' + label])
                if label == 'version':
                    need(out.strip() == ('airc ' + version).encode(), 'actual native AIRC package version differs')
                else:
                    need(bool(out.strip()), 'empty native CLI/ELF evidence')
        raw = files.read(prefix + '/' + archive_name, proof['builds'][iteration - 1]['archive_sha256'])
        payload = payload_archive(raw, arch, repo, source, policy)
        need(sha(raw) == manifest['artifacts'][archive_name] == sha(files.read(archive_name)), 'AIRC actual build/archive checksum differs')
        declared = {p['path']: {k: p[k] for k in ('sha256', 'size')} | {'mode': int(p['mode'], 8)} for p in proof['payloads']}
        need(payload == declared, 'AIRC actual payload/proof bytes differ')
    return proof


def validate_inputs(repository, source, tag, input_dir, *, source_policy=None, event_workflow=None):
    need(repository in REPOS, 'RELEASE_REPOSITORY must be exactly leogr/OpenShell or leogr/airc')
    need(source_policy is None or (repository == 'leogr/airc' and event_workflow is not None),
         'dynamic source policy is only accepted from the AIRC event adapter')
    policy = REPOS[repository] if source_policy is None else source_policy
    need(source == policy['source'], 'SOURCE_SHA must be the fixed reviewed Rust subject')
    need(re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9._-]{0,95}', tag) and '..' not in tag and not tag.endswith(('.', '.lock'))
         and tag.lower() not in ('latest', 'main', 'master', 'head'), 'new explicit safe immutable tag required')
    root = Path(input_dir).absolute()
    expected = {policy['slug'] + '-linux-' + arch for arch in ARCHES}
    need(root.is_dir() and set(p.name for p in root.iterdir()) == expected, 'exact same-run native amd64+arm64 artifact directories required')
    matrix, workflow, common = {}, event_workflow, {}
    for arch in ARCHES:
        files = Files(root / (policy['slug'] + '-linux-' + arch))
        manifest = common_manifest(files, repository, arch, workflow,
                                   source_policy=source_policy, event_workflow=event_workflow)
        workflow = manifest['workflow_definition']
        proof = (validate_openshell(files, manifest, arch) if repository == 'leogr/OpenShell' else
                 validate_airc(files, manifest, arch, source_policy, event_workflow is not None))
        if repository == 'leogr/OpenShell':
            for name in ('sandbox-policy.rego', 'source-proof.json'):
                raw = files.read(name)
                need(name not in common or common[name] == raw, 'cross-architecture common subject bytes differ')
                common[name] = raw
        matrix[arch] = {'files': files, 'manifest': manifest, 'proof': proof}
    need(matrix['amd64']['manifest']['recipe'] == matrix['arm64']['manifest']['recipe'], 'architecture recipes differ')
    return {'repository': repository, 'source': source, 'tree': policy['tree'], 'tag': tag,
            'workflow_definition': workflow, 'matrix': matrix, 'producer_build_complete': True,
            'publication_channel': 'published-event-release' if event_workflow else 'candidate-prerelease',
            'consumer_qualification': 'NOT RUN', 'run_head': source if event_workflow else workflow['commit']}


def fsync_json(path, value):
    raw = canonical(value)
    need(len(raw) <= MAX_JSON, 'bounded receipt required')
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW | os.O_CLOEXEC, 0o600)
    try:
        with os.fdopen(fd, 'wb', closefd=False) as out:
            out.write(raw)
            out.flush()
            os.fsync(fd)
    finally:
        os.close(fd)
    parent = os.open(Path(path).parent, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC)
    try:
        os.fsync(parent)
    finally:
        os.close(parent)


class GitHub:
    """Bounded gh, fixed hosts, token only in a private subprocess environment.

    Never persist raw gh diagnostics/headers or emit subprocess exception text.
    Mutation errors are uncertain regardless of the transient client's exit.
    No call is retried. Local receipts and downloaded public bytes are retained.
    """
    def __init__(self, repository, env, root):
        need(repository in REPOS and env.get('GH_TOKEN'), 'protected job GH_TOKEN missing')
        self.repository, self.root, self.token = repository, Path(root), env['GH_TOKEN']
        self.gh = shutil.which('gh', path=env.get('PATH'))
        need(self.gh and Path(self.gh).is_absolute(), 'gh executable unavailable')
        self.env = {key: env[key] for key in ('PATH', 'HOME', 'SSL_CERT_FILE', 'SSL_CERT_DIR') if env.get(key)}
        self.env.update(GH_TOKEN=self.token, GH_HOST='github.com', GH_PROMPT_DISABLED='1', GH_PAGER='cat',
                        NO_COLOR='1', LANG='C.UTF-8')
        self.sequence = 0
        self.known_effects = []
        self.deadline = time.monotonic() + 3600

    def call(self, endpoint, method='GET', payload=None, upload=None, download=None, expected=(200,), allow404=False, acknowledgement=None):
        need(method in ('GET', 'POST', 'PATCH') and not (payload is not None and upload is not None), 'unsupported GitHub operation')
        need(endpoint.startswith('repos/' + self.repository + '/') or endpoint.startswith('https://uploads.github.com/repos/' + self.repository + '/'), 'foreign GitHub endpoint')
        need(self.token not in endpoint and (payload is None or self.token.encode() not in canonical(payload)), 'credential material in public request')
        mutation = method != 'GET'
        need(not mutation or callable(acknowledgement), 'exact mutation acknowledgement validator required before effect')
        self.sequence += 1
        prefix = self.root / ('operation-' + str(self.sequence).zfill(3))
        request = {'schema': 1, 'method': method, 'endpoint': endpoint, 'mutation': mutation,
                   'request_sha256': sha(canonical(payload)) if payload is not None else None,
                   'upload_sha256': digest_regular(upload) if upload is not None else None,
                   'result_known': False, 'automatic_retry': False}
        if mutation:
            # O_EXCL plus fsync precedes even starting the network client.
            fsync_json(prefix.with_suffix('.intent.json'), request)
        argv = [self.gh, 'api', '--method', method, '-H', 'X-GitHub-Api-Version: 2022-11-28']
        if download is None:
            argv += ['--include', '-H', 'Accept: application/vnd.github+json']
        else:
            argv += ['-H', 'Accept: application/octet-stream']
        if upload is not None:
            argv += ['-H', 'Content-Type: application/octet-stream', '--input', str(upload)]
        elif payload is not None:
            path = prefix.with_suffix('.payload.json')
            fsync_json(path, payload)
            argv += ['--input', str(path)]
        argv.append(endpoint)
        timeout = min(900 if download is not None or upload is not None else 120,
                      max(0, self.deadline - time.monotonic()))
        need(timeout > 0, 'publisher total bound exhausted')
        process, selector = None, None
        output, diagnostics = bytearray(), hashlib.sha256()
        output_size, error_size = 0, 0
        target = None
        try:
            process = subprocess.Popen(argv, stdin=subprocess.DEVNULL, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                                       env=self.env, bufsize=0)
            selector = selectors.DefaultSelector()
            if download is not None:
                target = Path(download).open('xb')
            for name, stream in (('stdout', process.stdout), ('stderr', process.stderr)):
                os.set_blocking(stream.fileno(), False)
                selector.register(stream, selectors.EVENT_READ, name)
            until = time.monotonic() + timeout
            while selector.get_map() or process.poll() is None:
                if time.monotonic() >= until:
                    raise TimeoutError('bounded gh operation did not settle')
                for key, _ in selector.select(0.1):
                    try:
                        raw = os.read(key.fileobj.fileno(), 65536)
                    except BlockingIOError:
                        continue
                    if not raw:
                        selector.unregister(key.fileobj)
                    elif key.data == 'stderr':
                        diagnostics.update(raw)
                        error_size += len(raw)
                        need(error_size <= MAX_JSON, 'gh diagnostic bound exceeded')
                    else:
                        output_size += len(raw)
                        need(output_size <= (2 * 1024**3 if target else MAX_JSON), 'gh output bound exceeded')
                        if target:
                            target.write(raw)
                        else:
                            output.extend(raw)
            code = process.poll()
            if target:
                target.flush()
                os.fsync(target.fileno())
            status, value = code, None
            if download is None:
                raw = bytes(output)
                need(self.token.encode() not in raw, 'credential material in API response; body not retained')
                # gh --include renders one HTTP header block for these JSON
                # endpoints. Redirected binary asset calls use separate output.
                head, separator, body = raw.partition(b'\r\n\r\n')
                if not separator:
                    head, separator, body = raw.partition(b'\n\n')
                match = re.match(rb'^HTTP/\S+ ([0-9]{3})(?:\s|$)', head)
                need(separator and match, 'GitHub HTTP status/header evidence unavailable')
                status = int(match[1])
                if status == 404 and allow404 and not mutation:
                    return None
                need(status in expected and code == 0, 'GitHub status outside exact operation contract')
                if mutation:
                    transport = {**request, 'receipt_kind': 'http-transport-only', 'result_known': False,
                                 'http_status': status, 'gh_exit': code, 'response_sha256': sha(bytes(output)),
                                 'response_bytes': output_size, 'diagnostics_sha256': diagnostics.hexdigest()}
                    fsync_json(prefix.with_suffix('.transport.json'), transport)
                value = decode(body)
            else:
                need(code == 0, 'GitHub public-byte download failed')
                value = {'path': str(download), 'size': output_size}
            if mutation:
                # HTTP success alone does not establish the operation's exact ACK.
                acknowledgement(value)
                fsync_json(prefix.with_suffix('.complete.json'), {**transport,
                           'receipt_kind': 'exact-operation-acknowledgement', 'result_known': True})
                self.known_effects.append({'method': method, 'endpoint': endpoint,
                                           'receipt': prefix.name + '.complete.json'})
            return value
        except BaseException as error:
            # The local gh client is not an attach/use/agent client. Settling it
            # cannot establish whether a remote mutation occurred.
            if process is not None and process.poll() is None:
                process.kill()
                process.wait(timeout=10)
            if mutation:
                raise Uncertain('GitHub mutation exact acknowledgement unproved; retain intent and transport evidence; no replay',
                                self.known_effects) from None
            if self.known_effects:
                raise PartialPublication('GitHub verification read unavailable at ' + endpoint.split('?', 1)[0]
                                         + '; earlier acknowledged effects retained; no suffix/replay', self.known_effects) from None
            raise Refusal('GitHub read unavailable at ' + endpoint.split('?', 1)[0]
                          + '; required metadata/permission missing before publication effects') from None
        finally:
            if selector is not None:
                selector.close()
            if process is not None:
                process.stdout.close()
                process.stderr.close()
            if target:
                target.close()

    def list(self, endpoint, field=None):
        rows = []
        for page in range(1, 101):
            sep = '&' if '?' in endpoint else '?'
            value = self.call(endpoint + sep + 'per_page=100&page=' + str(page))
            chunk = value[field] if field else value
            need(isinstance(chunk, list), 'GitHub list schema mismatch')
            rows.extend(chunk)
            need(len(rows) <= MAX_FILES, 'GitHub list budget exceeded')
            if len(chunk) < 100:
                return rows
        raise Refusal('GitHub inventory incomplete at pagination bound')


def workflow_environment(raw):
    """Exact supported literal job form, not a permissive YAML interpreter."""
    text = raw.decode('utf-8')
    matches = list(re.finditer(r'^  publish:\s*$', text, re.M))
    need(len(matches) == 1, 'immutable workflow lacks one literal publish job')
    block = text[matches[0].end():]
    end = re.search(r'^  [A-Za-z0-9_-]+:\s*$', block, re.M)
    if end:
        block = block[:end.start()]
    environments = re.findall(r'^    environment:\s*([^\n#]+?)\s*(?:#.*)?$', block, re.M)
    need(environments == ['upstream-release'], 'immutable publisher job environment not literal upstream-release')
    need(re.search(r'^    if:\s*inputs\.publish\s*$', block, re.M)
         or re.search(r'^    if:.*inputs\.publish.*$', block, re.M), 'publish job is not explicitly opt-in')


def api_content(api, path, commit):
    value = api.call('repos/' + api.repository + '/contents/' + path + '?ref=' + commit)
    need(value.get('type') == 'file' and value.get('encoding') == 'base64', 'immutable workflow source is not a file')
    try:
        raw = base64.b64decode(value['content'], validate=False)
    except (ValueError, TypeError) as error:
        raise Refusal('invalid immutable workflow source encoding') from error
    need(len(raw) <= MAX_JSON and git_object('blob', raw).hex() == value['sha'], 'immutable source blob mismatch')
    return raw


def protected_context(plan, env, api):
    definition, repository = plan['workflow_definition'], plan['repository']
    required = {'GITHUB_ACTIONS': 'true', 'GITHUB_EVENT_NAME': 'workflow_dispatch',
                'GITHUB_REPOSITORY': repository, 'GITHUB_SHA': definition['commit'],
                'BUILD_DEFINITION': definition['commit'], 'GITHUB_RUN_ID': str(definition['run_id']),
                'GITHUB_RUN_ATTEMPT': '1', 'GITHUB_REF': 'refs/heads/release-ci',
                'GITHUB_REF_PROTECTED': 'true', 'GITHUB_WORKFLOW_REF': definition['workflow'], 'GITHUB_JOB': 'publish'}
    need(all(env.get(key) == value for key, value in required.items()), 'protected first-attempt CI context/configuration missing or mismatched')
    need(env.get('GITHUB_EVENT_PATH'), 'actual workflow_dispatch event path missing')
    event = decode(read_regular(env['GITHUB_EVENT_PATH'], MAX_JSON))
    need(event.get('inputs', {}).get('publish') in (True, 'true')
         and event['inputs'].get('release_tag') == plan['tag'], 'actual approved dispatch did not request this publication/tag')
    repo = api.call('repos/' + repository + '/actions/runs/' + str(definition['run_id']))
    owner = repo['repository']['owner']
    need(repo['repository']['full_name'] == repository and owner['login'] == 'leogr'
         and repo['actor']['id'] == owner['id'] and repo['event'] == 'workflow_dispatch'
         and repo['head_sha'] == definition['commit'] and repo['head_branch'] == 'release-ci'
         and repo['run_attempt'] == 1 and repo['status'] == 'in_progress', 'actual run actor/source/first-attempt mismatch')
    if repo.get('triggering_actor'):
        need(repo['triggering_actor']['id'] == owner['id'], 'actual dispatch actor is not repository owner')
    branch = api.call('repos/' + repository + '/branches/release-ci')
    need(branch.get('protected') is True, 'release-ci ref protection unavailable')
    path = definition['workflow'].split('/', 2)[2].split('@', 1)[0]
    need(repo['path'].split('@', 1)[0] == path, 'actual workflow path differs')
    workflow_environment(api_content(api, path, definition['commit']))
    need(api_content(api, 'ci/publish-release.py', definition['commit']) == Path(__file__).read_bytes(), 'running publisher differs from immutable W')
    need(api_content(api, 'ci/publish-release.sh', definition['commit']) == Path(__file__).with_suffix('.sh').read_bytes(), 'running publisher wrapper differs from immutable W')
    for arch in ARCHES:
        manifest, files = plan['matrix'][arch]['manifest'], plan['matrix'][arch]['files']
        native_name = 'release_native.py' if repository == 'leogr/OpenShell' else 'release-native.py'
        for name in (native_name, 'release-native.sh'):
            need(api_content(api, 'ci/' + name, definition['commit']) == files.read('evidence/recipe/' + name), 'actual producer recipe differs from W')
    approvals = api.call('repos/' + repository + '/actions/runs/' + str(definition['run_id']) + '/approvals')
    need(isinstance(approvals, list), 'actual protected approval history unavailable')
    relevant = [row for row in approvals if any(e.get('name') == 'upstream-release' for e in row.get('environments', []))]
    need(relevant and any(row.get('state') == 'approved' and row.get('user', {}).get('id') == owner['id'] for row in relevant),
         'protected upstream-release owner approval not observable; configure environment review/permissions before publication')
    jobs = api.list('repos/' + repository + '/actions/runs/' + str(definition['run_id']) + '/attempts/1/jobs', 'jobs')
    for arch in ARCHES:
        matches = [j for j in jobs if j['name'] == 'build (' + arch + ')']
        need(len(matches) == 1 and matches[0]['status'] == 'completed' and matches[0]['conclusion'] == 'success'
             and str(matches[0]['run_id']) == str(definition['run_id']) and matches[0]['head_sha'] == definition['commit'],
             'actual successful native matrix job missing: ' + arch)
    publishers = [j for j in jobs if j['name'] == 'publish']
    need(len(publishers) == 1 and publishers[0]['status'] == 'in_progress', 'actual running publish job not identified')


def same_run_archives(plan, api):
    definition, policy = plan['workflow_definition'], REPOS[plan['repository']]
    artifacts = api.list('repos/' + plan['repository'] + '/actions/runs/' + str(definition['run_id']) + '/artifacts', 'artifacts')
    for arch in ARCHES:
        name = policy['slug'] + '-linux-' + arch
        matches = [a for a in artifacts if a['name'] == name]
        need(len(matches) == 1 and matches[0].get('expired') is False, 'same-run native artifact missing/ambiguous')
        artifact = matches[0]
        need(artifact.get('workflow_run', {}).get('head_sha') == plan.get('run_head', definition['commit'])
             and artifact.get('workflow_run', {}).get('id') == int(definition['run_id'])
             and re.fullmatch(r'sha256:[0-9a-f]{64}', artifact.get('digest', '')), 'server same-run artifact digest/identity missing')
        destination = api.root / ('same-run-' + arch + '.zip')
        api.call('repos/' + api.repository + '/actions/artifacts/' + str(artifact['id']) + '/zip', download=destination)
        need('sha256:' + digest_regular(destination) == artifact['digest'], 'server Actions artifact ZIP digest mismatch')
        expected = plan['matrix'][arch]['files']
        observed, total = {}, 0
        with zipfile.ZipFile(destination) as archive:
            for entry in archive.infolist():
                name = relative(entry.filename.rstrip('/'))
                need(name not in observed and not entry.flag_bits & 1, 'duplicate/encrypted same-run artifact member')
                kind = (entry.external_attr >> 16) & 0o170000
                need(kind in (0, stat.S_IFREG, stat.S_IFDIR), 'same-run artifact special/symlink member')
                if entry.is_dir():
                    continue
                need(entry.file_size <= MAX_FILE and len(observed) < MAX_FILES, 'same-run artifact member budget')
                total += entry.file_size
                need(total <= MAX_TOTAL, 'same-run artifact total budget')
                digest = hashlib.sha256()
                length = 0
                with archive.open(entry) as source:
                    while True:
                        block = source.read(1024**2)
                        if not block:
                            break
                        length += len(block)
                        need(length <= entry.file_size, 'same-run artifact expansion exceeds declared size')
                        digest.update(block)
                need(length == entry.file_size and name in expected.inventory, 'same-run artifact inventory differs')
                local = expected.read(name)
                need(digest.hexdigest() == sha(local) and len(local) == length, 'downloaded local input differs from same-run artifact bytes')
                observed[name] = digest.hexdigest()
        need(set(observed) == set(expected.inventory), 'same-run artifact missing local input files')


def evidence_archive(files, destination):
    """Deterministic archive of explicitly exported evidence, never workspace."""
    with destination.open('xb') as raw:
        with gzip.GzipFile(filename='', fileobj=raw, mode='wb', mtime=0, compresslevel=6) as gz:
            with tarfile.open(fileobj=gz, mode='w', format=tarfile.PAX_FORMAT) as archive:
                for name in sorted(files.inventory):
                    data = files.read(name)
                    entry = tarfile.TarInfo(name)
                    entry.size, entry.mode, entry.uid, entry.gid, entry.mtime = len(data), 0o644, 0, 0, 0
                    entry.uname = entry.gname = ''
                    archive.addfile(entry, io.BytesIO(data))


def publication_assets(plan, root):
    assets, arch_manifests = {}, {}
    for arch in ARCHES:
        files, manifest = plan['matrix'][arch]['files'], plan['matrix'][arch]['manifest']
        names = set(manifest['artifacts']) | {manifest['build_proof']}
        for name in sorted(names):
            raw = files.read(name)
            if name in assets:
                need(read_regular(assets[name]) == raw, 'cross-architecture asset name collision')
                continue
            target = root / name
            with target.open('xb') as out:
                out.write(raw)
            assets[name] = target
        arch_manifests[arch] = manifest
        name = 'evidence-' + arch + '.tar.gz'
        target = root / name
        evidence_archive(files, target)
        assets[name] = target
    manifest = {'schema': 1, 'repository': plan['repository'], 'source_subject': {'commit': plan['source'], 'tree': plan['tree']},
                'workflow_definition': plan['workflow_definition'], 'architectures': arch_manifests,
                'producer_build_complete': True, 'publication_channel': plan['publication_channel'],
                'consumer_qualification': 'NOT RUN',
                'scope': ('Producer single-build/static/interface/source evidence; reproducibility not checked; no production readiness or yoloctl native/custom-base gate claim.'
                          if plan['repository'] == 'leogr/airc' else
                          'Producer reproducibility/static/interface/source evidence; no production readiness or yoloctl native/custom-base gate claim.'),
                'artifacts': {name: {'sha256': digest_regular(path), 'size': path.stat().st_size} for name, path in sorted(assets.items())}}
    if 'event_release' in plan:
        manifest['event_release'] = plan['event_release']
    fsync_json(root / 'CI-MANIFEST.json', manifest)
    assets['CI-MANIFEST.json'] = root / 'CI-MANIFEST.json'
    checksums = ''.join(digest_regular(path) + '  ' + name + '\n' for name, path in sorted(assets.items())).encode()
    with (root / 'SHA256SUMS').open('xb') as out:
        out.write(checksums)
    assets['SHA256SUMS'] = root / 'SHA256SUMS'
    return assets


def new_release(plan, assets, api):
    base = 'repos/' + plan['repository']
    tag, source = plan['tag'], plan['source']
    need(api.call(base + '/git/ref/tags/' + tag, allow404=True) is None, 'tag exists; no adoption/replacement/retry')
    need(not any(r.get('tag_name') == tag for r in api.list(base + '/releases')), 'release/draft exists; no adoption/replacement/retry')
    known = []

    def mutate(endpoint, method, *, payload=None, upload=None, expected, acknowledgement, effect):
        # GitHub.call validates this exact ACK before its semantic completion
        # receipt. A later read cannot revise this acknowledged effect's outcome.
        value = api.call(endpoint, method, payload=payload, upload=upload,
                         expected=expected, acknowledgement=acknowledgement)
        known.append(effect)
        return value

    def tag_ack(value):
        need(isinstance(value, dict) and value.get('ref') == 'refs/tags/' + tag
             and isinstance(value.get('object'), dict) and value['object'].get('sha') == source
             and value['object'].get('type') == 'commit', 'new tag exact acknowledgement mismatch')

    def draft_ack(value):
        need(isinstance(value, dict) and type(value.get('id')) is int and value['id'] > 0
             and value.get('tag_name') == tag and value.get('target_commitish') == source
             and value.get('draft') is True and value.get('prerelease') is True,
             'new draft exact acknowledgement mismatch')

    try:
        mutate(base + '/git/refs', 'POST', payload={'ref': 'refs/tags/' + tag, 'sha': source}, expected=(201,),
               acknowledgement=tag_ack, effect={'kind': 'new-tag', 'tag': tag, 'source': source})
        release = mutate(base + '/releases', 'POST', payload={'tag_name': tag, 'target_commitish': source, 'name': tag,
            'draft': True, 'prerelease': True, 'make_latest': 'false',
            'body': 'Candidate producer artifacts from reviewed same-run native builds. Consumer/custom-base/yoloctl runtime qualification NOT RUN; not production readiness.'},
            expected=(201,), acknowledgement=draft_ack, effect={'kind': 'new-draft', 'tag': tag})
        release_id, uploaded = release['id'], {}
        for name, path in sorted(assets.items()):
            digest, size = digest_regular(path), path.stat().st_size
            def asset_ack(value):
                need(isinstance(value, dict) and type(value.get('id')) is int and value['id'] > 0
                     and value.get('name') == name and value.get('state') == 'uploaded' and type(value.get('size')) is int
                     and value['size'] == size, 'upload exact acknowledgement identity/size mismatch')
                need(value.get('digest') is None or value['digest'] == 'sha256:' + digest, 'server upload digest differs')
            asset = mutate('https://uploads.github.com/' + base + '/releases/' + str(release_id) + '/assets?name=' + urllib.parse.quote(name, safe=''),
                           'POST', upload=path, expected=(201,), acknowledgement=asset_ack,
                           effect={'kind': 'uploaded-asset', 'release_id': release_id, 'name': name, 'sha256': digest})
            downloaded = api.root / ('verified-' + name)
            api.call(base + '/releases/assets/' + str(asset['id']), download=downloaded)
            need(digest_regular(downloaded) == digest, 'independent draft-asset download differs; do not publish')
            uploaded[name] = asset['id']
        actual = api.list(base + '/releases/' + str(release_id) + '/assets')
        need({a['name']: a['id'] for a in actual} == uploaded and len(actual) == len(uploaded), 'draft asset set changed; no publish')
        ref = api.call(base + '/git/ref/tags/' + tag)
        need(ref.get('object', {}).get('sha') == source and ref['object'].get('type') == 'commit', 'owned tag changed; no publish')
        def publish_ack(value):
            need(isinstance(value, dict) and type(value.get('id')) is int and value['id'] == release_id
                 and value.get('tag_name') == tag and value.get('draft') is False and value.get('prerelease') is True
                 and isinstance(value.get('published_at'), str)
                 and re.fullmatch(r'[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}(?:\.[0-9]+)?Z', value['published_at']),
                 'publication exact acknowledgement incomplete; no replay')
        mutate(base + '/releases/' + str(release_id), 'PATCH', payload={'draft': False, 'prerelease': True, 'make_latest': 'false'},
               expected=(200,), acknowledgement=publish_ack, effect={'kind': 'published-prerelease', 'release_id': release_id, 'tag': tag})
        return {'release_id': release_id, 'tag': tag, 'assets': uploaded, 'channel': 'candidate-prerelease',
                'source_subject': source, 'consumer_qualification': 'NOT RUN', 'automatic_retry': False}
    except Uncertain as error:
        if not error.known_effects:
            error.known_effects = list(known)
        raise
    except (Refusal, OSError, ValueError, TypeError, KeyError, AttributeError) as error:
        if known:
            # Known tag/draft/upload ACKs remain known. Failed verification is
            # a partial publication hold, never uncertainty about those ACKs.
            raise PartialPublication('Publication verification stopped after acknowledged effects; retain receipts/assets; no suffix/replay', known) from None
        raise


def main(argv=None, env=None, api_factory=GitHub):
    args, env = list(sys.argv[1:] if argv is None else argv), dict(os.environ if env is None else env)
    need(len(args) == 3, 'usage: publish-release.sh SOURCE_SHA NEW_RELEASE_TAG SAME_RUN_INPUT_DIR')
    repository, mode = env.get('RELEASE_REPOSITORY'), env.get('RELEASE_MODE', 'plan')
    need(mode in ('plan', 'apply'), 'RELEASE_MODE must be plan or explicit protected apply')
    need(env.get('GITHUB_RUN_ATTEMPT', '1') == '1', 'nonfirst GitHub run attempt refused; no replay')
    need(not env.get('GH_TOKEN') or env['GH_TOKEN'] not in '\n'.join(args), 'credential material in public arguments')
    if env.get('SOURCE_SUBJECT'):
        need(env['SOURCE_SUBJECT'] == args[0], 'SOURCE_SUBJECT env/argument mismatch')
    if env.get('RELEASE_TAG'):
        need(env['RELEASE_TAG'] == args[1], 'RELEASE_TAG env/argument mismatch')
    plan = validate_inputs(repository, *args)
    summary = {key: plan[key] for key in ('repository', 'source', 'tree', 'tag', 'workflow_definition',
                                         'producer_build_complete', 'publication_channel', 'consumer_qualification')}
    if mode == 'plan':
        print(json.dumps({'mode': 'PLAN', 'effects': False, **summary,
                          'apply_precondition': 'Explicit protected first-attempt owner-approved workflow_dispatch; gh read metadata and exact same-run artifact verification precede any mutation.'}, sort_keys=True))
        return 0
    need(env.get('RUNNER_TEMP') and env.get('GH_TOKEN'), 'protected runner scratch/GH_TOKEN configuration missing')
    parent = Path(env['RUNNER_TEMP']).absolute()
    need(parent.is_dir() and not any(p.is_symlink() for p in (parent, *parent.parents)), 'protected unaliased runner scratch required')
    root = parent / ('publication-' + REPOS[repository]['slug'] + '-' + str(plan['workflow_definition']['run_id']))
    root.mkdir(mode=0o700)  # Existing state is never resumed/adopted.
    fsync_json(root / 'publication-attempt.json', {'schema': 1, **summary, 'automatic_retry': False,
               'cross_run_rule': 'Uncertain prior operations require owner inspection; no automatic replay/recovery authority.'})
    api = api_factory(repository, env, root)
    protected_context(plan, env, api)
    same_run_archives(plan, api)
    assets = publication_assets(plan, root)
    result = new_release(plan, assets, api)
    # This boundary begins only after new_release returned: its final PATCH
    # exact ACK already passed and its operation .complete.json is durable.
    # A failure to persist that operation ACK remains Uncertain inside call().
    phase = 'aggregate-summary persistence'
    try:
        fsync_json(root / 'publication-complete.json', result)
        phase = 'stdout reporting'
        print(json.dumps({'mode': 'APPLY', 'result_known': True, **result}, sort_keys=True), flush=True)
    except OSError as error:
        raise KnownPublishedLocalReportingFailure(result, api.known_effects, root, phase, type(error).__name__) from None
    return 0


def error_report(error):
    # Controlled public reasons only; never include subprocess diagnostics,
    # exception bodies from I/O, credentials or remote response contents.
    reason = str(error) if isinstance(error, Refusal) else 'required input/configuration unavailable: ' + type(error).__name__
    report = {'status': 'UNCERTAIN' if isinstance(error, Uncertain) else 'PARTIAL PUBLICATION' if isinstance(error, PartialPublication) else 'BLOCKED',
              'known_effects': getattr(error, 'known_effects', []), 'reason': reason,
              'automatic_retry': False, 'consumer_qualification': 'NOT RUN'}
    if isinstance(error, KnownPublishedLocalReportingFailure):
        report.update(status='PUBLISHED_LOCAL_REPORTING_FAILURE', result_known=True,
                      publication=error.publication, local_reporting_complete=False,
                      local_failure_phase=error.phase, receipt_directory=error.receipt_directory)
    return report


if __name__ == '__main__':
    try:
        sys.exit(main())
    except (Refusal, OSError, ValueError, TypeError, KeyError, AttributeError, tarfile.TarError, zipfile.BadZipFile) as error:
        print(json.dumps(error_report(error)), file=sys.stderr)
        sys.exit(1)
