#!/usr/bin/env python3
"""Bounded read-only failure diagnostics; never evidence of a successful build.

Only local file reads/writes are used. No Docker, process, network, retry,
container cleanup or source/work/cache traversal is available in this module.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path, PurePosixPath
import re
import stat

PER_FILE = 1 << 20
TOTAL = 32 << 20
MAX_FILES = 256
MANIFEST_RESERVE = 1 << 20
MAX_SCAN = 512
SUBJECTS = {'leogr/OpenShell': 'e8605009bed50a05136efe106a0c1e2f283ee771',
            'leogr/airc': 'b76ba355863cc3eb51900a313d7647d9190eedfa'}
# Canonical JSON hashes bind code-reviewed literal W profiles. --profile cannot
# widen the code's allowlist; changing a profile requires a reviewed code change.
REVIEWED_PROFILES = {
    'leogr/OpenShell': 'b456eeeb633ce9d862f1a498688aa6679a8fe260f085fb5e25beb64fb76ab6ac',
    'leogr/airc': 'fe5565edce0442b4d743bea1d207054ea06aa28c06cb0e6516a835d0bc8b6ade',
}


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':'), allow_nan=False).encode()


def decode(raw):
    def unique(items):
        result = {}
        for key, value in items:
            if key in result:
                raise ValueError('duplicate JSON key')
            result[key] = value
        return result
    return json.loads(raw, object_pairs_hook=unique,
                      parse_constant=lambda _: (_ for _ in ()).throw(ValueError('nonfinite JSON')))


def digest(raw):
    return hashlib.sha256(raw).hexdigest()


def safe_relative(value):
    p = PurePosixPath(value)
    return (isinstance(value, str) and 0 < len(value.encode()) <= 256
            and not p.is_absolute() and '..' not in p.parts and '.' not in p.parts
            and str(p) == value)


def signature(st):
    return (st.st_dev, st.st_ino, st.st_mode, st.st_uid, st.st_nlink,
            st.st_size, st.st_mtime_ns, st.st_ctime_ns)


def accepted_uid(uid, allow_container_root=False):
    # Only a source-classified inner FILE leaf may use the UID0 exception.
    # Host metadata, command receipts and every ancestor remain CI-owned.
    return uid == os.geteuid() or (allow_container_root and uid == 0)


def open_relative(rootfd, name, directory=False, allow_container_root=False):
    if not safe_relative(name):
        raise ValueError('unsafe relative path')
    fd = os.dup(rootfd)
    try:
        parts = PurePosixPath(name).parts
        for index, part in enumerate(parts):
            flags = os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK | os.O_CLOEXEC
            if index < len(parts) - 1 or directory:
                flags |= os.O_DIRECTORY
            newfd = os.open(part, flags, dir_fd=fd)
            os.close(fd)
            fd = newfd
            st = os.fstat(fd)
            inner_leaf = allow_container_root and index == len(parts) - 1 and not directory
            if not accepted_uid(st.st_uid, inner_leaf):
                raise ValueError('unowned path component')
        result, fd = fd, None
        return result
    finally:
        if fd is not None:
            os.close(fd)


def open_absolute_directory(path):
    """Open every absolute directory component without following any symlink."""
    if not path.is_absolute():
        raise ValueError('absolute directory required')
    # Ancestors need traversal, not read/list authority (e.g. confined checks).
    # The final owned output directory remains readable for bounded collection.
    traversal = getattr(os, 'O_PATH', os.O_RDONLY) | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC
    readable = os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC
    fd = os.open('/', traversal)
    try:
        parts = path.parts[1:]
        for index, part in enumerate(parts):
            if part in ('.', '..'):
                raise ValueError('noncanonical directory')
            child = os.open(part, readable if index == len(parts) - 1 else traversal, dir_fd=fd)
            os.close(fd)
            fd = child
        result, fd = fd, None
        return result
    finally:
        if fd is not None:
            os.close(fd)


def read_prefix(rootfd, name, limit, allow_container_root=False):
    """Read at most the pre-budgeted prefix; never follow a link or block FIFO."""
    fd = None
    try:
        fd = open_relative(rootfd, name, allow_container_root=allow_container_root)
        before = os.fstat(fd)
        if not stat.S_ISREG(before.st_mode):
            return None, {'status': 'refused_type'}
        if not accepted_uid(before.st_uid, allow_container_root) or before.st_nlink != 1:
            return None, {'status': 'refused_owner_or_links'}
        amount = min(before.st_size, limit)
        chunks, remaining = [], amount
        while remaining:
            chunk = os.read(fd, min(65536, remaining))
            if not chunk:
                break
            chunks.append(chunk)
            remaining -= len(chunk)
        raw = b''.join(chunks)
        after = os.fstat(fd)
        stable = signature(before) == signature(after) and len(raw) == amount
        currentfd = None
        try:
            currentfd = open_relative(rootfd, name, allow_container_root=allow_container_root)
            stable = stable and signature(before) == signature(os.fstat(currentfd))
        except (OSError, ValueError):
            stable = False
        finally:
            if currentfd is not None:
                os.close(currentfd)
        return raw, {'status': 'captured' if stable else 'unstable',
                     'original_size': before.st_size, 'captured_size': len(raw),
                     'captured_prefix_sha256': digest(raw),
                     'truncated': before.st_size > len(raw), 'unchanged': stable}
    except FileNotFoundError:
        return None, {'status': 'missing'}
    except (OSError, ValueError):
        return None, {'status': 'refused_path_or_unavailable'}
    finally:
        if fd is not None:
            os.close(fd)


def expected_owner(env, repository):
    subject, definition = SUBJECTS[repository], env.get('GITHUB_SHA')
    if env.get('GITHUB_EVENT_NAME') == 'release':
        # Source S is the original tagged release; W is the workflow commit.
        # This changes identity validation only, never the closed read profile
        # or the collector's incomplete/no-positive-proof status.
        if repository != 'leogr/airc':
            raise ValueError('release-event diagnostics are AIRC only')
        subject, definition = env.get('GITHUB_SHA', ''), env.get('GITHUB_WORKFLOW_SHA', '')
        event_path = Path(env.get('GITHUB_EVENT_PATH', ''))
        if not event_path.is_file() or event_path.stat().st_size > 1024**2:
            raise ValueError('bounded original release event missing')
        event = decode(event_path.read_bytes())
        if (not isinstance(event, dict) or not isinstance(event.get('release'), dict)
                or not isinstance(event.get('repository'), dict)):
            raise ValueError('original release event object required')
        release = event.get('release', {})
        tag = release.get('tag_name', '')
        if (event.get('action') != 'published' or event.get('repository', {}).get('full_name') != repository
                or type(release.get('id')) is not int or release['id'] <= 0 or release.get('draft') is not False
                or not isinstance(tag, str) or not re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9._-]{0,95}', tag) or '..' in tag
                or tag.endswith(('.', '.lock')) or env.get('GITHUB_REF') != 'refs/tags/' + tag
                or env.get('GITHUB_WORKFLOW_REF') != repository + '/.github/workflows/native-release.yml@refs/tags/' + tag
                or not re.fullmatch('[0-9a-f]{40}', subject)):
            raise ValueError('original release diagnostic identity differs')
    if (env.get('GITHUB_ACTIONS') != 'true' or env.get('GITHUB_REPOSITORY') != repository
            or env.get('SOURCE_SUBJECT') != subject
            or env.get('BUILD_ARCH') not in ('amd64', 'arm64')
            or not re.fullmatch('[0-9a-f]{40}', env.get('BUILD_DEFINITION', ''))
            or env.get('BUILD_DEFINITION') != definition
            or any(not re.fullmatch('[1-9][0-9]{0,31}', env.get(k, ''))
                   for k in ('GITHUB_RUN_ID', 'GITHUB_RUN_ATTEMPT'))):
        raise ValueError('expected CI identity unavailable')
    return {'schema': 1, 'repository': repository, 'source_subject': subject,
            'workflow_definition': env['BUILD_DEFINITION'], 'run_id': env['GITHUB_RUN_ID'],
            'run_attempt': env['GITHUB_RUN_ATTEMPT'], 'arch': env['BUILD_ARCH']}


def load_profile(path):
    # Profile is a literal file in the reviewed definition W, not runtime config.
    fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK | os.O_CLOEXEC)
    try:
        st = os.fstat(fd)
        if not stat.S_ISREG(st.st_mode) or st.st_size > 65536:
            raise ValueError('profile is not bounded regular file')
        raw = os.read(fd, 65537)
        if signature(st) != signature(os.fstat(fd)):
            raise ValueError('profile changed')
    finally:
        os.close(fd)
    profile = decode(raw)
    if not isinstance(profile, dict):
        raise ValueError('profile must be object')
    repository = profile.get('repository')
    if (repository not in REVIEWED_PROFILES or digest(canonical(profile)) != REVIEWED_PROFILES[repository]
            or set(profile) != {'schema', 'repository', 'files', 'directories'} or profile['schema'] != 1):
        raise ValueError('profile is not the closed reviewed literal')
    if (len(profile['files']) > 64 or len(profile['directories']) > 16
            or not all(safe_relative(p) for p in profile['files'])
            or not all(safe_relative(p) for p in profile['directories'])):
        raise ValueError('invalid reviewed profile')
    return profile


def container_written_file(profile, name):
    """Closed source-shaped writer selector, independent of collected metadata.

    OpenShell's reviewed inner recipe writes only flat text outputs below the
    host-created evidence/build-N. AIRC's inner outputs are only build-N/build;
    its flat build-N ELF/interface stdout and host receipts stay CI-owned.
    Every directory and intermediate component is host-created and CI-only.
    """
    repository = profile.get('repository')
    if (repository not in REVIEWED_PROFILES or not safe_relative(name)
            or digest(canonical(profile)) != REVIEWED_PROFILES[repository]):
        return False
    path = PurePosixPath(name)
    parent = str(path.parent)
    scopes = {'leogr/OpenShell': ('evidence/build-1', 'evidence/build-2'),
              'leogr/airc': ('build-1/build', 'build-2/build')}
    if parent not in scopes[repository]:
        return False
    # Membership in the actual code-pinned profile is necessary in addition to
    # writer scope; a nearby unknown file never inherits the directory's role.
    pattern = profile['directories'].get(parent)
    return pattern is not None and re.fullmatch(pattern, path.name) is not None


def read_exit(exit_path, output):
    # The workflow-owned exit file is a sibling of output, not an arbitrary read.
    path = Path(exit_path)
    if (not path.is_absolute() or path.parent != output.parent
            or path.parent.resolve() != path.parent
            or re.fullmatch(r'[A-Za-z0-9_-][A-Za-z0-9_.-]{0,79}\.exit', path.name) is None):
        return {'status': 'refused_exit_path', 'mutation_outcome': 'not_inferred'}
    fd = None
    try:
        fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK | os.O_CLOEXEC)
        before = os.fstat(fd)
        if (not stat.S_ISREG(before.st_mode) or before.st_uid != os.geteuid()
                or before.st_nlink != 1 or before.st_size > 16):
            return {'status': 'refused_exit_file', 'mutation_outcome': 'not_inferred'}
        raw = os.read(fd, 17)
        if signature(before) != signature(os.fstat(fd)):
            return {'status': 'unstable_exit_file', 'mutation_outcome': 'not_inferred'}
        if re.fullmatch(rb'-?[0-9]{1,3}\n?', raw) is None:
            return {'status': 'invalid_exit_file', 'mutation_outcome': 'not_inferred'}
        return {'status': 'recorded', 'actual_exit': int(raw), 'mutation_outcome': 'not_inferred'}
    except FileNotFoundError:
        return {'status': 'missing', 'mutation_outcome': 'not_inferred'}
    except OSError:
        return {'status': 'refused_exit_file', 'mutation_outcome': 'not_inferred'}
    finally:
        if fd is not None:
            os.close(fd)


def write_exclusive(path, raw):
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW | os.O_CLOEXEC, 0o600)
    try:
        view = memoryview(raw)
        while view:
            count = os.write(fd, view)
            if count <= 0:
                raise OSError('short diagnostic write')
            view = view[count:]
        os.fsync(fd)
    finally:
        os.close(fd)


def collect(profile_path, output_path, destination_path, exit_path, build_outcome, env=None):
    """Return bounded diagnostics metadata; preserve source and actual build exit."""
    env = os.environ if env is None else env
    output, destination = Path(output_path), Path(destination_path)
    if (not destination.is_absolute() or destination.parent.resolve(strict=True) != destination.parent
            or destination.exists() or destination.is_symlink()):
        raise ValueError('diagnostics destination must be NEW with canonical existing parent')
    destination.mkdir(mode=0o700)
    report = {'schema': 1, 'kind': 'incomplete-build-diagnostics', 'positive_build_proof': False,
              'complete': False, 'mutation_outcome': 'not_inferred', 'files': [],
              'limits': {'per_file_bytes': PER_FILE, 'total_bytes': TOTAL, 'files': MAX_FILES,
                         'directory_entries_scanned': MAX_SCAN},
              'build_step_outcome': build_outcome if build_outcome in ('success', 'failure', 'cancelled', 'skipped', 'unknown') else 'invalid',
              'actual_exit_file': read_exit(exit_path, output),
              'owner_verified': False, 'output_status': 'not_inspected', 'scan_limited': False}
    rootfd = None
    copied, byte_count, scanned = 0, 0, 0
    try:
        try:
            profile = load_profile(profile_path)
            owner = expected_owner(env, profile['repository'])
            report['expected_owner'] = owner
        except (OSError, ValueError, TypeError, KeyError, RecursionError):
            report['output_status'] = 'profile_or_expected_identity_invalid'
            return finish(destination, report, copied, byte_count)
        try:
            canonical_output = output.is_absolute() and output.resolve() == output
        except (OSError, RuntimeError):
            canonical_output = False
        if not canonical_output:
            report['output_status'] = 'noncanonical_output_refused'
            return finish(destination, report, copied, byte_count)
        try:
            rootfd = open_absolute_directory(output)
        except FileNotFoundError:
            report['output_status'] = 'missing_output_preflight_or_setup_failure'
            return finish(destination, report, copied, byte_count)
        except OSError:
            report['output_status'] = 'output_unavailable_or_refused'
            return finish(destination, report, copied, byte_count)
        rootstat = os.fstat(rootfd)
        if rootstat.st_uid != os.geteuid():
            report['output_status'] = 'unowned_output_refused'
            return finish(destination, report, copied, byte_count)
        raw, marker = read_prefix(rootfd, 'BUILD-OWNER.json', 4096)
        try:
            valid = (raw is not None and marker['status'] == 'captured' and not marker['truncated']
                     and decode(raw) == owner)
        except (ValueError, KeyError):
            valid = False
        if not valid:
            report['output_status'] = 'missing_or_unmatched_owned_marker'
            report['owner_marker_status'] = marker['status']
            return finish(destination, report, copied, byte_count)
        initial_marker = raw
        report['owner_verified'], report['output_status'] = True, 'owned_output_partial_diagnostics'
        candidates = list(profile['files'])
        # Fixed nonrecursive directories; cap enumeration before list growth.
        for directory, pattern in profile['directories'].items():
            if scanned >= MAX_SCAN:
                report['scan_limited'] = True
                break
            fd = None
            try:
                fd = open_relative(rootfd, directory, directory=True)
                with os.scandir(fd) as entries:
                    for entry in entries:
                        if scanned >= MAX_SCAN or len(candidates) >= MAX_FILES - 1:
                            report['scan_limited'] = True
                            break
                        scanned += 1
                        if len(entry.name.encode()) <= 192 and re.fullmatch(pattern, entry.name):
                            name = directory + '/' + entry.name
                            if name not in candidates:
                                candidates.append(name)
            except (OSError, ValueError):
                report.setdefault('missing_or_refused_directories', []).append(directory)
            finally:
                if fd is not None:
                    os.close(fd)
        for name in candidates[:MAX_FILES - 1]:
            if copied >= MAX_FILES - 1 or byte_count >= TOTAL - MANIFEST_RESERVE:
                report['scan_limited'] = True
                report['files'].append({'path': name, 'status': 'budget_refused'})
                continue
            budget = min(PER_FILE, TOTAL - MANIFEST_RESERVE - byte_count)
            raw, info = read_prefix(rootfd, name, budget,
                                    allow_container_root=container_written_file(profile, name))
            info['path'] = name
            report['files'].append(info)
            if raw is None:
                continue
            target = destination / 'captured' / name
            target.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
            write_exclusive(target, raw)
            info['captured_path'] = 'captured/' + name
            copied, byte_count = copied + 1, byte_count + len(raw)
        final_marker, final_info = read_prefix(rootfd, 'BUILD-OWNER.json', 4096)
        # Original marker bytes must remain unchanged throughout collection.
        try:
            report['owner_marker_unchanged'] = (final_info.get('unchanged') is True
                and final_marker == initial_marker and not final_info.get('truncated') and decode(final_marker) == owner)
        except ValueError:
            report['owner_marker_unchanged'] = False
        return finish(destination, report, copied, byte_count)
    finally:
        if rootfd is not None:
            os.close(rootfd)


def finish(destination, report, copied, byte_count):
    report.update(copied_files=copied, captured_bytes=byte_count,
                  completeness='never a complete historical result or producer proof')
    raw = canonical(report) + b'\n'
    if len(raw) > MANIFEST_RESERVE:
        raise ValueError('bounded diagnostics manifest exceeded reserved budget')
    write_exclusive(destination / 'DIAGNOSTICS.json', raw)
    return report


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ('profile', 'output', 'destination', 'exit-file'):
        parser.add_argument('--' + name, required=True)
    parser.add_argument('--build-outcome', required=True, choices=('success', 'failure', 'cancelled', 'skipped', 'unknown'))
    args = parser.parse_args()
    collect(args.profile, args.output, args.destination, args.exit_file, args.build_outcome)


if __name__ == '__main__':
    main()
