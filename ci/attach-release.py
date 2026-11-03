#!/usr/bin/env python3
"""Attach checked same-run AIRC outputs to the ORIGINAL published release.

No tag/release creation, release PATCH, overwrite, deletion or mutation retry.
The owner publishing a release authorizes this narrow attachment path; manual
new-release publication retains its separate protected approval workflow.
"""
import importlib.util
import json
import os
from pathlib import Path
import re
import subprocess
import sys

HERE = Path(__file__).resolve().parent
sys.dont_write_bytecode = True


def load(name, filename):
    spec = importlib.util.spec_from_file_location(name, HERE / filename)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


pub = load('attachment_publisher', 'publish-release.py')
native = load('attachment_native', 'release-native.py')
REPO = 'leogr/airc'


def git(source, *args):
    return subprocess.check_output(['git', '-C', str(source), *args],
                                   stderr=subprocess.DEVNULL, timeout=30).decode().strip()


def tag_commit(api, tag):
    value = api.call('repos/' + REPO + '/git/ref/tags/' + tag)
    pub.need(isinstance(value, dict) and value.get('ref') == 'refs/tags/' + tag, 'actual release ref differs')
    obj = value.get('object', {})
    for _ in range(8):
        pub.need(isinstance(obj, dict) and pub.COMMIT.fullmatch(obj.get('sha', '')), 'bounded actual tag object missing')
        if obj.get('type') == 'commit':
            return obj['sha']
        pub.need(obj.get('type') == 'tag', 'tag does not resolve to a commit')
        annotated = api.call('repos/' + REPO + '/git/tags/' + obj['sha'])
        pub.need(isinstance(annotated, dict) and annotated.get('sha') == obj['sha'], 'annotated tag identity differs')
        obj = annotated.get('object', {})
    raise pub.Refusal('annotated release tag depth exceeds bound')


def current_release(api, event):
    release = api.call('repos/' + REPO + '/releases/' + str(event['id']))
    pub.need(isinstance(release, dict) and type(release.get('id')) is int and release['id'] == event['id']
             and release.get('tag_name') == event['tag'] and release.get('draft') is False
             and type(release.get('prerelease')) is bool
             and isinstance(release.get('published_at'), str)
             and re.fullmatch(r'[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}(?:\.[0-9]+)?Z', release['published_at']),
             'original published release changed/deleted; never replace it')
    by_tag = api.call('repos/' + REPO + '/releases/tags/' + event['tag'])
    pub.need(isinstance(by_tag, dict) and by_tag.get('id') == event['id'], 'tag now names a different release')
    pub.need(tag_commit(api, event['tag']) == event['source'], 'published tag moved from actual source S')
    return release


def actual_context(env, api, event):
    required = {'GITHUB_ACTIONS': 'true', 'GITHUB_REPOSITORY': REPO,
                'GITHUB_EVENT_NAME': 'release', 'GITHUB_RUN_ATTEMPT': '1',
                'GITHUB_JOB': 'attach'}
    pub.need(all(env.get(k) == v for k, v in required.items()),
             'first-attempt original published attachment context required')
    pub.need(re.fullmatch('[1-9][0-9]{0,19}', env.get('GITHUB_RUN_ID', '')),
             'bounded actual run identity missing')
    run = api.call('repos/' + REPO + '/actions/runs/' + env['GITHUB_RUN_ID'])
    pub.need(isinstance(run, dict) and isinstance(run.get('repository'), dict)
             and isinstance(run['repository'].get('owner'), dict)
             and isinstance(run.get('actor'), dict), 'actual run metadata objects missing')
    owner = run.get('repository', {}).get('owner', {})
    pub.need(run.get('repository', {}).get('full_name') == REPO and owner.get('login') == 'leogr'
             and type(owner.get('id')) is int and run.get('actor', {}).get('id') == owner['id']
             and run.get('event') == 'release' and run.get('head_sha') == event['source']
             and run.get('run_attempt') == 1 and run.get('status') == 'in_progress',
             'actual owner release run/source/attempt differs')
    if run.get('triggering_actor'):
        pub.need(isinstance(run['triggering_actor'], dict)
                 and run['triggering_actor'].get('id') == owner['id'], 'rerun actor is not repository owner')
    pub.need(run.get('path', '').split('@', 1)[0] == '.github/workflows/native-release.yml',
             'actual release workflow path differs')
    for name in ('attach-release.py', 'publish-release.py', 'release-native.py', 'release-native.sh'):
        pub.need(pub.api_content(api, 'ci/' + name, event['definition']) == (HERE / name).read_bytes(),
                 'running attachment/producer code differs from actual workflow W')
    workflow = pub.api_content(api, '.github/workflows/native-release.yml', event['definition']).decode()
    blocks = list(re.finditer(r'^  attach:\s*$', workflow, re.M))
    pub.need(len(blocks) == 1, 'immutable W lacks one literal attachment job')
    block = workflow[blocks[0].end():]
    following = re.search(r'^  [A-Za-z0-9_-]+:\s*$', block, re.M)
    if following:
        block = block[:following.start()]
    pub.need(re.search(r"^    if:.*github\.event_name == 'release'.*github\.event\.action == 'published'", block, re.M),
             'immutable W lacks the explicit release attachment job')
    jobs = api.list('repos/' + REPO + '/actions/runs/' + env['GITHUB_RUN_ID'] + '/attempts/1/jobs', 'jobs')
    for arch in pub.ARCHES:
        matches = [j for j in jobs if j.get('name') == 'build (' + arch + ')']
        pub.need(len(matches) == 1 and matches[0].get('status') == 'completed'
                 and matches[0].get('conclusion') == 'success'
                 and str(matches[0].get('run_id')) == env['GITHUB_RUN_ID']
                 and matches[0].get('head_sha') == event['source'],
                 'actual same-run successful native architecture missing: ' + arch)
    matches = [j for j in jobs if j.get('name') == 'attach']
    pub.need(len(matches) == 1 and matches[0].get('status') == 'in_progress',
             'actual running attachment job missing')
    current_release(api, event)


def source_policy(source, event, inputs):
    source = Path(source).resolve(strict=True)
    pub.need(git(source, 'rev-parse', 'HEAD') == event['source']
             and not git(source, 'status', '--porcelain', '--untracked-files=all'),
             'separate actual tagged source checkout is dirty or foreign')
    lock = source / 'Cargo.lock'
    pub.need(lock.is_file() and not lock.is_symlink() and lock.stat().st_size <= 4 * 1024**2,
             'bounded regular source lock required')
    policy = dict(pub.REPOS[REPO])
    first = pub.Files(Path(inputs).absolute() / 'airc-linux-amd64')
    raw = first.read('source.tar.gz')
    policy.update(source=event['source'], tree=git(source, 'rev-parse', 'HEAD^{tree}'),
                  archive=pub.sha(raw), lock=pub.sha(lock.read_bytes()))
    # The hash is not self-authority: reconstruct the entire Git tree and
    # compare it with the independently checked immutable S checkout.
    pub.source_archive(raw, policy)
    return policy


def bind_producer_recipe(plan):
    # actual_context already joins these local bytes to immutable W. Neither
    # an artifact's manifest nor its self-recorded recipe hash is authority.
    for arch in pub.ARCHES:
        files = plan['matrix'][arch]['files']
        for name in ('release-native.py', 'release-native.sh'):
            pub.need(files.read('evidence/recipe/' + name) == (HERE / name).read_bytes(),
                     'actual native producer recipe differs from immutable W: ' + arch)


def checked_assets(api, event, assets, existing, prefix):
    managed = {}
    for row in existing:
        pub.need(isinstance(row, dict), 'release asset metadata object missing')
        if row.get('name') not in assets:
            continue
        name = row['name']
        pub.need(name not in managed and type(row.get('id')) is int and row['id'] > 0
                 and row.get('state') == 'uploaded' and type(row.get('size')) is int,
                 'duplicate/incomplete managed release asset')
        local = assets[name]
        pub.need(row['size'] == local.stat().st_size, 'conflicting existing asset size; no overwrite')
        expected = pub.digest_regular(local)
        pub.need(row.get('digest') is None or row['digest'] == 'sha256:' + expected,
                 'conflicting existing asset digest; no overwrite')
        downloaded = api.root / (prefix + name)
        api.call('repos/' + REPO + '/releases/assets/' + str(row['id']), download=downloaded)
        pub.need(pub.digest_regular(downloaded) == expected
                 and downloaded.stat().st_size == row['size'],
                 'existing/public asset bytes differ from checked same-run output')
        managed[name] = row['id']
    return managed


def managed_ids(rows, names):
    result = {}
    for row in rows:
        pub.need(isinstance(row, dict), 'release asset metadata object missing')
        name = row.get('name')
        if name not in names:
            continue
        pub.need(name not in result and type(row.get('id')) is int and row['id'] > 0
                 and row.get('state') == 'uploaded' and row['id'] not in result.values(),
                 'duplicate/incomplete final managed asset identity')
        result[name] = row['id']
    return result


def attach_existing(api, event, assets):
    current_release(api, event)
    base = 'repos/' + REPO + '/releases/' + str(event['id'])
    original = api.list(base + '/assets')
    pub.need(all(isinstance(row, dict) for row in original), 'release asset metadata object missing')
    names = [row.get('name') for row in original if row.get('name') in assets]
    pub.need(len(names) == len(set(names)), 'duplicate managed release names')
    if names:
        pub.need(set(names) == set(assets), 'partial existing artifact set; inspect receipts, do not resume/replay')
        found = checked_assets(api, event, assets, original, 'reconciled-')
        current_release(api, event)
        final = api.list(base + '/assets')
        pub.need(managed_ids(final, assets) == found,
                 'existing complete asset identities changed during verification')
        return {'release_id': event['id'], 'tag': event['tag'], 'source_subject': event['source'],
                'assets': found, 'reconciled_existing_complete': True, 'effects': False,
                'historical_mutation_outcome_inferred': False, 'consumer_qualification': 'NOT RUN'}
    known, uploaded = [], {}
    try:
        for name, path in sorted(assets.items()):
            current_release(api, event)
            expected, size = pub.digest_regular(path), path.stat().st_size

            def ack(value):
                pub.need(isinstance(value, dict) and type(value.get('id')) is int and value['id'] > 0
                         and value.get('name') == name and value.get('state') == 'uploaded'
                         and type(value.get('size')) is int and value['size'] == size
                         and (value.get('digest') is None or value['digest'] == 'sha256:' + expected),
                         'attachment exact acknowledgement mismatch')

            asset = api.call('https://uploads.github.com/' + base + '/assets?name=' + name,
                             'POST', upload=path, expected=(201,), acknowledgement=ack)
            known.append({'kind': 'uploaded-asset', 'release_id': event['id'],
                          'id': asset['id'], 'name': name, 'sha256': expected})
            checked = checked_assets(api, event, {name: path}, [asset], 'verified-')
            uploaded.update(checked)
        current_release(api, event)
        final = api.list(base + '/assets')
        pub.need(managed_ids(final, assets) == uploaded,
                 'final attached asset identities changed')
        return {'release_id': event['id'], 'tag': event['tag'], 'source_subject': event['source'],
                'assets': uploaded, 'reconciled_existing_complete': False, 'effects': True,
                'historical_mutation_outcome_inferred': False, 'consumer_qualification': 'NOT RUN'}
    except pub.Uncertain as error:
        if not error.known_effects:
            error.known_effects = list(known)
        raise
    except (pub.Refusal, OSError, ValueError, TypeError, KeyError) as error:
        if known:
            raise pub.PartialPublication('Attachment stopped after acknowledged uploads; retain receipts; no replay',
                                         known) from None
        raise


def main(argv=None, env=None, api_factory=pub.GitHub):
    args = list(sys.argv[1:] if argv is None else argv)
    env = dict(os.environ if env is None else env)
    pub.need(len(args) == 4, 'usage: attach-release.py SOURCE_SHA TAG SAME_RUN_INPUT_DIR SOURCE_CHECKOUT')
    pub.need(env.get('RELEASE_MODE') == 'attach' and env.get('GH_TOKEN') and env.get('RUNNER_TEMP'),
             'explicit release attachment runner configuration missing')
    pub.need(env.get('GITHUB_ACTIONS') == 'true' and env.get('GITHUB_RUN_ATTEMPT') == '1'
             and env.get('GITHUB_JOB') == 'attach'
             and re.fullmatch('[1-9][0-9]{0,19}', env.get('GITHUB_RUN_ID', '')),
             'bounded first-attempt attachment identity required before local effects')
    event = native.release_event(env)
    pub.need(args[:2] == [event['source'], event['tag']], 'arguments do not match original published release')
    parent = Path(env['RUNNER_TEMP']).absolute()
    pub.need(parent.is_dir() and not any(p.is_symlink() for p in (parent, *parent.parents)),
             'unaliased runner scratch required')
    root = parent / ('attachment-airc-' + env.get('GITHUB_RUN_ID', 'invalid'))
    root.mkdir(mode=0o700)  # No automatic adoption of an earlier attempt.
    pub.fsync_json(root / 'attachment-attempt.json',
                   {'schema': 1, 'event_release': event, 'automatic_retry': False})
    api = api_factory(REPO, env, root)
    actual_context(env, api, event)
    policy = source_policy(args[3], event, args[2])
    definition = {'commit': event['definition'], 'repository': REPO, 'workflow': event['workflow'],
                  'run_id': env['GITHUB_RUN_ID'], 'run_attempt': '1'}
    plan = pub.validate_inputs(REPO, *args[:3], source_policy=policy, event_workflow=definition)
    bind_producer_recipe(plan)
    plan['event_release'] = event
    pub.same_run_archives(plan, api)
    assets = pub.publication_assets(plan, root)
    result = attach_existing(api, event, assets)
    try:
        pub.fsync_json(root / 'attachment-complete.json', result)
        print(json.dumps(result, sort_keys=True), flush=True)
    except OSError:
        # Every returned effect already has its exact API completion receipt.
        # A reporting failure cannot convert acknowledged uploads to uncertainty.
        print(json.dumps({'scope': 'release attachment', 'result_known': True,
                          'local_reporting_failed': True, 'result': result,
                          'automatic_retry': False}), file=sys.stderr)
        return 1
    return 0


if __name__ == '__main__':
    try:
        raise SystemExit(main())
    except pub.Uncertain as error:
        print(json.dumps({'scope': 'release attachment', 'result_known': False,
                          'known_effects': error.known_effects, 'automatic_retry': False}), file=sys.stderr)
        raise SystemExit(1)
    except pub.PartialPublication as error:
        print(json.dumps({'scope': 'release attachment', 'partial': True,
                          'known_effects': error.known_effects, 'automatic_retry': False}), file=sys.stderr)
        raise SystemExit(1)
    except (pub.Refusal, RuntimeError, OSError, ValueError, TypeError, KeyError):
        print('AIRC release attachment refused; retain receipts/diagnostics; no automatic retry', file=sys.stderr)
        raise SystemExit(1)
