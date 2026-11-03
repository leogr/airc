#!/usr/bin/env python3
"""Fail-closed native release adapter. No cleanup, retries or source writes."""
import gzip
import hashlib
import io
import json
import os
from pathlib import Path
import platform
import re
import selectors
import stat
import shutil
import struct
import subprocess
import sys
import tarfile
import time
import tomllib
import urllib.request

SOURCE = 'b76ba355863cc3eb51900a313d7647d9190eedfa'
TREE = '11b1a7aa80b6ce101ec849bb831a201f7ea1a0f0'
ARCHIVE = 'ed1df502a58411cd9574024ebcd1964c02d9a7fef14330b4f227c96957dcc21c'
LOCK = '3cb565afe685053c6f848ebde87138eaadc18c7be31458887d065eed782c3b05'
INDEX = 'sha256:c3a5ad77ff2e5ec99fffaf62cb518e61a9dcbd9e1d7526d281a380db0be3bb3b'
IMAGES = {'amd64':'sha256:e4693cd4f8e853454d5a576f6ef38166d0fa55161576d5883468a58267d1507c',
          'arm64':'sha256:ba511babd167c72f478edbfa2461f5499a7e9639e9793705e8b8090382731198'}
TARGETS = {'amd64':'x86_64-unknown-linux-musl', 'arm64':'aarch64-unknown-linux-musl'}
BINS = ('airc','aircd','airc-codex')
BUILD_ITERATIONS = (1,)
MAX_FILE = 128 * 1024**2
MAX_TOTAL = 512 * 1024**2
GIB = 1024**3
ADMITTED_DOCKER = None
DOCKER_ENV_KEYS = ('DOCKER_CONTEXT','DOCKER_HOST','DOCKER_TLS','DOCKER_TLS_VERIFY','DOCKER_CERT_PATH','DOCKER_API_VERSION')
DOCKER_INFO_FORMAT = '{'+','.join('"'+key+'":{{json .'+key+'}}' for key in ('ID','OSType','Architecture','NCPU','MemTotal','DockerRootDir','ServerVersion'))+'}'
NATIVE_FILESYSTEMS = frozenset(('ext2/ext3','ext2/ext3/ext4','ext4','xfs','btrfs','zfs'))


def require(ok, message):
    if not ok:
        raise RuntimeError(message)


def digest(data):
    return hashlib.sha256(data).hexdigest()


def write_json(path, value):
    with path.open('x') as f:
        json.dump(value, f, indent=2, sort_keys=True)
        f.write('\n')


def docker_environment():
    return {key:value for key,value in os.environ.items() if key not in DOCKER_ENV_KEYS}


def docker_argv(argv):
    require(ADMITTED_DOCKER is not None, 'Docker endpoint has not been admitted')
    require(argv and argv[0]=='docker', 'Docker command required')
    return ['docker','--host',ADMITTED_DOCKER['selection']['endpoint'],*argv[1:]]


def command(argv, cwd=None):
    is_docker=argv[0]=='docker'
    if is_docker:argv=docker_argv(argv)
    return subprocess.check_output(argv,cwd=cwd,stderr=subprocess.STDOUT,timeout=120,
        env=docker_environment() if is_docker else None).decode().strip()


def bounded_readonly(argv,limit=256*1024):
    # Only source-approved read-only context/info/stat commands use this path.
    # Bound both streams while receiving, not after an unbounded capture_output.
    proc=subprocess.Popen(argv,stdout=subprocess.PIPE,stderr=subprocess.PIPE,env=docker_environment())
    streams={'stdout':bytearray(),'stderr':bytearray()}
    deadline=time.monotonic()+30
    try:
        with selectors.DefaultSelector() as selected:
            selected.register(proc.stdout,selectors.EVENT_READ,'stdout')
            selected.register(proc.stderr,selectors.EVENT_READ,'stderr')
            while selected.get_map():
                require(time.monotonic()<deadline,'read-only Docker admission deadline exceeded')
                for key,_ in selected.select(timeout=0.25):
                    block=os.read(key.fileobj.fileno(),8192)
                    if not block:selected.unregister(key.fileobj);continue
                    streams[key.data].extend(block)
                    require(sum(len(v) for v in streams.values())<=limit,'read-only admission response exceeds bound')
        status=proc.wait(timeout=max(0.1,deadline-time.monotonic()))
    except BaseException:
        proc.kill();proc.wait(timeout=5)
        raise
    finally:
        proc.stdout.close();proc.stderr.close()
    return status,bytes(streams['stdout']),bytes(streams['stderr'])


def admission_read(argv,evidence,label,output,commands,limit=256*1024):
    status,out,err=bounded_readonly(argv,limit)
    require(status==0,'read-only native Docker admission unavailable: '+label)
    for stream,data in [('stdout',out),('stderr',err)]:
        (evidence/(label+'.'+stream)).write_bytes(data)
    commands['docker-'+label]={'argv':argv,'exit_code':status,
        **{stream:{'path':str((evidence/(label+'.'+stream)).relative_to(output)),
          'sha256':digest(data)} for stream,data in [('stdout',out),('stderr',err)]}}
    return out.decode()


def effective_selection(environment,read_context):
    context=environment.get('DOCKER_CONTEXT')
    if context:
        method='DOCKER_CONTEXT'
    elif environment.get('DOCKER_HOST'):
        return {'method':'DOCKER_HOST','context':None,'endpoint':environment['DOCKER_HOST']}
    else:
        method='default-context'
        context=read_context(None).strip()
    require(isinstance(context,str) and re.fullmatch(r'[A-Za-z0-9_.-]{1,128}',context),'bounded context name required')
    inspected=json.loads(read_context(context))
    require(isinstance(inspected,list) and len(inspected)==1 and inspected[0].get('Name')==context,'context inspection identity mismatch')
    endpoint=inspected[0].get('Endpoints',{}).get('docker',{}).get('Host')
    return {'method':method,'context':context,'endpoint':endpoint}


def local_endpoint(endpoint):
    require(isinstance(endpoint,str) and endpoint.startswith('unix:///') and len(endpoint)<=256
            and not any(c.isspace() or ord(c)<32 for c in endpoint)
            and not any(c in endpoint for c in ('?','#','%','\\')),'effective Docker endpoint must be a local UNIX socket')
    socket=Path(endpoint[len('unix://'):]).resolve(strict=True)
    require(stat.S_ISSOCK(socket.stat().st_mode),'Docker endpoint is not a local UNIX socket')
    return 'unix://'+str(socket)


def daemon_identity(info,arch):
    aliases={'x86_64':'amd64','amd64':'amd64','aarch64':'arm64','arm64':'arm64'}
    require(isinstance(info,dict) and info.get('OSType')=='linux' and aliases.get(info.get('Architecture'))==arch,
            'actual Docker daemon must be native Linux for requested architecture')
    require(type(info.get('NCPU')) is int and info['NCPU']>=2 and type(info.get('MemTotal')) is int
            and info['MemTotal']>=4*GIB,'Docker daemon needs at least 2 CPUs and 4GiB RAM')
    require(isinstance(info.get('ID'),str) and info['ID'] and isinstance(info.get('DockerRootDir'),str)
            and Path(info['DockerRootDir']).is_absolute(),'Docker daemon identity/storage unavailable')
    root=Path(info['DockerRootDir']).resolve(strict=True)
    require(root.is_dir(),'local Docker backing root unavailable')
    return {'id':info['ID'],'os':'linux','architecture':arch,'cpus':info['NCPU'],
            'mem_total':info['MemTotal'],'reported_docker_root_dir':info['DockerRootDir'],'docker_root_dir':str(root)}


def admit_docker(output,arch,commands,environment=None):
    global ADMITTED_DOCKER
    require(ADMITTED_DOCKER is None,'Docker admission cannot be replaced')
    evidence=output/'evidence/docker';evidence.mkdir(parents=True)
    def read_context(context):
        return admission_read(['docker','context','show'] if context is None else ['docker','context','inspect',context],
            evidence,'context-show' if context is None else 'context-inspect',output,commands,64*1024)
    selected_environment=os.environ if environment is None else environment
    selection=effective_selection(selected_environment,read_context)
    selection.update({'context_set':bool(selected_environment.get('DOCKER_CONTEXT')),
                      'host_set':bool(selected_environment.get('DOCKER_HOST')),
                      'host_ignored':bool(selected_environment.get('DOCKER_CONTEXT') and selected_environment.get('DOCKER_HOST'))})
    selected_endpoint=selection['endpoint']
    selection['endpoint']=local_endpoint(selected_endpoint)
    selection['selected_endpoint']=selected_endpoint
    socket_path=selection['endpoint'][len('unix://'):]
    socket_format='{"path":"%n","mode_hex":"%f","device":%d,"inode":%i,"uid":%u,"gid":%g}'
    socket=json.loads(admission_read(['stat','-Lc',socket_format,socket_path],evidence,'socket-stat',output,commands,4096))
    require(socket.get('path')==socket_path and stat.S_ISSOCK(int(socket.get('mode_hex','0'),16)), 'raw local socket stat mismatch')
    bound=['docker','--host',selection['endpoint']]
    info=json.loads(admission_read(bound+['info','--format',DOCKER_INFO_FORMAT],evidence,'daemon-info',output,commands))
    daemon=daemon_identity(info,arch)
    storage={}
    for key,path in [('output',output.resolve(strict=True)),('docker',Path(daemon['docker_root_dir']))]:
        filesystem=admission_read(['stat','-f','-c','%T',str(path)],evidence,key+'-filesystem',output,commands,4096).strip()
        require(filesystem in NATIVE_FILESYSTEMS,'native local backing filesystem required: '+key)
        free=shutil.disk_usage(path).free
        require(free>=20*GIB,'prerequisite: 20GiB free '+key+' backing storage')
        storage[key]={'path':str(path),'filesystem':filesystem,'free_bytes':free}
    resources(output,20)
    admission={'schema':1,'selection':selection,'daemon':daemon,'storage':storage,
        'floors':{'admission_disk_gib':20,'before_pull_gib':16,'build_gib':14,'ram_total_gib':4,'ram_available_gib':2},
        'bound_argv':bound,'docker_env_scrubbed':True,'socket':socket,
        'monitoring':{'path':'evidence/docker/resources.jsonl','paths':[storage['output']['path'],storage['docker']['path']],'minimum_disk_gib':14}}
    write_json(evidence/'admission.json',admission)
    ADMITTED_DOCKER=admission
    resources(output,20)
    return admission


def git(source, *args):
    return command(['git','-C',str(source),*args])


def resources(path, disk_gib):
    paths=[Path(path)]
    if ADMITTED_DOCKER is not None:paths.append(Path(ADMITTED_DOCKER['daemon']['docker_root_dir']))
    disks=[{'path':str(backing),'free_bytes':shutil.disk_usage(backing).free} for backing in paths]
    for item in disks:
        require(item['free_bytes'] >= disk_gib * GIB, f'prerequisite: {disk_gib}GiB free backing storage: '+item['path'])
    mem = dict(re.findall(r'^(MemTotal|MemAvailable):\s+(\d+)', Path('/proc/meminfo').read_text(), re.M))
    require(int(mem['MemTotal']) * 1024 >= 4 * GIB and int(mem['MemAvailable']) * 1024 >= 2 * GIB,
            'prerequisite: 4GiB RAM total and 2GiB available')
    if ADMITTED_DOCKER is not None:
        log=Path(ADMITTED_DOCKER['storage']['output']['path'])/ADMITTED_DOCKER['monitoring']['path']
        require(not log.exists() or log.stat().st_size<2*1024**2,'resource monitor evidence bound exceeded')
        with log.open('a') as stream:
            stream.write(json.dumps({'disk_floor_gib':disk_gib,'disks':disks,
                'mem_total':int(mem['MemTotal'])*1024,'mem_available':int(mem['MemAvailable'])*1024},sort_keys=True)+'\n')


def registry_bytes(kind, sha):
    # Anonymous public pull token stays only in memory; never write auth headers.
    token_url='https://auth.docker.io/token?service=registry.docker.io&scope=repository:library/rust:pull'
    with urllib.request.urlopen(token_url,timeout=120) as response:
        token=json.loads(response.read(65536))['token']
    request=urllib.request.Request('https://registry-1.docker.io/v2/library/rust/'+kind+'/'+sha,
        headers={'Authorization':'Bearer '+token,'Accept':','.join([
          'application/vnd.oci.image.index.v1+json','application/vnd.docker.distribution.manifest.list.v2+json',
          'application/vnd.oci.image.manifest.v1+json','application/vnd.docker.distribution.manifest.v2+json'])})
    with urllib.request.urlopen(request,timeout=120) as response:
        data=response.read(8*1024**2+1)
    require(len(data)<=8*1024**2 and 'sha256:'+digest(data)==sha, 'raw OCI bytes digest mismatch')
    return data


def export_publication(output, arch, commands):
    """New bounded export; no directory recursion or generic workspace upload."""
    export=output/'publication'
    export.mkdir(mode=0o700)
    paths={'CI-MANIFEST.json',f'build-proof-{arch}.json',f'airc-linux-{arch}.tar.gz','source.tar.gz',
           'pull-intent.json','pull.log','image.json','image-manifest.json','image-index.json',
           'image-index.raw.json','image-manifest.raw.json','image-config.raw.json',
           'evidence/recipe/release-native.py','evidence/recipe/release-native.sh','evidence/docker/admission.json','evidence/docker/resources.jsonl'}
    for record in commands.values():
        for stream in ('stdout','stderr'):
            paths.add(record[stream]['path'])
    for iteration in BUILD_ITERATIONS:
        prefix=f'build-{iteration}/'
        for name in ('create-intent.json','create-result.json','start-intent.json','start-result.json',
                     'inspect-completed.json','container.log','inner.sh',f'airc-linux-{arch}.tar.gz'):
            paths.add(prefix+name)
        for name in ('rustc.txt','cargo.txt','apk-packages.txt'):
            paths.add(prefix+'build/'+name)
        for label in ('upstream-check','build','release-tests'):
            for suffix in ('stdout','stderr','status'):
                paths.add(prefix+'build/'+label+'.'+suffix)
        for binary in BINS:
            for suffix in ('version','help'):
                paths.add(prefix+'build/'+binary+'.'+suffix)
    total=0
    for relative in sorted(paths):
        require(not relative.startswith('/') and '..' not in relative.split('/'), 'unsafe export path')
        source=output/relative
        require(source.is_file() and not source.is_symlink() and source.resolve().is_relative_to(output.resolve()), 'export must be confined regular file')
        require(source.stat().st_size<=MAX_TOTAL, 'oversized export evidence')
        total+=source.stat().st_size
        require(total<=2*1024**3, 'export evidence total exceeds 2GiB')
        target=export/relative
        target.parent.mkdir(parents=True,exist_ok=True)
        with source.open('rb') as reader, target.open('xb') as writer:
            shutil.copyfileobj(reader,writer)
        target.chmod(0o644)


def source_members(data, commit=None, archive_hash=None):
    commit = SOURCE if commit is None else commit
    archive_hash = ARCHIVE if archive_hash is None else archive_hash
    require(digest(data) == archive_hash, 'source archive hash mismatch')
    files = {}
    with tarfile.open(fileobj=io.BytesIO(data), mode='r:gz') as t:
        for m in t:
            parts = m.name.split('/')
            require(parts[0] == 'airc-' + commit, 'unexpected source prefix')
            n = '/'.join(parts[1:])
            require(not n.startswith('/') and '..' not in parts, 'unsafe source path')
            if m.isdir():
                continue
            require(m.isfile() and not m.issparse() and n and n not in files, 'nonregular/duplicate source member')
            require(0 <= m.size <= MAX_FILE, 'oversized source member')
            require(sum(len(v[0]) for v in files.values()) + m.size <= MAX_TOTAL, 'source total limit')
            files[n] = (t.extractfile(m).read(), 0o755 if m.mode & 0o111 else 0o644)
    require(sum(len(v[0]) for v in files.values()) <= MAX_TOTAL, 'source total limit')
    return files


def prepare_source_git(snapshot, expected_tree):
    # check-docs.py uses git ls-files. Give this fresh, verified archive its
    # own uncommitted index, without copying host/worktree Git state or hooks.
    require(re.fullmatch('[0-9a-f]{40}', expected_tree)
            and not (snapshot/'.git').exists() and not (snapshot/'.git').is_symlink(),
            'new source Git metadata and exact selected tree required')
    command(['git','init','--quiet','--template=','--object-format=sha1',
             '--initial-branch=ci-snapshot',str(snapshot)])
    command(['git','-C',str(snapshot),'add','--force','--all','--','.'])
    require(command(['git','-C',str(snapshot),'write-tree']) == expected_tree,
            'source snapshot Git tree differs from verified source')


def static_elf(data, arch):
    require(len(data) >= 64 and data[:6] == b'\x7fELF\x02\x01', 'expected ELF64 little endian')
    require(struct.unpack_from('<H',data,18)[0] == {'amd64':62,'arm64':183}[arch], 'ELF architecture mismatch')
    off = struct.unpack_from('<Q',data,32)[0]
    size, count = struct.unpack_from('<HH',data,54)
    require(size >= 56 and count <= 4096 and off + size * count <= len(data), 'invalid ELF program headers')
    for i in range(count):
        base = off + i * size
        kind = struct.unpack_from('<I',data,base)[0]
        require(kind != 3, 'ELF interpreter forbidden')
        if kind == 2:
            start = struct.unpack_from('<Q',data,base+8)[0]
            length = struct.unpack_from('<Q',data,base+32)[0]
            require(length % 16 == 0 and start + length <= len(data), 'invalid dynamic table')
            for pos in range(start,start+length,16):
                tag = struct.unpack_from('<q',data,pos)[0]
                require(tag != 1, 'ELF NEEDED dependency forbidden')


def package(path, files):
    require(all(not n.startswith('/') and '..' not in n.split('/') and len(data) <= MAX_FILE
                for n,(data,mode) in files.items()), 'unsafe archive payload')
    require(len(files) <= 10000 and sum(len(v[0]) for v in files.values()) <= MAX_TOTAL, 'archive bounds')
    with path.open('xb') as raw:
        with gzip.GzipFile(filename='',fileobj=raw,mode='wb',mtime=0,compresslevel=9) as gz:
            with tarfile.open(fileobj=gz,mode='w',format=tarfile.USTAR_FORMAT) as t:
                for name,(data,mode) in sorted(files.items()):
                    require(mode in (0o644,0o755), 'unsafe payload mode')
                    member = tarfile.TarInfo(name)
                    member.size, member.mode = len(data), mode
                    member.uid = member.gid = member.mtime = 0
                    member.uname = member.gname = ''
                    t.addfile(member,io.BytesIO(data))


NANOSECOND_DATE_CHECK = r'''import subprocess
import time

before = time.time_ns()
raw = subprocess.check_output(['date', '+%s%N'], text=True, timeout=5).strip()
after = time.time_ns()
if not (raw.isascii() and raw.isdecimal() and before <= int(raw) <= after):
    raise RuntimeError('named_sessions requires nanosecond date; install GNU coreutils')
print('nanosecond date capability: OK')
'''


INNER = r'''set -eu
export CARGO_HOME=/cache/cargo RUSTUP_HOME=/usr/local/rustup CARGO_TARGET_DIR=/build/target
export CARGO_BUILD_TARGET=$TARGET
export CARGO_BUILD_JOBS=2
export SOURCE_DATE_EPOCH=0 TZ=UTC LC_ALL=C PYTHONDONTWRITEBYTECODE=1
# Only this independently tree-verified read-only mount is trusted by root's
# Git process; no account/global configuration is written.
export GIT_CONFIG_COUNT=1 GIT_CONFIG_KEY_0=safe.directory GIT_CONFIG_VALUE_0=/source
export RUSTFLAGS='--remap-path-prefix=/source=airc-source --remap-path-prefix=/build=airc-build -C target-feature=+crt-static'
apk add --no-cache build-base linux-headers binutils python3 make bash git openssl perl coreutils
python3 - <<'AIRC_NANOSECOND_DATE_CHECK'
''' + NANOSECOND_DATE_CHECK + r'''AIRC_NANOSECOND_DATE_CHECK
apk info -vv > /build/apk-packages.txt
rustup component add rustfmt clippy --toolchain 1.99.0
rustc -vV > /build/rustc.txt
cargo -vV > /build/cargo.txt
rustc -vV | grep -Fx 'release: 1.99.0'
rustc -vV | grep -Fx "host: $TARGET"
test "$(uname -m)" = "$NATIVE_MACHINE"
cd /source
record() {
  label=$1; shift
  status=0
  "$@" > /build/$label.stdout 2> /build/$label.stderr || status=$?
  printf '%s\n' "$status" > /build/$label.status
  test "$status" = 0
}
record upstream-check make check
record build cargo build --locked --release --bins
record release-tests cargo test --locked --release --all-targets
for name in airc aircd airc-codex; do
  /build/target/$TARGET/release/$name --version > /build/$name.version
  # All three Clap entrypoints use Cargo's package name, airc, for --version.
  grep -Fx "airc $EXPECTED_VERSION" /build/$name.version
  /build/target/$TARGET/release/$name --help > /build/$name.help
  test -s /build/$name.help
done
# Assert concrete behavioral tests actually ran, not only a zero-test harness.
for test in tls_requires_trust_and_checks_hostname proxy_tls_requires_trust_and_checks_hostname history_survives_restart_and_reports_lost_references schema_one_migrates_without_losing_state_and_newer_versions_are_refused unix_mode_serves_irc_and_artifacts_without_tcp jsonl_v2_assembles_multiline_and_reports_correlated_receipts https_artifact_requires_trust_and_checks_hostname; do
  grep -E "test $test \\.\\.\\. ok$" /build/release-tests.stdout
done
'''


def container_start(name, output, work):
    # Intent/result records surround every one-use mutation. A lost reply stops;
    # neither docker create nor start nor stop is ever automatically replayed.
    write_json(work/'start-intent.json', {'container':name,'action':'start --attach','endpoint':ADMITTED_DOCKER['selection']['endpoint'],'replay':False})
    with (work/'container.log').open('xb') as log:
        p = subprocess.Popen(docker_argv(['docker','start','--attach',name]),stdout=log,stderr=subprocess.STDOUT,env=docker_environment())
        try:
            deadline = time.monotonic()+7200
            while p.poll() is None:
                require(time.monotonic() < deadline, 'container deadline exceeded')
                resources(output, 14)
                require(log.tell() <= 256*1024**2, 'container log budget exceeded')
                time.sleep(5)
        except BaseException:
            write_json(work/'stop-intent.json', {'container':name,'action':'stop','endpoint':ADMITTED_DOCKER['selection']['endpoint'],'replay':False})
            # Exact uniquely created owned container only. Failure is uncertain.
            try:
                result = subprocess.run(docker_argv(['docker','stop','--time','30',name]),stdout=log,stderr=log,timeout=45,env=docker_environment())
                write_json(work/'stop-result.json', {'exit_code':result.returncode})
            finally:
                raise
    write_json(work/'start-result.json', {'exit_code':p.returncode})
    require(p.returncode == 0, 'container execution failed; retain evidence, do not replay')
    raw=command(['docker','inspect','--format','{"Id":{{json .Id}},"Image":{{json .Image}},"State":{{json .State}}}',name])
    inspected=json.loads(raw)
    (work/'inspect-completed.json').write_text(raw+'\n')
    created=json.loads((work/'create-result.json').read_text())
    require(inspected['Id']==created['container_id'] and inspected['State']['Status']=='exited'
            and inspected['State']['ExitCode']==0 and not inspected['State'].get('OOMKilled'), 'container exit not proven')


def release_event(env):
    """The original published event binds S and release ID; W is separate."""
    require(env.get('GITHUB_EVENT_NAME') == 'release'
            and env.get('GITHUB_REPOSITORY') == 'leogr/airc', 'AIRC release event required')
    path = Path(env.get('GITHUB_EVENT_PATH', ''))
    require(path.is_file() and path.stat().st_size <= 1024**2, 'bounded original release event missing')
    def unique(pairs):
        result = {}
        for key, value in pairs:
            require(key not in result, 'duplicate original release event field')
            result[key] = value
        return result
    event = json.loads(path.read_bytes(), object_pairs_hook=unique)
    require(isinstance(event, dict) and isinstance(event.get('release'), dict)
            and isinstance(event.get('repository'), dict), 'original release event object required')
    release = event.get('release', {})
    tag = release.get('tag_name', '')
    require(event.get('action') == 'published' and event.get('repository', {}).get('full_name') == 'leogr/airc'
            and type(release.get('id')) is int and release['id'] > 0 and release.get('draft') is False
            and isinstance(tag, str) and re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9._-]{0,95}', tag) and '..' not in tag
            and not tag.endswith(('.', '.lock')), 'original published release identity invalid')
    require(env.get('GITHUB_REF') == 'refs/tags/' + tag
            and re.fullmatch(r'[0-9a-f]{40}', env.get('GITHUB_SHA', ''))
            and env.get('SOURCE_SUBJECT') == env['GITHUB_SHA'], 'release tag/source context differs')
    require(re.fullmatch(r'[0-9a-f]{40}', env.get('BUILD_DEFINITION', ''))
            and env.get('BUILD_DEFINITION') == env.get('GITHUB_WORKFLOW_SHA')
            and env.get('GITHUB_WORKFLOW_REF') == 'leogr/airc/.github/workflows/native-release.yml@refs/tags/' + tag,
            'actual workflow W/ref missing; do not substitute tagged source S')
    return {'id': release['id'], 'tag': tag, 'source': env['GITHUB_SHA'],
            'definition': env['BUILD_DEFINITION'], 'workflow': env['GITHUB_WORKFLOW_REF']}


def selected_source(source, sha, env):
    if env.get('GITHUB_EVENT_NAME') == 'release':
        event = release_event(env)
        require(sha == event['source'], 'release argument differs from actual tagged S')
        require(git(source, 'rev-parse', 'HEAD') == sha, 'release source checkout differs from S')
        lock = source / 'Cargo.lock'
        require(lock.is_file() and not lock.is_symlink() and lock.stat().st_size <= 4 * 1024**2,
                'bounded regular committed lockfile required')
        return {'source': sha, 'tree': git(source, 'rev-parse', 'HEAD^{tree}'),
                'lock': digest(lock.read_bytes()), 'archive': None}
    require(sha == SOURCE, 'manual dispatch retains fixed reviewed source')
    return {'source': SOURCE, 'tree': TREE, 'archive': ARCHIVE, 'lock': LOCK}


def build_owner(arch, source_sha=SOURCE):
    """Closed public ownership identity, checked before creating the output."""
    definition=os.environ.get('BUILD_DEFINITION','')
    require(os.environ.get('GITHUB_REPOSITORY')=='leogr/airc'
            and re.fullmatch(r'[0-9a-f]{40}',definition)
            and os.environ.get('SOURCE_SUBJECT')==source_sha
            and arch in IMAGES and os.environ.get('BUILD_ARCH')==arch,
            'validated source/workflow/architecture identity required for output ownership')
    if os.environ.get('GITHUB_EVENT_NAME') == 'release':
        event = release_event(os.environ)
        require(event['source'] == source_sha, 'release owner source mismatch')
    else:
        require(os.environ.get('GITHUB_SHA') == definition and source_sha == SOURCE,
                'manual build definition/source identity mismatch')
    run=os.environ.get('GITHUB_RUN_ID','');attempt=os.environ.get('GITHUB_RUN_ATTEMPT','')
    require(re.fullmatch(r'[1-9][0-9]{0,19}',run) and re.fullmatch(r'[1-9][0-9]{0,9}',attempt),
            'bounded positive CI run identity required for output ownership')
    return {'schema':1,'repository':'leogr/airc','source_subject':source_sha,
            'workflow_definition':definition,'run_id':run,'run_attempt':attempt,'arch':arch}


def initialize_output(output,arch,source_sha=SOURCE):
    owner=build_owner(arch,source_sha)
    output.mkdir(mode=0o700)
    # First file in our new output, before any build/registry/Docker effects.
    # Ownership is local provenance, never a claim that a mutation completed.
    with (output/'BUILD-OWNER.json').open('x') as stream:
        json.dump(owner,stream,sort_keys=True,indent=2);stream.write('\n')
        stream.flush();os.fsync(stream.fileno())
    for directory in (output,output.parent):
        descriptor=os.open(directory,os.O_RDONLY|os.O_DIRECTORY)
        try:os.fsync(descriptor)
        finally:os.close(descriptor)


def main(argv):
    require(len(argv)==5, 'usage: release-native.sh SOURCE_SHA ARCH SOURCE_CHECKOUT OUTPUT_NEW_DIR')
    _, sha, arch, src_arg, out_arg = argv
    require(re.fullmatch(r'[0-9a-f]{40}', sha) and arch in IMAGES, 'invalid source or architecture')
    require(platform.system()=='Linux' and platform.machine()=={'amd64':'x86_64','arm64':'aarch64'}[arch], 'native Linux host required')
    source = Path(src_arg).resolve(strict=True)
    subject = selected_source(source, sha, os.environ)
    requested = Path(out_arg).absolute()
    output = requested.parent.resolve(strict=True)/requested.name
    require(',' not in str(output), 'comma in Docker bind path forbidden')
    require(not output.exists() and output.parent.is_dir(), 'output must be a new directory under existing parent')
    require(source != output and source not in output.parents and output not in source.parents, 'output/source overlap')
    definitions = Path(__file__).resolve().parent.parent
    defroot = Path(git(definitions,'rev-parse','--show-toplevel'))
    require(defroot != source, 'build-definition checkout must be separate')
    require(defroot not in output.parents and output not in defroot.parents, 'output/definition overlap')
    require(git(source,'rev-parse','HEAD')==subject['source'] and git(source,'rev-parse','HEAD^{tree}')==subject['tree'], 'source commit/tree mismatch')
    require(not git(source,'status','--porcelain','--untracked-files=all'), 'source checkout dirty')
    require(not git(defroot,'status','--porcelain','--untracked-files=all'), 'build definitions dirty')
    require(os.environ.get('BUILD_DEFINITION')==git(defroot,'rev-parse','HEAD'), 'build-definition SHA not pinned by workflow')
    for key in ('GITHUB_RUN_ID','GITHUB_RUN_ATTEMPT','GITHUB_WORKFLOW_REF','GITHUB_SHA','GITHUB_REPOSITORY','RUNNER_NAME'):
        require(os.environ.get(key), f'CI identity missing {key}')
    require(os.environ['GITHUB_REPOSITORY']=='leogr/airc', 'workflow repository mismatch')
    if os.environ.get('GITHUB_EVENT_NAME') == 'release':
        release_event(os.environ)
    else:
        require(os.environ['GITHUB_SHA']==os.environ['BUILD_DEFINITION'], 'manual workflow identity mismatch')
    resources(output.parent,20)
    initialize_output(output,arch,subject['source'])
    try:
        release(source,output,defroot,arch,subject)
    except BaseException as e:
        write_json(output/'FAILED.json', {'error':str(e),'replay':False,'status':'prerequisite/failed/uncertain; inspect retained containers and evidence'})
        raise


def release(source, output, definitions, arch, subject=None):
    subject = subject or {'source': SOURCE, 'tree': TREE, 'archive': ARCHIVE, 'lock': LOCK}
    source_sha, source_tree, archive_sha, lock_sha = (subject[key] for key in ('source', 'tree', 'archive', 'lock'))
    channel = 'published-event-release' if os.environ.get('GITHUB_EVENT_NAME') == 'release' else 'candidate-prerelease'
    commands={}
    hostdir=output/'evidence/host';hostdir.mkdir(parents=True)
    for label,arg in [('system','-s'),('machine','-m'),('identity','-a')]:
        value=run_evidence(['uname',arg],hostdir,label,output,commands,'host-'+label)
        if label=='system':require(value.strip()=='Linux','actual host OS mismatch')
        if label=='machine':require(value.strip()=={'amd64':'x86_64','arm64':'aarch64'}[arch],'actual host CPU mismatch')
    recipedir=output/'evidence/recipe';recipedir.mkdir()
    for name in ('release-native.py','release-native.sh'):
        (recipedir/name).write_bytes(Path(__file__).with_name(name).read_bytes())
    url = f'https://codeload.github.com/leogr/airc/tar.gz/{source_sha}'
    with urllib.request.urlopen(url,timeout=120) as response:
        data = response.read(4*1024**2+1)
    require(len(data)<=4*1024**2, 'source archive download budget')
    # A release tag authorizes its actual immutable S. Its archive is also
    # compared against EVERY tracked checkout byte/mode below, not trusted as
    # its own source description. Manual dispatch keeps its independent pin.
    archive_sha = archive_sha or digest(data)
    files = source_members(data, source_sha, archive_sha)
    (output/'source.tar.gz').write_bytes(data)
    require(digest(files['Cargo.lock'][0])==lock_sha, 'lockfile mismatch')
    tracked = command(['git','-C',str(source),'ls-files','-z']).split('\0')
    require(set(filter(None,tracked))==set(files), 'source archive/checkout inventory mismatch')
    for n,(content,mode) in files.items():
        p=source/n
        require(not p.is_symlink() and p.is_file() and p.read_bytes()==content, f'source checkout differs: {n}')
        require(bool(p.stat().st_mode & 0o111)==bool(mode & 0o111), f'source mode differs: {n}')
    version = tomllib.loads(files['Cargo.toml'][0].decode())['package']['version']
    require(isinstance(version, str) and len(version) <= 128
            and re.fullmatch(r'[0-9]+\.[0-9]+\.[0-9]+(?:[-+][0-9A-Za-z.+-]+)?', version),
            'bounded literal package version required')
    admit_docker(output,arch,commands)
    image = 'docker.io/library/rust@'+IMAGES[arch]
    resources(output,16)
    write_json(output/'pull-intent.json', {'image':image,'replay':False})
    pulled=command(['docker','pull','--platform','linux/'+arch,image])
    (output/'pull.log').write_text(pulled+'\n')
    inspection=json.loads(command(['docker','image','inspect',image]))[0]
    require(inspection['Architecture']==arch and inspection['Os']=='linux' and any(v.endswith('@'+IMAGES[arch]) for v in inspection['RepoDigests']), 'image identity mismatch')
    write_json(output/'image.json',inspection)
    index_raw=registry_bytes('manifests',INDEX)
    manifest_raw=registry_bytes('manifests',IMAGES[arch])
    manifest=json.loads(manifest_raw);index=json.loads(index_raw)
    config_raw=registry_bytes('blobs',manifest['config']['digest'])
    config=json.loads(config_raw)
    require(config.get('architecture')==arch and config.get('os')=='linux', 'raw OCI config architecture mismatch')
    for name,data in [('image-index.raw.json',index_raw),('image-manifest.raw.json',manifest_raw),('image-config.raw.json',config_raw)]:
        (output/name).write_bytes(data)
    require(manifest.get('schemaVersion')==2 and manifest.get('config',{}).get('digest')==inspection['Id'], 'OCI manifest/config identity mismatch')
    require(any(m.get('digest')==IMAGES[arch] and m.get('platform',{}).get('architecture')==arch
                and m.get('platform',{}).get('os')=='linux' for m in index.get('manifests',[])), 'OCI index/native manifest mismatch')
    write_json(output/'image-manifest.json',manifest)
    write_json(output/'image-index.json',index)
    packages=[]
    for iteration in BUILD_ITERATIONS:
        resources(output,14)
        work=output/f'build-{iteration}'
        work.mkdir()
        snapshot=work/'source'; snapshot.mkdir()
        for n,(content,mode) in files.items():
            p=snapshot/n; p.parent.mkdir(parents=True,exist_ok=True); p.write_bytes(content); p.chmod(mode)
        prepare_source_git(snapshot, source_tree)
        build=work/'build'; build.mkdir()
        cache=work/'cache'; cache.mkdir()
        script=work/'inner.sh'; script.write_text(INNER)
        name=f'airc-{arch}-{os.environ["GITHUB_RUN_ID"]}-{os.environ["GITHUB_RUN_ATTEMPT"]}-{iteration}'
        require(re.fullmatch(r'[a-zA-Z0-9_.-]+',name), 'invalid container name')
        args=['docker','create','--name',name,'--platform','linux/'+arch,'--network','bridge',
              '--mount',f'type=bind,src={snapshot},dst=/source,readonly',
              '--mount',f'type=bind,src={build},dst=/build', '--mount',f'type=bind,src={cache},dst=/cache',
              '--mount',f'type=bind,src={script},dst=/inner.sh,readonly',
              '--env','TARGET='+TARGETS[arch],'--env','NATIVE_MACHINE='+platform.machine(),
              '--env','EXPECTED_VERSION='+version,
              image,'sh','/inner.sh']
        write_json(work/'create-intent.json', {'argv':docker_argv(args),'replay':False})
        cid=command(args)
        require(re.fullmatch('[a-f0-9]{64}',cid), 'container create reply uncertain')
        write_json(work/'create-result.json', {'container_id':cid,'name':name})
        container_start(name,output,work)
        for label,argv in [('upstream-check',['make','check']),('build',['cargo','build','--locked','--release','--bins']),('release-tests',['cargo','test','--locked','--release','--all-targets'])]:
            require((build/(label+'.status')).read_text().strip()=='0', 'missing successful command receipt')
            commands[f'{iteration}-{label}']={'argv':argv,'exit_code':0,
                **{stream:{'path':str((build/(label+'.'+stream)).relative_to(output)), 'sha256':digest((build/(label+'.'+stream)).read_bytes())} for stream in ('stdout','stderr')}}
        payload={n:v for n,v in files.items() if n=='LICENSE' or n.startswith(('skills/airc/','integrations/claude-plugin/'))}
        require('LICENSE' in payload and 'skills/airc/SKILL.md' in payload and any(n.startswith('integrations/claude-plugin/') for n in payload), 'missing source payload')
        for binary in BINS:
            binary_path=build/'target'/TARGETS[arch]/'release'/binary
            require(binary_path.stat().st_size <= MAX_FILE, 'oversized binary')
            content=binary_path.read_bytes()
            elf_cmd=[sys.executable,str(Path(__file__).resolve()),'--check-elf',str(build/'target'/TARGETS[arch]/'release'/binary),arch]
            run_evidence(elf_cmd,work,binary+'.elf',output,commands,f'{iteration}-{binary}-elf')
            payload['bin/'+binary]=(content,0o755)
            # Execute actual release ELF natively on host as a libc/base boundary check.
            actual_version=run_evidence([str(build/'target'/TARGETS[arch]/'release'/binary),'--version'],work,binary+'.version',output,commands,f'{iteration}-{binary}-version')
            require(actual_version.strip()=='airc '+version, 'native package version mismatch')
            require(run_evidence([str(build/'target'/TARGETS[arch]/'release'/binary),'--help'],work,binary+'.help',output,commands,f'{iteration}-{binary}-help'), 'empty native help')
        metadata={'repository':'leogr/airc','source_commit':source_sha,'source_tree':source_tree,'cargo_lock_sha256':lock_sha,'architecture':arch,'libc':'static-musl'}
        payload['RELEASE-IDENTITY.json']=((json.dumps(metadata,sort_keys=True)+'\n').encode(),0o644)
        archive=work/f'airc-linux-{arch}.tar.gz'
        package(archive,payload)
        packages.append((archive,payload))
    artifact=output/packages[0][0].name
    artifact.write_bytes(packages[0][0].read_bytes())
    recipe = digest((Path(__file__).read_bytes()+Path(__file__).with_suffix('.sh').read_bytes()))
    proof={'schema':1,'repository':'leogr/airc','source_commit':source_sha,'source_tree':source_tree,'source_archive_sha256':archive_sha,'cargo_lock_sha256':lock_sha,'version':version,
           'architecture':arch,'build_definition':{'commit':git(definitions,'rev-parse','HEAD'),'tree':git(definitions,'rev-parse','HEAD^{tree}'),'recipe_sha256':recipe},
           'github_run_id':os.environ['GITHUB_RUN_ID'],'github_run_attempt':os.environ['GITHUB_RUN_ATTEMPT'],'github_workflow_ref':os.environ['GITHUB_WORKFLOW_REF'],
           'docker_admission':{'path':'evidence/docker/admission.json','sha256':digest((output/'evidence/docker/admission.json').read_bytes())},
           'runner':{'name':os.environ['RUNNER_NAME'],'machine':platform.machine(),'system':platform.system()},
           'toolchain':{'image_digest':IMAGES[arch],'image_index':INDEX,'image_config':inspection['Id'],'rustc':(output/'build-1/build/rustc.txt').read_text(),'target':TARGETS[arch]},
           'reproducible':False,'reproducibility_check':'not_run',
           'builds':[{'iteration':i,'archive_sha256':digest(p.read_bytes())} for i,(p,_) in enumerate(packages,1)],
           'qualification':{'upstream_checks':True,'release_behavior_tests':True,'static_elf':True,'native_host_smoke':True,'custom_base':'not_run'},
           'producer_build_complete':True,'publication_channel':channel,'consumer_qualification':'not_run',
           'consumer_prerequisites':['Reviewed supported custom-base and yoloctl consumer/runtime qualification before production pins or consumption.'],
           'artifacts':[{'name':artifact.name,'sha256':digest(artifact.read_bytes()),'size':artifact.stat().st_size}],
           'commands':commands,
           'payloads':[{'path':n,'sha256':digest(v[0]),'size':len(v[0]),'mode':oct(v[1])} for n,v in sorted(packages[0][1].items())]}
    proof_path=output/f'build-proof-{arch}.json'
    write_json(proof_path,proof)
    write_json(output/'CI-MANIFEST.json', {'schema':1,'repository':'leogr/airc','architecture':arch,
        'source_subject':{'commit':source_sha,'tree':source_tree,'archive_sha256':archive_sha,'lock_sha256':lock_sha},
        'workflow_definition':{'commit':git(definitions,'rev-parse','HEAD'),'repository':os.environ['GITHUB_REPOSITORY'],
          'workflow':os.environ['GITHUB_WORKFLOW_REF'],'run_id':os.environ['GITHUB_RUN_ID'],'run_attempt':os.environ['GITHUB_RUN_ATTEMPT']},
        'recipe':{'sha256':recipe}, 'artifacts':{artifact.name:digest(artifact.read_bytes())},
        'build_proof':proof_path.name})
    export_publication(output,arch,commands)


def run_evidence(argv,work,label,output,commands,key):
    stdout=work/(label+'.stdout'); stderr=work/(label+'.stderr')
    with stdout.open('xb') as out, stderr.open('xb') as err:
        result=subprocess.run(argv,stdout=out,stderr=err,timeout=120)
    commands[key]={'argv':argv,'exit_code':result.returncode,
      **{stream:{'path':str(p.relative_to(output)),'sha256':digest(p.read_bytes())} for stream,p in [('stdout',stdout),('stderr',stderr)]}}
    require(result.returncode==0, 'command failed: '+key)
    return stdout.read_text()


if __name__=='__main__':
    if len(sys.argv)==4 and sys.argv[1]=='--check-elf':
        static_elf(Path(sys.argv[2]).read_bytes(),sys.argv[3]); print('ELF64 static: no INTERP or NEEDED; correct native architecture')
    else:
        main(sys.argv)
