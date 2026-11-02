#!/usr/bin/env python3
"""Bounded read-only hosted resource trial. No pull/build/cleanup/publication."""
import argparse
import json
import os
from pathlib import Path
import platform
import re
import selectors
import shutil
import stat
import subprocess
import time

GIB = 1024 ** 3
ENDPOINT = 'unix:///var/run/docker.sock'
NATIVE = ('ext2/ext3', 'xfs', 'btrfs', 'zfs')
FORMAT = '{"ID":{{json .ID}},"OSType":{{json .OSType}},"Architecture":{{json .Architecture}},"NCPU":{{json .NCPU}},"MemTotal":{{json .MemTotal}},"DockerRootDir":{{json .DockerRootDir}}}'

def require(value, message):
    if not value:
        raise ValueError(message)

def read_command(argv):
    environment = {k:v for k,v in os.environ.items() if not k.startswith('DOCKER_')}
    proc = subprocess.Popen(argv, stdout=subprocess.PIPE, stderr=subprocess.PIPE, env=environment)
    streams = {'stdout': bytearray(), 'stderr': bytearray()}
    deadline = time.monotonic() + 30
    try:
        with selectors.DefaultSelector() as selected:
            selected.register(proc.stdout, selectors.EVENT_READ, 'stdout')
            selected.register(proc.stderr, selectors.EVENT_READ, 'stderr')
            while selected.get_map():
                require(time.monotonic() < deadline, 'read-only command deadline exceeded')
                for key, _ in selected.select(timeout=0.25):
                    block = os.read(key.fileobj.fileno(), 8192)
                    if not block:
                        selected.unregister(key.fileobj)
                        continue
                    streams[key.data].extend(block)
                    require(sum(map(len, streams.values())) <= 256 * 1024, 'read-only response exceeds bound')
        status = proc.wait(timeout=max(0.1, deadline-time.monotonic()))
        require(status == 0, 'read-only command failed with exit ' + str(status))
        return bytes(streams['stdout']).decode('utf-8', 'strict').strip()
    except BaseException:
        if proc.poll() is None:
            proc.kill()
        proc.wait(timeout=5)
        raise
    finally:
        proc.stdout.close()
        proc.stderr.close()

def storage(path):
    resolved = path.resolve(strict=True)
    require(resolved.is_dir(), 'backing storage path is not a directory')
    filesystem = read_command(['stat', '-f', '-c', '%T', '--', str(resolved)])
    usage = shutil.disk_usage(resolved)
    return {'path':str(resolved), 'filesystem':filesystem,
            'total_bytes':usage.total, 'free_bytes':usage.free,
            'native_filesystem':filesystem in NATIVE,
            'initial_20gib_met':usage.free >= 20 * GIB}

def assess(arch, machine, daemon, memory, stores, cpus):
    aliases = {'x86_64':'amd64', 'amd64':'amd64', 'aarch64':'arm64', 'arm64':'arm64'}
    require(aliases.get(machine) == arch, 'host architecture is not requested native architecture')
    require(daemon.get('OSType') == 'linux' and aliases.get(daemon.get('Architecture')) == arch,
            'local Docker daemon is not requested native Linux architecture')
    require(isinstance(daemon.get('ID'),str) and daemon['ID'], 'daemon ID unavailable')
    require(type(daemon.get('NCPU')) is int and daemon['NCPU'] >= 2 and cpus >= 2,
            'CPU floor not met')
    require(type(daemon.get('MemTotal')) is int and daemon['MemTotal'] >= 8 * GIB,
            'Docker memory floor not met')
    require(memory['total_bytes'] >= 8 * GIB and memory['available_bytes'] >= 2 * GIB,
            'host memory floor not met')
    require(len(stores) == 2 and all(s['native_filesystem'] and s['initial_20gib_met'] for s in stores.values()),
            'existing 20GiB initial native output/Docker storage guard not met')

def trial(output, arch):
    # Fresh private result directory only; no adoption or cleanup of runner data.
    output.mkdir(mode=0o700)
    result = {'schema':1, 'architecture':arch, 'status':'UNQUALIFIED',
              'resource_only':True, 'build_executed':False, 'release_qualified':False,
              'effects':'Only a fresh task result directory; no Docker mutation, pull, build, deletion or publication',
              'runner_label':os.environ.get('RUNNER_OS','') + '/' + os.environ.get('RUNNER_ARCH',''),
              'run_id':os.environ.get('GITHUB_RUN_ID',''), 'definition':os.environ.get('GITHUB_SHA',''),
              'guards_unchanged':{'initial_disk_gib':20, 'before_pull_gib':16, 'before_build_gib':14,
                                  'openshell_memory_total_gib':8, 'host_memory_available_gib':2}}
    try:
        require(not os.environ.get('DOCKER_CONTEXT') and not os.environ.get('DOCKER_HOST'),
                'unexpected Docker selection environment; fixed standard local endpoint required')
        require(stat.S_ISSOCK(Path('/var/run/docker.sock').stat().st_mode), 'standard local Docker socket unavailable')
        require(platform.system() == 'Linux', 'native Linux required')
        with Path('/proc/meminfo').open() as memory_file:
            memory_text = memory_file.read(65537)
        require(len(memory_text) <= 65536, 'memory report exceeds bound')
        memory = dict(re.findall(r'^(MemTotal|MemAvailable):\s+(\d+)', memory_text, re.M))
        result['host'] = {'machine':platform.machine(), 'cpus':os.cpu_count() or 0,
                          'memory':{'total_bytes':int(memory['MemTotal'])*1024,
                                    'available_bytes':int(memory['MemAvailable'])*1024}}
        daemon = json.loads(read_command(['docker', '--host', ENDPOINT, 'info', '--format', FORMAT]))
        result['daemon'] = daemon
        require(isinstance(daemon.get('DockerRootDir'),str) and Path(daemon['DockerRootDir']).is_absolute(),
                'actual DockerRootDir unavailable')
        result['storage'] = {'output':storage(output), 'docker':storage(Path(daemon['DockerRootDir']))}
        result['local_endpoint'] = ENDPOINT
        assess(arch, result['host']['machine'], daemon, result['host']['memory'],
               result['storage'], result['host']['cpus'])
        result['status'] = 'RESOURCE_ADMISSION_PASS'
    except (Exception, KeyboardInterrupt) as error:
        result['failure'] = type(error).__name__ + ': ' + str(error)
    # The report is written on failed admission too. It is not an artifact consumed by publisher.
    payload = json.dumps(result, indent=2, sort_keys=True) + '\n'
    require(len(payload.encode()) <= 65536, 'result bound exceeded')
    with (output/'RESOURCE-TRIAL.json').open('x') as stream:
        stream.write(payload)
    return result

def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--architecture', required=True, choices=('amd64','arm64'))
    parser.add_argument('--output', required=True, type=Path)
    args = parser.parse_args()
    result = trial(args.output, args.architecture)
    print(json.dumps({'status':result['status'], 'build_executed':False, 'release_qualified':False}))
    return 0 if result['status'] == 'RESOURCE_ADMISSION_PASS' else 1

if __name__ == '__main__':
    raise SystemExit(main())
