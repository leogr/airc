"""Local Git/file and mock checks; no Docker, Rust, network or compiled ELF."""
import hashlib
import importlib.util
import io
import json
from pathlib import Path
import struct
import subprocess
import tarfile
import tempfile
import unittest
from unittest.mock import patch

spec=importlib.util.spec_from_file_location('release_native',Path(__file__).with_name('release-native.py'))
r=importlib.util.module_from_spec(spec)
spec.loader.exec_module(r)


def elf(arch='amd64',kind=None,tag=0):
    b=bytearray(136)
    b[:6]=b'\x7fELF\x02\x01'
    struct.pack_into('<H',b,18,62 if arch=='amd64' else 183)
    struct.pack_into('<Q',b,32,64)
    struct.pack_into('<HH',b,54,56,0 if kind is None else 1)
    if kind is not None:
        struct.pack_into('<I',b,64,kind)
        struct.pack_into('<Q',b,72,120)
        struct.pack_into('<Q',b,96,16)
        struct.pack_into('<q',b,120,tag)
    return bytes(b)


class NativeReleaseTests(unittest.TestCase):
    def setUp(self):
        reset=patch.object(r,'ADMITTED_DOCKER',None)
        reset.start();self.addCleanup(reset.stop)

    def test_successful_single_build_export_never_requires_a_second_build(self):
        spec=importlib.util.spec_from_file_location('publisher_fixture',Path(__file__).with_name('test_publish_release.py'))
        fixture=importlib.util.module_from_spec(spec);spec.loader.exec_module(fixture)
        files,manifest,proof=fixture.airc_fixture()
        files.objects.update({'CI-MANIFEST.json':manifest,'image-manifest.json':{},'image-index.json':{},
                              'pull-intent.json':{}})
        for name in ('pull.log','build-1/container.log','build-1/inner.sh',
                     'build-1/build/cargo.txt','build-1/build/apk-packages.txt'):
            files.data[name]=b'fixture raw evidence\n'
        for binary in r.BINS:
            files.data['build-1/build/'+binary+'.version']=b'airc 0.1.0\n'
            files.data['build-1/build/'+binary+'.help']=b'fixture native help\n'
        with tempfile.TemporaryDirectory() as temp:
            output=Path(temp)
            for name,data in {**files.data,**{k:json.dumps(v).encode() for k,v in files.objects.items()}}.items():
                target=output/name;target.parent.mkdir(parents=True,exist_ok=True);target.write_bytes(data)
            r.export_publication(output,'amd64',proof['commands'])
            exported=output/'publication'
            self.assertEqual((exported/'airc-linux-amd64.tar.gz').read_bytes(),files.data['airc-linux-amd64.tar.gz'])
            self.assertEqual((exported/'build-1/airc-linux-amd64.tar.gz').read_bytes(),files.data['airc-linux-amd64.tar.gz'])
            self.assertTrue((exported/'build-1/build/release-tests.stdout').is_file())
            self.assertFalse((exported/'build-2').exists())

    def test_archive_git_inventory_includes_tracked_ignored_files_and_checks_exact_tree(self):
        files={'.gitignore':(b'ignored.md\ntarget/\n',0o644),
               'README.md':(b'# Source\n',0o644), 'ignored.md':(b'# Tracked despite ignore\n',0o644),
               'run.sh':(b'#!/bin/sh\n',0o755)}
        def obj(kind, raw):
            return hashlib.sha1(kind.encode()+b' '+str(len(raw)).encode()+b'\0'+raw).digest()
        body=b''.join((('100755' if mode==0o755 else '100644')+' '+name).encode()+b'\0'+obj('blob',raw)
                      for name,(raw,mode) in sorted(files.items()))
        expected=obj('tree',body).hex()
        with tempfile.TemporaryDirectory() as temp:
            root=Path(temp)
            for name,(raw,mode) in files.items():
                (root/name).write_bytes(raw);(root/name).chmod(mode)
            r.prepare_source_git(root,expected)
            listed=subprocess.check_output(['git','-C',str(root),'ls-files','-z','--cached','--others',
                                            '--exclude-standard','--','*.md']).decode().split('\0')
            self.assertEqual(set(filter(None,listed)),{'README.md','ignored.md'})
            self.assertFalse((root/'.git/hooks').exists())
            with self.assertRaises(RuntimeError):r.prepare_source_git(root,expected)
            self.assertEqual({name:(root/name).read_bytes() for name in files},
                             {name:raw for name,(raw,_) in files.items()})
        with tempfile.TemporaryDirectory() as temp:
            with self.assertRaisesRegex(RuntimeError,'differs from verified source'):
                r.prepare_source_git(Path(temp),expected)
        self.assertIn('GIT_CONFIG_COUNT=1 GIT_CONFIG_KEY_0=safe.directory GIT_CONFIG_VALUE_0=/source',r.INNER)

    def test_required_nanosecond_date_capability_is_not_silently_seconds(self):
        import subprocess
        import time
        for raw in ('1700000000\\n', '1700000000000000000\\n', '%s%N\\n', ''):
            value = raw.replace('\\n', '\n')
            with self.subTest(raw=raw), patch.object(subprocess, 'check_output', return_value=value), \
                 patch.object(time, 'time_ns', side_effect=[1700000000000000001, 1700000000000000003]):
                with self.assertRaises(RuntimeError):
                    exec(r.NANOSECOND_DATE_CHECK, {})
        with patch.object(subprocess, 'check_output', return_value='1700000000000000002\n'), \
             patch.object(time, 'time_ns', side_effect=[1700000000000000001, 1700000000000000003]):
            exec(r.NANOSECOND_DATE_CHECK, {})
        self.assertIn('perl coreutils', r.INNER)
        self.assertLess(r.INNER.index('AIRC_NANOSECOND_DATE_CHECK'), r.INNER.index('record upstream-check'))

    def test_both_native_static_architectures(self):
        for arch in ('amd64','arm64'):
            r.static_elf(elf(arch),arch)
            r.static_elf(elf(arch,2),arch)

    def test_rejects_interp_needed_wrong_arch_and_truncation(self):
        for data,arch in [(elf(kind=3),'amd64'),(elf(kind=2,tag=1),'amd64'),(elf(),'arm64'),(elf()[:32],'amd64'),(b'not ELF','amd64')]:
            with self.subTest(data=data[:8],arch=arch), self.assertRaises(RuntimeError):
                r.static_elf(data,arch)

    def test_deterministic_archive_regular_sorted_metadata_and_modes(self):
        payload={'skills/airc/scripts/monitor':(b'#!/bin/sh\n',0o755),'LICENSE':(b'license',0o644),'bin/airc':(elf(),0o755)}
        with tempfile.TemporaryDirectory() as d:
            first=Path(d)/'first'; second=Path(d)/'second'
            r.package(first,payload)
            r.package(second,dict(reversed(list(payload.items()))))
            self.assertEqual(first.read_bytes(),second.read_bytes())
            with tarfile.open(first) as t:
                self.assertEqual(t.getnames(),sorted(payload))
                for m in t:
                    self.assertTrue(m.isfile()); self.assertEqual((m.uid,m.gid,m.mtime),(0,0,0))
                    self.assertEqual(m.mode,payload[m.name][1])
                    self.assertEqual(t.extractfile(m).read(),payload[m.name][0])

    def test_archive_does_not_replace_existing_path(self):
        with tempfile.TemporaryDirectory() as d:
            p=Path(d)/'existing';p.write_bytes(b'preserve')
            with self.assertRaises(FileExistsError): r.package(p,{})
            self.assertEqual(p.read_bytes(),b'preserve')

    def test_unsafe_archive_entries_refused(self):
        with tempfile.TemporaryDirectory() as d:
            for n in ('/absolute','../outside','a/../../escape'):
                with self.assertRaises(RuntimeError): r.package(Path(d)/'out',{n:(b'x',0o644)})
            with self.assertRaises(RuntimeError): r.package(Path(d)/'mode',{'safe':(b'x',0o777)})

    def test_pinned_source_hash_and_link_rejection(self):
        out=io.BytesIO()
        with tarfile.open(fileobj=out,mode='w:gz') as t:
            m=tarfile.TarInfo('airc-'+r.SOURCE+'/bad');m.type=tarfile.SYMTYPE;m.linkname='/etc/passwd';t.addfile(m)
        data=out.getvalue()
        with self.assertRaises(RuntimeError): r.source_members(data)
        with patch.object(r,'ARCHIVE',r.digest(data)),self.assertRaises(RuntimeError):r.source_members(data)

    def test_resource_guards_not_silently_lowered(self):
        with patch.object(r.shutil,'disk_usage',return_value=type('Disk',(),{'free':19*r.GIB})()):
            with self.assertRaises(RuntimeError):r.resources(Path('.'),20)

    def test_host_mismatch_rejected_before_any_mutation(self):
        with patch.object(r.platform,'system',return_value='Darwin'),patch.object(r.subprocess,'check_output') as cmd:
            with self.assertRaises(RuntimeError):r.main(['adapter',r.SOURCE,'amd64','source','output'])
            cmd.assert_not_called()

    def test_publication_rejects_reference_traversal(self):
        with tempfile.TemporaryDirectory() as d:
            output=Path(d)/'out';output.mkdir()
            outside=Path(d)/'private';outside.write_bytes(b'not exportable')
            record={'stdout':{'path':'../private'},'stderr':{'path':'../private'}}
            with self.assertRaises(RuntimeError):r.export_publication(output,'amd64',{'host':record})
            self.assertEqual(list((output/'publication').iterdir()),[])
            self.assertEqual(outside.read_bytes(),b'not exportable')

    def test_publication_does_not_replace_existing_export(self):
        with tempfile.TemporaryDirectory() as d:
            output=Path(d);(output/'publication').mkdir()
            (output/'publication'/'preserved').write_bytes(b'owned')
            with self.assertRaises(FileExistsError):r.export_publication(output,'arm64',{})
            self.assertEqual((output/'publication'/'preserved').read_bytes(),b'owned')

    def test_uncertain_mutation_is_not_replayed(self):
        with tempfile.TemporaryDirectory() as d:
            work=Path(d)
            admitted={'selection':{'endpoint':'unix:///admitted.sock'}}
            with patch.object(r,'ADMITTED_DOCKER',admitted),patch.object(r.subprocess,'Popen',side_effect=TimeoutError('lost start reply')) as start:
                with self.assertRaises(TimeoutError):r.container_start('unique-container',work,work)
                self.assertEqual(start.call_count,1)
                self.assertTrue((work/'start-intent.json').is_file())
                self.assertFalse((work/'start-result.json').exists())


class DockerAdmissionTests(unittest.TestCase):
    def setUp(self):
        reset=patch.object(r,'ADMITTED_DOCKER',None)
        reset.start();self.addCleanup(reset.stop)

    def test_context_env_overrides_host_and_remote_is_refused(self):
        read=unittest.mock.Mock(return_value=json.dumps([{'Name':'remote','Endpoints':{'docker':{'Host':'ssh://remote'}}}]))
        selected=r.effective_selection({'DOCKER_CONTEXT':'remote','DOCKER_HOST':'unix:///local.sock'},read)
        self.assertEqual(selected['method'],'DOCKER_CONTEXT');read.assert_called_once_with('remote')
        with self.assertRaises(RuntimeError):r.local_endpoint(selected['endpoint'])

    def test_remote_host_and_remote_default_context_are_refused(self):
        read=unittest.mock.Mock()
        selected=r.effective_selection({'DOCKER_HOST':'tcp://remote:2376'},read)
        read.assert_not_called()
        with self.assertRaises(RuntimeError):r.local_endpoint(selected['endpoint'])
        read.side_effect=['remote-default',json.dumps([{'Name':'remote-default','Endpoints':{'docker':{'Host':'ssh://remote'}}}])]
        selected=r.effective_selection({},read)
        self.assertEqual(selected['method'],'default-context')
        with self.assertRaises(RuntimeError):r.local_endpoint(selected['endpoint'])

    def admission_fixture(self,output,root,wrong_arch=False):
        calls=[]
        def probe(argv,limit):
            calls.append(argv)
            if argv[:3]==['docker','context','show']:out=b'default\n'
            elif argv[:3]==['docker','context','inspect']:
                out=json.dumps([{'Name':'default','Endpoints':{'docker':{'Host':'unix:///admitted.sock'}}}]).encode()
            elif argv[0]=='stat' and argv[1]=='-Lc':
                out=json.dumps({'path':'/admitted.sock','mode_hex':'c1b0','device':1,'inode':2,'uid':0,'gid':0}).encode()
            elif argv[0]=='stat':out=b'ext2/ext3\n'
            else:
                self.assertEqual(argv,['docker','--host','unix:///admitted.sock','info','--format',r.DOCKER_INFO_FORMAT])
                out=json.dumps({'ID':'local-daemon','OSType':'linux','Architecture':'aarch64' if wrong_arch else 'x86_64',
                    'NCPU':2,'MemTotal':8*r.GIB,'DockerRootDir':str(root)}).encode()
            return 0,out,b''
        return calls,probe

    def test_positive_local_admission_bound_before_any_mutation_and_monitors_both_paths(self):
        with tempfile.TemporaryDirectory() as d:
            output=Path(d)/'output';output.mkdir();root=Path(d)/'docker';root.mkdir()
            calls,probe=self.admission_fixture(output,root)
            with patch.object(r,'bounded_readonly',side_effect=probe),patch.object(r,'local_endpoint',return_value='unix:///admitted.sock'), \
                 patch.object(r.shutil,'disk_usage',return_value=type('Disk',(),{'free':30*r.GIB})()), \
                 patch.object(r.Path,'read_text',return_value='MemTotal: 8388608 kB\nMemAvailable: 4194304 kB\n'):
                records={};admitted=r.admit_docker(output,'amd64',records,environment={})
                self.assertEqual(admitted['selection']['method'],'default-context')
                r.resources(output,14)
                self.assertEqual(r.docker_argv(['docker','pull','image']),['docker','--host','unix:///admitted.sock','pull','image'])
                with patch.dict(r.os.environ,{'DOCKER_CONTEXT':'changed','DOCKER_HOST':'ssh://changed','DOCKER_TLS_VERIFY':'1'}):
                    self.assertNotIn('DOCKER_CONTEXT',r.docker_environment());self.assertNotIn('DOCKER_HOST',r.docker_environment())
                    self.assertEqual(r.docker_argv(['docker','start','owned'])[:3],['docker','--host','unix:///admitted.sock'])
                self.assertTrue(all('pull' not in call and 'create' not in call and 'start' not in call for call in calls))
                self.assertIn('docker-daemon-info',records);self.assertIn('docker-socket-stat',records)
            snapshots=[json.loads(line) for line in (output/'evidence/docker/resources.jsonl').read_text().splitlines()]
            self.assertEqual({x['path'] for x in snapshots[-1]['disks']},{str(output),str(root)})
            self.assertEqual(snapshots[-1]['disk_floor_gib'],14)

    def test_wrong_daemon_arch_precedes_every_mutation(self):
        with tempfile.TemporaryDirectory() as d:
            output=Path(d)/'output';output.mkdir();root=Path(d)/'docker';root.mkdir()
            calls,probe=self.admission_fixture(output,root,wrong_arch=True)
            with patch.object(r,'bounded_readonly',side_effect=probe),patch.object(r,'local_endpoint',return_value='unix:///admitted.sock'):
                with self.assertRaises(RuntimeError):r.admit_docker(output,'amd64',{},environment={})
            self.assertIsNone(r.ADMITTED_DOCKER)
            self.assertTrue(all('pull' not in call and 'create' not in call and 'start' not in call for call in calls))

    def test_low_daemon_backing_with_ample_output_fails_before_mutation(self):
        with tempfile.TemporaryDirectory() as d:
            output=Path(d)/'output';output.mkdir();root=Path(d)/'docker';root.mkdir()
            calls,probe=self.admission_fixture(output,root)
            def space(path):return type('Disk',(),{'free':(19 if Path(path)==root else 40)*r.GIB})()
            with patch.object(r,'bounded_readonly',side_effect=probe),patch.object(r,'local_endpoint',return_value='unix:///admitted.sock'), \
                 patch.object(r.shutil,'disk_usage',side_effect=space):
                with self.assertRaises(RuntimeError):r.admit_docker(output,'amd64',{},environment={})
            self.assertIsNone(r.ADMITTED_DOCKER)
            self.assertTrue(all('pull' not in call and 'create' not in call and 'start' not in call for call in calls))

    def test_unadmitted_mutation_is_refused(self):
        with patch.object(r.subprocess,'check_output') as execute:
            with self.assertRaises(RuntimeError):r.command(['docker','pull','image'])
            execute.assert_not_called()


class FailedBuildOwnershipTests(unittest.TestCase):
    def environment(self):
        return {'SOURCE_SUBJECT':r.SOURCE,'BUILD_ARCH':'amd64','BUILD_DEFINITION':'a'*40,
                'GITHUB_SHA':'a'*40,'GITHUB_REPOSITORY':'leogr/airc','GITHUB_RUN_ID':'123',
                'GITHUB_RUN_ATTEMPT':'1','GITHUB_WORKFLOW_REF':'leogr/airc/.github/workflows/native-release.yml@refs/heads/review',
                'RUNNER_NAME':'dedicated-native-test'}

    def test_owner_first_file_before_effects_and_no_success_publication_inventory_change(self):
        with tempfile.TemporaryDirectory() as d,patch.dict(r.os.environ,self.environment(),clear=True):
            output=Path(d)/'new'
            r.initialize_output(output,'amd64')
            self.assertEqual([p.name for p in output.iterdir()],['BUILD-OWNER.json'])
            owner=json.loads((output/'BUILD-OWNER.json').read_text())
            self.assertEqual(owner,{'schema':1,'repository':'leogr/airc','source_subject':r.SOURCE,
                'workflow_definition':'a'*40,'run_id':'123','run_attempt':'1','arch':'amd64'})
            self.assertFalse((output/'publication').exists())
            # Owner is separate diagnostics provenance, never a release artifact.
            import inspect
            self.assertNotIn('BUILD-OWNER.json',inspect.getsource(r.export_publication))

    def test_invalid_ci_owner_refuses_before_output_creation(self):
        for key,value in [('BUILD_ARCH','arm64'),('SOURCE_SUBJECT','b'*40),('GITHUB_SHA','b'*40),
                          ('GITHUB_REPOSITORY','other/airc'),('GITHUB_RUN_ID','../other'),('GITHUB_RUN_ATTEMPT','0')]:
            environment=self.environment();environment[key]=value
            with tempfile.TemporaryDirectory() as d,patch.dict(r.os.environ,environment,clear=True):
                output=Path(d)/'new'
                with self.subTest(key=key),self.assertRaises(RuntimeError):r.initialize_output(output,'amd64')
                self.assertFalse(output.exists())

    def test_owned_output_is_never_adopted_or_replaced(self):
        with tempfile.TemporaryDirectory() as d,patch.dict(r.os.environ,self.environment(),clear=True):
            output=Path(d)/'existing';output.mkdir();(output/'preserved').write_bytes(b'prior bytes')
            with self.assertRaises(FileExistsError):r.initialize_output(output,'amd64')
            self.assertEqual((output/'preserved').read_bytes(),b'prior bytes')
            self.assertFalse((output/'BUILD-OWNER.json').exists())

    def mock_git(self,source,definition):
        def git(path,*args):
            if args==('rev-parse','--show-toplevel'):return str(definition)
            if args==('rev-parse','HEAD'):return r.SOURCE if Path(path)==source else 'a'*40
            if args==('rev-parse','HEAD^{tree}'):return r.TREE
            if args[0]=='status':return ''
            raise AssertionError(args)
        return git

    def test_setup_nonzero_and_lost_reply_failures_keep_owner_without_fake_completion(self):
        for failure in (RuntimeError('setup prerequisite unavailable'),RuntimeError('build returned nonzero'),TimeoutError('lost start reply')):
            with tempfile.TemporaryDirectory() as d,patch.dict(r.os.environ,self.environment(),clear=True):
                base=Path(d);source=base/'source';source.mkdir();definition=base/'definition';definition.mkdir();output=base/'new'
                def effect(*args):
                    self.assertTrue((output/'BUILD-OWNER.json').is_file())
                    # A real failed path may have only partial intended evidence.
                    (output/'pull-intent.json').write_text('{"replay":false}')
                    raise failure
                with patch.object(r.platform,'system',return_value='Linux'),patch.object(r.platform,'machine',return_value='x86_64'), \
                     patch.object(r,'git',side_effect=self.mock_git(source,definition)),patch.object(r,'resources'), \
                     patch.object(r,'release',side_effect=effect),patch.object(r.subprocess,'check_output') as commands:
                    with self.assertRaises(type(failure)):r.main(['adapter',r.SOURCE,'amd64',str(source),str(output)])
                    commands.assert_not_called()
                self.assertTrue((output/'BUILD-OWNER.json').exists());self.assertTrue((output/'FAILED.json').exists())
                self.assertTrue((output/'pull-intent.json').exists());self.assertFalse((output/'publication').exists())
                self.assertFalse((output/'build-1/start-result.json').exists())
                self.assertNotIn('result_known',json.loads((output/'FAILED.json').read_text()))

    def test_preflight_failure_leaves_no_output_or_mutation_claim(self):
        with tempfile.TemporaryDirectory() as d,patch.dict(r.os.environ,self.environment(),clear=True):
            base=Path(d);source=base/'source';source.mkdir();definition=base/'definition';definition.mkdir();output=base/'new'
            with patch.object(r.platform,'system',return_value='Linux'),patch.object(r.platform,'machine',return_value='x86_64'), \
                 patch.object(r,'git',side_effect=self.mock_git(source,definition)),patch.object(r,'resources',side_effect=RuntimeError('storage floor')), \
                 patch.object(r,'release') as effects:
                with self.assertRaises(RuntimeError):r.main(['adapter',r.SOURCE,'amd64',str(source),str(output)])
                effects.assert_not_called()
            self.assertFalse(output.exists())

    def test_diagnostics_profile_has_only_fixed_logs_ids_and_admission_paths(self):
        profile=json.loads(Path(__file__).with_name('diagnostics-profile.json').read_text())
        self.assertEqual(set(profile),{'schema','repository','files','directories'})
        self.assertEqual(profile['repository'],'leogr/airc');self.assertEqual(profile['schema'],1)
        self.assertIn('BUILD-OWNER.json',profile['files']);self.assertIn('build-1/stop-intent.json',profile['files'])
        self.assertIn('evidence/docker/resources.jsonl',profile['files'])
        for name in [*profile['files'],*profile['directories']]:
            self.assertFalse(name.startswith('/'));self.assertNotIn('..',name.split('/'))
            self.assertFalse(any(p in name.split('/') for p in ('source','cache','target','home','publication')))
            self.assertFalse(name.endswith(('.tar.gz','.rs','.toml')))
        import re
        for pattern in profile['directories'].values():
            expression=re.compile(pattern)
            self.assertIsNone(expression.fullmatch('credentials.json'))
            self.assertIsNone(expression.fullmatch('nested/build.stdout'))


if __name__=='__main__':unittest.main()
