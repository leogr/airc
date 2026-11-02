import copy
import importlib.util
import json
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

spec = importlib.util.spec_from_file_location('hosted_resources', Path(__file__).with_name('hosted_resources.py'))
m = importlib.util.module_from_spec(spec)
spec.loader.exec_module(m)

class Resources(unittest.TestCase):
    def setUp(self):
        self.daemon = {'ID':'owned-local-daemon', 'OSType':'linux', 'Architecture':'x86_64',
                       'NCPU':4, 'MemTotal':16*m.GIB, 'DockerRootDir':'/var/lib/docker'}
        self.memory = {'total_bytes':16*m.GIB, 'available_bytes':10*m.GIB}
        self.stores = {k:{'native_filesystem':True,'initial_20gib_met':True} for k in ('output','docker')}
    def check(self, arch='amd64', machine='x86_64'):
        return m.assess(arch,machine,self.daemon,self.memory,self.stores,4)
    def test_native_amd64_admission_does_not_qualify_build(self):
        self.assertIsNone(self.check())
    def test_native_arm64_admission(self):
        self.daemon['Architecture']='aarch64'
        self.assertIsNone(self.check('arm64','aarch64'))
    def test_wrong_host_architecture(self):
        with self.assertRaises(ValueError):self.check(machine='aarch64')
    def test_wrong_daemon_architecture(self):
        self.daemon['Architecture']='aarch64'
        with self.assertRaises(ValueError):self.check()
    def test_each_low_backing_filesystem_refuses(self):
        for key in self.stores:
            with self.subTest(storage=key):
                old=copy.deepcopy(self.stores);self.stores[key]['initial_20gib_met']=False
                with self.assertRaises(ValueError):self.check()
                self.stores=old
    def test_non_native_storage_refuses(self):
        self.stores['docker']['native_filesystem']=False
        with self.assertRaises(ValueError):self.check()
    def test_memory_and_cpu_refuse(self):
        self.daemon['NCPU']=1
        with self.assertRaises(ValueError):self.check()
        self.daemon['NCPU']=4;self.memory['available_bytes']=m.GIB
        with self.assertRaises(ValueError):self.check()
    def test_trial_remote_environment_records_failure_without_docker(self):
        with tempfile.TemporaryDirectory() as temp, patch.dict(os.environ,{'DOCKER_HOST':'tcp://not-local'},clear=True), patch.object(m,'read_command') as command:
            root=Path(temp)/'new'; result=m.trial(root,'amd64')
            self.assertEqual(result['status'],'UNQUALIFIED');command.assert_not_called()
            self.assertFalse(result['build_executed']);self.assertFalse(result['release_qualified'])
            self.assertEqual(json.loads((root/'RESOURCE-TRIAL.json').read_text()),result)
    def test_actual_trial_docker_error_retains_failure(self):
        fake_stat=type('S',(),{'st_mode':0o140600})()
        with tempfile.TemporaryDirectory() as temp, patch.dict(os.environ,{},clear=True), patch.object(m.Path,'stat',return_value=fake_stat), patch.object(m,'read_command',side_effect=ValueError('unavailable')):
            result=m.trial(Path(temp)/'new','amd64')
            self.assertEqual(result['status'],'UNQUALIFIED');self.assertIn('unavailable',result['failure'])
            self.assertFalse(result['release_qualified'])
    def test_actual_storage_preserves_20gib_threshold(self):
        usage = type('U',(),{'total':100*m.GIB, 'free':20*m.GIB})()
        with tempfile.TemporaryDirectory() as temp, patch.object(m,'read_command',return_value='ext2/ext3'), patch.object(m.shutil,'disk_usage',return_value=usage):
            self.assertTrue(m.storage(Path(temp))['initial_20gib_met'])
            usage.free -= 1
            self.assertFalse(m.storage(Path(temp))['initial_20gib_met'])
    def test_existing_output_is_not_adopted(self):
        with tempfile.TemporaryDirectory() as temp:
            with self.assertRaises(FileExistsError):m.trial(Path(temp),'amd64')
    def test_actual_trial_positive_remains_resource_only(self):
        def command(argv):
            if argv[0]=='docker':return json.dumps(self.daemon)
            raise AssertionError(argv)
        with tempfile.TemporaryDirectory() as temp, patch.dict(os.environ,{},clear=True), patch.object(m.Path,'stat',return_value=type('S',(),{'st_mode':0o140600})()), patch.object(m,'read_command',side_effect=command), patch.object(m,'storage',return_value={'native_filesystem':True,'initial_20gib_met':True}):
            result=m.trial(Path(temp)/'new','amd64')
            self.assertEqual(result['status'],'RESOURCE_ADMISSION_PASS')
            self.assertFalse(result['build_executed']);self.assertFalse(result['release_qualified'])

if __name__ == '__main__':unittest.main()
