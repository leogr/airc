"""Boundary tests for the distributed monitor helper; no running chat required."""
import fcntl
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

HELPER = Path(__file__).resolve().parents[1] / "skills/airc/scripts/airc-monitor.py"


class MonitorTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.directory = Path(self.temp.name)
        self.path = self.directory / "monitor.json"

    def call(self, *args, path=None):
        return subprocess.run([sys.executable, str(HELPER), "--file", str(path or self.path), *args], capture_output=True, text=True, timeout=5, check=False)

    def record(self, handle="job-1", path=None):
        return self.call("record", "--session", "work", "--handle", handle, "--mode", "native", "--wake", "mentions", "--airc", "/usr/local/bin/airc", "--skill", str(HELPER.parent.parent / "SKILL.md"), path=path)

    def test_one_handle_retained_until_observed_outcome(self):
        first = self.record()
        self.assertEqual(first.returncode, 0, first.stderr)
        value = json.loads(first.stdout)
        self.assertIn("no future model wake", value["evidence"])
        self.assertEqual(value["record"]["handle"], "job-1")
        self.assertEqual(self.record().stdout, first.stdout)
        self.assertNotEqual(self.record("job-2").returncode, 0)
        self.assertNotEqual(self.call("handling", "--handle", "wrong", "--outcome", "completed").returncode, 0)
        self.assertEqual(json.loads(self.call("status").stdout), value)
        result = self.call("handling", "--handle", "job-1", "--outcome", "completed")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(json.loads(result.stdout)["record"]["handle"], "")
        self.assertEqual(self.record("job-2").returncode, 0)
        self.assertEqual(self.path.stat().st_mode & 0o777, 0o600)

    def test_rejects_symlinks_hardlinks_public_files_and_malformed_state(self):
        self.assertEqual(self.record().returncode, 0)
        link = self.directory / "link.json"
        link.symlink_to(self.path)
        self.assertNotEqual(self.record(path=link).returncode, 0)
        nested = self.directory / "alias"
        nested.symlink_to(self.directory, target_is_directory=True)
        self.assertNotEqual(self.record(path=nested / "else.json").returncode, 0)
        hard = self.directory / "hard.json"
        os.link(self.path, hard)
        self.assertNotEqual(self.call("status").returncode, 0)
        hard.unlink()
        self.path.chmod(0o644)
        self.assertNotEqual(self.call("status").returncode, 0)
        self.path.chmod(0o600)
        self.path.write_text("{}")
        self.assertNotEqual(self.call("status").returncode, 0)
        self.path.write_text("x" * 16385)
        self.assertNotEqual(self.call("status").returncode, 0)
        self.directory.chmod(0o755)
        self.assertNotEqual(self.record().returncode, 0)

    def test_stable_lock_rejects_concurrent_updates_and_is_not_replaced(self):
        self.assertEqual(self.record().returncode, 0)
        lock_path = self.directory / "monitor.json.lock"
        inode = lock_path.stat().st_ino
        with lock_path.open("rb") as lock:
            fcntl.flock(lock, fcntl.LOCK_EX)
            self.assertNotEqual(self.call("handling", "--handle", "job-1", "--outcome", "cancelled").returncode, 0)
        self.assertEqual(self.call("handling", "--handle", "job-1", "--outcome", "cancelled").returncode, 0)
        self.assertEqual(self.record("job-2").returncode, 0)
        self.assertEqual(lock_path.stat().st_ino, inode)


if __name__ == "__main__":
    unittest.main()
