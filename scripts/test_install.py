"""Test installer feedback and command paths using disposable executables."""
from pathlib import Path
import os
import shutil
import subprocess
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[1]


class InstallTests(unittest.TestCase):
    def fixture(self, root, content):
        archive = root / "archive"
        archive.mkdir()
        shutil.copyfile(ROOT / "install.sh", archive / "install.sh")
        binary = archive / "localzie-connector"
        binary.write_text(content)
        binary.chmod(0o755)
        return archive

    def test_success_prints_a_working_setup_command_even_with_spaces_and_quotes(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            archive = self.fixture(root, '#!/bin/sh\nif [ "$1" = version ]; then echo v0.0.0-test; else printf ok > "$INSTALL_TEST_MARKER"; fi\n')
            destination = root / "user's bin"
            marker = root / "started"
            env = dict(os.environ, LOCALZIE_INSTALL_DIR=str(destination), INSTALL_TEST_MARKER=str(marker))
            result = subprocess.run(["sh", str(archive / "install.sh")], env=env, text=True, capture_output=True)
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertIn("Installed Localzie Connector", result.stdout)
            command = next(line.strip() for line in result.stdout.splitlines() if line.endswith(" setup"))
            subprocess.run(["sh", "-c", command], env=env, check=True)
            self.assertEqual(marker.read_text(), "ok")

    def test_invalid_executable_reports_failure_and_keeps_previous_install(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            archive = self.fixture(root, "#!/bin/sh\nexit 99\n")
            destination = root / "bin"
            destination.mkdir()
            old = destination / "localzie-connector"
            old.write_text("previous version")
            env = dict(os.environ, LOCALZIE_INSTALL_DIR=str(destination))
            result = subprocess.run(["sh", str(archive / "install.sh")], env=env, text=True, capture_output=True)
            self.assertNotEqual(result.returncode, 0)
            self.assertIn("installation failed", result.stderr)
            self.assertEqual(old.read_text(), "previous version")

    def test_command_on_path_uses_the_normal_setup_name(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            archive = self.fixture(root, "#!/bin/sh\necho v0.0.0-test\n")
            destination = root / "bin"
            destination.mkdir()
            env = dict(os.environ, LOCALZIE_INSTALL_DIR=str(destination), PATH=str(destination) + os.pathsep + os.environ["PATH"])
            result = subprocess.run(["sh", str(archive / "install.sh")], env=env, text=True, capture_output=True)
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertIn("  localzie-connector setup", result.stdout)
