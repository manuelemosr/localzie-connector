"""Regression checks for release isolation, notices and immutable publishing."""
import hashlib
import json
from pathlib import Path
import subprocess
import tarfile
import tempfile
import unittest
from unittest.mock import patch

import build_release
import publish_release


class ReleaseTests(unittest.TestCase):
    def test_archive_excludes_private_files_and_retains_notices(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            project, module, goroot = root / "project", root / "dependency", root / "go"
            for directory in (project, module, goroot):
                directory.mkdir()
            for name in ("LICENSE", "NOTICE", "THIRD_PARTY_NOTICES.md", "install.sh"):
                (project / name).write_text("Public project notice")
            (project / "connector.json").write_text("PRIVATE_TEST_PAYLOAD")
            (project / "home.jwt").write_text("PRIVATE_TEST_PAYLOAD")
            (module / "LICENSE").write_text("Dependency license notice")
            (module / "NOTICE").write_text("Dependency attribution")
            (module / "private.db").write_text("PRIVATE_TEST_PAYLOAD")
            (goroot / "LICENSE").write_text("Go license")
            binary = root / "binary"
            binary.write_bytes(b"synthetic executable")
            archive = root / "release.tar.gz"
            metadata = {"version": "v1.0.0", "revision": "a" * 40}
            modules = {"example.com/module": {"Path": "example.com/module", "Version": "v1.0.0", "Dir": str(module)}}
            with patch.object(build_release, "ROOT", project), patch.object(build_release, "run", return_value=str(goroot)):
                build_release.make_archive(archive, binary, metadata, modules, 123)
            with tarfile.open(archive) as package:
                members = package.getmembers()
                self.assertTrue(all(not m.name.startswith("/") and ".." not in Path(m.name).parts for m in members))
                contents = b"".join(package.extractfile(m).read() for m in members if m.isfile())
                self.assertNotIn(b"PRIVATE_TEST_PAYLOAD", contents)
                self.assertNotIn(str(root).encode(), contents)
                self.assertIn(b"Dependency license notice", contents)
                self.assertIn(b"Dependency attribution", contents)
                executable = package.getmember("release/localzie-connector")
                self.assertEqual(executable.mode, 0o755)
                self.assertTrue(all(m.uid == 0 and m.gid == 0 and m.mtime == 123 for m in members))

    def fixture(self, directory):
        version, revision = "v1.0.0", "a" * 40
        files = []
        for goos, arch in build_release.TARGETS:
            name = f"localzie-connector_{version}_{goos}_{arch}.tar.gz"
            (directory / name).write_bytes(b"synthetic archive")
            files.append(name)
        manifest = directory / "BUILD_MANIFEST.json"
        manifest.write_text(json.dumps({"version": version, "revision": revision, "dirty": False}))
        files.append(manifest.name)
        (directory / "SHA256SUMS").write_text("".join(hashlib.sha256((directory / name).read_bytes()).hexdigest() + "  " + name + "\n" for name in files))
        return files

    def test_changed_download_is_rejected_before_github_is_contacted(self):
        with tempfile.TemporaryDirectory() as folder:
            directory = Path(folder)
            files = self.fixture(directory)
            (directory / files[0]).write_bytes(b"changed")
            with patch.object(publish_release.subprocess, "run") as github:
                with self.assertRaisesRegex(ValueError, "checksum"):
                    publish_release.publish(directory)
                github.assert_not_called()

    def test_published_release_and_wrong_source_draft_are_not_overwritten(self):
        for release in ({"isDraft": False, "targetCommitish": "a" * 40},
                        {"isDraft": True, "targetCommitish": "b" * 40}):
            with self.subTest(release=release), tempfile.TemporaryDirectory() as folder:
                directory = Path(folder)
                self.fixture(directory)
                result = subprocess.CompletedProcess([], 0, json.dumps(release), "")
                with patch.object(publish_release.subprocess, "run", return_value=result) as github:
                    with self.assertRaises(ValueError):
                        publish_release.publish(directory)
                    self.assertEqual(github.call_count, 1)

    def test_unversioned_or_shell_like_versions_are_rejected(self):
        for version in ("main", "v1.0.0;whoami", "../v1.0.0", "v01.0.0"):
            with self.subTest(version=version), patch.object(build_release, "run") as command:
                with self.assertRaises(ValueError):
                    build_release.build(version, Path("unused"))
                command.assert_not_called()


if __name__ == "__main__":
    unittest.main()
