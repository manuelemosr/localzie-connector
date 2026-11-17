#!/usr/bin/env python3
"""Publish verified release files into a reviewable GitHub draft."""
import argparse
import hashlib
import json
from pathlib import Path
import subprocess

from build_release import TARGETS, VERSION


def publish(directory):
    manifest = json.loads((directory / "BUILD_MANIFEST.json").read_text())
    version, revision = manifest["version"], manifest["revision"]
    if not VERSION.fullmatch(version) or manifest["dirty"]:
        raise ValueError("Only clean, versioned builds can become releases")
    expected = {f"localzie-connector_{version}_{goos}_{arch}.tar.gz" for goos, arch in TARGETS}
    expected.add("BUILD_MANIFEST.json")
    checksums = {}
    for line in (directory / "SHA256SUMS").read_text().splitlines():
        digest, name = line.split("  ", 1)
        if name not in expected or name in checksums:
            raise ValueError("Unexpected or duplicate release file")
        if hashlib.sha256((directory / name).read_bytes()).hexdigest() != digest:
            raise ValueError("Release checksum failed: " + name)
        checksums[name] = digest
    if set(checksums) != expected:
        raise ValueError("Release is missing one or more expected downloads")
    existing = subprocess.run(["gh", "release", "view", version, "--json", "isDraft,targetCommitish"], text=True, capture_output=True)
    if existing.returncode == 0 and not json.loads(existing.stdout)["isDraft"]:
        raise ValueError("This release is already published; published downloads are never overwritten")
    if existing.returncode == 0 and json.loads(existing.stdout)["targetCommitish"] != revision:
        raise ValueError("This draft belongs to a different source commit; use a new version")
    notes = """Localzie's open-source computer companion.

Get [Localzie for iPhone](https://apps.apple.com/app/id6813506926).

Download the archive for your computer, extract it, then run `./localzie-connector setup`.

| Computer | Download ending |
| --- | --- |
| Mac with Apple silicon (M1 or newer) | `darwin_arm64.tar.gz` |
| Mac with Intel | `darwin_amd64.tar.gz` |
| Linux x64 | `linux_amd64.tar.gz` |
| Linux ARM64 | `linux_arm64.tar.gz` |

Each archive contains the connector, notices, dependency licenses and build metadata. `SHA256SUMS` verifies the downloads.

This is a developer preview. Keep the connector's terminal open while using it. Fresh installations connect at home; Localzie's automatic remote activation is still being added. Mac binaries are not Developer ID signed/notarized yet. Windows is not included.

This draft is ready to review. Publish it to make its downloads public.
"""
    if existing.returncode != 0:
        args = ["gh", "release", "create", version, "--draft", "--target", revision,
                "--title", "Localzie Connector " + version, "--notes-file", "-"]
        if "-" in version:
            args.append("--prerelease")
        subprocess.run(args, input=notes, text=True, check=True)
    subprocess.run(["gh", "release", "upload", version, "--clobber",
                    *(str(directory / name) for name in sorted(expected | {"SHA256SUMS"}))], check=True)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("directory", type=Path)
    args = parser.parse_args()
    publish(args.directory)
