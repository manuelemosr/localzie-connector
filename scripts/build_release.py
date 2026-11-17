#!/usr/bin/env python3
"""Build only the public connector and its notices into portable release archives."""
from __future__ import annotations

import argparse
import gzip
import hashlib
import io
import json
import os
from pathlib import Path
import re
import subprocess
import tarfile
import tempfile

ROOT = Path(__file__).resolve().parents[1]
TARGETS = (("darwin", "arm64"), ("darwin", "amd64"), ("linux", "amd64"), ("linux", "arm64"))
VERSION = re.compile(r"v(?:0|[1-9][0-9]*)\.(?:0|[1-9][0-9]*)\.(?:0|[1-9][0-9]*)(?:-[0-9A-Za-z]+(?:[.-][0-9A-Za-z]+)*)?")
NOTICE = re.compile(r"^(?:licen[cs]e|copying|copyright|notice|unlicense|patents)(?:$|[._-])", re.I)


def run(*args, env=None):
    return subprocess.check_output(args, cwd=ROOT, env=env, text=True).strip()


def json_stream(raw):
    decoder = json.JSONDecoder()
    while raw.strip():
        raw = raw.lstrip()
        value, end = decoder.raw_decode(raw)
        yield value
        raw = raw[end:]


def linked_modules(env):
    modules = {}
    for package in json_stream(run("go", "list", "-deps", "-json", "./cmd/localzie-connector", env=env)):
        module = package.get("Module")
        if module and not module.get("Main"):
            if module.get("Replace") or not module.get("Version"):
                raise ValueError("Release dependencies must use pinned public modules, without replacements")
            modules[module["Path"]] = module
    return modules


def license_files(directory):
    root = Path(directory)
    return sorted(p for p in root.rglob("*") if p.is_file() and not p.is_symlink()
                  and NOTICE.match(p.name) and p.suffix not in {".go", ".sum", ".mod"})


def add_bytes(archive, name, content, epoch, mode=0o644):
    info = tarfile.TarInfo(name)
    info.size, info.mtime, info.mode = len(content), epoch, mode
    archive.addfile(info, io.BytesIO(content))


def make_archive(destination, binary, metadata, modules, epoch):
    prefix = destination.name.removesuffix(".tar.gz")
    # Explicit file allowlist: never archive a checkout, state, config or identity.
    with destination.open("wb") as output, gzip.GzipFile(fileobj=output, filename="", mode="wb", mtime=epoch) as compressed:
        with tarfile.open(fileobj=compressed, mode="w") as archive:
            add_bytes(archive, prefix + "/localzie-connector", binary.read_bytes(), epoch, 0o755)
            for name in ("LICENSE", "NOTICE", "THIRD_PARTY_NOTICES.md", "install.sh"):
                add_bytes(archive, prefix + "/" + name, (ROOT / name).read_bytes(), epoch, 0o755 if name == "install.sh" else 0o644)
            readme = ("Localzie Connector " + metadata["version"] + "\n\n"
                      "Localzie for iPhone: https://apps.apple.com/app/id6813506926\n\n"
                      "Run ./install.sh to install the connector. It confirms success and prints\n"
                      "the exact command to start setup. Or run ./localzie-connector setup\n"
                      "directly from this folder. Keep the connector terminal open.\n"
                      "Setup codes are private, single-use and expire after ten minutes.\n\n"
                      "This is a developer preview. New installations connect at home;\n"
                      "automatic remote activation is still being added by Localzie.\n"
                      "Mac downloads are not Developer ID signed or notarized yet.\n\n"
                      "Help: https://github.com/manuelemosr/localzie-connector\n")
            add_bytes(archive, prefix + "/README.txt", readme.encode(), epoch)
            add_bytes(archive, prefix + "/BUILD.json", (json.dumps(metadata, indent=2) + "\n").encode(), epoch)
            inventory = []
            for module in sorted(modules.values(), key=lambda m: m["Path"]):
                files = license_files(module["Dir"])
                if not files:
                    raise ValueError("No license notices found for " + module["Path"])
                paths = []
                for source in files:
                    relative = source.relative_to(module["Dir"]).as_posix()
                    name = "THIRD_PARTY_LICENSES/" + module["Path"] + "@" + module["Version"] + "/" + relative
                    add_bytes(archive, prefix + "/" + name, source.read_bytes(), epoch)
                    paths.append(name)
                inventory.append({"module": module["Path"], "version": module["Version"], "notices": paths})
            goroot = Path(run("go", "env", "GOROOT"))
            for name in ("LICENSE", "PATENTS"):
                source = goroot / name
                if source.exists():
                    add_bytes(archive, prefix + "/THIRD_PARTY_LICENSES/go/" + name, source.read_bytes(), epoch)
            add_bytes(archive, prefix + "/DEPENDENCIES.json", (json.dumps(inventory, indent=2) + "\n").encode(), epoch)


def build(version, output, allow_dirty=False):
    if not VERSION.fullmatch(version):
        raise ValueError("Use a version such as v0.1.0-preview.1 or v1.0.0")
    dirty = bool(run("git", "status", "--porcelain"))
    if dirty and not allow_dirty:
        raise ValueError("Commit your changes before building a release")
    revision = run("git", "rev-parse", "HEAD")
    epoch = int(run("git", "show", "-s", "--format=%ct", "HEAD"))
    output = output.resolve()
    if output.exists() and any(output.iterdir()):
        raise ValueError("Use an empty output directory so old release files cannot be included")
    output.mkdir(parents=True, exist_ok=True)
    go_version = run("go", "version")
    manifest = {"version": version, "revision": revision, "dirty": dirty, "go": go_version, "archives": []}
    with tempfile.TemporaryDirectory(prefix="localzie-release-") as temp:
        binary = Path(temp) / "localzie-connector"
        for goos, goarch in TARGETS:
            env = dict(os.environ, GOOS=goos, GOARCH=goarch, CGO_ENABLED="0", GOWORK="off", GOFLAGS="-mod=readonly")
            subprocess.run(["go", "build", "-trimpath", "-buildvcs=true", "-ldflags",
                            "-s -w -X main.version=" + version, "-o", str(binary),
                            "./cmd/localzie-connector"], cwd=ROOT, env=env, check=True)
            modules = linked_modules(env)
            metadata = {"version": version, "revision": revision, "dirty": dirty,
                        "os": goos, "arch": goarch, "go": go_version}
            archive = output / f"localzie-connector_{version}_{goos}_{goarch}.tar.gz"
            make_archive(archive, binary, metadata, modules, epoch)
            manifest["archives"].append({"file": archive.name, "os": goos, "arch": goarch})
            print("Built " + archive.name, flush=True)
    (output / "BUILD_MANIFEST.json").write_text(json.dumps(manifest, indent=2) + "\n")
    files = sorted(p for p in output.iterdir() if p.is_file())
    (output / "SHA256SUMS").write_text("".join(hashlib.sha256(p.read_bytes()).hexdigest() + "  " + p.name + "\n" for p in files))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--version", required=True)
    parser.add_argument("--output", type=Path, default=ROOT / "dist")
    parser.add_argument("--allow-dirty", action="store_true", help="local packaging checks only; never used by release CI")
    args = parser.parse_args()
    try:
        build(args.version, args.output, args.allow_dirty)
    except (ValueError, subprocess.CalledProcessError) as error:
        parser.exit(1, str(error) + "\n")
