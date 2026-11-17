# Build and publish downloads

GitHub builds the connector for four kinds of computer and puts the downloads into a draft release. Publishing the draft makes them available on the repository's **Releases** page. No separate hosting service or personal access token secret is needed for the workflow.

## Use the GitHub website

1. Open **Actions > Release > Run workflow**.
2. Keep the `main` branch selected and enter a version such as `v0.1.0-preview.1`.
3. Click **Run workflow**. GitHub verifies modules, vets and tests the code, builds all four archives and uploads them to a draft release.
4. Open **Releases**, review the draft and its downloads, then click **Publish release**.

Use a preview version while the product is in preview. The workflow marks versions containing `-preview`, `-beta` or another prerelease identifier as prereleases. A version such as `v1.0.0` is a regular release when published.

## Use a version tag

From a clean, committed checkout:

```sh
git tag v0.1.0-preview.1
git push origin v0.1.0-preview.1
```

That starts the same workflow. It creates a draft first, so you can review before publishing. Use a new version for new code. A rerun can repair an incomplete draft of the same source commit; published downloads are never overwritten.

## Downloads

| Computer | Archive ending |
| --- | --- |
| Mac with Apple silicon | `darwin_arm64.tar.gz` |
| Mac with Intel | `darwin_amd64.tar.gz` |
| Linux x64 | `linux_amd64.tar.gz` |
| Linux ARM64 | `linux_arm64.tar.gz` |

Each archive contains `localzie-connector`, short setup instructions, the project license/notices, dependency licenses, `DEPENDENCIES.json` and build metadata. Extract it, open a terminal in that folder and run `./localzie-connector setup`. Customers do not need Go installed.

`SHA256SUMS` verifies the archives and build manifest. On macOS use `shasum -a 256 -c SHA256SUMS`; on Linux use `sha256sum -c SHA256SUMS`, with all release files downloaded into the same directory. Checksums detect changed/corrupted files; they are not Apple signing or an independent publisher signature.

## Local packaging check

```sh
python3 scripts/build_release.py --version v0.1.0-preview.1
```

This requires a clean checkout and writes `dist/`. An empty output directory is required. Local development can use `--allow-dirty --output /tmp/localzie-release-check`; CI never enables dirty releases. The build uses pinned dependencies, `CGO_ENABLED=0`, trimmed build paths and an explicit archive allowlist. No app, relay, configuration or live identity files are packaged.

The Mac downloads are not Apple Developer ID signed/notarized yet, so macOS may require you to approve the download. A native signed installer, Windows builds and automatic updates are separate work. No physical-phone acceptance is implied by packaging or CI checks.

References: [GitHub Releases](https://docs.github.com/en/repositories/releasing-projects-on-github/about-releases), [GitHub CLI release creation](https://cli.github.com/manual/gh_release_create), [workflow authentication](https://docs.github.com/en/actions/tutorials/authenticate-with-github_token).
