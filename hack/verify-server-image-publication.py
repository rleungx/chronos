#!/usr/bin/env python3
"""Fail-closed operational contract for Chronos server-image publication."""

import json
import re
import sys
from pathlib import Path

REPOSITORY = "ghcr.io/rleungx/chronos"
DIGEST = re.compile(r"sha256:[0-9a-f]{64}")

def reject(message: str) -> None:
    raise ValueError(message)

def extract(source: str, pattern: str, message: str) -> str:
    match = re.search(pattern, source)
    if not match:
        reject(message)
    return match.group(0)

def job(source: str, name: str) -> str:
    return extract(source, rf"(?ms)^  {re.escape(name)}:\n.*?(?=^  [\w-]+:\n|\Z)", f"missing job: {name}")

def step(source: str, name: str) -> str:
    return extract(source, rf"(?ms)^      - name: {re.escape(name)}\n.*?(?=^      - |\Z)", f"missing step: {name}")

def commands(source: str) -> str:
    match = re.search(r"(?ms)^        run: \|\n((?:^          .*(?:\n|\Z))+)", source)
    if not match:
        reject("operational step requires a non-empty multiline run body")
    lines = match.group(1).splitlines()
    return "\n".join(line.strip() for line in lines if line.strip() and not line.lstrip().startswith("#"))

def require(source: str, *patterns: str) -> None:
    for pattern in patterns:
        if not re.search(pattern, source, re.MULTILINE):
            reject(f"missing operational source: {pattern}")

def valid_digest(value: str, label: str) -> None:
    if not DIGEST.fullmatch(value):
        reject(f"invalid {label} digest: {value}")

def alias_decision(status: int, observed: str, candidate: str, strict: bool = False) -> str:
    valid_digest(candidate, "candidate")
    if status == 404:
        if strict: reject("postflight alias is not canonical")
        return "publish"
    if status == 200:
        valid_digest(observed, "observed")
        if observed == candidate:
            return "idempotent"
    reject(f"alias lookup rejected: status={status} observed={observed}")

def release_record(repository: str, version: str, commit: str, tar: str, config: str, manifest: str) -> dict:
    if repository != REPOSITORY or not re.fullmatch(r"[0-9]+\.[0-9]+\.[0-9]+", version):
        reject("invalid release repository or version")
    if not re.fullmatch(r"[0-9a-f]{40}", commit):
        reject("invalid full release commit")
    for value, label in ((tar, "tar"), (config, "config"), (manifest, "manifest")):
        valid_digest(value, label)
    return {
        "schema_version": 1, "repository": repository, "version": version, "commit": commit,
        "tag_policy": "mutable_discovery_alias", "local_tar_sha256": tar,
        "candidate_config_digest": config, "canonical_manifest_digest": manifest,
        "immutable_reference": f"{repository}@{manifest}",
        "aliases": {"version": f"{repository}:{version}", "commit": f"{repository}:{commit}"},
    }

def release_asset_decision(status: int, candidate: dict) -> str:
    expected = release_record(candidate.get("repository", ""), candidate.get("version", ""), candidate.get("commit", ""), candidate.get("local_tar_sha256", ""), candidate.get("candidate_config_digest", ""), candidate.get("canonical_manifest_digest", ""))
    if candidate != expected:
        reject("incomplete or extended release authority schema")
    if status == 404:
        return "publish"
    reject(f"existing or failed release lookup rejected: status={status}")

def verify_source(source: str, helm: str) -> None:
    publish, package = job(source, "publish-server-release"), job(source, "package-release-artifacts")
    image, runtime, scan = (job(source, name) for name in ("container-image", "container-runtime-check", "container-vulnerability-scan"))
    require(
        publish,
        r"^    if: github\.event_name == 'push' && github\.ref_type == 'tag' && startsWith\(github\.ref, 'refs/tags/v'\)$",
        r"^    runs-on: ubuntu-latest$", r"^    timeout-minutes: 30$", r"^      group: publish-server-release-\$\{\{ github\.ref \}\}$", r"^      cancel-in-progress: false$",
        r"^      VERSION: \$\{\{ needs\.release-version\.outputs\.version \}\}$", r"^      COMMIT: \$\{\{ github\.sha \}\}$",
        r"^      GH_TOKEN: \$\{\{ github\.token \}\}$", r"^      REGISTRY_API: https://ghcr\.io/v2/rleungx/chronos/manifests$",
        r"^      REGISTRY_USERNAME: \$\{\{ github\.actor \}\}$", r"^      REGISTRY_PASSWORD: \$\{\{ github\.token \}\}$",
        r"^      contents: write$", r"^      packages: write$", r"^      attestations: write$", r"^      id-token: write$",
        r"^      - release-version$", r"^      - container-image$", r"^      - package-release-artifacts$",
    )
    require(package, r"^      contents: read$")
    for name, artifact in (("Download release candidate", "chronos-release-candidate"), ("Download container image", "chronos-rc-image")):
        require(step(publish, name), r"^        uses: actions/download-artifact@[0-9a-f]{40}$", rf"^          name: {artifact}$")
    require(step(publish, "Check out publication verifier"), r"^        uses: actions/checkout@[0-9a-f]{40}$")
    require(step(publish, "Download release candidate"), r"^          path: artifacts/release$")
    require(step(publish, "Log in to GHCR"), r"^        uses: docker/login-action@[0-9a-f]{40}$", r"^          registry: ghcr\.io$", r"username: \$\{\{ github\.actor \}\}", r"password: \$\{\{ github\.token \}\}")
    require(step(image, "Upload container image"), r"^          name: chronos-rc-image$", r"chronos-rc-image\.tar$", r"chronos-rc-image\.tar\.sha256$")
    for owner in (image, runtime, scan, publish):
        require(owner, r"sha256sum (?:chronos-rc-image\.tar > |-c )chronos-rc-image\.tar\.sha256")
    load = commands(step(publish, "Load container image"))
    pre = commands(step(publish, "Preflight mutable discovery aliases"))
    push = commands(step(publish, "Publish exact container image aliases"))
    post = commands(step(publish, "Verify aliases and create release authority"))
    require(load, r"docker load -i chronos-rc-image\.tar", r"candidate_config=\$\(docker image inspect chronos:rc --format '\{\{\.Id\}\}'\)", r"candidate_config=.*GITHUB_OUTPUT")
    require(
        pre, r"registry_token=.*ghcr\.io/token", r"Authorization: Bearer \$registry_token",
        r"version_status=.*curl .*--retry 3.*%\{http_code\}.*\$VERSION.*version\.json", r"version_observed=.*\.config\.digest.*version\.json",
        r"commit_status=.*curl .*--retry 3.*%\{http_code\}.*\$COMMIT.*commit\.json", r"commit_observed=.*\.config\.digest.*commit\.json",
        r'alias-decision "\$version_status" "\$version_observed" "\$candidate_config"',
        r'alias-decision "\$commit_status" "\$commit_observed" "\$candidate_config"', r"version_decision=.*GITHUB_OUTPUT", r"commit_decision=.*GITHUB_OUTPUT",
    )
    require(
        push, r'docker tag chronos:rc "\$CHRONOS_IMAGE_REPOSITORY:\$VERSION"', r'docker tag chronos:rc "\$CHRONOS_IMAGE_REPOSITORY:\$COMMIT"',
        r'docker push "\$CHRONOS_IMAGE_REPOSITORY:\$VERSION"', r'docker push "\$CHRONOS_IMAGE_REPOSITORY:\$COMMIT"',
        r"steps\.preflight\.outputs\.version_decision", r"steps\.preflight\.outputs\.commit_decision", r"registry_token=.*ghcr\.io/token", r"Authorization: Bearer \$registry_token", r"curl .*--retry 3.*\$VERSION.*canonical\.headers", r"canonical_digest=.*Docker-Content-Digest.*canonical\.headers", r"digest=.*GITHUB_OUTPUT",
    )
    require(
        post, r"canonical_digest=.*steps\.publish\.outputs\.digest", r"candidate_config=.*steps\.load\.outputs\.candidate_config", r"registry_token=.*ghcr\.io/token", r"Authorization: Bearer \$registry_token",
        r"version_status=.*curl .*--retry 3.*%\{http_code\}.*\$VERSION.*version\.headers", r"version_observed=.*Docker-Content-Digest.*version\.headers",
        r"commit_status=.*curl .*--retry 3.*%\{http_code\}.*\$COMMIT.*commit\.headers", r"commit_observed=.*Docker-Content-Digest.*commit\.headers",
        r'alias-verify "\$version_status" "\$version_observed" "\$canonical_digest"', r'alias-verify "\$commit_status" "\$commit_observed" "\$canonical_digest"',
        r"release-json chronos-server-image\.json .*\$CHRONOS_IMAGE_REPOSITORY.*\$VERSION.*\$COMMIT.*\$tar_sha.*\$candidate_config.*\$canonical_digest",
    )
    attest = step(publish, "Attest server image")
    require(attest, r"^        uses: actions/attest-build-provenance@[0-9a-f]{40}$", r"subject-name: \$\{\{ env\.CHRONOS_IMAGE_REPOSITORY \}\}", r"subject-digest: \$\{\{ steps\.publish\.outputs\.digest \}\}", r"push-to-registry: true", r"create-storage-record: false")
    release_step = step(publish, "Publish GitHub release"); release = commands(release_step)
    require(release, r"api\.github\.com/repos/\$GITHUB_REPOSITORY/releases/tags/\$GITHUB_REF_NAME", r"Authorization: Bearer \$GH_TOKEN", r"release-asset-decision .*\$release_status.*chronos-server-image\.json", r'"\$release_decision" != publish \|\| gh release create ', r"artifacts/release/\*", r"chronos-server-image\.json")
    ordered = ("Verify container image checksum", "Load container image", "Preflight mutable discovery aliases", "Publish exact container image aliases", "Verify aliases and create release authority", "Attest server image", "Publish GitHub release")
    names = re.findall(r"(?m)^      - name: (.+)$", publish)
    if [names.index(name) for name in ordered] != sorted(names.index(name) for name in ordered) or names[-1] != ordered[-1] or not publish.rstrip().endswith(release_step.rstrip()):
        reject("publication authority steps are out of order or release is not last")
    require(source, rf"^  CHRONOS_IMAGE_REPOSITORY: {REPOSITORY}$")
    require(helm, rf"^  repository: {REPOSITORY}$")
    if re.search(r"docker\s+(build|buildx\s+build)|build-push-action|FORCE_BUILD=1", publish):
        reject("publisher must not rebuild")
    if len(re.findall(r"(?m)^\s+packages: write$", source)) != 1 or source.count("gh release create ") != 1:
        reject("publication requires one tag-only package/release authority")
    if re.search(r"(?m)retention-days:\s*1\s*$", image):
        reject("image artifact retention is too short")

def fixture() -> str:
    pin = "a" * 40
    def named(name: str, *body: str) -> str:
        return f"      - name: {name}\n" + "".join(f"        {line}\n" for line in body)
    checksum = named("Verify container image checksum", "run: sha256sum -c chronos-rc-image.tar.sha256")
    producer = named("Create container image checksum", "run: sha256sum chronos-rc-image.tar > chronos-rc-image.tar.sha256") + named("Upload container image", f"uses: actions/upload-artifact@{pin}", "with:", "  name: chronos-rc-image", "  path: |", "    chronos-rc-image.tar", "    chronos-rc-image.tar.sha256")
    flow = named("Check out publication verifier", f"uses: actions/checkout@{pin}") + named("Download release candidate", f"uses: actions/download-artifact@{pin}", "with:", "  name: chronos-release-candidate", "  path: artifacts/release") + named("Download container image", f"uses: actions/download-artifact@{pin}", "with:", "  name: chronos-rc-image") + checksum
    flow += named("Log in to GHCR", f"uses: docker/login-action@{pin}", "with:", "  registry: ghcr.io", "  username: ${{ github.actor }}", "  password: ${{ github.token }}")
    flow += named("Load container image", "id: load", "run: |", "  docker load -i chronos-rc-image.tar", "  candidate_config=$(docker image inspect chronos:rc --format '{{.Id}}')", '  echo "candidate_config=$candidate_config" >> "$GITHUB_OUTPUT"')
    flow += named("Preflight mutable discovery aliases", "id: preflight", "run: |", "  candidate_config=${{ steps.load.outputs.candidate_config }}", '  registry_token=$(curl --retry 3 -fsS -u "$REGISTRY_USERNAME:$REGISTRY_PASSWORD" "https://ghcr.io/token?scope=repository:rleungx/chronos:pull,push" | jq -er .token)', "  version_status=$(curl --retry 3 -H \"Authorization: Bearer $registry_token\" -w '%{http_code}' $REGISTRY_API/$VERSION -o version.json)", "  version_observed=$(jq -r .config.digest version.json)", "  commit_status=$(curl --retry 3 -H \"Authorization: Bearer $registry_token\" -w '%{http_code}' $REGISTRY_API/$COMMIT -o commit.json)", "  commit_observed=$(jq -r .config.digest commit.json)", '  version_decision=$(python3 hack/verify-server-image-publication.py alias-decision "$version_status" "$version_observed" "$candidate_config")', '  commit_decision=$(python3 hack/verify-server-image-publication.py alias-decision "$commit_status" "$commit_observed" "$candidate_config")', '  echo "version_decision=$version_decision" >> "$GITHUB_OUTPUT"', '  echo "commit_decision=$commit_decision" >> "$GITHUB_OUTPUT"')
    flow += named("Publish exact container image aliases", "id: publish", "run: |", '  docker tag chronos:rc "$CHRONOS_IMAGE_REPOSITORY:$VERSION"', '  docker tag chronos:rc "$CHRONOS_IMAGE_REPOSITORY:$COMMIT"', '  test "${{ steps.preflight.outputs.version_decision }}" != publish || docker push "$CHRONOS_IMAGE_REPOSITORY:$VERSION"', '  test "${{ steps.preflight.outputs.commit_decision }}" != publish || docker push "$CHRONOS_IMAGE_REPOSITORY:$COMMIT"', '  registry_token=$(curl --retry 3 -fsS -u "$REGISTRY_USERNAME:$REGISTRY_PASSWORD" "https://ghcr.io/token?scope=repository:rleungx/chronos:pull" | jq -er .token)', '  curl --retry 3 -H "Authorization: Bearer $registry_token" $REGISTRY_API/$VERSION -D canonical.headers -o /dev/null', "  canonical_digest=$(awk '/Docker-Content-Digest/ {print $2}' canonical.headers | tr -d '\\r')", '  echo "digest=$canonical_digest" >> "$GITHUB_OUTPUT"')
    flow += named("Verify aliases and create release authority", "run: |", "  canonical_digest=${{ steps.publish.outputs.digest }}", "  candidate_config=${{ steps.load.outputs.candidate_config }}", '  registry_token=$(curl --retry 3 -fsS -u "$REGISTRY_USERNAME:$REGISTRY_PASSWORD" "https://ghcr.io/token?scope=repository:rleungx/chronos:pull" | jq -er .token)', "  version_status=$(curl --retry 3 -H \"Authorization: Bearer $registry_token\" -w '%{http_code}' $REGISTRY_API/$VERSION -D version.headers -o /dev/null)", "  version_observed=$(awk '/Docker-Content-Digest/ {print $2}' version.headers | tr -d '\\r')", "  commit_status=$(curl --retry 3 -H \"Authorization: Bearer $registry_token\" -w '%{http_code}' $REGISTRY_API/$COMMIT -D commit.headers -o /dev/null)", "  commit_observed=$(awk '/Docker-Content-Digest/ {print $2}' commit.headers | tr -d '\\r')", '  python3 hack/verify-server-image-publication.py alias-verify "$version_status" "$version_observed" "$canonical_digest"', '  python3 hack/verify-server-image-publication.py alias-verify "$commit_status" "$commit_observed" "$canonical_digest"', "  tar_sha=\"sha256:$(cut -d' ' -f1 chronos-rc-image.tar.sha256)\"", '  python3 hack/verify-server-image-publication.py release-json chronos-server-image.json "$CHRONOS_IMAGE_REPOSITORY" "$VERSION" "$COMMIT" "$tar_sha" "$candidate_config" "$canonical_digest"')
    flow += named("Attest server image", f"uses: actions/attest-build-provenance@{pin}", "with:", "  subject-name: ${{ env.CHRONOS_IMAGE_REPOSITORY }}", "  subject-digest: ${{ steps.publish.outputs.digest }}", "  push-to-registry: true", "  create-storage-record: false")
    flow += named("Publish GitHub release", "run: |", '  release_status=$(curl --retry 3 -H "Authorization: Bearer $GH_TOKEN" -w \'%{http_code}\' "https://api.github.com/repos/$GITHUB_REPOSITORY/releases/tags/$GITHUB_REF_NAME" -o release.json)', '  release_decision=$(python3 hack/verify-server-image-publication.py release-asset-decision "$release_status" chronos-server-image.json)', '  test "$release_decision" != publish || gh release create "$GITHUB_REF_NAME" artifacts/release/* chronos-server-image.json')
    return f"""env:
  CHRONOS_IMAGE_REPOSITORY: {REPOSITORY}
jobs:
  container-image:
    steps:
{producer}  container-runtime-check:
    steps:
{checksum}  container-vulnerability-scan:
    steps:
{checksum}  package-release-artifacts:
    permissions:
      contents: read
  publish-server-release:
    if: github.event_name == 'push' && github.ref_type == 'tag' && startsWith(github.ref, 'refs/tags/v')
    runs-on: ubuntu-latest
    timeout-minutes: 30
    concurrency:
      group: publish-server-release-${{{{ github.ref }}}}
      cancel-in-progress: false
    env:
      VERSION: ${{{{ needs.release-version.outputs.version }}}}
      COMMIT: ${{{{ github.sha }}}}
      GH_TOKEN: ${{{{ github.token }}}}
      REGISTRY_API: https://ghcr.io/v2/rleungx/chronos/manifests
      REGISTRY_USERNAME: ${{{{ github.actor }}}}
      REGISTRY_PASSWORD: ${{{{ github.token }}}}
    permissions:
      contents: write
      packages: write
      attestations: write
      id-token: write
    needs:
      - release-version
      - container-image
      - package-release-artifacts
    steps:
{flow}"""

def must_reject(action, label: str) -> None:
    try:
        action()
    except (ValueError, OSError, json.JSONDecodeError):
        return
    reject(f"accepted unsafe fixture: {label}")

def self_test() -> None:
    digest, other = "sha256:" + "a" * 64, "sha256:" + "b" * 64
    record = release_record(REPOSITORY, "1.2.3", "c" * 40, digest, digest, digest)
    assert alias_decision(404, "", digest) == "publish" and alias_decision(200, digest, digest) == "idempotent"
    assert alias_decision(200, digest, digest, True) == "idempotent"
    assert release_asset_decision(404, record) == "publish"
    for case in ((404, "", "bad"), (200, "bad", digest), (200, other, digest), (401, "", digest), (429, "", digest), (500, "", digest), (0, "", digest)):
        must_reject(lambda case=case: alias_decision(*case), str(case))
    must_reject(lambda: release_asset_decision(200, record), "existing release")
    must_reject(lambda: alias_decision(404, "", digest, True), "postflight 404")
    must_reject(lambda: release_asset_decision(404, {k: v for k, v in record.items() if k != "immutable_reference"}), "incomplete release asset")
    base, helm = fixture(), f"image:\n  repository: {REPOSITORY}\n"
    verify_source(base, helm)
    mutations = (
        ("--format '{{.Id}}'", "--format '{{.RepoTags}}'"), (".config.digest version.json", ".config.digest commit.json"),
        ("Log in to GHCR", "Missing registry login"), ("uses: actions/attest-build-provenance@", "x-uses: actions/attest-build-provenance@"),
        ("needs.release-version.outputs.version", "github.ref_name"), ("${{ github.sha }}", "${{ github.sha_short }}"),
        ("REGISTRY_API: https://ghcr.io/v2/rleungx/chronos/manifests", "REGISTRY_API_REMOVED: true"),
        ('"$tar_sha" "$candidate_config" "$canonical_digest"', '"$tar_sha" "$canonical_digest"'),
        ("packages: write", "packages: read"), ("docker tag chronos:rc", "docker build . && docker tag chronos:rc"),
        ("chronos-server-image.json", "server-image.txt"),
    )
    for old, new in mutations:
        must_reject(lambda old=old, new=new: verify_source(base.replace(old, new, 1), helm), old)
    attest_block, release_block = step(base, "Attest server image"), step(base, "Publish GitHub release")
    must_reject(lambda: verify_source(base.replace(attest_block + release_block, release_block + attest_block), helm), "release before attestation")
    post_block = step(base, "Verify aliases and create release authority")
    must_reject(lambda: verify_source(base.replace(post_block, post_block.replace("version_status=$(curl --retry 3", "version_status=$(curl", 1)), helm), "postflight retry")
    must_reject(lambda: verify_source(base.replace(post_block, post_block.replace("alias-verify", "alias-decision", 1)), helm), "postflight publish result")
    must_reject(lambda: verify_source(base + "      - run: false\n", helm), "trailing unnamed step")
    noop = base
    for name in ("Preflight mutable discovery aliases", "Publish exact container image aliases", "Verify aliases and create release authority"):
        old = step(noop, name)
        noop = noop.replace(old, re.sub(r"(?ms)^        run: \|\n(?:^          .*(?:\n|\Z))+", "        run: echo no-op\n        # retained operational tokens\n", old))
    must_reject(lambda: verify_source(noop, helm), "no-op operational bodies")
    print("[server-image-publication] verifier self-test passed")

def main() -> int:
    try:
        args = sys.argv[1:]
        if args == ["--self-test"]:
            self_test()
        elif len(args) == 4 and args[0] in ("alias-decision", "alias-verify"):
            print(alias_decision(int(args[1]), args[2], args[3], args[0] == "alias-verify"))
        elif len(args) == 8 and args[0] == "release-json":
            Path(args[1]).write_text(json.dumps(release_record(*args[2:]), sort_keys=True, indent=2) + "\n")
        elif len(args) == 3 and args[0] == "release-asset-decision":
            candidate = json.loads(Path(args[2]).read_text())
            print(release_asset_decision(int(args[1]), candidate))
        elif not args:
            root = Path(__file__).resolve().parent.parent
            verify_source((root / ".github/workflows/release-candidate.yml").read_text(), (root / "deploy/helm/chronos/values.yaml").read_text())
            print("[server-image-publication] source contract verified")
        else:
            reject("usage: verifier [--self-test|alias-decision ...|release-json ...|release-asset-decision ...]")
    except (ValueError, OSError, json.JSONDecodeError) as error:
        print(f"server image publication contract rejected: {error}", file=sys.stderr)
        return 1
    return 0

if __name__ == "__main__":
    sys.exit(main())
