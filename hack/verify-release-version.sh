#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

is_stable_semver() {
  local version=$1
  [[ "${version}" =~ ^(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)$ ]]
}

require_single_value() {
  local label=$1
  local value=$2
  if [[ -z "${value}" || "${value}" == *$'\n'* ]]; then
    echo "${label} must contain exactly one version, got '${value}'" >&2
    return 1
  fi
}

require_version() {
  local label=$1
  local observed=$2
  local expected=$3
  require_single_value "${label}" "${observed}" || return 1
  if [[ "${observed}" != "${expected}" ]]; then
    echo "${label} version mismatch: expected=${expected} observed=${observed}" >&2
    return 1
  fi
}

verify_release_version() {
  local root=$1
  local release_tag=${2:-}
  local cargo_version
  local lock_version
  local chart_version
  local chart_app_version
  local image_tag
  local docker_version

  cargo_version="$(
    awk '
      $0 == "[package]" { in_package = 1; next }
      /^\[/ { in_package = 0 }
      in_package && $1 == "version" {
        value = $3
        gsub(/^"|"$/, "", value)
        print value
      }
    ' "${root}/Cargo.toml"
  )"
  require_single_value "Cargo.toml package" "${cargo_version}" || return 1
  if ! is_stable_semver "${cargo_version}"; then
    echo "Cargo.toml package version must be stable SemVer without prerelease or build metadata: ${cargo_version}" >&2
    return 1
  fi

  lock_version="$(
    awk '
      $0 == "name = \"chronos\"" {
        if (getline > 0 && $1 == "version") {
          value = $3
          gsub(/^"|"$/, "", value)
          print value
        }
      }
    ' "${root}/Cargo.lock"
  )"
  chart_version="$(
    awk '$1 == "version:" { value = $2; gsub(/^"|"$/, "", value); print value }' \
      "${root}/deploy/helm/chronos/Chart.yaml"
  )"
  chart_app_version="$(
    awk '$1 == "appVersion:" { value = $2; gsub(/^"|"$/, "", value); print value }' \
      "${root}/deploy/helm/chronos/Chart.yaml"
  )"
  image_tag="$(
    awk '
      $0 == "image:" { in_image = 1; next }
      in_image && /^[^[:space:]]/ { in_image = 0 }
      in_image && $1 == "tag:" {
        value = $2
        gsub(/^"|"$/, "", value)
        print value
      }
    ' "${root}/deploy/helm/chronos/values.yaml"
  )"
  docker_version="$(
    awk '
      function extract_version(line, value) {
        if (line !~ /org\.opencontainers\.image\.version="[^"]*"/) {
          return
        }
        value = line
        sub(/^.*org\.opencontainers\.image\.version="/, "", value)
        sub(/".*$/, "", value)
        print value
      }
      /^[[:space:]]*#/ { next }
      !in_label && /^[[:space:]]*LABEL[[:space:]]+/ {
        in_label = 1
        line = $0
        sub(/^[[:space:]]*LABEL[[:space:]]+/, "", line)
        extract_version(line)
        if (line !~ /\\[[:space:]]*$/) {
          in_label = 0
        }
        next
      }
      in_label {
        line = $0
        extract_version(line)
        if (line !~ /\\[[:space:]]*$/) {
          in_label = 0
        }
      }
    ' "${root}/Dockerfile"
  )"

  require_version "Cargo.lock chronos package" "${lock_version}" "${cargo_version}" || return 1
  require_version "Helm chart" "${chart_version}" "${cargo_version}" || return 1
  require_version "Helm appVersion" "${chart_app_version}" "${cargo_version}" || return 1
  require_version "Helm default image tag" "${image_tag}" "${cargo_version}" || return 1
  require_version "Docker OCI label" "${docker_version}" "${cargo_version}" || return 1

  if ! awk '
    $0 == "## [Unreleased]" { found = 1 }
    END { exit !found }
  ' "${root}/CHANGELOG.md"; then
    echo "CHANGELOG.md is missing the [Unreleased] heading" >&2
    return 1
  fi

  if [[ -n "${release_tag}" ]]; then
    if [[ "${release_tag}" != v* ]] || ! is_stable_semver "${release_tag#v}"; then
      echo "root release tag must be v followed by stable SemVer without prerelease or build metadata, got '${release_tag}'" >&2
      return 1
    fi
    if [[ "${release_tag#v}" != "${cargo_version}" ]]; then
      echo "root release tag mismatch: expected=v${cargo_version} observed=${release_tag}" >&2
      return 1
    fi
    if ! awk -v expected="${cargo_version}" '
      $0 == "## [" expected "]" || index($0, "## [" expected "] - ") == 1 { found = 1 }
      END { exit !found }
    ' "${root}/CHANGELOG.md"; then
      echo "CHANGELOG.md is missing a release heading for ${cargo_version}" >&2
      return 1
    fi
  fi

  echo "[release-version] verified version=${cargo_version}${release_tag:+ tag=${release_tag}}"
}

write_fixture() {
  local root=$1
  mkdir -p "${root}/deploy/helm/chronos"
  cat >"${root}/Cargo.toml" <<'EOF'
[package]
name = "chronos"
version = "1.2.3"
EOF
  cat >"${root}/Cargo.lock" <<'EOF'
[[package]]
name = "chronos"
version = "1.2.3"
EOF
  cat >"${root}/deploy/helm/chronos/Chart.yaml" <<'EOF'
apiVersion: v2
name: chronos
version: 1.2.3
appVersion: "1.2.3"
EOF
  cat >"${root}/deploy/helm/chronos/values.yaml" <<'EOF'
image:
  repository: example/chronos
  tag: "1.2.3"
EOF
  cat >"${root}/Dockerfile" <<'EOF'
LABEL org.opencontainers.image.title="chronos" \
      org.opencontainers.image.version="1.2.3"
EOF
  cat >"${root}/CHANGELOG.md" <<'EOF'
## [Unreleased]

## [1.2.3] - 2026-07-25
EOF
}

expect_failure() {
  local label=$1
  shift
  if "$@" >/dev/null 2>&1; then
    echo "release-version verifier accepted ${label}" >&2
    return 1
  fi
}

self_test() {
  local test_dir
  test_dir="$(mktemp -d)"
  trap 'rm -rf "${test_dir}"' RETURN

  write_fixture "${test_dir}"
  verify_release_version "${test_dir}" "v1.2.3"

  expect_failure "a non-SemVer tag" verify_release_version "${test_dir}" "v1"
  expect_failure "a SemVer tag with a leading zero" \
    verify_release_version "${test_dir}" "v01.2.3"
  expect_failure "a prerelease root tag" \
    verify_release_version "${test_dir}" "v1.2.3-rc.1"
  expect_failure "a root tag with build metadata" \
    verify_release_version "${test_dir}" "v1.2.3+build.1"
  expect_failure "a tag that differs from the package version" \
    verify_release_version "${test_dir}" "v1.2.4"

  local hostile_tag
  local injection_marker="${test_dir}/injected"
  hostile_tag='v$$(touch>'"${injection_marker}"')'
  git check-ref-format "refs/tags/${hostile_tag}"
  expect_failure "a hostile but valid git tag" \
    env CHRONOS_RELEASE_TAG="${hostile_tag}" \
    make --no-print-directory -C "${REPO_ROOT}" release-version-check
  if [[ -e "${injection_marker}" ]]; then
    echo "release-version Make path executed hostile tag content" >&2
    return 1
  fi

  write_fixture "${test_dir}"
  sed -i.bak 's/version = "1.2.3"/version = "1.2.4"/' "${test_dir}/Cargo.lock"
  expect_failure "Cargo.lock drift" verify_release_version "${test_dir}"

  write_fixture "${test_dir}"
  sed -i.bak 's/version = "1.2.3"/version = "1.2.3-rc.1"/' "${test_dir}/Cargo.toml"
  expect_failure "a prerelease root package version" verify_release_version "${test_dir}"

  write_fixture "${test_dir}"
  sed -i.bak 's/version: 1.2.3/version: 1.2.4/' \
    "${test_dir}/deploy/helm/chronos/Chart.yaml"
  expect_failure "Helm chart version drift" verify_release_version "${test_dir}"

  write_fixture "${test_dir}"
  sed -i.bak 's/appVersion: "1.2.3"/appVersion: "1.2.4"/' \
    "${test_dir}/deploy/helm/chronos/Chart.yaml"
  expect_failure "Helm appVersion drift" verify_release_version "${test_dir}"

  write_fixture "${test_dir}"
  sed -i.bak 's/tag: "1.2.3"/tag: "1.2.4"/' \
    "${test_dir}/deploy/helm/chronos/values.yaml"
  expect_failure "Helm image tag drift" verify_release_version "${test_dir}"

  write_fixture "${test_dir}"
  sed -i.bak 's/version="1.2.3"/version="1.2.4"/' "${test_dir}/Dockerfile"
  expect_failure "Docker label drift" verify_release_version "${test_dir}"

  write_fixture "${test_dir}"
  sed -i.bak 's/^LABEL /# LABEL /' "${test_dir}/Dockerfile"
  expect_failure "a comment-only Docker version label" verify_release_version "${test_dir}"

  write_fixture "${test_dir}"
  sed -i.bak 's/^LABEL org.opencontainers.image.title="chronos" \\/RUN true/' \
    "${test_dir}/Dockerfile"
  expect_failure "an orphaned Docker version value" verify_release_version "${test_dir}"

  write_fixture "${test_dir}"
  sed -i.bak 's/\[1.2.3\]/[1.2.4]/' "${test_dir}/CHANGELOG.md"
  expect_failure "missing matching tagged changelog heading" \
    verify_release_version "${test_dir}" "v1.2.3"

  write_fixture "${test_dir}"
  sed -i.bak 's/\[Unreleased\]/[Development]/' "${test_dir}/CHANGELOG.md"
  expect_failure "missing Unreleased changelog heading" verify_release_version "${test_dir}"

  echo "[release-version] verifier self-test passed"
}

case "${1:-}" in
  --self-test)
    self_test
    ;;
  "")
    verify_release_version "${REPO_ROOT}" "${CHRONOS_RELEASE_TAG:-}"
    ;;
  *)
    echo "usage: $0 [--self-test]" >&2
    exit 1
    ;;
esac
