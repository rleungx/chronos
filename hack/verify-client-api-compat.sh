#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
LANGUAGE="${1:-}"
OLD_REF="${2:-}"
APIDIFF_VERSION="v0.0.0-20250620022241-b7579e27df2b"
JAPICMP_VERSION="0.26.1"
JAPICMP_SHA256="4b4f0301861d97cd2a05af3f7fa9173cc1dd3831b1284b2b9e2f81c4f1c2f858"

if [[ ! "$LANGUAGE" =~ ^(go|java|cpp)$ || -z "$OLD_REF" ]]; then
  echo "usage: $0 <go|java|cpp> <old-git-ref|--self>" >&2
  exit 2
fi

work_dir="$(mktemp -d -t chronos-client-api.XXXXXX)"
cleanup() {
  rm -rf "${work_dir}"
}
trap cleanup EXIT

old_root="${REPO_ROOT}"
if [[ "$OLD_REF" != "--self" ]]; then
  old_root="${work_dir}/old"
  mkdir -p "$old_root"
  git -C "$REPO_ROOT" archive "$OLD_REF" | tar -x -C "$old_root"
fi

verify_go() {
  local old_api="${work_dir}/old.go.api"
  local new_api="${work_dir}/new.go.api"
  local module="github.com/rleungx/chronos/clients/go"
  (
    cd "${old_root}/clients/go"
    go run "golang.org/x/exp/cmd/apidiff@${APIDIFF_VERSION}" -m -w "$old_api" "$module"
  )
  (
    cd "${REPO_ROOT}/clients/go"
    go run "golang.org/x/exp/cmd/apidiff@${APIDIFF_VERSION}" -m -w "$new_api" "$module"
  )
  local report
  report="$(go run "golang.org/x/exp/cmd/apidiff@${APIDIFF_VERSION}" -m -incompatible "$old_api" "$new_api")"
  if [[ -n "$report" ]]; then
    printf '%s\n' "$report" >&2
    echo "Go client contains backward-incompatible public API changes" >&2
    return 1
  fi
}

java_jar() {
  local root=$1
  local output
  gradle --no-daemon -q -p "${root}/clients/java" clean jar >&2
  output="$(find "${root}/clients/java/build/libs" -maxdepth 1 -type f -name '*.jar' ! -name '*-sources.jar' ! -name '*-javadoc.jar' -print -quit)"
  [[ -n "$output" ]] || {
    echo "Java client jar was not produced under ${root}" >&2
    return 1
  }
  printf '%s\n' "$output"
}

verify_java() {
  local old_jar
  local new_jar
  local tool="${JAPICMP_JAR:-${work_dir}/japicmp-${JAPICMP_VERSION}.jar}"
  old_jar="$(java_jar "$old_root")"
  if [[ "$old_root" == "$REPO_ROOT" ]]; then
    new_jar="$old_jar"
  else
    new_jar="$(java_jar "$REPO_ROOT")"
  fi
  if [[ -z "${JAPICMP_JAR:-}" ]]; then
    curl --connect-timeout 15 --max-time 120 --retry 3 --retry-all-errors -fsSL \
      "https://repo.maven.apache.org/maven2/com/github/siom79/japicmp/japicmp/${JAPICMP_VERSION}/japicmp-${JAPICMP_VERSION}-jar-with-dependencies.jar" \
      -o "$tool"
  fi
  local actual_sha256
  if command -v sha256sum >/dev/null 2>&1; then
    actual_sha256="$(sha256sum "$tool" | awk '{print $1}')"
  else
    actual_sha256="$(shasum -a 256 "$tool" | awk '{print $1}')"
  fi
  [[ "$actual_sha256" == "$JAPICMP_SHA256" ]] || {
    echo "japicmp checksum mismatch: ${actual_sha256}" >&2
    return 1
  }
  java -jar "$tool" \
    --old "$old_jar" \
    --new "$new_jar" \
    --include 'chronos.client.*' \
    --only-incompatible \
    --ignore-missing-classes \
    --error-on-binary-incompatibility \
    --error-on-source-incompatibility
}

cpp_dump() {
  local root=$1
  local name=$2
  local build="${work_dir}/${name}-cpp-build"
  local install="${work_dir}/${name}-cpp-install"
  local dump="${work_dir}/${name}.abi"
  cmake -S "${root}/clients/cpp" -B "$build" \
    -DCHRONOS_CLIENT_VERSION=1.0.0 \
    -DBUILD_SHARED_LIBS=ON \
    -DCMAKE_BUILD_TYPE=Debug \
    -DCMAKE_POSITION_INDEPENDENT_CODE=ON >&2
  cmake --build "$build" --target chronos_client_lib >&2
  cmake --install "$build" --prefix "$install" >&2
  local library
  library="$(find "$build" -maxdepth 2 -type f \( -name 'libchronos_client.so' -o -name 'libchronos_client.dylib' \) -print -quit)"
  [[ -n "$library" ]] || {
    echo "shared C++ client library was not produced under ${build}" >&2
    return 1
  }
  abi-dumper "$library" -o "$dump" -lver "$name" -public-headers "${install}/include" >&2
  printf '%s\n' "$dump"
}

verify_cpp() {
  command -v abi-dumper >/dev/null
  command -v abi-compliance-checker >/dev/null
  local old_dump
  local new_dump
  old_dump="$(cpp_dump "$old_root" old)"
  if [[ "$old_root" == "$REPO_ROOT" ]]; then
    new_dump="$old_dump"
  else
    new_dump="$(cpp_dump "$REPO_ROOT" new)"
  fi
  abi-compliance-checker \
    -l chronos_client \
    -old "$old_dump" \
    -new "$new_dump" \
    -strict \
    -report-path "${work_dir}/cpp-compat-report.html"
}

case "$LANGUAGE" in
  go) verify_go ;;
  java) verify_java ;;
  cpp) verify_cpp ;;
esac

echo "[client-api-compat] ${LANGUAGE} is compatible with ${OLD_REF}"
