#!/bin/bash
# Copyright 2026 Alibaba Group Holding Ltd.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Regression test: a workload image without certutil must receive an
# actionable Chromium/Chrome NSS warning without making bootstrap fail.

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
BOOTSTRAP="$ROOT_DIR/bootstrap.sh"
TESTDIR="$(mktemp -d)"
trap 'rm -rf "$TESTDIR"' EXIT

mkdir -p "$TESTDIR/home" "$TESTDIR/empty-bin"
printf '%s\n' 'test certificate' > "$TESTDIR/mitm-ca.pem"

# Load the production helper without executing the rest of bootstrap.sh.
awk '
  /^trust_mitm_ca_nss\(\) \{/ { capture = 1 }
  capture { print }
  capture && /^}/ { exit }
' "$BOOTSTRAP" > "$TESTDIR/trust_mitm_ca_nss.sh"
# shellcheck source=/dev/null
. "$TESTDIR/trust_mitm_ca_nss.sh"

set +e
HOME="$TESTDIR/home" PATH="$TESTDIR/empty-bin" \
  trust_mitm_ca_nss "$TESTDIR/mitm-ca.pem" 2> "$TESTDIR/stderr"
status=$?
set -e

if [ "$status" -ne 0 ]; then
  echo "FAIL: missing certutil made NSS trust setup fail with status $status" >&2
  exit 1
fi
if ! grep -q 'certutil not found' "$TESTDIR/stderr"; then
  echo "FAIL: missing certutil did not emit an actionable warning" >&2
  exit 1
fi
if ! grep -q 'nss-tools' "$TESTDIR/stderr"; then
  echo "FAIL: warning did not name the Alpine nss-tools package" >&2
  exit 1
fi
if ! grep -q 'libnss3-tools' "$TESTDIR/stderr"; then
  echo "FAIL: warning did not name the Debian/Ubuntu libnss3-tools package" >&2
  exit 1
fi

echo "PASS: missing certutil warns with package guidance and remains best-effort"
