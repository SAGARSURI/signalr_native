#!/usr/bin/env bash
# Milestone 2: Dart checks + Android build + Kotlin unit tests.
# Usage: bash tool/verify_android.sh   (from the package root)
set -u
ROOT="$(pwd)"

if command -v flutter >/dev/null 2>&1; then
  FLUTTER=(flutter); DART=(dart)
elif [ -x "$HOME/fvm/default/bin/flutter" ]; then
  FLUTTER=("$HOME/fvm/default/bin/flutter"); DART=("$HOME/fvm/default/bin/dart")
elif command -v fvm >/dev/null 2>&1; then
  FLUTTER=(fvm flutter); DART=(fvm dart)
else
  echo "flutter not found"; exit 1
fi
step() { printf '\n==================== %s ====================\n' "$1"; }

step "flutter + dev dependencies"
"${FLUTTER[@]}" --version | head -1
grep -q '^  pigeon:' pubspec.yaml || "${FLUTTER[@]}" pub add --dev pigeon flutter_lints
"${FLUTTER[@]}" pub get >/dev/null && echo "pub get ok"

step "pigeon"
mkdir -p lib/src/generated android/src/main/kotlin/com/faciletech/signalr_native ios/signalr_native/Sources/signalr_native
"${DART[@]}" run pigeon --input pigeons/messages.dart && echo "pigeon ok"

step "android plugin build file (from flutter create --template=plugin)"
if ! ls android/build.gradle* >/dev/null 2>&1; then
  TPL="$(mktemp -d)/signalr_native"
  "${FLUTTER[@]}" create --template=plugin --org com.faciletech --project-name signalr_native \
    --platforms android,ios "$TPL" >/dev/null
  for f in build.gradle build.gradle.kts settings.gradle settings.gradle.kts; do
    [ -f "$TPL/android/$f" ] && cp "$TPL/android/$f" "android/$f"
  done
  if [ -f android/build.gradle.kts ]; then
    printf '\napply(from = "signalr-deps.gradle")\n' >> android/build.gradle.kts
  else
    printf '\napply from: "signalr-deps.gradle"\n' >> android/build.gradle
  fi
fi
ls android
cat android/build.gradle* 2>/dev/null

step "example app scaffold"
if [ ! -d example/android ]; then
  (cd example && "${FLUTTER[@]}" create --org com.faciletech --project-name signalr_native_example \
    --platforms android,ios . >/dev/null) && echo "example created"
fi
(cd example && "${FLUTTER[@]}" pub get >/dev/null) && echo "example pub get ok"

step "analyze"
"${FLUTTER[@]}" analyze lib test example/lib
echo "analyze exit code: $?"

step "dart tests"
"${FLUTTER[@]}" test 2>&1 | tail -3
echo "dart test exit code: ${PIPESTATUS[0]}"

step "android debug build (compiles plugin Kotlin, Pigeon glue, SignalR deps)"
(set -o pipefail; cd example && "${FLUTTER[@]}" build apk --debug 2>&1 | grep -vE '^\s*$' | tail -40)
echo "apk build exit code: $?"

step "kotlin unit tests"
JBR="/Applications/Android Studio.app/Contents/jbr/Contents/Home"
if [ -d "$JBR" ]; then export JAVA_HOME="$JBR"; fi
echo "JAVA_HOME=${JAVA_HOME:-<unset>}"
(set -o pipefail; cd example/android && ./gradlew --console=plain :signalr_native:testDebugUnitTest 2>&1 \
  | grep -E 'PASSED|FAILED|SKIPPED|BUILD|tests completed|^e: |Exception|expected' | tail -60)
echo "kotlin test exit code: $?"
REPORTS="$(find "$ROOT/example/build" "$ROOT/android/build" -path '*test-results/testDebugUnitTest*' -name '*.xml' 2>/dev/null)"
for r in $REPORTS; do
  grep -o '<testsuite name="[^"]*" tests="[0-9]*" skipped="[0-9]*" failures="[0-9]*" errors="[0-9]*"' "$r"
  grep -A3 '<failure' "$r" | head -12
done
