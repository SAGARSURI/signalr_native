#!/usr/bin/env bash
# Generates Pigeon code, then analyzes and tests the Dart layer.
# Usage: bash tool/verify.sh   (from the package root)
set -u

if command -v flutter >/dev/null 2>&1; then
  FLUTTER=(flutter); DART=(dart)
elif [ -x "$HOME/fvm/default/bin/flutter" ]; then
  FLUTTER=("$HOME/fvm/default/bin/flutter"); DART=("$HOME/fvm/default/bin/dart")
elif command -v fvm >/dev/null 2>&1; then
  FLUTTER=(fvm flutter); DART=(fvm dart)
else
  echo "flutter not found (tried PATH, ~/fvm/default, fvm)"; exit 1
fi

step() { printf '\n==================== %s ====================\n' "$1"; }

step "flutter version"
"${FLUTTER[@]}" --version

step "dev dependencies (newest compatible pigeon + flutter_lints)"
grep -q '^  pigeon:' pubspec.yaml || "${FLUTTER[@]}" pub add --dev pigeon flutter_lints
"${FLUTTER[@]}" pub get
grep -E '^  (pigeon|flutter_lints):' pubspec.yaml

step "pigeon"
mkdir -p lib/src/generated android/src/main/kotlin/com/faciletech/signalr_native ios/signalr_native/Sources/signalr_native
"${DART[@]}" run pigeon --input pigeons/messages.dart
echo "pigeon exit code: $?"
ls -1 lib/src/generated android/src/main/kotlin/com/faciletech/signalr_native ios/signalr_native/Sources/signalr_native

step "analyze"
"${FLUTTER[@]}" analyze lib test
echo "analyze exit code: $?"

step "test"
"${FLUTTER[@]}" test --reporter expanded
echo "test exit code: $?"
