#!/usr/bin/env bash
# Checks run by CI (.github/workflows/ci.yml) and the pre-push hook (.githooks/pre-push).
# Unlike tool/verify*.sh, it stops at the first failure and exits non-zero.
# Usage: bash tool/ci.sh <dart|android|ios>   (from the package root)
set -euo pipefail

case "${1:-}" in
  dart | android | ios) ;;
  *) echo "usage: bash tool/ci.sh <dart|android|ios>"; exit 2 ;;
esac

if command -v flutter >/dev/null 2>&1; then
  FLUTTER=(flutter); DART=(dart)
elif [ -x "$HOME/fvm/default/bin/flutter" ]; then
  FLUTTER=("$HOME/fvm/default/bin/flutter"); DART=("$HOME/fvm/default/bin/dart")
elif command -v fvm >/dev/null 2>&1; then
  FLUTTER=(fvm flutter); DART=(fvm dart)
else
  echo "flutter not found (tried PATH, ~/fvm/default, fvm)"; exit 1
fi
ROOT="$(pwd)"
step() { printf '\n==================== %s ====================\n' "$1"; }

# Generated Pigeon code is not committed.
generate() {
  step "pub get + pigeon"
  "${FLUTTER[@]}" pub get
  mkdir -p lib/src/generated android/src/main/kotlin/com/faciletech/signalr_native ios/signalr_native/Sources/signalr_native
  "${DART[@]}" run pigeon --input pigeons/messages.dart
}

# The example's platform folders are not committed either; flutter create keeps example/lib.
example() {
  if [ ! -d example/android ] || [ ! -d example/ios ]; then
    (cd example && "${FLUTTER[@]}" create --org com.faciletech --project-name signalr_native_example \
      --platforms android,ios . >/dev/null)
  fi
  (cd example && "${FLUTTER[@]}" pub get)
}

dart_checks() {
  generate
  step "format"
  find lib test pigeons example/lib -name '*.dart' -not -path 'lib/src/generated/*' -print0 \
    | xargs -0 "${DART[@]}" format --output=none --set-exit-if-changed
  step "analyze"
  (cd example && "${FLUTTER[@]}" pub get >/dev/null)
  "${FLUTTER[@]}" analyze lib test example/lib
  step "dart tests"
  "${FLUTTER[@]}" test
}

android_checks() {
  generate
  example
  step "android debug build"
  (cd example && "${FLUTTER[@]}" build apk --debug)
  step "kotlin unit tests"
  (cd example/android && ./gradlew --console=plain :signalr_native:testDebugUnitTest)
}

ios_checks() {
  if [ ! -f ios/signalr_native/Package.swift ]; then
    echo "no iOS adapter on this branch; skipping"
    return
  fi
  generate
  step "swift core unit tests"
  # The plugin package depends on Flutter's generated FlutterFramework package, which only
  # exists inside an app build, so the Flutter-free core is tested from a throwaway package.
  local core="$ROOT/build/ios_core_tests"
  local rev
  rev="$(sed -n 's/.*revision: "\([0-9a-f]*\)".*/\1/p' ios/signalr_native/Package.swift)"
  rm -rf "$core" && mkdir -p "$core/Sources/SignalRNativeCore" "$core/Tests/SignalRNativeCoreTests"
  cp ios/signalr_native/Sources/signalr_native/Core/*.swift "$core/Sources/SignalRNativeCore/"
  cp ios/signalr_native/Tests/SignalRNativeCoreTests/*.swift "$core/Tests/SignalRNativeCoreTests/"
  cat > "$core/Package.swift" <<PKG
// swift-tools-version: 5.10
import PackageDescription

let package = Package(
    name: "SignalRNativeCore",
    platforms: [.macOS(.v13)],
    dependencies: [.package(url: "https://github.com/dotnet/signalr-client-swift", revision: "$rev")],
    targets: [
        .target(name: "SignalRNativeCore", dependencies: [.product(name: "SignalRClient", package: "signalr-client-swift")]),
        .testTarget(name: "SignalRNativeCoreTests", dependencies: ["SignalRNativeCore"]),
    ]
)
PKG
  (cd "$core" && swift test)
  example
  step "ios simulator debug build"
  (cd example && "${FLUTTER[@]}" build ios --simulator --debug)
}

"${1}_checks"
