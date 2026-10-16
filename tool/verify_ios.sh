#!/usr/bin/env bash
# Milestone 3: Dart checks + Swift core unit tests + iOS simulator build.
# Usage: bash tool/verify_ios.sh   (from the package root, on macOS with Xcode)
set -u
ROOT="$(pwd)"
CLIENT_URL="https://github.com/dotnet/signalr-client-swift"
CLIENT_REV="$(sed -n 's/.*revision: "\([0-9a-f]*\)".*/\1/p' ios/signalr_native/Package.swift)"

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

step "toolchain"
"${FLUTTER[@]}" --version | head -1
xcodebuild -version | head -1
swift --version 2>&1 | head -1

step "pub get + pigeon"
"${FLUTTER[@]}" pub get >/dev/null && echo "pub get ok"
mkdir -p lib/src/generated android/src/main/kotlin/com/faciletech/signalr_native ios/signalr_native/Sources/signalr_native
"${DART[@]}" run pigeon --input pigeons/messages.dart && echo "pigeon ok"

step "analyze + dart tests"
"${FLUTTER[@]}" analyze lib test example/lib
echo "analyze exit code: $?"
"${FLUTTER[@]}" test 2>&1 | tail -3
echo "dart test exit code: ${PIPESTATUS[0]}"

step "swift core unit tests (macOS host, no Flutter)"
# The plugin package depends on Flutter's generated FlutterFramework package, which only exists
# inside an app build, so the Flutter-free core (Sources/signalr_native/Core) is tested from a
# throwaway package that compiles the same files.
CORE="$ROOT/build/ios_core_tests"
rm -rf "$CORE" && mkdir -p "$CORE/Sources/SignalRNativeCore" "$CORE/Tests/SignalRNativeCoreTests"
cp ios/signalr_native/Sources/signalr_native/Core/*.swift "$CORE/Sources/SignalRNativeCore/"
cp ios/signalr_native/Tests/SignalRNativeCoreTests/*.swift "$CORE/Tests/SignalRNativeCoreTests/"
cat > "$CORE/Package.swift" <<PKG
// swift-tools-version: 5.10
import PackageDescription

let package = Package(
    name: "SignalRNativeCore",
    platforms: [.macOS(.v13)],
    dependencies: [.package(url: "$CLIENT_URL", revision: "$CLIENT_REV")],
    targets: [
        .target(name: "SignalRNativeCore", dependencies: [.product(name: "SignalRClient", package: "signalr-client-swift")]),
        .testTarget(name: "SignalRNativeCoreTests", dependencies: ["SignalRNativeCore"]),
    ]
)
PKG
(set -o pipefail; cd "$CORE" && swift test 2>&1 | grep -vE '^\[[0-9]+/[0-9]+\]|^Test Case .* (started|passed)' | tail -60)
echo "swift test exit code: $?"

step "example app scaffold"
if [ ! -d example/ios ]; then
  (cd example && "${FLUTTER[@]}" create --org com.faciletech --project-name signalr_native_example \
    --platforms android,ios . >/dev/null) && echo "example created"
fi
(cd example && "${FLUTTER[@]}" pub get >/dev/null) && echo "example pub get ok"

step "ios simulator debug build (compiles plugin Swift, Pigeon glue, SignalR client via SPM)"
(set -o pipefail; cd example && "${FLUTTER[@]}" build ios --simulator --debug 2>&1 | grep -vE '^\s*$' | tail -60)
echo "ios build exit code: $?"
