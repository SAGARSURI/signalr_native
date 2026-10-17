// swift-tools-version: 5.10
// 5.10 is what the official Swift SignalR client requires.

import PackageDescription

let package = Package(
    name: "signalr_native",
    platforms: [
        // Flutter's own minimum (the FlutterFramework package); the Swift client needs 14+.
        .iOS("15.0")
    ],
    products: [
        .library(name: "signalr-native", targets: ["signalr_native"])
    ],
    dependencies: [
        .package(name: "FlutterFramework", path: "../FlutterFramework"),
        // Official client (dotnet/signalr-client-swift). Pinned past v1.0.0 for the MessagePack
        // UTF-8 string length fix (#112); move to the next tagged release once there is one.
        .package(url: "https://github.com/dotnet/signalr-client-swift", revision: "3938376ded66a4b40b923a7e5dd90ff895ec9f08"),
    ],
    targets: [
        .target(
            name: "signalr_native",
            dependencies: [
                .product(name: "FlutterFramework", package: "FlutterFramework"),
                .product(name: "SignalRClient", package: "signalr-client-swift"),
            ]
        )
    ]
)
