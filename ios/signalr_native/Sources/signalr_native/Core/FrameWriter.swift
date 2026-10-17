import Foundation

/// One message in a data-plane batch. See lib/src/frame_codec.dart for the layout.
struct DataEntry {
    static let kindInvocation: UInt8 = 0
    static let kindStreamItem: UInt8 = 1
    static let kindStreamEnd: UInt8 = 2

    let kind: UInt8
    let id: Int64
    let receivedAtMicros: Int64
    let args: [Data]
    let error: Data?

    static func invocation(methodIndex: Int, receivedAtMicros: Int64, args: [Data]) -> DataEntry {
        DataEntry(kind: kindInvocation, id: Int64(methodIndex), receivedAtMicros: receivedAtMicros, args: args, error: nil)
    }

    static func streamItem(streamId: Int64, receivedAtMicros: Int64, item: Data) -> DataEntry {
        DataEntry(kind: kindStreamItem, id: streamId, receivedAtMicros: receivedAtMicros, args: [item], error: nil)
    }

    static func streamEnd(streamId: Int64, receivedAtMicros: Int64, error: String?) -> DataEntry {
        DataEntry(kind: kindStreamEnd, id: streamId, receivedAtMicros: receivedAtMicros, args: [],
                  error: error.map { Data($0.utf8) })
    }
}

/// Encodes batches in the layout Dart's FrameCodec reads. Mirrors FrameWriter.kt.
enum FrameWriter {
    static let version: UInt8 = 1
    static let headerSize = 12
    static let messageHeaderSize = 16

    static func encode(handle: Int64, entries: [DataEntry]) -> Data {
        var size = headerSize
        for e in entries {
            size += messageHeaderSize
            if e.kind == DataEntry.kindStreamEnd {
                size += 4 + (e.error?.count ?? 0)
            } else {
                for a in e.args { size += 4 + a.count }
            }
        }

        var out = Data(count: size)
        out.withUnsafeMutableBytes { (raw: UnsafeMutableRawBufferPointer) in
            var o = 0
            func u8(_ v: UInt8) { raw[o] = v; o += 1 }
            func u16(_ v: UInt16) { raw.storeBytes(of: v.littleEndian, toByteOffset: o, as: UInt16.self); o += 2 }
            func u32(_ v: UInt32) { raw.storeBytes(of: v.littleEndian, toByteOffset: o, as: UInt32.self); o += 4 }
            func i64(_ v: Int64) { raw.storeBytes(of: v.littleEndian, toByteOffset: o, as: Int64.self); o += 8 }
            func bytes(_ d: Data) {
                guard !d.isEmpty else { return }
                d.withUnsafeBytes { src in
                    UnsafeMutableRawBufferPointer(rebasing: raw[o..<(o + d.count)]).copyMemory(from: src)
                }
                o += d.count
            }

            u8(0x53); u8(0x52); u8(version); u8(0)
            u32(UInt32(truncatingIfNeeded: handle))
            u32(UInt32(entries.count))
            for e in entries {
                u8(e.kind)
                u8(e.kind == DataEntry.kindStreamEnd ? 0 : UInt8(e.args.count))
                u16(0)
                u32(UInt32(truncatingIfNeeded: e.id))
                i64(e.receivedAtMicros)
                if e.kind == DataEntry.kindStreamEnd {
                    u32(UInt32(e.error?.count ?? 0))
                    if let err = e.error { bytes(err) }
                } else {
                    for a in e.args {
                        u32(UInt32(a.count))
                        bytes(a)
                    }
                }
            }
        }
        return out
    }
}
