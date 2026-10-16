import 'dart:async';
import 'dart:convert';

import 'package:flutter/material.dart';
import 'package:signalr_native/signalr_native.dart';

void main() => runApp(const MaterialApp(home: BenchPage()));

/// Connects to a hub, counts messages per server method and shows bridge latency.
class BenchPage extends StatefulWidget {
  const BenchPage({super.key});

  @override
  State<BenchPage> createState() => _BenchPageState();
}

class _BenchPageState extends State<BenchPage> {
  final _url = TextEditingController(text: 'http://10.0.2.2:5000/hubs/bench');
  final _methods = TextEditingController(text: 'Quotes');
  PayloadMode _mode = PayloadMode.args;
  SignalRHub? _hub;
  HubState _state = const HubDisconnected();
  final Map<String, int> _counts = {};
  final List<String> _log = [];
  int _maxBridgeMicros = 0;
  final List<StreamSubscription<Object?>> _subs = [];

  Future<void> _connect() async {
    await _disconnect();
    final methods = _methods.text.split(',').map((s) => s.trim()).where((s) => s.isNotEmpty).toSet();
    final hub = SignalRHub(SignalRConfig(
      url: Uri.parse(_url.text.trim()),
      serverMethods: methods,
      payloadMode: _mode,
      logLevel: SignalRLogLevel.debug,
      observer: _Observer(this),
    ));
    _hub = hub;
    _subs.add(hub.states.listen((s) => setState(() => _state = s)));
    for (final m in methods) {
      _subs.add(hub.on(m).listen((msg) => _counts[m] = (_counts[m] ?? 0) + 1));
    }
    try {
      await hub.start();
    } on SignalRException catch (e) {
      _addLog('start failed: $e');
    }
  }

  Future<void> _disconnect() async {
    for (final s in _subs) {
      await s.cancel();
    }
    _subs.clear();
    await _hub?.dispose();
    _hub = null;
  }

  Future<void> _invoke() async {
    try {
      final r = await _hub!.invoke('Echo', ['ping']);
      _addLog('Echo → ${r.payload == null ? 'null' : utf8.decode(r.payload!)} '
          '(total ${r.trace.total.inMicroseconds} µs, bridge ${r.trace.bridge.inMicroseconds} µs)');
    } on SignalRException catch (e) {
      _addLog('invoke failed: $e');
    }
  }

  void _addLog(String line) => setState(() {
        _log.insert(0, line);
        if (_log.length > 200) _log.removeLast();
      });

  @override
  void dispose() {
    unawaited(_disconnect());
    super.dispose();
  }

  @override
  Widget build(BuildContext context) {
    return Scaffold(
      appBar: AppBar(title: Text('signalr_native · $_state')),
      body: Padding(
        padding: const EdgeInsets.all(12),
        child: Column(crossAxisAlignment: CrossAxisAlignment.stretch, children: [
          TextField(controller: _url, decoration: const InputDecoration(labelText: 'Hub URL')),
          TextField(controller: _methods, decoration: const InputDecoration(labelText: 'Server methods (comma-separated)')),
          SegmentedButton<PayloadMode>(
            segments: const [
              ButtonSegment(value: PayloadMode.args, label: Text('args')),
              ButtonSegment(value: PayloadMode.envelope, label: Text('envelope')),
            ],
            selected: {_mode},
            onSelectionChanged: (s) => setState(() => _mode = s.single),
          ),
          Wrap(spacing: 8, children: [
            FilledButton(onPressed: _connect, child: const Text('Connect')),
            OutlinedButton(onPressed: _disconnect, child: const Text('Disconnect')),
            OutlinedButton(onPressed: _hub == null ? null : _invoke, child: const Text('Invoke Echo')),
            OutlinedButton(onPressed: () => setState(() {}), child: const Text('Refresh')),
          ]),
          Text('Messages: $_counts   max bridge latency: $_maxBridgeMicros µs'),
          const Divider(),
          Expanded(child: ListView(children: [for (final l in _log) Text(l, style: const TextStyle(fontSize: 11))])),
        ]),
      ),
    );
  }
}

class _Observer extends SignalRObserver {
  _Observer(this.page);
  final _BenchPageState page;

  @override
  void onLog(SignalRLogRecord record) => page._addLog('$record${record.traceId == null ? '' : ' [${record.traceId}]'}');

  @override
  void onInboundBatch(InboundBatchTrace trace) {
    final us = trace.oldestBridgeLatency.inMicroseconds;
    if (us > page._maxBridgeMicros) page._maxBridgeMicros = us;
  }
}
