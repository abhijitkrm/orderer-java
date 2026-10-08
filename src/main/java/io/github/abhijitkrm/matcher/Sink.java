//! The only I/O seam: every book event flows through `onEvent`. Journals,
//! market-data fan-out and gateways attach here — the core never does I/O.
package io.github.abhijitkrm.matcher;

import io.github.abhijitkrm.matcher.Types.Event;

@FunctionalInterface
public interface Sink {
    void onEvent(long seq, Event ev);

    /// Records (seq, event) pairs for tests and replay.
    final class Vec implements Sink {
        public final java.util.List<Long> seqs = new java.util.ArrayList<>();
        public final java.util.List<Event> events = new java.util.ArrayList<>();

        public void onEvent(long seq, Event ev) {
            seqs.add(seq);
            events.add(ev);
        }

        /// Canonical stream (one JSON line per event) — golden-comparable form.
        public String canonical() {
            StringBuilder sb = new StringBuilder(seqs.size() * 64);
            for (int i = 0; i < seqs.size(); i++) {
                sb.append(Event.canonical(seqs.get(i), events.get(i))).append('\n');
            }
            return sb.toString();
        }
    }

    /// Folds each event into a checksum — benchmark consumption without storing.
    final class Null implements Sink {
        public long acc = 0;
        public void onEvent(long seq, Event ev) {
            acc += seq * 0x9E3779B1L + Event.fold(ev);
        }
    }

    /// Emits canonical lines to a consumer (e.g. System.out::println).
    final class Lines implements Sink {
        private final java.util.function.Consumer<String> out;
        public Lines(java.util.function.Consumer<String> out) { this.out = out; }
        public void onEvent(long seq, Event ev) { out.accept(Event.canonical(seq, ev)); }
    }
}
