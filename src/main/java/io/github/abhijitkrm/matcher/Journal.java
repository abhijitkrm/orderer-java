//! Journaling (spec/JOURNAL.md): append-only canonical command/event lines.
//! The journal is I/O policy on top of the deterministic core — it never
//! changes matching semantics.
package io.github.abhijitkrm.matcher;

import io.github.abhijitkrm.matcher.Types.Command;
import io.github.abhijitkrm.matcher.Types.Event;
import io.github.abhijitkrm.matcher.Types.OType;
import java.util.function.Consumer;

public final class Journal {
    private Journal() {}

    /// Canonical command line (SCHEMA.md). `sym >= 0` adds `"symbol":N` for the
    /// `engine:true` tagged form.
    public static String commandCanonical(Command cmd, long sym) {
        String sf = sym < 0 ? "" : ",\"symbol\":" + sym;
        if (cmd instanceof Command.New c) {
            String px = c.otype() == OType.Limit
                    ? ",\"price\":" + c.price() + ",\"qty\":" + c.qty()
                    : ",\"qty\":" + c.qty();
            return "{\"cmd\":\"new\"" + sf + ",\"order_id\":" + c.orderId()
                    + ",\"side\":\"" + c.side().str() + "\",\"otype\":\"" + c.otype().str()
                    + "\"" + px + ",\"tif\":\"" + c.tif().str() + "\"}";
        }
        if (cmd instanceof Command.Cancel c)
            return "{\"cmd\":\"cancel\"" + sf + ",\"order_id\":" + c.orderId() + "}";
        Command.Replace c = (Command.Replace) cmd;
        return "{\"cmd\":\"replace\"" + sf + ",\"order_id\":" + c.orderId()
                + ",\"price\":" + c.price() + ",\"qty\":" + c.qty() + "}";
    }

    /// Append-only command journal: every command is serialized before apply.
    /// `sym < 0` writes untagged lines; `sym >= 0` writes engine-tagged lines.
    public static final class CmdJournal {
        private final Consumer<String> write;
        private final long sym;

        public CmdJournal(Consumer<String> write) { this(write, -1); }
        public CmdJournal(Consumer<String> write, long sym) {
            this.write = write;
            this.sym = sym;
        }

        public void record(Command cmd) { write.accept(commandCanonical(cmd, sym) + "\n"); }
    }

    /// Sink decorator: journals each event line then forwards to inner.
    public static final class EvtJournal implements Sink {
        private final Sink inner;
        private final Consumer<String> write;

        public EvtJournal(Sink inner, Consumer<String> write) {
            this.inner = inner;
            this.write = write;
        }

        public void onEvent(long seq, Event ev) {
            write.accept(Event.canonical(seq, ev) + "\n");
            inner.onEvent(seq, ev);
        }
    }

    /// Journals one symbol-tagged engine event — for applyTagged callbacks.
    public static void journalEvent(long sym, long seq, Event ev, Consumer<String> write) {
        write.accept(Event.canonical(seq, sym, ev) + "\n");
    }
}
