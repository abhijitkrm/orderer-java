//! orderrecover — spec/HARNESS.md §4.3 (mirrors matcherrecover).
//!   orderrecover <snapshot> <tail-file> [--partitions P] [--partition-map F]
//!   orderrecover --journal-dir DIR [--snap PATH] [--binary] [--repair] [--partitions P] [--partition-map F]
//! Tail form: restore, submit every tail line without "format" through a
//! pipeline, print the replayed events. Journal form: recover from journals
//! (after the optional snapshot's cut; by default the directory's newest
//! checkpoint). Malformed/corrupt input exits 2; --repair first truncates torn
//! tails (spec/JOURNAL.md §5.1).
package io.github.abhijitkrm.orderer.tools;

import io.github.abhijitkrm.matcher.OrderBook.Config;
import io.github.abhijitkrm.matcher.Types.Command;
import io.github.abhijitkrm.matcher.Types.Event;
import io.github.abhijitkrm.orderer.Core;
import io.github.abhijitkrm.orderer.Egress;
import io.github.abhijitkrm.orderer.Flat;
import io.github.abhijitkrm.orderer.Harness;
import io.github.abhijitkrm.orderer.Harness.Args;
import io.github.abhijitkrm.orderer.Journal;
import io.github.abhijitkrm.orderer.Pipeline;
import io.github.abhijitkrm.orderer.Recover;
import io.github.abhijitkrm.orderer.Routing.PartitionMap;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

public final class OrderRecover {
    static void tailForm(String snapPath, String tailPath, PartitionMap map) {
        Pipeline.Snapshot snap;
        Recover.Restored<Core.FifoCore> restored;
        try {
            snap = Recover.readSnapshot(Path.of(snapPath));
            restored = Recover.restore(Core.FifoCore::new, Config.defaults(), map, snap);
        } catch (RuntimeException e) {
            throw Harness.die(e.getMessage());
        }
        String text = Harness.readText(tailPath);
        List<Long> syms = new ArrayList<>();
        List<Command> cmds = new ArrayList<>();
        String[] lines = text.split("\n", -1);
        for (int i = 0; i < lines.length; i++) {
            String line = lines[i].endsWith("\r") ? lines[i].substring(0, lines[i].length() - 1) : lines[i];
            if (line.isEmpty() || line.contains("\"format\"")) continue;
            Command cmd = Flat.parseCommand(line);
            String sf = Flat.get(line, "symbol");
            Long sym = sf == null ? Long.valueOf(0) : Flat.parseU64(sf);
            if (cmd == null || sym == null || sym < 0 || sym > 0xFFFFFFFFL)
                throw Harness.die(tailPath + ":" + (i + 1) + ": malformed journal line: " + line);
            syms.add(sym);
            cmds.add(cmd);
        }
        long[] s = new long[syms.size()];
        for (int i = 0; i < s.length; i++) s[i] = syms.get(i);
        Egress.Collect col = Egress.collect(true);
        try (Pipeline<Core.FifoCore> p = Pipeline.builder().bookConfig(restored.book()).partitionMap(map)
                .egress(col.factory()).initial(new Pipeline.Initial<>(restored.cores(), snap.iseq() + 1)).build()) {
            if (p.publishBatch(s, cmds.toArray(new Command[0])) != Pipeline.Status.Ok) throw Harness.fail("pipeline closed");
            p.drain();
            p.shutdown();
        } catch (RuntimeException e) {
            throw Harness.fail(e.getMessage());
        }
        Harness.print(col.handle().listing());
    }

    static void journalForm(String dir, String snapPath, Journal.Format fmt, boolean repair, PartitionMap map) {
        List<StringBuilder> parts = new ArrayList<>();
        for (int p = 0; p < map.partitions(); p++) parts.add(new StringBuilder());
        try {
            if (repair)
                for (Journal.Repaired r : Journal.repairDir(Path.of(dir), fmt)) System.err.println("repaired " + r.path() + " " + r.bytes());
            Pipeline.Snapshot snap = null;
            if (snapPath != null) snap = Recover.readSnapshot(Path.of(snapPath));
            else {
                List<Journal.Checkpoint> cps = Journal.listCheckpoints(Path.of(dir));
                if (!cps.isEmpty()) snap = Recover.readSnapshot(cps.get(cps.size() - 1).path());
            }
            Recover.recover(Core.FifoCore::new, Config.defaults(), map, snap, new Recover.JournalSource(Path.of(dir), fmt),
                    (p, s, seq, ev) -> parts.get(p).append(Event.canonical(seq, s, ev)).append('\n'));
        } catch (RuntimeException e) {
            throw Harness.die(e.getMessage());
        }
        StringBuilder out = new StringBuilder();
        for (StringBuilder p : parts) out.append(p);
        Harness.print(out.toString());
    }

    public static void main(String[] argv) {
        String usage = "orderrecover <snapshot> <tail-file> [--partitions P] [--partition-map F]\n"
                + "       orderrecover --journal-dir DIR [--snap PATH] [--binary] [--repair] [--partitions P] [--partition-map F]";
        Args a = new Args(argv, usage, List.of("--partitions", "--partition-map", "--journal-dir", "--snap"), List.of("--binary", "--repair"));
        PartitionMap map = Harness.partitionMap(a);
        String dir = a.get("--journal-dir");
        if (dir != null && a.positional.isEmpty()) {
            journalForm(dir, a.get("--snap"), a.flag("--binary") ? Journal.Format.Binary : Journal.Format.Jsonl, a.flag("--repair"), map);
        } else if (dir == null && a.positional.size() == 2 && a.get("--snap") == null && !a.flag("--binary") && !a.flag("--repair")) {
            tailForm(a.positional.get(0), a.positional.get(1), map);
        } else {
            throw Harness.die(usage);
        }
        System.exit(0);
    }
}
