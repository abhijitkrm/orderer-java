//! Shared plumbing for the spec/HARNESS.md tools (tools/*.java).
package io.github.abhijitkrm.orderer;

import io.github.abhijitkrm.matcher.OrderBook.Config;
import io.github.abhijitkrm.matcher.Types.Command;
import io.github.abhijitkrm.orderer.Pipeline.Snapshot;
import io.github.abhijitkrm.orderer.Routing.PartitionMap;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public final class Harness {
    private Harness() {}

    /// spec/HARNESS.md §5: usage / input / config / corruption errors exit 2.
    public static RuntimeException die(String msg) {
        System.err.println(msg);
        System.exit(2);
        return new IllegalStateException();
    }
    /// Internal failures exit 1.
    public static RuntimeException fail(String msg) {
        System.err.println(msg);
        System.exit(1);
        return new IllegalStateException();
    }

    public static final class Corpus {
        public Config book;
        public boolean engine;
        public long[] syms;
        public Command[] cmds;
    }

    public static String readText(String path) {
        try { return Files.readString(Path.of(path), StandardCharsets.UTF_8); }
        catch (IOException | RuntimeException e) { throw die(path + ": cannot read"); }
    }

    /// A command file (spec/HARNESS.md §1), strictly.
    public static Corpus parseCorpus(String text, String path) {
        String[] lines = text.split("\n", -1);
        int count = lines.length;
        if (count > 0 && lines[count - 1].isEmpty()) count--;
        if (text.isEmpty() || count == 0) throw die(path + ": empty file");
        Corpus c = new Corpus();
        String hdr = strip(lines[0]);
        c.book = Flat.parseHeader(hdr);
        c.engine = "true".equals(Flat.get(hdr, "engine"));
        List<Command> cmds = new ArrayList<>();
        long[] syms = new long[count];
        for (int i = 1; i < count; i++) {
            String line = strip(lines[i]);
            if (line.trim().isEmpty()) continue;
            Command cmd = Flat.parseCommand(line);
            if (cmd == null) throw die(path + ":" + (i + 1) + ": malformed command: " + line);
            long sym = 0;
            if (c.engine) {
                Long s = Flat.u64(line, "symbol");
                if (s == null || s < 0 || s > 0xFFFFFFFFL) throw die(path + ":" + (i + 1) + ": missing symbol: " + line);
                sym = s;
            }
            syms[cmds.size()] = sym;
            cmds.add(cmd);
        }
        c.cmds = cmds.toArray(new Command[0]);
        c.syms = java.util.Arrays.copyOf(syms, c.cmds.length);
        return c;
    }

    static String strip(String l) { return l.endsWith("\r") ? l.substring(0, l.length() - 1) : l; }

    public static Corpus loadCorpus(String path) { return parseCorpus(readText(path), path); }

    /// Minimal argv parser: positionals plus --flag / --opt value.
    public static final class Args {
        public final List<String> positional = new ArrayList<>();
        private final Map<String, String> opts = new HashMap<>();
        private final Set<String> flags = new HashSet<>();

        public Args(String[] argv, String usage, List<String> valued, List<String> flagNames) {
            for (int i = 0; i < argv.length; i++) {
                String a = argv[i];
                if (a.startsWith("--")) {
                    if (valued.contains(a)) {
                        if (i + 1 >= argv.length) throw die(a + " needs a value\nusage: " + usage);
                        opts.put(a, argv[++i]);
                    } else if (flagNames.contains(a)) {
                        flags.add(a);
                    } else {
                        throw die("unknown option " + a + "\nusage: " + usage);
                    }
                } else {
                    positional.add(a);
                }
            }
        }
        public String get(String k) { return opts.get(k); }
        public boolean flag(String k) { return flags.contains(k); }
        public long num(String k, long def, long max) {
            String v = opts.get(k);
            if (v == null) return def;
            Long u = Flat.parseU64(v);
            if (u == null || u < 0 || u > max) throw die(k + ": not a number: " + v);
            return u;
        }
    }

    public static final List<String> COMMON_VALUED = List.of("--partitions", "--partition-map", "--journal-dir");
    public static final List<String> COMMON_FLAGS = List.of("--binary");

    public static PartitionMap partitionMap(Args a) {
        int p = (int) a.num("--partitions", 1, 0xFFFFFFFFL);
        try {
            String path = a.get("--partition-map");
            if (path != null) {
                String text = readText(path);
                try { return PartitionMap.parseTable(text, p); }
                catch (Routing.RoutingError e) { throw die(path + ": " + e.getMessage()); }
            }
            return new PartitionMap(p);
        } catch (Routing.RoutingError e) {
            throw die(e.getMessage());
        }
    }

    public record Common(PartitionMap map, Journal.Config2 journal) {}

    public static Common common(Args a) {
        if (a.flag("--binary") && a.get("--journal-dir") == null) throw die("--binary requires --journal-dir");
        Journal.Config2 j = null;
        String d = a.get("--journal-dir");
        if (d != null)  // harness runs need complete files, not power-loss safety
            j = Journal.Config2.of(Path.of(d), a.flag("--binary") ? Journal.Format.Binary : Journal.Format.Jsonl)
                    .withFsync(Journal.FsyncPolicy.never()).withEvents(true);
        return new Common(partitionMap(a), j);
    }

    public record Run(String listing, Snapshot snapshot) {}

    /// Run a corpus through a fresh pipeline (one producer, file order), drain,
    /// optionally snapshot, shut down. Returns the spec/HARNESS.md §3 listing.
    public static Run runCorpus(Corpus corpus, Common c, boolean tagged, boolean snapshot) {
        Egress.Collect col = Egress.collect(tagged);
        var b = Pipeline.builder().bookConfig(corpus.book).partitionMap(c.map()).egress(col.factory());
        if (c.journal() != null) b.journal(c.journal());
        Pipeline<Core.FifoCore> p;
        try { p = b.build(); }
        catch (RuntimeException e) { throw die(e.getMessage()); }
        Snapshot snap = null;
        try {
            if (p.publishBatch(corpus.syms, corpus.cmds) != Pipeline.Status.Ok) throw fail("pipeline closed");
            p.drain();
            if (snapshot) snap = p.snapshot();
            p.shutdown();
        } catch (RuntimeException e) {
            throw fail(e.getMessage());
        }
        return new Run(col.handle().listing(), snap);
    }

    private static final PrintStream OUT = new PrintStream(new java.io.FileOutputStream(java.io.FileDescriptor.out), false);

    public static void print(String s) {
        byte[] b = s.getBytes(StandardCharsets.UTF_8);
        OUT.write(b, 0, b.length);
        OUT.flush();
    }
}
