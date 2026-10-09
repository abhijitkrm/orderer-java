//! orderrun — spec/HARNESS.md §4.1 (mirrors matcherrun).
//!   orderrun <cmd-file> [--partitions P] [--partition-map F] [--journal-dir D] [--binary] [--snap PATH]
//!            [--checkpoint-every K] [--durable]
//! Runs the file through a pipeline (one producer, file order), drains, prints
//! every event symbol-tagged, grouped by partition. --snap then writes the
//! merged snapshot to PATH and its cut to PATH.meta.
package io.github.abhijitkrm.orderer.tools;

import io.github.abhijitkrm.orderer.Harness;
import io.github.abhijitkrm.orderer.Harness.Args;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

public final class OrderRun {
    public static void main(String[] argv) throws Exception {
        String usage = "orderrun <cmd-file> [--partitions P] [--partition-map F] [--journal-dir D] [--binary] [--snap PATH]"
                + " [--checkpoint-every K] [--durable]";
        List<String> valued = new ArrayList<>(Harness.COMMON_VALUED);
        valued.add("--snap");
        valued.add("--checkpoint-every");
        List<String> flags = new ArrayList<>(Harness.COMMON_FLAGS);
        flags.add("--durable");
        Args a = new Args(argv, usage, valued, flags);
        if (a.positional.size() != 1) throw Harness.die(usage);
        Harness.Corpus c = Harness.loadCorpus(a.positional.get(0));
        Harness.Common com = Harness.common(a);
        String snapPath = a.get("--snap");
        long k = a.get("--checkpoint-every") == null ? 0 : a.num("--checkpoint-every", 0, Long.MAX_VALUE);
        if (a.get("--checkpoint-every") != null && k == 0) throw Harness.die("--checkpoint-every: K must be at least 1");
        Harness.Run r = Harness.runCorpus(c, com, true, new Harness.RunOpts(snapPath != null, k, a.flag("--durable")));
        Harness.print(r.listing());
        if (snapPath != null) r.snapshot().write(Path.of(snapPath));
        System.exit(0);
    }
}
