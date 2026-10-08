//! orderrun — spec/HARNESS.md §4.1 (mirrors matcherrun).
//!   orderrun <cmd-file> [--partitions P] [--partition-map F] [--journal-dir D] [--binary] [--snap PATH]
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
        String usage = "orderrun <cmd-file> [--partitions P] [--partition-map F] [--journal-dir D] [--binary] [--snap PATH]";
        List<String> valued = new ArrayList<>(Harness.COMMON_VALUED);
        valued.add("--snap");
        Args a = new Args(argv, usage, valued, Harness.COMMON_FLAGS);
        if (a.positional.size() != 1) throw Harness.die(usage);
        Harness.Corpus c = Harness.loadCorpus(a.positional.get(0));
        Harness.Common com = Harness.common(a);
        String snapPath = a.get("--snap");
        Harness.Run r = Harness.runCorpus(c, com, true, snapPath != null);
        Harness.print(r.listing());
        if (snapPath != null) r.snapshot().write(Path.of(snapPath));
        System.exit(0);
    }
}
