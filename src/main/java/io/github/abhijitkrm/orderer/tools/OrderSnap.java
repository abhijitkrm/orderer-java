//! ordersnap — spec/HARNESS.md §4.4 (mirrors matchersnap).
//!   ordersnap <cmd-file> [--partitions P] [--partition-map F] [--journal-dir D] [--binary]
//! Runs the file, drains, prints the merged matcher-snap/1 snapshot.
package io.github.abhijitkrm.orderer.tools;

import io.github.abhijitkrm.orderer.Harness;
import io.github.abhijitkrm.orderer.Harness.Args;

public final class OrderSnap {
    public static void main(String[] argv) {
        String usage = "ordersnap <cmd-file> [--partitions P] [--partition-map F] [--journal-dir D] [--binary]";
        Args a = new Args(argv, usage, Harness.COMMON_VALUED, Harness.COMMON_FLAGS);
        if (a.positional.size() != 1) throw Harness.die(usage);
        Harness.Corpus c = Harness.loadCorpus(a.positional.get(0));
        Harness.Common com = Harness.common(a);
        Harness.print(Harness.runCorpus(c, com, true, true).snapshot().body());
        System.exit(0);
    }
}
