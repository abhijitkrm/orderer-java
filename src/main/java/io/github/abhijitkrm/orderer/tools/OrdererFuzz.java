//! ordererfuzz — spec/HARNESS.md §4.2 (mirrors matcherfuzz).
//!   ordererfuzz <cmd-file> [--partitions P] [--partition-map F] [--journal-dir D] [--binary]
//! Like orderrun, but symbol-tagged only for engine files. The CHECKED build
//! (scripts/build-harness.sh) runs it with assertions enabled (-ea).
package io.github.abhijitkrm.orderer.tools;

import io.github.abhijitkrm.orderer.Harness;
import io.github.abhijitkrm.orderer.Harness.Args;

public final class OrdererFuzz {
    public static void main(String[] argv) {
        String usage = "ordererfuzz <cmd-file> [--partitions P] [--partition-map F] [--journal-dir D] [--binary]";
        Args a = new Args(argv, usage, Harness.COMMON_VALUED, Harness.COMMON_FLAGS);
        if (a.positional.size() != 1) throw Harness.die(usage);
        Harness.Corpus c = Harness.loadCorpus(a.positional.get(0));
        Harness.Common com = Harness.common(a);
        Harness.print(Harness.runCorpus(c, com, c.engine, false).listing());
        System.exit(0);
    }
}
