package io.github.abhijitkrm.orderer.tools;

import io.github.abhijitkrm.matcher.OrderBook;
import io.github.abhijitkrm.matcher.Types.Command;
import io.github.abhijitkrm.orderer.Consensus.Entry;
import io.github.abhijitkrm.orderer.cluster.Client;
import io.github.abhijitkrm.orderer.cluster.ClientServer;
import io.github.abhijitkrm.orderer.cluster.Codec;
import io.github.abhijitkrm.orderer.cluster.DedupSm;
import io.github.abhijitkrm.orderer.cluster.PipelineSm;
import io.github.abhijitkrm.orderer.cluster.RecorderSm;
import io.github.abhijitkrm.orderer.cluster.Replica;
import io.github.abhijitkrm.orderer.cluster.StateMachine;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

/// ordernode: one replica of a replicated cluster, as a process. Same command
/// line and client protocol as orderer-rust's orderer-node, orderer-cpp's
/// orderer_node and orderer-go's orderernode, so replicas of every language can
/// share a cluster.
///
///   ordernode --id N --members 0=HOST:PORT,1=HOST:PORT,... --client HOST:PORT
///             --dir DIR [--compact-every N] [--digest-every N]
///             [--fsync on|off] [--audit] [--sm pipeline|recorder] [--partitions P]
///
/// The state machine is the matching pipeline (default) or the language-neutral
/// RecorderSm, wrapped in DedupSm. --audit appends (log index, first order id)
/// for every applied batch to DIR/applied.log (truncated at start), as the other
/// nodes do. ORDERER_KEY in the environment sets the pre-shared cluster key.
public final class OrderNode {
    private OrderNode() {}

    static void die(String m) { System.err.println("ordernode: " + m); System.exit(2); }

    /// Records (index, first order id) of every applied batch, then delegates.
    static final class Audited implements StateMachine {
        private final StateMachine inner;
        private final FileOutputStream log;
        Audited(StateMachine inner, FileOutputStream log) { this.inner = inner; this.log = log; }

        @Override public void apply(long index, Entry e) {
            if (log != null) {
                try {
                    Codec.Batch b = Codec.decodeBatch(e.data());
                    if (b.size() > 0) {
                        Command c0 = b.cmds()[0];
                        long id = c0 instanceof Command.New n ? n.orderId()
                                : c0 instanceof Command.Cancel c ? c.orderId() : ((Command.Replace) c0).orderId();
                        byte[] rec = new byte[16];
                        io.github.abhijitkrm.orderer.cluster.Buf.put64(rec, 0, index);
                        io.github.abhijitkrm.orderer.cluster.Buf.put64(rec, 8, id);
                        log.write(rec);   // straight to the OS: survives kill -9 of this process
                    }
                } catch (Codec.CodecException | IOException x) { /* not a batch */ }
            }
            inner.apply(index, e);
        }
        @Override public byte[] snapshot(long i) { return inner.snapshot(i); }
        @Override public void restore(long i, byte[] d) { inner.restore(i, d); }
        @Override public byte[] digest(long i) { return inner.digest(i); }
    }

    public static void main(String[] args) throws Exception {
        int id = -1, partitions = 2;
        String members = null, client = null, dir = null, smKind = "pipeline";
        long compact = 0, digest = 0;
        boolean fsync = true, audit = false;
        for (int i = 0; i < args.length; i++) {
            String a = args[i];
            switch (a) {
                case "--id" -> id = Integer.parseInt(args[++i]);
                case "--members" -> members = args[++i];
                case "--client" -> client = args[++i];
                case "--dir" -> dir = args[++i];
                case "--compact-every" -> compact = Long.parseLong(args[++i]);
                case "--digest-every" -> digest = Long.parseLong(args[++i]);
                case "--fsync" -> fsync = args[++i].equals("on");
                case "--audit" -> audit = true;
                case "--sm" -> smKind = args[++i];
                case "--partitions" -> partitions = Integer.parseInt(args[++i]);
                default -> die("unknown argument " + a);
            }
        }
        if (id < 0 || members == null || client == null || dir == null) die("--id, --members, --client and --dir are required");
        if (!smKind.equals("pipeline") && !smKind.equals("recorder")) die("--sm " + smKind + ": want pipeline or recorder");
        Replica.Config cfg = new Replica.Config(id, Client.parseMembers(members), Path.of(dir, "state"));
        cfg.compactEvery = compact;
        cfg.digestEvery = digest;
        cfg.fsync = fsync;
        cfg.maxPending = 1 << 16;
        Files.createDirectories(Path.of(dir));
        String k = System.getenv("ORDERER_KEY");
        byte[] key = k == null || k.isEmpty() ? null : k.getBytes(java.nio.charset.StandardCharsets.UTF_8);
        cfg.authKey = key;
        StateMachine inner = smKind.equals("recorder") ? new RecorderSm() : PipelineSm.counting(partitions, OrderBook.Config.defaults());
        FileOutputStream log = audit ? new FileOutputStream(Path.of(dir, "applied.log").toFile(), false) : null;
        Replica r;
        try { r = Replica.start(cfg, new DedupSm(new Audited(inner, log)), null); }
        catch (IOException e) { die("start: " + e.getMessage()); return; }
        new ClientServer(client, r, key);
        System.out.println("ordernode " + id + " ready (client port " + client + ")");
        System.out.flush();
        Thread.currentThread().join();
    }
}
