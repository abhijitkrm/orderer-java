// The replication runtime's storage and wire layer against fixtures written by
// orderer-rust (vectors/replication-wire, from `wirefix`): messages, WAL frame
// bodies, command batches, WAL files in every damaged state, and snapshots.
// Decoding must give the same values, re-encoding the same bytes, recovery the
// same disk image, and files written here must equal the Rust files byte for byte.
//
//   ClusterWireTest [vectors-dir]
package io.github.abhijitkrm.orderer;

import static io.github.abhijitkrm.orderer.T.check;

import io.github.abhijitkrm.matcher.Types.Command;
import io.github.abhijitkrm.orderer.Consensus.Disk;
import io.github.abhijitkrm.orderer.Consensus.Entry;
import io.github.abhijitkrm.orderer.Consensus.Msg;
import io.github.abhijitkrm.orderer.Consensus.Persist;
import io.github.abhijitkrm.orderer.cluster.Auth;
import io.github.abhijitkrm.orderer.cluster.Buf;
import io.github.abhijitkrm.orderer.cluster.Codec;
import io.github.abhijitkrm.orderer.cluster.Crc;
import io.github.abhijitkrm.orderer.cluster.SnapStore;
import io.github.abhijitkrm.orderer.cluster.Wal;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.stream.Stream;

public final class ClusterWireTest {
    static Path WIRE = Path.of("vectors", "replication-wire");

    static byte[] unhex(String s) { return ReplicationTest.unhex(s.isEmpty() ? "-" : s); }

    static List<String[]> fixtureLines(String name) {
        List<String[]> v = new ArrayList<>();
        for (String line : T.slurp(WIRE.resolve(name)).split("\n")) {
            int bar = line.indexOf(" | ");
            if (bar >= 0) v.add(new String[] {line.substring(0, bar), line.substring(bar + 3)});
        }
        return v;
    }

    static Path scratch(String name) throws IOException {
        Path d = Files.createTempDirectory("orderer-java-wire-" + name);
        return d;
    }

    static void rmrf(Path p) {
        try (Stream<Path> s = Files.walk(p)) { s.sorted(java.util.Comparator.reverseOrder()).forEach(x -> x.toFile().delete()); }
        catch (IOException ignored) {}
    }

    static String fmtDisk(Disk d) {
        return d.term + ":" + (d.votedFor < 0 ? "-" : String.valueOf(d.votedFor)) + ":" + d.base + ":" + d.baseTerm + ":" + ReplicationTest.fmtEntries(d.log);
    }

    static Persist mk(long term, int vote, long from, List<Entry> es, long[] comp) {
        Persist p = new Persist();
        p.term = term; p.votedFor = vote; p.from = from; p.entries = es;
        if (comp != null) { p.hasCompact = true; p.compactIndex = comp[0]; p.compactTerm = comp[1]; }
        return p;
    }

    static Entry e(long term, int... d) {
        byte[] b = new byte[d.length];
        for (int i = 0; i < d.length; i++) b[i] = (byte) d[i];
        return new Entry(term, b);
    }

    public static void main(String[] args) {
        if (args.length > 0) WIRE = Path.of(args[0]).resolve("replication-wire");
        T.runAll(new Object[][] {
            {"crc_matches_the_check_value", (T.Body) () -> {
                check(Crc.crc32c("123456789".getBytes(StandardCharsets.US_ASCII)) == 0xE3069283L, "check value");
            }},
            {"sha256_and_hmac_match_the_standard_vectors", (T.Body) () -> {
                check(java.util.HexFormat.of().formatHex(Auth.hmac("Jefe".getBytes(), "what do ya want for nothing?".getBytes()))
                        .equals("5bdcc146bf60754e6a042426089575c75a003f089d2739839dec58b964ec3843"), "hmac");
            }},
            {"sessions_detect_tampering_replay_reordering_and_reflection", (T.Body) () -> {
                byte[] k = new byte[32];
                Arrays.fill(k, (byte) 7);
                Auth.Session a = new Auth.Session(k, 0), b = new Auth.Session(k, 1);
                byte[] one = "one".getBytes(), two = "two".getBytes(), twp = "twp".getBytes();
                byte[] m1 = a.seal(1, one, 0, 3), m2 = a.seal(1, two, 0, 3);
                check(b.open(1, one, 0, 3, m1, 0), "first frame");
                check(!b.open(1, one, 0, 3, m1, 0), "replay");
                check(!b.open(1, twp, 0, 3, m2, 0), "altered body");
                check(!b.open(2, two, 0, 3, m2, 0), "altered kind");
                check(b.open(1, two, 0, 3, m2, 0), "rejections must not advance state");
                a.seal(1, one, 0, 3);
                byte[] m4 = a.seal(1, one, 0, 3);
                check(!b.open(1, one, 0, 3, m4, 0), "a dropped frame");
                Auth.Session a2 = new Auth.Session(k, 0);
                check(!a2.open(1, one, 0, 3, a2.seal(1, one, 0, 3), 0), "reflection");
                byte[] k2 = k.clone();
                k2[0] = 8;
                check(!new Auth.Session(k2, 1).open(1, one, 0, 3, new Auth.Session(k, 0).seal(1, one, 0, 3), 0), "wrong key");
            }},
            {"messages_decode_and_re_encode_exactly_as_rust_wrote_them", (T.Body) () -> {
                List<String[]> lines = fixtureLines("msgs.txt");
                check(lines.size() >= 8, "only ", lines.size(), " message fixtures");
                for (String[] l : lines) {
                    byte[] bytes = unhex(l[0]);
                    Msg m = Codec.decodeMsg(bytes);
                    check(ReplicationTest.fmtMsg(m).equals(l[1]), "decoded ", ReplicationTest.fmtMsg(m), " want ", l[1]);
                    check(Arrays.equals(Codec.encodeMsg(m, new Buf()).toArray(), bytes), l[1], ": re-encoded differently");
                    for (int n = 0; n < bytes.length; n++) {
                        boolean threw = false;
                        try { Codec.decodeMsg(Arrays.copyOf(bytes, n)); } catch (Codec.CodecException x) { threw = true; }
                        check(threw, l[1], ": prefix of ", n, " bytes decoded");
                    }
                }
            }},
            {"hostile_input_never_crashes_the_decoders", (T.Body) () -> {
                Random rng = new Random(99);
                for (int i = 0; i < 20000; i++) {
                    byte[] junk = new byte[rng.nextInt(64)];
                    rng.nextBytes(junk);
                    try { Codec.decodeMsg(junk); } catch (Codec.CodecException ignored) {}
                    try { Codec.decodePersist(junk, 0, junk.length); } catch (Codec.CodecException ignored) {}
                    try { Codec.decodeBatch(junk); } catch (Codec.CodecException ignored) {}
                }
                // A hostile entry count must not allocate.
                byte[] ev = new Buf().u8(3).bytes(new byte[24]).u32(0xFFFFFFFFL).toArray();
                boolean threw = false;
                try { Codec.decodeMsg(ev); } catch (Codec.CodecException x) { threw = true; }
                check(threw, "hostile entry count accepted");
            }},
            {"wal_frame_bodies_round_trip", (T.Body) () -> {
                for (String[] l : fixtureLines("persists.txt")) {
                    byte[] body = unhex(l[0]);
                    Persist p = Codec.decodePersist(body, 0, body.length);
                    String got = p.term + " " + (p.votedFor < 0 ? "-" : String.valueOf(p.votedFor)) + " " + p.from + " "
                            + (p.hasCompact ? p.compactIndex + ":" + p.compactTerm : "-") + " " + ReplicationTest.fmtEntries(p.entries);
                    check(got.equals(l[1]), "decoded ", got, " want ", l[1]);
                    check(Arrays.equals(Codec.encodePersist(p, new Buf()).toArray(), body), l[1], ": re-encoded differently");
                }
            }},
            {"command_batches_round_trip_including_sessions", (T.Body) () -> {
                String[] tifs = {"gtc", "ioc", "fok", "post_only"};
                for (String[] l : fixtureLines("batches.txt")) {
                    byte[] body = unhex(l[0]);
                    String[] w = l[1].trim().split("\\s+");
                    Codec.Batch b = Codec.decodeBatch(body);
                    Codec.SessionKey s = Codec.sessionOf(body);
                    if (w[0].equals("-")) check(s == null, "plain batch decoded as a session");
                    else check(s != null && s.client() == Long.parseUnsignedLong(w[0]) && s.seq() == Long.parseUnsignedLong(w[1]), "session ", s, " want ", w[0], " ", w[1]);
                    int want = w.length - 3;   // "client seq |" then commands
                    check(b.size() == want, b.size(), " commands, want ", want);
                    for (int i = 0; i < b.size() && i < want; i++) {
                        Command c = b.cmds()[i];
                        String got;
                        if (c instanceof Command.New n) {
                            got = b.syms()[i] + ":new:" + n.side().str() + ":" + n.otype().str() + ":" + tifs[n.tif().ordinal()] + ":" + n.orderId() + ":" + n.price() + ":" + n.qty();
                        } else if (c instanceof Command.Cancel x) {
                            got = b.syms()[i] + ":cancel:-:-:-:" + x.orderId() + ":0:0";
                        } else {
                            Command.Replace x = (Command.Replace) c;
                            got = b.syms()[i] + ":replace:-:-:-:" + x.orderId() + ":" + x.price() + ":" + x.qty();
                        }
                        check(got.equals(w[3 + i]), "command ", i, ": ", got, " want ", w[3 + i]);
                    }
                    if (body.length > 0)
                        check(Arrays.equals(Codec.encodeBatch(b.syms(), b.cmds(), s), body), "re-encoded ", l[0].substring(0, Math.min(40, l[0].length())), " differently");
                }
            }},
            {"wal_recovery_matches_rust_for_every_damage_pattern", (T.Body) () -> {
                List<Path> files = new ArrayList<>();
                try (Stream<Path> s = Files.list(WIRE)) { s.filter(p -> p.getFileName().toString().startsWith("wal-") && p.toString().endsWith(".bin")).sorted().forEach(files::add); }
                check(files.size() >= 15, "only ", files.size(), " WAL fixtures");
                for (Path f : files) {
                    String name = f.getFileName().toString();
                    String tag = name.substring(4, name.length() - 4);
                    String want = T.slurp(WIRE.resolve("wal-" + tag + ".expect")).replaceAll("\n+$", "");
                    Path dir = scratch(tag);
                    Files.copy(f, dir.resolve("raft.wal"));
                    String got;
                    try {
                        Wal.Opened o = Wal.open(dir);
                        got = "ok " + fmtDisk(o.disk()) + " frames=" + o.repair().frames() + " torn=" + o.repair().tornBytes();
                        o.wal().close();
                    } catch (Wal.WalException x) {
                        got = x.kind == Wal.WalException.Kind.Corrupt ? "corrupt " + x.offset
                                : x.kind == Wal.WalException.Kind.BadHeader ? "badheader" : "io " + x.getMessage();
                    }
                    check(got.equals(want), tag, "\n  want: ", want, "\n  got:  ", got);
                    rmrf(dir);
                }
            }},
            {"a_wal_written_here_is_byte_identical_to_the_one_rust_wrote", (T.Body) () -> {
                Path dir = scratch("identical");
                Wal.Opened o = Wal.open(dir);
                Wal w = o.wal();
                w.append(mk(1, 1, 1, List.of(e(1, 1), e(1, 2), e(1, 3)), null));
                w.append(mk(2, 2, 3, List.of(e(2, 9, 9)), null));
                w.append(mk(2, 2, 4, List.of(e(2), e(2, 4)), null));
                w.append(mk(3, -1, 6, List.of(), new long[] {3, 2}));
                w.sync(true);
                w.close();
                check(Arrays.equals(Files.readAllBytes(dir.resolve("raft.wal")), Files.readAllBytes(WIRE.resolve("wal-good.bin"))), "WAL bytes differ from Rust's");
                // Appending after recovery lands cleanly; a rewrite keeps the state.
                w = Wal.open(dir).wal();
                w.append(mk(3, -1, 6, List.of(e(3, 7)), null));
                w.sync(true);
                w.rewrite();
                w.append(mk(3, -1, 7, List.of(e(3, 8)), null));
                w.sync(true);
                w.close();
                o = Wal.open(dir);
                check(o.disk().base == 3 && o.disk().baseTerm == 2 && o.disk().log.size() == 4, fmtDisk(o.disk()));
                check(o.repair().tornBytes() == 0, "torn bytes");
                o.wal().close();
                rmrf(dir);
            }},
            {"snapshots_read_and_write_identically_to_rust", (T.Body) () -> {
                Path dir = scratch("snap");
                SnapStore store = new SnapStore(dir);
                for (String idx : new String[] {"42", "7"}) {
                    String[] f = T.slurp(WIRE.resolve("snap-" + idx + ".expect")).trim().split("\\s+");
                    long index = Long.parseLong(f[0]), term = Long.parseLong(f[1]), len = Long.parseLong(f[2]);
                    byte[] data = unhex(f.length > 4 ? f[4] : "-");
                    String name = String.format("snap-%020d.snap", index);
                    Files.copy(WIRE.resolve(name), dir.resolve(name));
                    SnapStore.Loaded got = store.get(index);
                    check(got != null && got.meta().term() == term && got.meta().len() == len && Arrays.equals(got.data(), data), "snapshot ", idx);
                    // Write the same snapshot here: the file must equal Rust's.
                    Path dir2 = scratch("snap2-" + idx);
                    new SnapStore(dir2).put(index, term, data);
                    check(Arrays.equals(Files.readAllBytes(dir2.resolve(name)), Files.readAllBytes(WIRE.resolve(name))), "snapshot ", idx, " differs from Rust's");
                    rmrf(dir2);
                }
                check(store.list().equals(List.of(7L, 42L)), "list: ", store.list());
                // Corruption and truncation are detected.
                Path p42 = dir.resolve(String.format("snap-%020d.snap", 42));
                byte[] bytes = Files.readAllBytes(p42);
                bytes[bytes.length - 1] ^= 1;
                Files.write(p42, bytes);
                check(store.get(42) == null, "bit flip not detected");
                Files.write(p42, Arrays.copyOf(bytes, bytes.length - 3));
                check(store.get(42) == null, "truncation not detected");
                // Streaming receive verifies length and checksum.
                byte[] blob = new byte[100000];
                for (int i = 0; i < blob.length; i++) blob[i] = (byte) i;
                long crc = Crc.crc32c(blob);
                SnapStore.Incoming in = store.receive(9, 1, blob.length, crc);
                for (int i = 0; i < blob.length; i += 4096) in.write(blob, i, Math.min(4096, blob.length - i));
                in.finish();
                check(store.get(9) != null && Arrays.equals(store.get(9).data(), blob), "received snapshot unreadable");
                SnapStore.Incoming bad = store.receive(10, 1, 4, crc);
                bad.write("abcd".getBytes(), 0, 4);
                boolean threw = false;
                try { bad.finish(); } catch (Wal.WalException x) { threw = true; }
                check(threw && store.get(10) == null, "checksum mismatch accepted or published");
                rmrf(dir);
            }},
        });
    }
}
