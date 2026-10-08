// Ring protocol tests (orderer-rust tests/ring.rs, ported): wrap,
// multi-producer integrity, batching, gating, try-publish CAS path, stalled
// producers, barrier dependencies, wait strategies, multicast.
package io.github.abhijitkrm.orderer;

import static io.github.abhijitkrm.orderer.T.check;

import io.github.abhijitkrm.orderer.Disruptor.Consumer;
import io.github.abhijitkrm.orderer.Disruptor.Handler;
import io.github.abhijitkrm.orderer.Disruptor.Publish;
import io.github.abhijitkrm.orderer.Disruptor.RingBuilder;
import io.github.abhijitkrm.orderer.Disruptor.WaitStrategy;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

public final class RingTest {
    /// Mutable slot: three longs.
    static long[] cell() { return new long[3]; }

    static <T> void drainN(Consumer<T> c, long n, Handler<T> f) {
        long deadline = System.nanoTime() + 20_000_000_000L, got = 0;
        while (got < n) {
            got += c.poll(f);
            if (System.nanoTime() > deadline) { check(false, "timed out at ", got); return; }
            if (got < n) Thread.yield();
        }
    }

    static Thread start(Runnable r) { Thread t = new Thread(r); t.start(); return t; }
    static void join(Thread... ts) throws InterruptedException { for (Thread t : ts) t.join(); }

    static void spscWrap() throws Exception {
        RingBuilder<long[]> b = new RingBuilder<>(8, RingTest::cell);
        b.consumer();
        var built = b.buildSingle();
        var p = built.producer();
        Consumer<long[]> c = built.consumers().get(0);
        final long N = 100_000;
        Thread reader = start(() -> {
            long[] expect = {0};
            drainN(c, N, (v, seq, eob) -> {
                if (v[0] != expect[0] || seq != expect[0]) check(false, v[0]);
                expect[0]++;
            });
        });
        for (long i = 0; i < N; i++) { long x = i; p.publish(s -> s[0] = x); }
        join(reader);
    }

    static void multiProducerRun(int batch) throws Exception {
        final int PRODUCERS = 4, PER = 100_000;
        RingBuilder<long[]> b = new RingBuilder<>(1024, RingTest::cell);
        b.consumer();
        var built = b.buildMulti();
        var p = built.producer();
        Consumer<long[]> c = built.consumers().get(0);
        Thread reader = start(() -> {
            long[] next = new long[PRODUCERS];
            long[] last = {-1};
            int[] bad = {0};
            drainN(c, (long) PRODUCERS * PER, (m, seq, eob) -> {
                if (seq != last[0] + 1) bad[0]++;
                last[0] = seq;
                if (m[2] != ((m[0] * 31) ^ m[1])) bad[0]++;
                if (m[1] != next[(int) m[0]]) bad[0]++;
                next[(int) m[0]]++;
            });
            check(bad[0] == 0, bad[0], " ordering/torn-slot violations");
        });
        List<Thread> ws = new ArrayList<>();
        for (long id = 0; id < PRODUCERS; id++) {
            long pid = id;
            ws.add(start(() -> {
                for (long i = 0; i < PER;) {
                    int n = (int) Math.min(batch, PER - i);
                    long base = i;
                    p.publishBatch(n, (k, s) -> { s[0] = pid; s[1] = base + k; s[2] = (pid * 31) ^ (base + k); });
                    i += n;
                }
            }));
        }
        for (Thread w : ws) w.join();
        reader.join();
    }

    static void batchClaimConsumeAndStaging() {
        RingBuilder<long[]> b = new RingBuilder<>(16, RingTest::cell);
        b.consumer();
        var built = b.buildSingle();
        var p = built.producer();
        Consumer<long[]> c = built.consumers().get(0);
        p.publishBatch(5, (i, s) -> s[0] = i * 10L);
        List<Boolean> eobs = new ArrayList<>();
        check(c.poll((v, s, e) -> eobs.add(e)) == 5);
        check(eobs.get(4) && !eobs.get(0), "end_of_batch on the last only");
        p.stage()[0] = 1;
        p.stage()[0] = 2;
        check(p.staged() == 2);
        check(c.poll((v, s, e) -> {}) == 0, "staged is invisible");
        p.commit();
        check(c.poll((v, s, e) -> {}) == 2);
        p.publishBatch(10, (i, s) -> s[0] = i);
        c.setMaxBatch(4);
        check(c.poll((v, s, e) -> {}) == 4);
    }

    static void gatingBlocksAndTryPublishFull() throws Exception {
        RingBuilder<long[]> b = new RingBuilder<>(4, RingTest::cell);
        b.consumer();
        var built = b.buildSingle();
        var p = built.producer();
        Consumer<long[]> c = built.consumers().get(0);
        for (int i = 0; i < 4; i++) { int x = i; check(p.tryPublish(s -> s[0] = x) == Publish.Ok); }
        check(p.tryPublish(s -> s[0] = 99) == Publish.Full);
        AtomicBoolean done = new AtomicBoolean();
        Thread w = start(() -> { p.publish(s -> s[0] = 4); done.set(true); });
        T.sleep(50);
        check(!done.get(), "producer must not lap the consumer");
        List<Long> seen = new ArrayList<>();
        c.poll((v, s, e) -> seen.add(v[0]));
        w.join();
        c.poll((v, s, e) -> seen.add(v[0]));
        check(seen.equals(List.of(0L, 1L, 2L, 3L, 4L)), "zero loss under Block: ", seen);
    }

    static void stagedWorkIsCommittedBeforeWaiting() throws Exception {
        RingBuilder<long[]> b = new RingBuilder<>(4, RingTest::cell);
        b.consumer();
        var built = b.buildSingle();
        var p = built.producer();
        Consumer<long[]> c = built.consumers().get(0);
        List<Long> seen = new ArrayList<>();
        Thread r = start(() -> drainN(c, 10, (v, s, e) -> seen.add(v[0])));
        for (int i = 0; i < 10; i++) p.stage()[0] = i;
        p.commit();
        r.join();
        check(seen.size() == 10 && seen.get(9) == 9);
    }

    static void tryPublishCasNeverLeaksClaims() {
        RingBuilder<long[]> b = new RingBuilder<>(8, RingTest::cell);
        b.consumer();
        var built = b.buildMulti();
        var p = built.producer();
        Consumer<long[]> c = built.consumers().get(0);
        for (int i = 0; i < 8; i++) { int x = i; p.tryPublish(s -> s[0] = x); }
        check(p.tryPublish(s -> s[0] = 99) == Publish.Full);
        check(p.tryPublishBatch(3, (i, s) -> s[0] = 99) == Publish.Full);
        check(p.control().published() == 7, "a failed try leaves the cursor untouched");
        check(c.poll((v, s, e) -> {}) == 8);
        check(p.tryPublish(s -> s[0] = 8) == Publish.Ok);
    }

    static void tryAndBlockProducersNeverDoubleClaim() throws Exception {
        final int PER = 20_000;
        RingBuilder<long[]> b = new RingBuilder<>(64, RingTest::cell);
        b.consumer();
        var built = b.buildMulti();
        var p = built.producer();
        Consumer<long[]> c = built.consumers().get(0);
        AtomicLong accepted = new AtomicLong();
        AtomicBoolean stop = new AtomicBoolean();
        long[] delivered = {0};
        Thread r = start(() -> {
            Set<Long> seen = new HashSet<>();
            long[] last = {-1};
            int[] bad = {0};
            for (;;) {
                boolean stopping = stop.get();
                int n = c.poll((v, seq, e) -> {
                    if (seq != last[0] + 1) bad[0]++;
                    last[0] = seq;
                    if (!seen.add(v[0])) bad[0]++;
                });
                if (n == 0) {
                    if (stopping) break;
                    Thread.yield();
                }
            }
            check(bad[0] == 0, "delivered twice / out of order");
            delivered[0] = seen.size();
        });
        List<Thread> ws = new ArrayList<>();
        for (long id = 0; id < 4; id++) {
            long pid = id;
            ws.add(start(() -> {
                for (int i = 0; i < PER; i++) {
                    long v = (pid << 32) | i;
                    if (pid % 2 == 0) { p.publish(s -> s[0] = v); accepted.incrementAndGet(); }
                    else if (p.tryPublish(s -> s[0] = v) == Publish.Ok) accepted.incrementAndGet();
                }
            }));
        }
        for (Thread w : ws) w.join();
        stop.set(true);
        r.join();
        check(delivered[0] == accepted.get(), delivered[0], " vs ", accepted.get());
    }

    static void stalledProducerGatesOnlyItsOwnSlot() throws Exception {
        RingBuilder<long[]> b = new RingBuilder<>(16, RingTest::cell);
        b.consumer();
        var built = b.buildMulti();
        var p = built.producer();
        Consumer<long[]> c = built.consumers().get(0);
        p.publish(s -> s[0] = 0);
        p.publish(s -> s[0] = 1);
        CyclicBarrier claimed = new CyclicBarrier(2), release = new CyclicBarrier(2);
        Thread a = start(() -> p.publish(s -> {
            try { claimed.await(); release.await(); } catch (Exception e) { throw new RuntimeException(e); }
            s[0] = 2;
        }));
        claimed.await();
        p.publish(s -> s[0] = 3);
        p.publish(s -> s[0] = 4);
        List<Long> seen = new ArrayList<>();
        c.poll((v, s, e) -> seen.add(v[0]));
        check(seen.equals(List.of(0L, 1L)), "consumer stops at the unpublished slot: ", seen);
        release.await();
        a.join();
        c.poll((v, s, e) -> seen.add(v[0]));
        check(seen.equals(List.of(0L, 1L, 2L, 3L, 4L)), seen);
    }

    static void barrierDependencyOrdersStages() throws Exception {
        final long N = 200_000;
        RingBuilder<long[]> b = new RingBuilder<>(256, RingTest::cell);
        int ida = b.consumer();
        b.consumer(ida);
        var built = b.buildSingle();
        var p = built.producer();
        Consumer<long[]> ca = built.consumers().get(0), cb = built.consumers().get(1);
        Disruptor.Sequence aSeq = ca.sequence();
        Thread ta = start(() -> drainN(ca, N, (v, s, e) -> Thread.onSpinWait()));
        int[] bad = {0};
        Thread tb = start(() -> drainN(cb, N, (v, seq, e) -> {
            if (v[0] != seq) bad[0]++;
            if (aSeq.get() < seq) bad[0]++;
        }));
        for (long i = 0; i < N; i++) { long x = i; p.publish(s -> s[0] = x); }
        join(ta, tb);
        check(bad[0] == 0, "stage B overtook stage A");
    }

    static void waitStrategiesAllDeliver() throws Exception {
        for (WaitStrategy w : List.of(WaitStrategy.busySpin(), WaitStrategy.yieldWait(), WaitStrategy.backoff(), WaitStrategy.blocking())) {
            RingBuilder<long[]> b = new RingBuilder<>(16, RingTest::cell);
            b.consumer(w);
            var built = b.buildSingle();
            var p = built.producer();
            var ctl = p.control();
            Consumer<long[]> c = built.consumers().get(0);
            List<Long> seen = new ArrayList<>();
            Thread r = start(() -> { while (c.waitPoll((v, s, e) -> seen.add(v[0]))) {} });
            for (int i = 0; i < 20; i++) {
                int x = i;
                p.publish(s -> s[0] = x);
                if (i % 5 == 0) T.sleep(3);
            }
            while (ctl.consumed() < 19) Thread.yield();
            ctl.alert();
            r.join();
            check(seen.size() == 20 && seen.get(19) == 19, w.kind());
        }
    }

    static void multicastAllSeeEverythingSlowestGates() {
        RingBuilder<long[]> b = new RingBuilder<>(8, RingTest::cell);
        for (int i = 0; i < 3; i++) b.consumer();
        var built = b.buildSingle();
        var p = built.producer();
        var cs = built.consumers();
        for (int i = 0; i < 8; i++) { int x = i; p.publish(s -> s[0] = x); }
        check(cs.get(0).poll((v, s, e) -> {}) == 8);
        check(cs.get(1).poll((v, s, e) -> {}) == 8);
        check(p.tryPublish(s -> s[0] = 8) == Publish.Full, "third consumer gates");
        check(cs.get(2).poll((v, s, e) -> {}) == 8);
        check(p.tryPublish(s -> s[0] = 8) == Publish.Ok);
    }

    public static void main(String[] args) {
        T.runAll(new Object[][] {
            {"spsc_wrap", (T.Body) RingTest::spscWrap},
            {"multi_producer_integrity_single_claims", (T.Body) () -> multiProducerRun(1)},
            {"multi_producer_integrity_batched_claims", (T.Body) () -> multiProducerRun(37)},
            {"batch_claim_consume_and_staging", (T.Body) RingTest::batchClaimConsumeAndStaging},
            {"gating_blocks_and_try_publish_full", (T.Body) RingTest::gatingBlocksAndTryPublishFull},
            {"staged_work_is_committed_before_waiting", (T.Body) RingTest::stagedWorkIsCommittedBeforeWaiting},
            {"try_publish_cas_never_leaks_claims", (T.Body) RingTest::tryPublishCasNeverLeaksClaims},
            {"try_and_block_producers_never_double_claim", (T.Body) RingTest::tryAndBlockProducersNeverDoubleClaim},
            {"stalled_producer_gates_only_its_own_slot", (T.Body) RingTest::stalledProducerGatesOnlyItsOwnSlot},
            {"barrier_dependency_orders_stages", (T.Body) RingTest::barrierDependencyOrdersStages},
            {"wait_strategies_all_deliver", (T.Body) RingTest::waitStrategiesAllDeliver},
            {"multicast_all_see_everything_slowest_gates", (T.Body) RingTest::multicastAllSeeEverythingSlowestGates},
        });
    }
}
