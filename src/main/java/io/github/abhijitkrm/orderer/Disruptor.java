//! The LMAX Disruptor in Java (orderer-rust's orderer-disruptor, ported).
//!
//! Protocol: write a slot only under an unpublished claim (granted once every
//! gating consumer has passed seq - size); read it only after an acquire of
//! the cursor / availability flag the writer released after writing, and
//! only until the reader's own sequence (released after reading) passes it.
//! Slots are mutable flyweights created once by a factory — publishing
//! rewrites them in place, so the ring itself never allocates.
package io.github.abhijitkrm.orderer;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicIntegerArray;
import java.util.concurrent.locks.LockSupport;
import java.util.function.Supplier;

public final class Disruptor {
    private Disruptor() {}

    public static final long INITIAL = -1;

    /// A counter padded to sit alone on its cache lines (128 B each side).
    public static final class Sequence {
        @SuppressWarnings("unused") long p0, p1, p2, p3, p4, p5, p6, p7, p8, p9, p10, p11, p12, p13, p14, p15;
        private volatile long value = INITIAL;
        @SuppressWarnings("unused") long q0, q1, q2, q3, q4, q5, q6, q7, q8, q9, q10, q11, q12, q13, q14, q15;
        private static final VarHandle V;
        static {
            try { V = MethodHandles.lookup().findVarHandle(Sequence.class, "value", long.class); }
            catch (ReflectiveOperationException e) { throw new ExceptionInInitializerError(e); }
        }
        public long get() { return (long) V.getAcquire(this); }
        public void set(long v) { V.setRelease(this, v); }
        long getAndAdd(long n) { return (long) V.getAndAdd(this, n); }
        boolean cas(long expect, long next) { return V.compareAndSet(this, expect, next); }
    }

    static long minOf(Sequence[] seqs, long floor) {
        long m = floor;
        for (Sequence s : seqs) m = Math.min(m, s.get());
        return m;
    }

    // ---- wait strategies -------------------------------------------------------------

    public enum WaitKind { BusySpin, Yield, Backoff, Blocking }

    public record WaitStrategy(WaitKind kind, int spin, int yields, long parkMinNanos, long parkMaxNanos) {
        public static WaitStrategy busySpin() { return new WaitStrategy(WaitKind.BusySpin, 0, 0, 0, 0); }
        public static WaitStrategy yieldWait() { return new WaitStrategy(WaitKind.Yield, 100, 0, 0, 0); }
        public static WaitStrategy backoff() { return new WaitStrategy(WaitKind.Backoff, 256, 64, 20_000, 1_000_000); }
        public static WaitStrategy blocking() { return new WaitStrategy(WaitKind.Blocking, 100, 0, 0, 0); }
    }

    /// Wakes Blocking waiters (bounded 1 ms park, so a missed signal only delays).
    static final class Notifier {
        private final java.util.concurrent.ConcurrentLinkedQueue<Thread> waiters = new java.util.concurrent.ConcurrentLinkedQueue<>();
        private final java.util.concurrent.atomic.AtomicInteger count = new java.util.concurrent.atomic.AtomicInteger();
        void signal() { if (count.get() != 0) wakeAll(); }
        void wakeAll() { for (Thread t : waiters) LockSupport.unpark(t); }
        void block() {
            Thread me = Thread.currentThread();
            waiters.add(me);
            count.incrementAndGet();
            LockSupport.parkNanos(1_000_000);
            count.decrementAndGet();
            waiters.remove(me);
        }
    }

    static final class Waiter {
        private final WaitStrategy s;
        private int step;
        private long park;
        Waiter(WaitStrategy s) { this.s = s; }
        void reset() { step = 0; }
        void idle(Notifier n) {
            switch (s.kind()) {
                case BusySpin -> Thread.onSpinWait();
                case Yield -> { if (step < s.spin()) { step++; Thread.onSpinWait(); } else Thread.yield(); }
                case Backoff -> {
                    if (step < s.spin()) { step++; Thread.onSpinWait(); }
                    else if (step < s.spin() + s.yields()) { step++; Thread.yield(); }
                    else {
                        if (step == s.spin() + s.yields()) { step++; park = s.parkMinNanos(); }
                        LockSupport.parkNanos(park);
                        park = Math.min(park * 2, s.parkMaxNanos());
                    }
                }
                case Blocking -> { if (step < s.spin()) { step++; Thread.onSpinWait(); } else n.block(); }
            }
        }
    }

    // ---- shared ring state -----------------------------------------------------------

    public enum ProducerKind { Single, Multi }
    public enum Publish { Ok, Full, Alerted }

    static final class Shared<T> {
        final Object[] slots;
        final long size, mask;
        final int shift;
        final ProducerKind kind;
        final Sequence cursor = new Sequence();       // Single: published. Multi: claimed.
        final AtomicIntegerArray available;           // Multi: lap of each slot's last publish
        final Sequence gatingCache = new Sequence();
        final Sequence[] gating;
        final Notifier notifier = new Notifier();
        volatile boolean alerted;

        Shared(int n, ProducerKind k, Sequence[] gating, Supplier<T> factory) {
            slots = new Object[n];
            for (int i = 0; i < n; i++) slots[i] = factory.get();
            size = n;
            mask = n - 1;
            shift = Integer.numberOfTrailingZeros(n);
            kind = k;
            this.gating = gating;
            if (k == ProducerKind.Multi) {
                available = new AtomicIntegerArray(n);
                for (int i = 0; i < n; i++) available.set(i, -1);
            } else {
                available = null;
            }
        }

        @SuppressWarnings("unchecked") T slot(long s) { return (T) slots[(int) (s & mask)]; }
        long minGating() { return minOf(gating, Long.MAX_VALUE); }
        int lap(long s) { return (int) (s >>> shift); }
        void setAvailable(long s) { available.setRelease((int) (s & mask), lap(s)); }
        boolean isAvailable(long s) { return available.getAcquire((int) (s & mask)) == lap(s); }
        long publishedUpto(long lo, long limit) {
            long hi = Math.min(cursor.get(), limit);
            if (kind == ProducerKind.Single) return hi;
            for (long s = lo; s <= hi; s++) if (!isAvailable(s)) return s - 1;
            return hi;
        }
        long publishedHighWater() {
            if (kind == ProducerKind.Single) return cursor.get();
            long floor = Math.min(minGating(), cursor.get());
            return publishedUpto(floor + 1, Long.MAX_VALUE);
        }
        void alert() { alerted = true; notifier.wakeAll(); }
    }

    static void producerBackoff(int[] step) {
        if (step[0] < 64) { step[0]++; Thread.onSpinWait(); } else Thread.yield();
    }

    public static final class RingControl<T> {
        private final Shared<T> s;
        RingControl(Shared<T> s) { this.s = s; }
        public void alert() { s.alert(); }
        public boolean isAlerted() { return s.alerted; }
        public long published() { return s.publishedHighWater(); }
        public long consumed() { return s.minGating(); }
    }

    /// Writes a slot in place.
    @FunctionalInterface
    public interface Fill<T> { void fill(T slot); }

    @FunctionalInterface
    public interface FillIndexed<T> { void fill(int i, T slot); }

    /// The only producer of a Single ring: stage() + commit() publish a whole
    /// batch with one release store.
    public static final class SingleProducer<T> {
        private final Shared<T> s;
        private long next, published, cachedGate;
        private final int[] step = new int[1];

        SingleProducer(Shared<T> s) {
            this.s = s;
            next = published = cachedGate = s.cursor.get();
        }
        public int size() { return (int) s.size; }
        public int staged() { return (int) (next - published); }

        private boolean hasRoom(long n) {
            long wrap = next + n - s.size;
            if (wrap <= cachedGate) return true;
            cachedGate = s.minGating();
            return wrap <= cachedGate;
        }
        private Publish waitRoom(long n) {
            if (hasRoom(n)) return Publish.Ok;
            commit();  // consumers can't free space they can't see
            step[0] = 0;
            while (!hasRoom(n)) {
                if (s.alerted) return Publish.Alerted;
                producerBackoff(step);
            }
            return Publish.Ok;
        }
        /// Claim + fill the next slot; invisible until commit().
        public T stage() {
            if (waitRoom(1) != Publish.Ok) return null;
            return s.slot(++next);
        }
        public void commit() {
            if (next != published) {
                published = next;
                s.cursor.set(next);
                s.notifier.signal();
            }
        }
        public Publish publish(Fill<T> f) {
            T slot = stage();
            if (slot == null) return Publish.Alerted;
            f.fill(slot);
            commit();
            return Publish.Ok;
        }
        public Publish tryPublish(Fill<T> f) {
            if (!hasRoom(1)) return Publish.Full;
            return publish(f);
        }
        public Publish publishBatch(int n, FillIndexed<T> f) {
            if (waitRoom(n) != Publish.Ok) return Publish.Alerted;
            for (int i = 0; i < n; i++) f.fill(i, s.slot(++next));
            commit();
            return Publish.Ok;
        }
        public RingControl<T> control() { return new RingControl<>(s); }
    }

    /// A producer of a Multi ring; share it across threads.
    public static final class MultiProducer<T> {
        private final Shared<T> s;
        MultiProducer(Shared<T> s) { this.s = s; }
        public int size() { return (int) s.size; }

        private boolean roomFor(long hi) {
            long wrap = hi - s.size;
            if (wrap <= s.gatingCache.get()) return true;
            long g = s.minGating();
            s.gatingCache.set(g);
            return wrap <= g;
        }
        private void fill(long lo, long hi, FillIndexed<T> f) {
            for (long q = lo; q <= hi; q++) f.fill((int) (q - lo), s.slot(q));
            for (long q = lo; q <= hi; q++) s.setAvailable(q);
            s.notifier.signal();
        }
        public Publish publishBatch(int n, FillIndexed<T> f) {
            long hi = s.cursor.getAndAdd(n) + n;
            int[] step = {0};
            while (!roomFor(hi)) {
                if (s.alerted) return Publish.Alerted;
                producerBackoff(step);
            }
            fill(hi - n + 1, hi, f);
            return Publish.Ok;
        }
        public Publish publish(Fill<T> f) { return publishBatch(1, (i, t) -> f.fill(t)); }
        /// Claim by CAS only if it fits: a getAndAdd claim could not be backed out.
        public Publish tryPublishBatch(int n, FillIndexed<T> f) {
            for (;;) {
                if (s.alerted) return Publish.Alerted;
                long cur = s.cursor.get(), hi = cur + n;
                if (!roomFor(hi)) return Publish.Full;
                if (s.cursor.cas(cur, hi)) { fill(cur + 1, hi, f); return Publish.Ok; }
            }
        }
        public Publish tryPublish(Fill<T> f) { return tryPublishBatch(1, (i, t) -> f.fill(t)); }
        public RingControl<T> control() { return new RingControl<>(s); }
    }

    @FunctionalInterface
    public interface Handler<T> { void onEvent(T ev, long seq, boolean endOfBatch); }

    /// One consumer: a barrier (cursor + upstream consumers) plus its own watermark.
    public static final class Consumer<T> {
        private final Shared<T> s;
        private final Sequence[] deps;
        private final Sequence seq;
        private long next;
        private long maxBatch = 1024;
        private final Waiter waiter;

        Consumer(Shared<T> s, Sequence[] deps, Sequence seq, WaitStrategy w) {
            this.s = s;
            this.deps = deps;
            this.seq = seq;
            this.next = seq.get() + 1;
            this.waiter = new Waiter(w);
        }
        public Sequence sequence() { return seq; }
        public void setMaxBatch(int n) { maxBatch = n; }
        public boolean isAlerted() { return s.alerted; }
        public long available() {
            long limit = next + maxBatch - 1;
            return deps.length == 0 ? s.publishedUpto(next, limit) : minOf(deps, limit);
        }
        /// Everything available (up to the batch cap), without waiting.
        public int poll(Handler<T> h) {
            long avail = available();
            if (avail < next) return 0;
            for (long q = next; q <= avail; q++) h.onEvent(s.slot(q), q, q == avail);
            int n = (int) (avail - next + 1);
            next = avail + 1;
            seq.set(avail);
            s.notifier.signal();
            return n;
        }
        /// Wait for at least one event, then poll; false once alerted and empty.
        public boolean waitPoll(Handler<T> h) {
            for (;;) {
                if (poll(h) > 0) { waiter.reset(); return true; }
                if (s.alerted) return false;
                waiter.idle(s.notifier);
            }
        }
        public void idle() { waiter.idle(s.notifier); }
        public void resetIdle() { waiter.reset(); }
    }

    public record Built<P, T>(P producer, List<Consumer<T>> consumers) {}

    /// Declare consumers and their upstream dependencies, then build producer
    /// and consumers at once. The producer gates on terminal consumers.
    public static final class RingBuilder<T> {
        private final int size;
        private final Supplier<T> factory;
        private final List<int[]> deps = new ArrayList<>();
        private final List<WaitStrategy> waits = new ArrayList<>();

        public RingBuilder(int size, Supplier<T> factory) {
            if (size < 1 || Integer.bitCount(size) != 1) throw new IllegalArgumentException("ring size must be a power of two");
            this.size = size;
            this.factory = factory;
        }
        public int consumer(WaitStrategy w, int... dependsOn) {
            for (int d : dependsOn) if (d >= deps.size()) throw new IllegalArgumentException("declare dependencies first");
            deps.add(dependsOn);
            waits.add(w);
            return deps.size() - 1;
        }
        public int consumer(int... dependsOn) { return consumer(WaitStrategy.backoff(), dependsOn); }

        private Built<Shared<T>, T> build(ProducerKind k) {
            Sequence[] seqs = new Sequence[deps.size()];
            boolean[] depended = new boolean[deps.size()];
            for (int i = 0; i < seqs.length; i++) seqs[i] = new Sequence();
            for (int[] d : deps) for (int x : d) depended[x] = true;
            List<Sequence> gating = new ArrayList<>();
            for (int i = 0; i < seqs.length; i++) if (!depended[i]) gating.add(seqs[i]);
            Shared<T> sh = new Shared<>(size, k, gating.toArray(new Sequence[0]), factory);
            List<Consumer<T>> cons = new ArrayList<>();
            for (int i = 0; i < seqs.length; i++) {
                Sequence[] ds = new Sequence[deps.get(i).length];
                for (int j = 0; j < ds.length; j++) ds[j] = seqs[deps.get(i)[j]];
                cons.add(new Consumer<>(sh, ds, seqs[i], waits.get(i)));
            }
            return new Built<>(sh, cons);
        }
        public Built<SingleProducer<T>, T> buildSingle() {
            var b = build(ProducerKind.Single);
            return new Built<>(new SingleProducer<>(b.producer()), b.consumers());
        }
        public Built<MultiProducer<T>, T> buildMulti() {
            var b = build(ProducerKind.Multi);
            return new Built<>(new MultiProducer<>(b.producer()), b.consumers());
        }
    }
}
