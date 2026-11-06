package io.github.abhijitkrm.orderer.cluster;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

/// A test hook that cuts links: allows(from, to) is checked on every send and receive.
public final class Filter {
    private final Set<Long> blocked = new HashSet<>();

    private static long key(int a, int b) { return ((long) a << 32) | b; }

    public synchronized void block(int a, int b) { blocked.add(key(a, b)); blocked.add(key(b, a)); }
    public void isolate(int n, List<Integer> all) { for (int o : all) if (o != n) block(n, o); }
    public synchronized void heal() { blocked.clear(); }
    public synchronized boolean allows(int from, int to) { return !blocked.contains(key(from, to)); }
}
