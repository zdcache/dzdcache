package io.github.dzdcache.benchmark;

import java.time.Duration;
import java.util.function.BooleanSupplier;

final class Waits {
    private Waits() { }

    static void until(String what, BooleanSupplier condition) { until(what, Duration.ofMinutes(2), condition); }

    static void until(String what, Duration timeout, BooleanSupplier condition) {
        long deadline = System.nanoTime() + timeout.toNanos();
        while (!condition.getAsBoolean()) {
            if (System.nanoTime() > deadline) throw new IllegalStateException("Timed out waiting until " + what);
            try { Thread.sleep(50); } catch (InterruptedException e) { Thread.currentThread().interrupt(); throw new IllegalStateException(e); }
        }
    }
}
