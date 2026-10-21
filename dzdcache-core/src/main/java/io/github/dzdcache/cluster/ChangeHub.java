package io.github.dzdcache.cluster;

import io.github.dzdcache.cache.Change;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import java.util.ArrayDeque;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.function.Consumer;
import java.util.function.Predicate;

public final class ChangeHub implements AutoCloseable {
    public record Position(long index, int ordinal) implements Comparable<Position> {
        public static Position parse(String text) {
            int dash = text == null ? -1 : text.indexOf('-');
            if (dash < 1) throw new IllegalArgumentException("A change position looks like <index>-<ordinal>");
            try { return new Position(Long.parseLong(text.substring(0, dash)), Integer.parseInt(text.substring(dash + 1))); }
            catch (NumberFormatException e) { throw new IllegalArgumentException("A change position looks like <index>-<ordinal>"); }
        }
        @Override public int compareTo(Position other) {
            return index != other.index ? Long.compare(index, other.index) : Integer.compare(ordinal, other.ordinal);
        }
        @Override public String toString() { return index + "-" + ordinal; }
    }

    public record Event(Position position, Change change) { }

    public interface Subscription extends AutoCloseable { @Override void close(); }

    private static final int QUEUE = 10_000;

    private final int capacity;
    private final ArrayDeque<Event> buffer = new ArrayDeque<>();
    private final List<Subscriber> subscribers = new CopyOnWriteArrayList<>();
    private final Counter events;
    private final Counter resets;
    private Position horizon;
    private Position latest;

    public ChangeHub(int capacity, MeterRegistry meters) {
        if (capacity < 1) throw new IllegalArgumentException("changes-buffer must be positive");
        this.capacity = capacity;
        events = meters.counter("dzdcache.changes.events");
        resets = meters.counter("dzdcache.changes.resets");
        Gauge.builder("dzdcache.changes.subscribers", subscribers, List::size).register(meters);
    }

    synchronized void publish(long index, List<Change> changes) {
        for (int ordinal = 0; ordinal < changes.size(); ordinal++) add(new Event(new Position(index, ordinal), changes.get(ordinal)));
    }

    synchronized void reset(long index, long at) {
        add(new Event(new Position(index, -1), new Change(null, 0, null, 0, Change.Kind.RESET, null, at)));
    }

    private void add(Event event) {
        buffer.addLast(event);
        latest = event.position();
        if (event.change().kind() == Change.Kind.RESET) horizon = later(horizon, event.position());
        if (buffer.size() > capacity) horizon = later(horizon, buffer.removeFirst().position());
        events.increment();
        for (Subscriber subscriber : subscribers) subscriber.offer(event);
    }

    public synchronized Position latest() { return latest; }

    synchronized List<Event> recent() { return List.copyOf(buffer); }

    private static Position later(Position current, Position candidate) {
        return current == null || candidate.compareTo(current) > 0 ? candidate : current;
    }

    public synchronized Subscription subscribe(Position from, Predicate<Change> filter, Consumer<Event> listener) {
        Subscriber subscriber = new Subscriber(filter, listener);
        if (from != null) {
            if (horizon != null && from.compareTo(horizon) < 0) subscriber.reset(from.index());
            for (Event event : buffer) if (event.position().compareTo(from) > 0) subscriber.offer(event);
        }
        subscribers.add(subscriber);
        subscriber.start();
        return subscriber;
    }

    @Override public void close() { for (Subscriber subscriber : subscribers) subscriber.close(); }

    private final class Subscriber implements Subscription {
        private final Predicate<Change> filter;
        private final Consumer<Event> listener;
        private final LinkedBlockingQueue<Event> queue = new LinkedBlockingQueue<>(QUEUE);
        private volatile boolean closed;
        private Thread worker;

        Subscriber(Predicate<Change> filter, Consumer<Event> listener) { this.filter = filter; this.listener = listener; }

        void offer(Event event) {
            if (closed || event.change().kind() != Change.Kind.RESET && filter != null && !filter.test(event.change())) return;
            if (!queue.offer(event)) reset(event.position().index());
        }

        void reset(long index) {
            queue.clear();
            resets.increment();
            queue.offer(new Event(new Position(index, -1), new Change(null, 0, null, 0, Change.Kind.RESET, null, System.currentTimeMillis())));
        }

        void start() {
            worker = Thread.ofVirtual().name("dzdcache-changes").start(() -> {
                try {
                    while (!closed) listener.accept(queue.take());
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } catch (RuntimeException e) {
                    close();
                }
            });
        }

        @Override public void close() {
            closed = true;
            subscribers.remove(this);
            if (worker != null) worker.interrupt();
        }
    }
}
