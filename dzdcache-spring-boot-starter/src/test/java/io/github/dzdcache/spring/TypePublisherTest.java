package io.github.dzdcache.spring;

import io.github.dzdcache.cache.CacheException;
import io.github.dzdcache.cache.TypeDescriptor;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class TypePublisherTest {
    private static final TypeDescriptor STUDENTS = new TypeDescriptor(TypeDescriptor.JAVA, "students", 1, 10, null, "s1", null);
    private static final TypeDescriptor COURSES = new TypeDescriptor(TypeDescriptor.JAVA, "courses", 1, 10, null, "c1", null);

    private static final class FakeCluster implements TypePublisher.Cluster {
        volatile boolean ready = true;
        volatile String clusterId = "cluster-1";
        final List<String> published = new java.util.concurrent.CopyOnWriteArrayList<>();
        final List<String> unavailable = new ArrayList<>();
        @Override public boolean ready() { return ready; }
        @Override public String clusterId() { return clusterId; }
        @Override public void publish(TypeDescriptor type) {
            if (unavailable.remove(type.name())) throw new CacheException(503, "No master");
            published.add(clusterId + ":" + type.name());
        }
    }

    @Test void typesArePublishedOncePerClusterAndAgainWhenTheNodeEndsUpInANewCluster() {
        FakeCluster cluster = new FakeCluster();
        TypePublisher publisher = new TypePublisher(cluster, List.of(STUDENTS, COURSES), 1);
        publisher.publishToTheCurrentCluster();
        publisher.publishToTheCurrentCluster();
        assertThat(cluster.published).containsExactly("cluster-1:students", "cluster-1:courses");
        cluster.clusterId = "cluster-2";
        publisher.publishToTheCurrentCluster();
        assertThat(cluster.published).containsExactly("cluster-1:students", "cluster-1:courses", "cluster-2:students", "cluster-2:courses");
    }

    @Test void nothingIsPublishedWhileTheNodeIsNotReadyOrInNoCluster() {
        FakeCluster cluster = new FakeCluster();
        TypePublisher publisher = new TypePublisher(cluster, List.of(STUDENTS), 1);
        cluster.ready = false;
        publisher.publishToTheCurrentCluster();
        cluster.ready = true;
        cluster.clusterId = null;
        publisher.publishToTheCurrentCluster();
        assertThat(cluster.published).isEmpty();
        cluster.clusterId = "cluster-1";
        publisher.publishToTheCurrentCluster();
        assertThat(cluster.published).containsExactly("cluster-1:students");
    }

    @Test void aClusterWithoutAMasterIsTriedAgainUntilEveryTypeIsPublished() {
        FakeCluster cluster = new FakeCluster();
        cluster.unavailable.add("courses");
        TypePublisher publisher = new TypePublisher(cluster, List.of(STUDENTS, COURSES), 1);
        publisher.publishToTheCurrentCluster();
        publisher.publishToTheCurrentCluster();
        publisher.publishToTheCurrentCluster();
        assertThat(cluster.published).containsExactly("cluster-1:students", "cluster-1:students", "cluster-1:courses");
    }

    @Test void theRunningPublisherFollowsTheNodeIntoANewClusterUntilItIsStopped() throws Exception {
        FakeCluster cluster = new FakeCluster();
        Thread running = Thread.ofVirtual().start(new TypePublisher(cluster, List.of(STUDENTS), 1));
        await(() -> cluster.published.contains("cluster-1:students"));
        cluster.clusterId = "cluster-2";
        await(() -> cluster.published.contains("cluster-2:students"));
        running.interrupt();
        running.join(5_000);
        assertThat(running.isAlive()).isFalse();
    }

    private static void await(java.util.function.BooleanSupplier condition) throws InterruptedException {
        long deadline = System.nanoTime() + 5_000_000_000L;
        while (!condition.getAsBoolean()) {
            if (System.nanoTime() > deadline) throw new AssertionError("condition not reached");
            Thread.sleep(5);
        }
    }
}
