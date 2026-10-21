package io.github.dzdcache.cluster;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;

class ClusterSimulationWithDiskTest extends ClusterSimulationTest {
    @BeforeAll static void persistent() { SimulatedCluster.persistentByDefault = true; }
    @AfterAll static void inMemory() { SimulatedCluster.persistentByDefault = false; }
}
