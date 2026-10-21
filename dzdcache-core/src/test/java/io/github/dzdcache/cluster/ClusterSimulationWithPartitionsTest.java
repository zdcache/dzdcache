package io.github.dzdcache.cluster;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;

class ClusterSimulationWithPartitionsTest extends ClusterSimulationTest {
    @BeforeAll static void partitioned() { SimulatedCluster.partitionsByDefault = 4; }
    @AfterAll static void single() { SimulatedCluster.partitionsByDefault = 1; }
}
