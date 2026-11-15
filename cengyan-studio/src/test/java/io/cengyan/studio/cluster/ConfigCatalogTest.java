package io.cengyan.studio.cluster;

import io.cengyan.studio.cluster.ClusterState.Role;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class ConfigCatalogTest {
    @Test void transactionPlanLimitIsConfigurableAndFitsRpc() {
        ConfigCatalog catalog = ConfigCatalog.load();
        ConfigCatalog.Entry storage = catalog.entry(Role.STORAGE, "transaction_plan_max_bytes").orElseThrow();
        ConfigCatalog.Entry compute = catalog.entry(Role.COMPUTE, "transaction_plan_max_bytes").orElseThrow();
        assertEquals("16777216", storage.defaultValue());
        assertEquals(storage.defaultValue(), compute.defaultValue());
        Map<String, String> values = new LinkedHashMap<>();
        catalog.entries(Role.STORAGE).forEach(entry -> values.put(entry.key(), entry.defaultValue()));
        assertFalse(catalog.crossCheck(Role.STORAGE, values).containsKey("transaction_plan_max_bytes"));
        values.put("transaction_plan_max_bytes", String.valueOf(64 * 1024 * 1024));
        assertTrue(catalog.crossCheck(Role.STORAGE, values).containsKey("transaction_plan_max_bytes"));
    }
}
