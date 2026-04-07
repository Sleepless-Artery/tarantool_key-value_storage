package org.sleepless_artery.kv_service.config;

import io.tarantool.client.box.TarantoolBoxClient;
import io.tarantool.client.factory.TarantoolFactory;


/**
 * Configuration class for creating a {@link TarantoolBoxClient}.
 */
public class TarantoolClientConfig {

    private final TarantoolBoxClient client;

    public TarantoolClientConfig(String host, int port) throws Exception {
        this.client = TarantoolFactory.box()
                .withHost(host)
                .withPort(port)
                .build();
    }

    public TarantoolBoxClient client() {
        return client;
    }
}