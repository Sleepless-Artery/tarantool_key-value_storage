package org.sleepless_artery.kv_service.domain;

/**
 * Key-value entry stored in the KV storage.
 */
public record KVEntry(
        String key,
        byte[] value
) {}