package org.sleepless_artery.kv_service.repository;

import io.tarantool.client.box.TarantoolBoxClient;
import org.sleepless_artery.kv_service.domain.KVEntry;

import java.util.ArrayList;
import java.util.List;


/**
 * Tarantool-based repository for key-value storage operations.
 */
public class KVRepository {

    private final TarantoolBoxClient client;

    private static final String SPACE_NAME = "KV";


    public KVRepository(TarantoolBoxClient client) {
        this.client = client;
    }


    /**
     * Inserts or updates a value by key.
     *
     * @param key   entry key
     * @param value entry value
     */
    public void put(String key, byte[] value) {
        client.space(SPACE_NAME)
                .upsert(
                        List.of(key, value),
                        List.of(
                                List.of("=", 1, value)
                        )
                )
                .join();
    }


    /**
     * Retrieves value by key.
     *
     * @param key entry key
     * @return {@link  KVEntry} or {@code null} if entry does not exist
     */
    public KVEntry get(String key) {
        var result = client.space(SPACE_NAME)
                .select(List.of(key))
                .join()
                .get();

        if (result == null || result.isEmpty()) {
            return null;
        }

        var tuple = result.getFirst().get();
        var foundKey = (String) tuple.getFirst();
        var value = tuple.get(1);

        if (foundKey == null) {
            return null;
        }

        return new KVEntry(foundKey, (byte[]) value);
    }


    /**
     * Deletes value by key.
     *
     * @param key entry key to delete
     */
    public void delete(String key) {
        try {
            client.space(SPACE_NAME)
                    .delete(
                            List.of(key)
                    )
                    .join();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }


    /**
     * Retrieves a batch of entries in key range.
     *
     * @param keySince start key (inclusive)
     * @param keyTo    end key (inclusive)
     * @param limit    max number of entries
     * @return {@link List} list of entries ({@link KVEntry})
     */
    public List<KVEntry> rangeBatch(String keySince, String keyTo, int limit) {
        try {
            var rawResult = client.call(
                    "kv_range",
                            List.of(keySince, keyTo, limit)
                    )
                    .join()
                    .get();

            if (rawResult == null || rawResult.isEmpty()) {
                return List.of();
            }

            return !(rawResult.getFirst() instanceof List<?> rows)
                    ? List.of()
                    : getEntries(rows);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }


    /**
     * Counts total number of stored entries.
     *
     * @return entry count
     */
    public long count() {
        try {
            var count = client.call("kv_count")
                    .join()
                    .get()
                    .getFirst();

            if (count instanceof Number) {
                return ((Number) count).longValue();
            }

            return 0L;
        } catch (Exception e) {
            throw new RuntimeException("Failed to count KV records", e);
        }
    }


    private ArrayList<KVEntry> getEntries(List<?> list) {
        var entries = new ArrayList<KVEntry>();

        for (Object item : list) {
            if (!(item instanceof List<?> tuple) || tuple.isEmpty()) {
                continue;
            }

            String key = (String) tuple.getFirst();
            Object value = tuple.size() > 1 ? tuple.get(1) : null;

            entries.add(new KVEntry(
                    key,
                    value == null ? null : (byte[]) value
            ));
        }

        return entries;
    }
}