package org.sleepless_artery.kv_service.service;

import com.google.protobuf.ByteString;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import org.sleepless_artery.kv_service.domain.KVEntry;
import org.sleepless_artery.kv_service.proto.*;
import org.sleepless_artery.kv_service.repository.KVRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * gRPC service implementation for key-value operations.
 *
 * <p>Provides CRUD, range streaming, and count operations.</p>
 */
public class KVGrpcService extends KVServiceGrpc.KVServiceImplBase {

    private final KVRepository repository;

    private static final Logger log = LoggerFactory.getLogger(KVGrpcService.class);

    private static final int BATCH_SIZE = 10_000;


    public KVGrpcService(KVRepository repository) {
        this.repository = repository;
    }


    /**
     * Stores a value by key.
     *
     * @param request gRPC request containing key and value
     * @param responseObserver response stream
     */
    @Override
    public void put(PutRequest request, StreamObserver<PutResponse> responseObserver) {
        String key = request.getKey();

        try {
            log.debug("Put request: key={}", key);
            validateKey(key);

            var value = request.getValue().toByteArray();
            repository.put(key, value);

            responseObserver.onNext(
                    PutResponse.newBuilder().build()
            );
            responseObserver.onCompleted();

            log.debug("Put completed: key={}, valuePresent={}", key, value != null);

        } catch (IllegalArgumentException e) {
            log.warn("Invalid put request: key={}, error={}", key, e.getMessage());
            responseObserver.onError(
                    Status.INVALID_ARGUMENT
                            .withDescription(e.getMessage())
                            .withCause(e)
                            .asRuntimeException()
            );
        } catch (Exception e) {
            log.error("Put failed: key={}", key, e);
            responseObserver.onError(
                    Status.INTERNAL
                            .withDescription("Internal server error")
                            .withCause(e)
                            .asRuntimeException()
            );
        }
    }


    /**
     * Retrieves value by key.
     *
     * @param request gRPC request with key
     * @param responseObserver response stream
     */
    @Override
    public void get(GetRequest request, StreamObserver<GetResponse> responseObserver) {
        String key = request.getKey();

        try {
            log.debug("Get request: key={}", key);

            validateKey(key);

            var entry = repository.get(key);

            if (entry == null) {
                log.debug("Get completed: key={} not found", key);

                responseObserver.onNext(
                        GetResponse.newBuilder().build()
                );

                responseObserver.onCompleted();
                return;
            }

            GetResponse.Builder builder = GetResponse.newBuilder();

            if (entry.value() != null) {
                builder.setValue(
                        ByteString.copyFrom(entry.value())
                );
            }

            responseObserver.onNext(builder.build());
            responseObserver.onCompleted();

            log.debug("Get completed: key={}, valuePresent={}", key, entry.value() != null);

        } catch (IllegalArgumentException e) {
            log.warn("Invalid get request: key={}, error={}", key, e.getMessage());
            responseObserver.onError(
                    Status.INVALID_ARGUMENT
                            .withDescription(e.getMessage())
                            .withCause(e)
                            .asRuntimeException()
            );

        } catch (Exception e) {
            log.error("Get failed: key={}", key, e);
            responseObserver.onError(
                    Status.INTERNAL
                            .withDescription("Internal server error")
                            .withCause(e)
                            .asRuntimeException()
            );
        }
    }


    /**
     * Deletes entry by key.
     *
     * @param request gRPC request with key
     * @param responseObserver response stream
     */
    @Override
    public void delete(DeleteRequest request, StreamObserver<DeleteResponse> responseObserver) {
        String key = request.getKey();

        try {
            log.debug("Delete request: key={}", key);
            validateKey(key);

            repository.delete(key);

            responseObserver.onNext(
                    DeleteResponse.newBuilder().build()
            );
            responseObserver.onCompleted();

            log.debug("Delete completed: key={}", key);

        } catch (IllegalArgumentException e) {
            log.warn("Invalid delete request: key={}, error={}", key, e.getMessage());
            responseObserver.onError(
                    Status.INVALID_ARGUMENT
                            .withDescription(e.getMessage())
                            .withCause(e)
                            .asRuntimeException()
            );

        } catch (Exception e) {
            log.error("Delete failed: key={}", key, e);
            responseObserver.onError(
                    Status.INTERNAL
                            .withDescription("Internal server error")
                            .withCause(e)
                            .asRuntimeException()
            );
        }
    }


    /**
     * Streams key-value pairs within a key range.
     *
     * @param request range request (from, to)
     * @param responseObserver streaming response
     */
    @Override
    public void range(RangeRequest request, StreamObserver<KeyValue> responseObserver) {
        String currentKey = request.getKeySince();
        String keyTo = request.getKeyTo();

        try {
            log.debug("Range request: from={} to={}", currentKey, keyTo);

            validateRangeKeys(currentKey, keyTo);

            if (currentKey.compareTo(keyTo) > 0) {
                throw new IllegalArgumentException("key_since cannot be greater than key_to");
            }

            while (true) {
                var batch = repository.rangeBatch(currentKey, keyTo, BATCH_SIZE);

                if (batch == null || batch.isEmpty()) {
                    break;
                }

                for (KVEntry entry : batch) {
                    KeyValue.Builder builder = KeyValue.newBuilder()
                            .setKey(entry.key());

                    if (entry.value() != null) {
                        builder.setValue(ByteString.copyFrom(entry.value()));
                    }

                    responseObserver.onNext(builder.build());
                }

                currentKey = batch.getLast().key() + "\0";
            }
            responseObserver.onCompleted();

            log.debug("Range completed: from={} to={}", currentKey, keyTo);

        } catch (IllegalArgumentException e) {
            log.warn("Invalid range request: from={}, to={}, error={}", currentKey, keyTo, e.getMessage());
            responseObserver.onError(
                    Status.INVALID_ARGUMENT
                            .withDescription(e.getMessage())
                            .withCause(e)
                            .asRuntimeException()
            );

        } catch (Exception e) {
            log.error("Range failed: from={} to={}", currentKey, keyTo, e);
            responseObserver.onError(
                    Status.INTERNAL
                            .withDescription("Internal server error")
                            .withCause(e)
                            .asRuntimeException()
            );
        }
    }


    /**
     * Returns total number of stored entries.
     *
     * @param request count request
     * @param responseObserver response stream
     */
    @Override
    public void count(CountRequest request, StreamObserver<CountResponse> responseObserver) {
        try {
            log.debug("Count request");

            long count = repository.count();

            responseObserver.onNext(
                    CountResponse.newBuilder()
                            .setCount(count)
                            .build()
            );
            responseObserver.onCompleted();

            log.debug("Count completed: count={}", count);

        } catch (Exception e) {
            log.error("Count failed", e);
            responseObserver.onError(
                    Status.INTERNAL
                            .withDescription("Internal server error")
                            .withCause(e)
                            .asRuntimeException()
            );
        }
    }

    private void validateKey(String key) {
        if (key == null) {
            throw new IllegalArgumentException("Key cannot be null");
        }
    }

    private void validateRangeKeys(String keySince, String keyTo) {
        if (keySince == null) {
            throw new IllegalArgumentException("key_since cannot be null");
        }
        if (keyTo == null || keyTo.isEmpty()) {
            throw new IllegalArgumentException("key_to cannot be null or empty");
        }
        if (keySince.compareTo(keyTo) > 0) {
            throw new IllegalArgumentException("key_since cannot be greater than key_to");
        }
    }
}