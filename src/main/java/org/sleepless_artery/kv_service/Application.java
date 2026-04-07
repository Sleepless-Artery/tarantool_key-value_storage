package org.sleepless_artery.kv_service;

import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.protobuf.services.ProtoReflectionService;
import org.sleepless_artery.kv_service.config.TarantoolClientConfig;
import org.sleepless_artery.kv_service.repository.KVRepository;
import org.sleepless_artery.kv_service.service.KVGrpcService;

import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;


public class Application {

    private static final String HOST = "localhost";
    private static final int TARANTOOL_PORT = 3301;
    private static final int GRPC_PORT = 9090;

    private static final Logger log = Logger.getLogger(Application.class.getName());


    public static void main(String[] args) throws Exception {
        log.info(() -> String.format(
                "Starting application: tarantoolHost=%s, tarantoolPort=%d, grpcPort=%d",
                HOST, TARANTOOL_PORT, GRPC_PORT
        ));

        TarantoolClientConfig config = new TarantoolClientConfig(HOST, TARANTOOL_PORT);
        KVRepository repository = new KVRepository(config.client());
        KVGrpcService service = new KVGrpcService(repository);

        Server server = ServerBuilder
                .forPort(GRPC_PORT)
                .addService(service)
                .addService(ProtoReflectionService.newInstance())
                .build();

        try {
            server.start();
            log.info(() -> "gRPC server started, listening on port " + GRPC_PORT);

            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                log.info("Shutdown signal received, stopping gRPC server...");
                server.shutdown();
                log.info("gRPC server stopped");
            }));

            log.info("Application started successfully");
            server.awaitTermination();

        } catch (IOException e) {
            log.log(Level.SEVERE, "Failed to start gRPC server", e);
            throw e;

        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.log(Level.WARNING, "Application interrupted", e);
            throw e;

        } catch (Exception e) {
            log.log(Level.SEVERE, "Application terminated with unexpected error", e);
            throw e;
        }
    }
}