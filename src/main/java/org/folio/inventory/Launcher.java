package org.folio.inventory;

import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import org.folio.inventory.common.VertxAssistant;

import java.lang.invoke.MethodHandles;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class Launcher {
  private static final VertxAssistant vertxAssistant = new VertxAssistant();
  private static String inventoryModuleDeploymentId;

  public static void main(String[] args)
    throws InterruptedException, ExecutionException, TimeoutException {

    Logging.initialiseFormat();

    Runtime.getRuntime().addShutdownHook(new Thread(Launcher::stop));

    Map<String, Object> config = new HashMap<>();

    String portString = System.getProperty("http.port", System.getProperty("port", "9403"));
    Integer port = Integer.valueOf(portString);

    String storageType = System.getProperty(
      "org.folio.metadata.inventory.storage.type", null);

    String storageLocation = System.getProperty(
      "org.folio.metadata.inventory.storage.location", null);

    putNonNullConfig("storage.type", storageType, config);
    putNonNullConfig("storage.location", storageLocation, config);
    putNonNullConfig("port", port, config);

    start(config);
  }

  private static void start(Map<String, Object> config)
    throws InterruptedException, ExecutionException, TimeoutException {

    final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    vertxAssistant.start();

    log.info("Server Starting");

    CompletableFuture<String> deployed = new CompletableFuture<>();

    vertxAssistant.deployVerticle(InventoryVerticle.class.getName(),
      config, deployed);

    deployed.thenAccept(v -> log.info("Server Started"));

    inventoryModuleDeploymentId = deployed.get(20, TimeUnit.SECONDS);
  }

  private static void stop() {
    final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    CompletableFuture<Void> undeployed = new CompletableFuture<>();
    CompletableFuture<Void> stopped = new CompletableFuture<>();
    CompletableFuture<Void> all = CompletableFuture.allOf(undeployed, stopped);

    log.info("Server Stopping");

    vertxAssistant.undeployVerticle(inventoryModuleDeploymentId, undeployed);

    undeployed.thenAccept(v -> vertxAssistant.stop(stopped));

    all.thenAccept(v -> log.info("Server Stopped"));
  }

  private static void putNonNullConfig(
    String key,
    Object value,
    Map<String, Object> config) {

    if(value != null) {
      config.put(key, value);
    }
  }
}
