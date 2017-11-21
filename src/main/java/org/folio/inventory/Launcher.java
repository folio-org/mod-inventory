package org.folio.inventory;

import org.folio.inventory.common.VertxAssistant;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class Launcher {
  private static VertxAssistant vertxAssistant = new VertxAssistant();
  private static String inventoryModuleDeploymentId;

  public static void main(String[] args)
    throws InterruptedException, ExecutionException, TimeoutException {

    Runtime.getRuntime().addShutdownHook(new Thread(() -> Launcher.stop()));

    Map<String, Object> config = new HashMap<>();

    Integer port = Integer.getInteger("port", 9403);

    String storageType = System.getProperty(
      "org.folio.metadata.inventory.storage.type", null);

    String storageLocation = System.getProperty(
      "org.folio.metadata.inventory.storage.location", null);

    putNonNullConfig("storage.type", storageType, config);
    putNonNullConfig("storage.location", storageLocation, config);
    putNonNullConfig("port", port, config);

    start(config);
  }

  private static void start(Map config)
    throws InterruptedException, ExecutionException, TimeoutException {

    vertxAssistant.start();

    System.out.println("Server Starting");

    CompletableFuture<String> deployed = new CompletableFuture();

    vertxAssistant.deployVerticle(InventoryVerticle.class.getName(),
      config, deployed);

    deployed.thenAccept(v -> System.out.println("Server Started"));

    inventoryModuleDeploymentId = deployed.get(20, TimeUnit.SECONDS);
  }

  private static void stop() {
    CompletableFuture undeployed = new CompletableFuture();
    CompletableFuture stopped = new CompletableFuture();
    CompletableFuture all = CompletableFuture.allOf(undeployed, stopped);

    System.out.println("Server Stopping");

    vertxAssistant.undeployVerticle(inventoryModuleDeploymentId, undeployed);

    undeployed.thenAccept(v -> {
      vertxAssistant.stop(stopped);
    });

    all.thenAccept(v -> System.out.println("Server Stopped"));
  }

  private static void putNonNullConfig(String key, Object value, Map config) {
    if(value != null) {
      config.put(key, value);
    }
  }
}
