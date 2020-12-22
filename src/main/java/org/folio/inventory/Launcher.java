package org.folio.inventory;

import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import org.folio.DataImportEventTypes;
import org.folio.inventory.common.VertxAssistant;

import java.lang.invoke.MethodHandles;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.folio.inventory.dataimport.util.KafkaConfigConstants.KAFKA_ENV;
import static org.folio.inventory.dataimport.util.KafkaConfigConstants.KAFKA_HOST;
import static org.folio.inventory.dataimport.util.KafkaConfigConstants.KAFKA_PORT;
import static org.folio.inventory.dataimport.util.KafkaConfigConstants.KAFKA_REPLICATION_FACTOR;
import static org.folio.inventory.dataimport.util.KafkaConfigConstants.OKAPI_URL;

public class Launcher {
  private static final VertxAssistant vertxAssistant = new VertxAssistant();
  private static String inventoryModuleDeploymentId;
  private static String consumerVerticleDeploymentId;
  private static String marcInstHridSetConsumerVerticleDeploymentId;

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

    Map<String, Object> consumerVerticlesConfig = getConsumerVerticleConfig();
    start(config);
    startConsumerVerticles(consumerVerticlesConfig);
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

  private static void startConsumerVerticles(Map<String, Object> consumerVerticlesConfig)
    throws InterruptedException, ExecutionException, TimeoutException {

    CompletableFuture<String> future1 = new CompletableFuture<>();
    CompletableFuture<String> future2 = new CompletableFuture<>();
    vertxAssistant.deployVerticle(DataImportConsumerVerticle.class.getName(),
      consumerVerticlesConfig, future1);
    vertxAssistant.deployVerticle(MarcBibInstanceHridSetConsumerVerticle.class.getName(),
      consumerVerticlesConfig, future2);

    consumerVerticleDeploymentId = future1.get(20, TimeUnit.SECONDS);
    marcInstHridSetConsumerVerticleDeploymentId = future2.get(20, TimeUnit.SECONDS);
  }

  private static void stop() {
    final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    CompletableFuture<Void> stopped = new CompletableFuture<>();

    log.info("Server Stopping");

    vertxAssistant.undeployVerticle(inventoryModuleDeploymentId)
      .thenCompose(v -> vertxAssistant.undeployVerticle(consumerVerticleDeploymentId))
      .thenCompose(v -> vertxAssistant.undeployVerticle(marcInstHridSetConsumerVerticleDeploymentId))
      .thenAccept(v -> vertxAssistant.stop(stopped));

    stopped.thenAccept(v -> log.info("Server Stopped"));
  }

  private static void putNonNullConfig(
    String key,
    Object value,
    Map<String, Object> config) {

    if(value != null) {
      config.put(key, value);
    }
  }

  private static Map<String, Object> getConsumerVerticleConfig() {
    Map<String, Object> configMap = new HashMap<>();
    configMap.put(KAFKA_HOST, System.getProperty(KAFKA_HOST, "kafka"));
    configMap.put(KAFKA_PORT, System.getProperty(KAFKA_PORT, "9092"));
    configMap.put(OKAPI_URL, System.getProperty(OKAPI_URL, "http://okapi:9130"));
    configMap.put(KAFKA_REPLICATION_FACTOR, System.getProperty(KAFKA_REPLICATION_FACTOR, "1"));
    configMap.put(KAFKA_ENV, System.getProperty(KAFKA_ENV, "folio"));

    String storageType = System.getProperty("org.folio.metadata.inventory.storage.type");
    String storageLocation = System.getProperty("org.folio.metadata.inventory.storage.location");
    putNonNullConfig("storage.type", storageType, configMap);
    putNonNullConfig("storage.location", storageLocation, configMap);
    return configMap;
  }
}
