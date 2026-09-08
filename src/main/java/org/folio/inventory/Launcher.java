package org.folio.inventory;

import static org.folio.inventory.dataimport.util.KafkaConfigConstants.KAFKA_ENV;
import static org.folio.inventory.dataimport.util.KafkaConfigConstants.KAFKA_HOST;
import static org.folio.inventory.dataimport.util.KafkaConfigConstants.KAFKA_MAX_REQUEST_SIZE;
import static org.folio.inventory.dataimport.util.KafkaConfigConstants.KAFKA_PORT;
import static org.folio.inventory.dataimport.util.KafkaConfigConstants.KAFKA_REPLICATION_FACTOR;
import static org.folio.inventory.dataimport.util.KafkaConfigConstants.OKAPI_URL;

import java.lang.invoke.MethodHandles;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.inventory.common.VertxAssistant;
import org.folio.inventory.verticle.CancelledJobExecutionConsumerVerticle;
import org.folio.inventory.verticle.ConsortiumInstanceSharingConsumerVerticle;
import org.folio.inventory.verticle.DataImportConsumerVerticle;
import org.folio.inventory.verticle.InstanceIngressConsumerVerticle;
import org.folio.inventory.verticle.InventoryVerticle;
import org.folio.inventory.verticle.MarcBibUpdateConsumerVerticle;
import org.folio.inventory.verticle.MarcHridSetConsumerVerticle;

public class Launcher {

  private static final String DATA_IMPORT_CONSUMER_VERTICLE_INSTANCES_NUMBER_CONFIG =
    "inventory.kafka.DataImportConsumerVerticle.instancesNumber";
  private static final String MARC_BIB_INSTANCE_HRID_SET_CONSUMER_VERTICLE_INSTANCES_NUMBER_CONFIG =
    "inventory.kafka.MarcBibInstanceHridSetConsumerVerticle.instancesNumber";
  private static final String MARC_BIB_UPDATE_CONSUMER_VERTICLE_INSTANCES_NUMBER_CONFIG =
    "inventory.kafka.MarcBibUpdateConsumerVerticle.instancesNumber";
  private static final String CONSORTIUM_INSTANCE_SHARING_CONSUMER_VERTICLE_NUMBER_CONFIG =
    "inventory.kafka.ConsortiumInstanceSharingConsumerVerticle.instancesNumber";
  private static final String INSTANCE_INGRESS_VERTICLE_NUMBER_CONFIG =
    "inventory.kafka.InstanceIngressConsumerVerticle.instancesNumber";
  private static final int CANCELLED_JOBS_CONSUMER_VERTICLE_INSTANCES_NUMBER = 1;
  private static final VertxAssistant VERTX_ASSISTANT = new VertxAssistant();

  private static String inventoryModuleDeploymentId;
  private static String consumerVerticleDeploymentId;
  private static String marcInstHridSetConsumerVerticleDeploymentId;
  private static String marcBibUpdateConsumerVerticleDeploymentId;
  private static String consortiumInstanceSharingVerticleDeploymentId;
  private static String instanceIngressConsumerVerticleDeploymentId;
  private static String cancelledJobsConsumerVerticleDeploymentId;

  @SuppressWarnings("checkstyle:UncommentedMain")
  public static void main(String[] args) throws InterruptedException, ExecutionException, TimeoutException {
    Logging.initialiseFormat();

    Runtime.getRuntime().addShutdownHook(new Thread(Launcher::stop));

    start(prepareConfig());

    if (isKafkaInitializationEnabled()) {
      Map<String, Object> consumerConfig = getConsumerVerticleConfig();
      startConsumerVerticles(consumerConfig);
    } else {
      final Logger log = LogManager.getLogger(MethodHandles.lookup().lookupClass());
      log.warn("""
        *******
        WARNING: The module is running in Traffics Diversion mode (there is no Consumers to accept Kafka messages)
        *******
        """);
    }
  }

  private static void start(Map<String, Object> config) {
    final Logger log = LogManager.getLogger(MethodHandles.lookup().lookupClass());
    VERTX_ASSISTANT.start();
    log.info("Server Starting");

    CompletableFuture<String> deployed = new CompletableFuture<>();
    VERTX_ASSISTANT.deployVerticle(InventoryVerticle.class.getName(), config, deployed);
    deployed.thenAccept(v -> log.info("Server Started"));

    try {
      inventoryModuleDeploymentId = deployed.get(20, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      log.error("Failed to start server", e);
      Thread.currentThread().interrupt();
    } catch (ExecutionException | TimeoutException e) {
      log.error("Failed to start server", e);
    }
  }

  private static boolean isKafkaInitializationEnabled() {
    var kafkaInitProperty = System.getProperty("org.folio.metadata.inventory.kafka.consumers.initialized", "true");
    return Boolean.parseBoolean(kafkaInitProperty);
  }

  private static Map<String, Object> prepareConfig() {
    Map<String, Object> config = new HashMap<>();
    String portString = System.getProperty("http.port", System.getProperty("port", "9403"));
    Integer port = Integer.valueOf(portString);
    putNonNullConfig("port", port, config);

    String storageType = System.getProperty("org.folio.metadata.inventory.storage.type", null);
    putNonNullConfig("storage.type", storageType, config);

    String storageLocation = System.getProperty("org.folio.metadata.inventory.storage.location", null);
    putNonNullConfig("storage.location", storageLocation, config);
    return config;
  }

  private static void startConsumerVerticles(Map<String, Object> consumerConfig)
    throws InterruptedException, ExecutionException, TimeoutException {
    int dataImportConsumerVerticleNumber =
      Integer.parseInt(System.getenv().getOrDefault(DATA_IMPORT_CONSUMER_VERTICLE_INSTANCES_NUMBER_CONFIG, "3"));
    int instanceHridSetConsumerVerticleNumber = Integer.parseInt(
      System.getenv().getOrDefault(MARC_BIB_INSTANCE_HRID_SET_CONSUMER_VERTICLE_INSTANCES_NUMBER_CONFIG, "3"));
    int marcBibUpdateConsumerVerticleNumber =
      Integer.parseInt(System.getenv().getOrDefault(MARC_BIB_UPDATE_CONSUMER_VERTICLE_INSTANCES_NUMBER_CONFIG, "3"));
    int consortiumInstanceSharingVerticleNumber =
      Integer.parseInt(System.getenv().getOrDefault(CONSORTIUM_INSTANCE_SHARING_CONSUMER_VERTICLE_NUMBER_CONFIG, "3"));
    int instanceIngressConsumerVerticleNumber =
      Integer.parseInt(System.getenv().getOrDefault(INSTANCE_INGRESS_VERTICLE_NUMBER_CONFIG, "3"));

    CompletableFuture<String> future1 = new CompletableFuture<>();
    CompletableFuture<String> future2 = new CompletableFuture<>();
    CompletableFuture<String> future4 = new CompletableFuture<>();
    CompletableFuture<String> future5 = new CompletableFuture<>();
    CompletableFuture<String> future6 = new CompletableFuture<>();
    CompletableFuture<String> future7 = new CompletableFuture<>();

    VERTX_ASSISTANT.deployVerticle(DataImportConsumerVerticle.class.getName(),
      consumerConfig, dataImportConsumerVerticleNumber, future1);
    VERTX_ASSISTANT.deployVerticle(MarcHridSetConsumerVerticle.class.getName(),
      consumerConfig, instanceHridSetConsumerVerticleNumber, future2);
    VERTX_ASSISTANT.deployVerticle(MarcBibUpdateConsumerVerticle.class.getName(),
      consumerConfig, marcBibUpdateConsumerVerticleNumber, future4);
    VERTX_ASSISTANT.deployVerticle(ConsortiumInstanceSharingConsumerVerticle.class.getName(),
      consumerConfig, consortiumInstanceSharingVerticleNumber, future5);
    VERTX_ASSISTANT.deployVerticle(InstanceIngressConsumerVerticle.class.getName(),
      consumerConfig, instanceIngressConsumerVerticleNumber, future6);
    VERTX_ASSISTANT.deployVerticle(CancelledJobExecutionConsumerVerticle.class.getName(),
      consumerConfig, CANCELLED_JOBS_CONSUMER_VERTICLE_INSTANCES_NUMBER, future7);

    consumerVerticleDeploymentId = future1.get(20, TimeUnit.SECONDS);
    marcInstHridSetConsumerVerticleDeploymentId = future2.get(20, TimeUnit.SECONDS);
    marcBibUpdateConsumerVerticleDeploymentId = future4.get(20, TimeUnit.SECONDS);
    consortiumInstanceSharingVerticleDeploymentId = future5.get(20, TimeUnit.SECONDS);
    instanceIngressConsumerVerticleDeploymentId = future6.get(20, TimeUnit.SECONDS);
    cancelledJobsConsumerVerticleDeploymentId = future7.get(20, TimeUnit.SECONDS);
  }

  private static void stop() {
    final Logger log = LogManager.getLogger(MethodHandles.lookup().lookupClass());

    CompletableFuture<Void> stopped = new CompletableFuture<>();
    log.info("Server Stopping");

    VERTX_ASSISTANT.undeployVerticle(inventoryModuleDeploymentId)
      .thenCompose(v -> VERTX_ASSISTANT.undeployVerticle(consumerVerticleDeploymentId))
      .thenCompose(v -> VERTX_ASSISTANT.undeployVerticle(marcInstHridSetConsumerVerticleDeploymentId))
      .thenCompose(v -> VERTX_ASSISTANT.undeployVerticle(marcBibUpdateConsumerVerticleDeploymentId))
      .thenCompose(v -> VERTX_ASSISTANT.undeployVerticle(consortiumInstanceSharingVerticleDeploymentId))
      .thenCompose(v -> VERTX_ASSISTANT.undeployVerticle(instanceIngressConsumerVerticleDeploymentId))
      .thenCompose(v -> VERTX_ASSISTANT.undeployVerticle(cancelledJobsConsumerVerticleDeploymentId))
      .thenAccept(v -> VERTX_ASSISTANT.stop(stopped));

    stopped.thenAccept(v -> log.info("Server Stopped"));
  }

  private static Map<String, Object> getConsumerVerticleConfig() {
    Map<String, Object> configMap = new HashMap<>();
    configMap.put(KAFKA_HOST, System.getenv().getOrDefault(KAFKA_HOST, "kafka"));
    configMap.put(KAFKA_PORT, System.getenv().getOrDefault(KAFKA_PORT, "9092"));
    configMap.put(OKAPI_URL, System.getenv().getOrDefault(OKAPI_URL, "http://okapi:9130"));
    configMap.put(KAFKA_REPLICATION_FACTOR, System.getenv().getOrDefault(KAFKA_REPLICATION_FACTOR, "1"));
    configMap.put(KAFKA_ENV, System.getenv().getOrDefault(KAFKA_ENV, "folio"));
    configMap.put(KAFKA_MAX_REQUEST_SIZE, System.getenv().getOrDefault(KAFKA_MAX_REQUEST_SIZE, "4000000"));

    String storageType = System.getProperty("org.folio.metadata.inventory.storage.type");
    String storageLocation = System.getProperty("org.folio.metadata.inventory.storage.location");
    putNonNullConfig("storage.type", storageType, configMap);
    putNonNullConfig("storage.location", storageLocation, configMap);
    return configMap;
  }

  private static void putNonNullConfig(String key, Object value, Map<String, Object> config) {
    if (value != null) {
      config.put(key, value);
    }
  }
}
