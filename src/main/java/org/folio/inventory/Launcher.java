package org.folio.inventory;

import io.vertx.core.Context;
import io.vertx.core.Vertx;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.inventory.common.VertxAssistant;
import org.folio.inventory.consortium.cache.ConsortiumDataCache;

import java.lang.invoke.MethodHandles;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.folio.inventory.dataimport.util.KafkaConfigConstants.KAFKA_ENV;
import static org.folio.inventory.dataimport.util.KafkaConfigConstants.KAFKA_HOST;
import static org.folio.inventory.dataimport.util.KafkaConfigConstants.KAFKA_MAX_REQUEST_SIZE;
import static org.folio.inventory.dataimport.util.KafkaConfigConstants.KAFKA_PORT;
import static org.folio.inventory.dataimport.util.KafkaConfigConstants.KAFKA_REPLICATION_FACTOR;
import static org.folio.inventory.dataimport.util.KafkaConfigConstants.OKAPI_URL;

public class Launcher {
  private static final String DATA_IMPORT_CONSUMER_VERTICLE_INSTANCES_NUMBER_CONFIG = "inventory.kafka.DataImportConsumerVerticle.instancesNumber";
  private static final String MARC_BIB_INSTANCE_HRID_SET_CONSUMER_VERTICLE_INSTANCES_NUMBER_CONFIG = "inventory.kafka.MarcBibInstanceHridSetConsumerVerticle.instancesNumber";
  private static final String QUICK_MARC_CONSUMER_VERTICLE_INSTANCES_NUMBER_CONFIG = "inventory.kafka.QuickMarcConsumerVerticle.instancesNumber";
  private static final String MARC_BIB_UPDATE_CONSUMER_VERTICLE_INSTANCES_NUMBER_CONFIG = "inventory.kafka.MarcBibUpdateConsumerVerticle.instancesNumber";
  private static final String CONSORTIUM_INSTANCE_SHARING_CONSUMER_VERTICLE_NUMBER_CONFIG = "inventory.kafka.ConsortiumInstanceSharingConsumerVerticle.instancesNumber";
  private static final VertxAssistant vertxAssistant = new VertxAssistant();

  private static String inventoryModuleDeploymentId;
  private static String consumerVerticleDeploymentId;
  private static String marcInstHridSetConsumerVerticleDeploymentId;
  private static String quickMarcConsumerVerticleDeploymentId;
  private static String marcBibUpdateConsumerVerticleDeploymentId;
  private static String consortiumInstanceSharingVerticleDeploymentId;

  public static void main(String[] args)
    throws InterruptedException, ExecutionException, TimeoutException {

    Logging.initialiseFormat();

    Runtime.getRuntime().addShutdownHook(new Thread(Launcher::stop));

    Map<String, Object> config = new HashMap<>();

    String portString = System.getProperty("http.port", System.getProperty("port", "9403"));
    Integer port = Integer.valueOf(portString);

    String storageType = System.getProperty(
      "org.folio.metadata.inventory.storage.type", null);

    String kafkaConsumersToBeInitialized = System.getProperty(
      "org.folio.metadata.inventory.kafka.consumers.initialized", "true");

    String storageLocation = System.getProperty(
      "org.folio.metadata.inventory.storage.location", null);

    putNonNullConfig("storage.type", storageType, config);
    putNonNullConfig("storage.location", storageLocation, config);
    putNonNullConfig("port", port, config);

    start(config);

    if(Boolean.parseBoolean(kafkaConsumersToBeInitialized)){
      Map<String, Object> consumerVerticlesConfig = getConsumerVerticleConfig();
      startConsumerVerticles(consumerVerticlesConfig);
    } else {
      final Logger log = LogManager.getLogger(MethodHandles.lookup().lookupClass());
      log.warn("\n*******\n*  WARNING: The module is running in Traffics Diversion mode (there is no Consumers to accept DI Kafka messages)\n*******");
    }
  }

  private static void start(Map<String, Object> config)
    throws InterruptedException, ExecutionException, TimeoutException {

    final Logger log = LogManager.getLogger(MethodHandles.lookup().lookupClass());

    vertxAssistant.start();

    log.info("Server Starting");

    initConsortiumDataCache(vertxAssistant.getVertx(), vertxAssistant.getVertx().getOrCreateContext());

    CompletableFuture<String> deployed = new CompletableFuture<>();

    vertxAssistant.deployVerticle(new InventoryVerticle(getConsortiumDataCache(vertxAssistant.getVertx().getOrCreateContext())),
      config, deployed);

    deployed.thenAccept(v -> log.info("Server Started"));

    inventoryModuleDeploymentId = deployed.get(20, TimeUnit.SECONDS);
  }

  private static void startConsumerVerticles(Map<String, Object> consumerVerticlesConfig)
    throws InterruptedException, ExecutionException, TimeoutException {
    int dataImportConsumerVerticleNumber = Integer.parseInt(System.getenv().getOrDefault(DATA_IMPORT_CONSUMER_VERTICLE_INSTANCES_NUMBER_CONFIG, "3"));
    int instanceHridSetConsumerVerticleNumber = Integer.parseInt(System.getenv().getOrDefault(MARC_BIB_INSTANCE_HRID_SET_CONSUMER_VERTICLE_INSTANCES_NUMBER_CONFIG, "3"));
    int quickMarcConsumerVerticleNumber = Integer.parseInt(System.getenv().getOrDefault(QUICK_MARC_CONSUMER_VERTICLE_INSTANCES_NUMBER_CONFIG, "1"));
    int marcBibUpdateConsumerVerticleNumber = Integer.parseInt(System.getenv().getOrDefault(MARC_BIB_UPDATE_CONSUMER_VERTICLE_INSTANCES_NUMBER_CONFIG, "3"));
    int consortiumInstanceSharingVerticleNumber = Integer.parseInt(System.getenv().getOrDefault(CONSORTIUM_INSTANCE_SHARING_CONSUMER_VERTICLE_NUMBER_CONFIG, "3"));

    CompletableFuture<String> future1 = new CompletableFuture<>();
    CompletableFuture<String> future2 = new CompletableFuture<>();
    CompletableFuture<String> future3 = new CompletableFuture<>();
    CompletableFuture<String> future4 = new CompletableFuture<>();
    CompletableFuture<String> future5 = new CompletableFuture<>();
    vertxAssistant.deployVerticle(new DataImportConsumerVerticle(getConsortiumDataCache(vertxAssistant.getVertx().getOrCreateContext())),
      consumerVerticlesConfig, dataImportConsumerVerticleNumber, future1);
    vertxAssistant.deployVerticle(MarcHridSetConsumerVerticle.class.getName(),
      consumerVerticlesConfig, instanceHridSetConsumerVerticleNumber, future2);
    vertxAssistant.deployVerticle(QuickMarcConsumerVerticle.class.getName(),
      consumerVerticlesConfig, quickMarcConsumerVerticleNumber, future3);
    vertxAssistant.deployVerticle(MarcBibUpdateConsumerVerticle.class.getName(),
      consumerVerticlesConfig, marcBibUpdateConsumerVerticleNumber, future4);
    vertxAssistant.deployVerticle(ConsortiumInstanceSharingConsumerVerticle.class.getName(),
      consumerVerticlesConfig, consortiumInstanceSharingVerticleNumber, future5);

    consumerVerticleDeploymentId = future1.get(20, TimeUnit.SECONDS);
    marcInstHridSetConsumerVerticleDeploymentId = future2.get(20, TimeUnit.SECONDS);
    quickMarcConsumerVerticleDeploymentId = future3.get(20, TimeUnit.SECONDS);
    marcBibUpdateConsumerVerticleDeploymentId = future4.get(20, TimeUnit.SECONDS);
    consortiumInstanceSharingVerticleDeploymentId = future5.get(20, TimeUnit.SECONDS);
  }

  private static void stop() {
    final Logger log = LogManager.getLogger(MethodHandles.lookup().lookupClass());

    CompletableFuture<Void> stopped = new CompletableFuture<>();

    log.info("Server Stopping");

    vertxAssistant.undeployVerticle(inventoryModuleDeploymentId)
      .thenCompose(v -> vertxAssistant.undeployVerticle(consumerVerticleDeploymentId))
      .thenCompose(v -> vertxAssistant.undeployVerticle(marcInstHridSetConsumerVerticleDeploymentId))
      .thenCompose(v -> vertxAssistant.undeployVerticle(quickMarcConsumerVerticleDeploymentId))
      .thenCompose(v -> vertxAssistant.undeployVerticle(marcBibUpdateConsumerVerticleDeploymentId))
      .thenCompose(v -> vertxAssistant.undeployVerticle(consortiumInstanceSharingVerticleDeploymentId))
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

  public static void initConsortiumDataCache(Vertx vertx, Context context) {
    ConsortiumDataCache consortiumDataCache = new ConsortiumDataCache(vertx, vertx.createHttpClient());
    context.put(ConsortiumDataCache.class.getName(), consortiumDataCache);
  }

  public static ConsortiumDataCache getConsortiumDataCache(Context context) {
    return context.get(ConsortiumDataCache.class.getName());
  }
}
