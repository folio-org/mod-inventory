package org.folio.inventory;

import io.vertx.core.DeploymentOptions;
import io.vertx.core.json.JsonObject;
import org.folio.inventory.common.VertxAssistant;
import org.folio.kafka.KafkaConfig;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import static org.folio.inventory.KafkaUtility.KAFKA_ENV_VALUE;
import static org.folio.inventory.KafkaUtility.MAX_REQUEST_SIZE;
import static org.folio.inventory.KafkaUtility.getKafkaHostAndPort;
import static org.folio.inventory.KafkaUtility.startKafka;
import static org.folio.inventory.KafkaUtility.stopKafka;
import static org.folio.inventory.dataimport.util.KafkaConfigConstants.KAFKA_ENV;
import static org.folio.inventory.dataimport.util.KafkaConfigConstants.KAFKA_HOST;
import static org.folio.inventory.dataimport.util.KafkaConfigConstants.KAFKA_MAX_REQUEST_SIZE;
import static org.folio.inventory.dataimport.util.KafkaConfigConstants.KAFKA_PORT;
import static org.folio.inventory.dataimport.util.KafkaConfigConstants.KAFKA_REPLICATION_FACTOR;

public abstract class KafkaTest {
  protected static VertxAssistant vertxAssistant = new VertxAssistant();
  protected static KafkaConfig kafkaConfig;
  protected static DeploymentOptions deploymentOptions;

  @BeforeClass
  public static void beforeAll() {
    startKafka();
    vertxAssistant.start();

    kafkaConfig = KafkaConfig.builder()
      .envId(KAFKA_ENV_VALUE)
      .kafkaHost(getKafkaHostAndPort()[0])
      .kafkaPort(getKafkaHostAndPort()[1])
      .maxRequestSize(MAX_REQUEST_SIZE)
      .build();

    deploymentOptions = new DeploymentOptions()
      .setConfig(new JsonObject()
        .put(KAFKA_HOST, getKafkaHostAndPort()[0])
        .put(KAFKA_PORT, getKafkaHostAndPort()[1])
        .put(KAFKA_REPLICATION_FACTOR, "1")
        .put(KAFKA_ENV, KAFKA_ENV_VALUE)
        .put(KAFKA_MAX_REQUEST_SIZE, MAX_REQUEST_SIZE));
  }

  @AfterClass
  public static void afterAll() {
    stopKafka();
    vertxAssistant.stop();
  }
}
