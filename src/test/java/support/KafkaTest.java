package support;

import static org.folio.inventory.dataimport.util.KafkaConfigConstants.KAFKA_ENV;
import static org.folio.inventory.dataimport.util.KafkaConfigConstants.KAFKA_HOST;
import static org.folio.inventory.dataimport.util.KafkaConfigConstants.KAFKA_MAX_REQUEST_SIZE;
import static org.folio.inventory.dataimport.util.KafkaConfigConstants.KAFKA_PORT;
import static org.folio.inventory.dataimport.util.KafkaConfigConstants.KAFKA_REPLICATION_FACTOR;
import static support.KafkaUtility.KAFKA_ENV_VALUE;
import static support.KafkaUtility.MAX_REQUEST_SIZE;

import io.vertx.core.DeploymentOptions;
import io.vertx.core.json.JsonObject;
import org.folio.dataimport.testsupport.kafka.KafkaExtension;
import org.folio.dataimport.testsupport.rest.BaseWireMockTest;
import org.folio.inventory.common.VertxAssistant;
import org.folio.kafka.KafkaConfig;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.extension.RegisterExtension;

public abstract class KafkaTest extends BaseWireMockTest {

  @RegisterExtension
  protected static final KafkaExtension KAFKA = new KafkaExtension();

  protected static VertxAssistant vertxAssistant;
  protected static KafkaConfig kafkaConfig;
  protected static DeploymentOptions deploymentOptions;

  @BeforeAll
  public static void beforeAll() {
    // Reduce metadata refresh interval so pattern-subscribed consumers discover newly-created
    // topics quickly in tests rather than waiting the default 30 s.
    System.setProperty(KafkaConfig.KAFKA_CONSUMER_METADATA_MAX_AGE_CONFIG, "1000");
    vertxAssistant = new VertxAssistant();
    vertxAssistant.start();

    var bootstrapServers = KAFKA.getSupport().getBootstrapServers();
    KafkaUtility.setBootstrapServers(bootstrapServers);
    var hostAndPort = bootstrapServers.split(":");

    kafkaConfig = KafkaConfig.builder()
      .envId(KAFKA_ENV_VALUE)
      .kafkaHost(hostAndPort[0])
      .kafkaPort(hostAndPort[1])
      .maxRequestSize(MAX_REQUEST_SIZE)
      .build();

    deploymentOptions = new DeploymentOptions()
      .setConfig(new JsonObject()
        .put(KAFKA_HOST, hostAndPort[0])
        .put(KAFKA_PORT, hostAndPort[1])
        .put(KAFKA_REPLICATION_FACTOR, "1")
        .put(KAFKA_ENV, KAFKA_ENV_VALUE)
        .put(KAFKA_MAX_REQUEST_SIZE, MAX_REQUEST_SIZE));
  }

  @AfterAll
  public static void afterAll() {
    vertxAssistant.stop();
  }
}
