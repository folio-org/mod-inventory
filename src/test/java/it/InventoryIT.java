package it;

import static io.restassured.RestAssured.when;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;

import io.restassured.RestAssured;
import io.restassured.builder.RequestSpecBuilder;
import io.restassured.http.ContentType;
import java.nio.file.Path;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.images.builder.ImageFromDockerfile;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

/**
 * Integration test executed in "mvn verify" phase.
 *
 * <p>Check the shaded fat uber jar (this is not tested in "mvn test" unit tests).
 *
 * <p>Check the Dockerfile (this is not tested in "mvn test" unit tests).
 *
 * <p>Test /admin/health.
 *
 * <p>Test logging.
 */
@Testcontainers
class InventoryIT {

  private static final Logger LOG = LoggerFactory.getLogger(InventoryIT.class);
  /** Container logging, requires log4j-slf4j-impl in test scope */
  private static final boolean IS_LOG_ENABLED = false;
  private static final Network NETWORK = Network.newNetwork();

  @Container
  private static final KafkaContainer KAFKA =
      new KafkaContainer(DockerImageName.parse("apache/kafka-native:3.8.0"))
      .withNetwork(NETWORK)
      .withNetworkAliases("ourkafka");

  @Container
  private static final GenericContainer<?> MOD_INVENTORY =
      new GenericContainer<>(
          new ImageFromDockerfile("mod-inventory").withFileFromPath(".", Path.of(".")))
      .dependsOn(KAFKA)
      .withNetwork(NETWORK)
      .withNetworkAliases("mod-inventory")
      .withExposedPorts(9403)
      .withEnv("KAFKA_HOST", "ourkafka")
      .withEnv("KAFKA_PORT", "9092");

  @BeforeAll
  static void beforeAll() {
    RestAssured.reset();
    RestAssured.enableLoggingOfRequestAndResponseIfValidationFails();
    RestAssured.baseURI = "http://" + MOD_INVENTORY.getHost() + ":" + MOD_INVENTORY.getFirstMappedPort();
    if (IS_LOG_ENABLED) {
      KAFKA.followOutput(new Slf4jLogConsumer(LOG).withSeparateOutputStreams());
      MOD_INVENTORY.followOutput(new Slf4jLogConsumer(LOG).withSeparateOutputStreams());
    }
  }

  @BeforeEach
  void beforeEach() {
    RestAssured.requestSpecification = null;  // unset X-Okapi-Tenant etc.
  }

  @Test
  void health() {
    // request without X-Okapi-Tenant
    when().
      get("/admin/health").
    then().
      statusCode(200).
      body(is("OK")).
      contentType(ContentType.TEXT);
  }

  /**
   * Test logging. It broke several times caused by dependency order in pom.xml or by configuration:
   * <a href="https://issues.folio.org/browse/EDGPATRON-90">https://issues.folio.org/browse/EDGPATRON-90</a>
   * <a href="https://issues.folio.org/browse/CIRCSTORE-263">https://issues.folio.org/browse/CIRCSTORE-263</a>
   */
  @Test
  void canLog() {
    setTenant("logtenant");

    var path = "/inventory/instances/1000464a-cf2f-4218-a12c-08e3a09888e6";

    when().
      get(path).
    then().
      statusCode(404);

    assertThat(MOD_INVENTORY.getLogs(), containsString("Handling GET " + path));
  }

  private void setTenant(String tenant) {
    RestAssured.requestSpecification = new RequestSpecBuilder()
        .addHeader("X-Okapi-Url", "http://mod-inventory:9403")  // returns 404 for all other APIs
        .addHeader("X-Okapi-Tenant", tenant)
        .setContentType(ContentType.JSON)
        .build();
  }

}
