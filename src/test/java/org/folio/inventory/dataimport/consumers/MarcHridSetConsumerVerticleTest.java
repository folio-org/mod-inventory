package org.folio.inventory.dataimport.consumers;

import io.vertx.core.DeploymentOptions;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import net.mguenther.kafka.junit.EmbeddedKafkaCluster;
import org.folio.inventory.MarcHridSetConsumerVerticle;
import org.junit.AfterClass;
import org.junit.Test;
import org.junit.runner.RunWith;

import static net.mguenther.kafka.junit.EmbeddedKafkaCluster.provisionWith;
import static net.mguenther.kafka.junit.EmbeddedKafkaClusterConfig.defaultClusterConfig;
import static org.folio.inventory.dataimport.util.KafkaConfigConstants.KAFKA_ENV;
import static org.folio.inventory.dataimport.util.KafkaConfigConstants.KAFKA_HOST;
import static org.folio.inventory.dataimport.util.KafkaConfigConstants.KAFKA_MAX_REQUEST_SIZE;
import static org.folio.inventory.dataimport.util.KafkaConfigConstants.KAFKA_PORT;
import static org.folio.inventory.dataimport.util.KafkaConfigConstants.KAFKA_REPLICATION_FACTOR;

@RunWith(VertxUnitRunner.class)
public class MarcHridSetConsumerVerticleTest {

  private static final String TENANT_ID = "diku";
  private static final String KAFKA_ENV_NAME = "test-env";
  private static Vertx vertx = Vertx.vertx();

  public static EmbeddedKafkaCluster cluster;

  @Test
  public void shouldDeployVerticle(TestContext context) {
    Async async = context.async();
    cluster = provisionWith(defaultClusterConfig());
    cluster.start();
    String[] hostAndPort = cluster.getBrokerList().split(":");
    DeploymentOptions options = new DeploymentOptions()
      .setConfig(new JsonObject()
        .put(KAFKA_HOST, hostAndPort[0])
        .put(KAFKA_PORT, hostAndPort[1])
        .put(KAFKA_REPLICATION_FACTOR, "1")
        .put(KAFKA_ENV, KAFKA_ENV_NAME)
        .put(KAFKA_MAX_REQUEST_SIZE, "1048576"))
      .setWorker(true);

    Promise<String> promise = Promise.promise();
    vertx.deployVerticle(MarcHridSetConsumerVerticle.class.getName(), options, promise);

    promise.future().onComplete(ar -> {
      context.assertTrue(ar.succeeded());
      async.complete();
    });

  }

  @AfterClass
  public static void tearDownClass(TestContext context) {
    Async async = context.async();
    vertx.close(ar -> async.complete());
  }

}
