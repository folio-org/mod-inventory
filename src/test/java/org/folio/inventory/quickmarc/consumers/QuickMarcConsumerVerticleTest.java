package org.folio.inventory.quickmarc.consumers;

import static net.mguenther.kafka.junit.EmbeddedKafkaCluster.provisionWith;
import static net.mguenther.kafka.junit.EmbeddedKafkaClusterConfig.useDefaults;

import static org.folio.inventory.dataimport.util.KafkaConfigConstants.KAFKA_ENV;
import static org.folio.inventory.dataimport.util.KafkaConfigConstants.KAFKA_HOST;
import static org.folio.inventory.dataimport.util.KafkaConfigConstants.KAFKA_PORT;
import static org.folio.inventory.dataimport.util.KafkaConfigConstants.KAFKA_REPLICATION_FACTOR;

import io.vertx.core.DeploymentOptions;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import net.mguenther.kafka.junit.EmbeddedKafkaCluster;
import org.junit.AfterClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;

import org.folio.inventory.QuickMarcConsumerVerticle;

@RunWith(VertxUnitRunner.class)
public class QuickMarcConsumerVerticleTest {

  private static final String KAFKA_ENV_NAME = "test-env";
  private static final Vertx VERTX = Vertx.vertx();

  @ClassRule
  public static EmbeddedKafkaCluster cluster = provisionWith(useDefaults());

  @AfterClass
  public static void tearDownClass(TestContext context) {
    Async async = context.async();
    VERTX.close(ar -> async.complete());
  }

  @Test
  public void shouldDeployVerticle(TestContext context) {
    Async async = context.async();
    String[] hostAndPort = cluster.getBrokerList().split(":");
    DeploymentOptions options = new DeploymentOptions()
      .setConfig(new JsonObject()
        .put(KAFKA_HOST, hostAndPort[0])
        .put(KAFKA_PORT, hostAndPort[1])
        .put(KAFKA_REPLICATION_FACTOR, "1")
        .put(KAFKA_ENV, KAFKA_ENV_NAME))
      .setWorker(true);

    Promise<String> promise = Promise.promise();
    VERTX.deployVerticle(QuickMarcConsumerVerticle.class.getName(), options, promise);

    promise.future().onComplete(ar -> {
      context.assertTrue(ar.succeeded());
      async.complete();
    });

  }
}