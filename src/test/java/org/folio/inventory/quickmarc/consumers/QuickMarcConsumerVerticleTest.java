package org.folio.inventory.quickmarc.consumers;

import io.vertx.core.Promise;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.folio.inventory.KafkaTest;
import org.junit.Test;
import org.junit.runner.RunWith;

import org.folio.inventory.QuickMarcConsumerVerticle;

@RunWith(VertxUnitRunner.class)
public class QuickMarcConsumerVerticleTest extends KafkaTest {
  @Test
  public void shouldDeployVerticle(TestContext context) {
    Async async = context.async();

    Promise<String> promise = Promise.promise();
    vertxAssistant.getVertx().deployVerticle(QuickMarcConsumerVerticle.class.getName(), deploymentOptions, promise);

    promise.future().onComplete(ar -> {
      context.assertTrue(ar.succeeded());
      async.complete();
    });
  }
}
