package org.folio.inventory.dataimport.consumers;

import io.vertx.core.Promise;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.folio.inventory.InstanceIngressConsumerVerticle;
import org.folio.inventory.KafkaTest;
import org.junit.Test;
import org.junit.runner.RunWith;

@RunWith(VertxUnitRunner.class)
public class InstanceIngressConsumerVerticleTest extends KafkaTest {
  @Test
  public void shouldDeployVerticle(TestContext context) {
    Async async = context.async();
    Promise<String> promise = Promise.promise();
    vertxAssistant.getVertx().deployVerticle(InstanceIngressConsumerVerticle.class.getName(), deploymentOptions, promise);

    promise.future().onComplete(ar -> {
      context.assertTrue(ar.succeeded());
      async.complete();
    });
  }
}
