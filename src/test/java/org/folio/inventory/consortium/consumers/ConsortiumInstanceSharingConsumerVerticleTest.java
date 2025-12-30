package org.folio.inventory.consortium.consumers;

import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.folio.inventory.ConsortiumInstanceSharingConsumerVerticle;
import org.folio.inventory.KafkaTest;
import org.junit.Test;
import org.junit.runner.RunWith;

@RunWith(VertxUnitRunner.class)
public class ConsortiumInstanceSharingConsumerVerticleTest extends KafkaTest {
  @Test
  public void shouldDeployVerticle(TestContext context) {

    Async async = context.async();
    vertxAssistant.getVertx()
      .deployVerticle(ConsortiumInstanceSharingConsumerVerticle.class.getName(), deploymentOptions)
      .onComplete(ar -> {
        context.assertTrue(ar.succeeded());
        async.complete();
      });
  }
}
