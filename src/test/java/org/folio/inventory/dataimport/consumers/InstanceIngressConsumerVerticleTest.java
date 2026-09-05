package org.folio.inventory.dataimport.consumers;

import static org.junit.jupiter.api.Assertions.assertTrue;

import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.folio.inventory.InstanceIngressConsumerVerticle;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import support.KafkaTest;

@ExtendWith(VertxExtension.class)
class InstanceIngressConsumerVerticleTest extends KafkaTest {

  @Test
  void shouldDeployVerticle(VertxTestContext testContext) {
    vertxAssistant.getVertx()
      .deployVerticle(InstanceIngressConsumerVerticle.class.getName(), deploymentOptions)
      .onComplete(ar -> testContext.verify(() -> {
        assertTrue(ar.succeeded());
        testContext.completeNow();
      }));
  }
}
