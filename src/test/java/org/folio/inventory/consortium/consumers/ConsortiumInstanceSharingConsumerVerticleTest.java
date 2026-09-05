package org.folio.inventory.consortium.consumers;

import static org.junit.jupiter.api.Assertions.assertNotNull;

import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.folio.inventory.ConsortiumInstanceSharingConsumerVerticle;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import support.KafkaTest;

@ExtendWith(VertxExtension.class)
class ConsortiumInstanceSharingConsumerVerticleTest extends KafkaTest {

  @Test
  void shouldDeployVerticle(VertxTestContext testContext) {
    vertxAssistant.getVertx()
      .deployVerticle(ConsortiumInstanceSharingConsumerVerticle.class.getName(), deploymentOptions)
      .onComplete(testContext.succeeding(id -> testContext.verify(() -> {
        assertNotNull(id);
        testContext.completeNow();
      })));
  }
}
