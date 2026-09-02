package org.folio.inventory;

import static java.time.Duration.ofSeconds;
import static org.folio.DataImportEventTypes.DI_JOB_CANCELLED;
import static support.KafkaUtility.sendEvent;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.testcontainers.shaded.org.awaitility.Awaitility.await;

import io.vertx.core.Future;
import io.vertx.core.json.Json;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Stream;
import org.folio.inventory.dataimport.cache.CancelledJobsIdsCache;
import org.folio.kafka.headers.FolioKafkaHeaders;
import org.folio.rest.jaxrs.model.Event;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import support.KafkaTest;

@ExtendWith(VertxExtension.class)
class CancelledJobExecutionConsumerVerticleTest extends KafkaTest {

  private static final String TENANT_ID = "diku";

  private CancelledJobsIdsCache cancelledJobsIdsCache;
  private String verticleDeploymentId;

  @BeforeEach
  void setUp(VertxTestContext testContext) {
    cancelledJobsIdsCache = new CancelledJobsIdsCache();
    deployVerticle(cancelledJobsIdsCache).onComplete(ar -> testContext.verify(() -> {
      assertTrue(ar.succeeded());
      testContext.completeNow();
    }));
  }

  @AfterEach
  void tearDown(VertxTestContext testContext) {
    undeployVerticle().onComplete(ar -> testContext.verify(() -> {
      assertTrue(ar.succeeded());
      testContext.completeNow();
    }));
  }

  @Test
  void shouldReadAndPutMultipleJobIdsToCache() {
    List<String> ids = generateJobIds(100);

    sendJobIdsToKafka(ids);

    await().atMost(ofSeconds(3))
      .untilAsserted(() -> ids.forEach(id -> assertTrue(cancelledJobsIdsCache.contains((id)))));
  }

  @Test
  void shouldReadAllEventsFromTopicIfVerticleWasRestarted(VertxTestContext testContext) {
    List<String> idsBatch1 = generateJobIds(100);
    sendJobIdsToKafka(idsBatch1);
    await().atMost(ofSeconds(3)).until(() -> idsBatch1.stream()
      .allMatch(id -> cancelledJobsIdsCache.contains(id)));

    // stop currently deployed verticle
    org.folio.dataimport.testsupport.vertx.VertxTestUtil.await(undeployVerticle());

    List<String> idsBatch2 = generateJobIds(200);
    sendJobIdsToKafka(idsBatch2);

    // redeploy the verticle
    cancelledJobsIdsCache = new CancelledJobsIdsCache();
    org.folio.dataimport.testsupport.vertx.VertxTestUtil.await(deployVerticle(cancelledJobsIdsCache));

    // verify that the verticle has read all events
    // including previously consumed events and newly produced events
    await().atMost(ofSeconds(3))
      .untilAsserted(() -> idsBatch1.forEach(id -> assertTrue(cancelledJobsIdsCache.contains((id)))));
    await().atMost(ofSeconds(3))
      .untilAsserted(() -> idsBatch2.forEach(id -> assertTrue(cancelledJobsIdsCache.contains((id)))));

    testContext.completeNow();
  }

  private Future<String> deployVerticle(CancelledJobsIdsCache cancelledJobsIdsCache) {
    CompletableFuture<String> future = new CompletableFuture<>();
    vertxAssistant.deployVerticle(
      () -> new CancelledJobExecutionConsumerVerticle(cancelledJobsIdsCache),
      CancelledJobExecutionConsumerVerticle.class.getName(),
      deploymentOptions.getConfig().getMap(),
      1,
      future
    );

    return Future.fromCompletionStage(future)
      .onSuccess(deploymentId -> verticleDeploymentId = deploymentId);
  }

  private Future<Void> undeployVerticle() {
    return vertxAssistant.getVertx().undeploy(verticleDeploymentId);
  }

  private List<String> generateJobIds(int idsNumber) {
    return Stream.iterate(0, i -> i < idsNumber, i -> ++i)
      .map(i -> UUID.randomUUID().toString())
      .toList();
  }

  private void sendJobIdsToKafka(List<String> ids) {
    for (String id : ids) {
      Event event = new Event().withEventPayload(id);
      Map<String, String> kafkaHeaders = Map.of(
        FolioKafkaHeaders.TENANT_ID, TENANT_ID
      );
      sendEvent(kafkaHeaders, TENANT_ID, DI_JOB_CANCELLED.value(), "1", Json.encode(event));
    }
  }
}
