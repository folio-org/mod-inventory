package org.folio.inventory;

import io.vertx.core.DeploymentOptions;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.json.Json;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.folio.inventory.dataimport.cache.CancelledJobsIdsCache;
import org.folio.rest.jaxrs.model.Event;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.stream.Stream;

import static java.time.Duration.ofSeconds;
import static org.folio.DataImportEventTypes.DI_JOB_CANCELLED;
import static org.folio.inventory.CancelledJobExecutionConsumerVerticle.TEST_MODE_PARAM;
import static org.folio.inventory.KafkaUtility.sendEvent;
import static org.testcontainers.shaded.org.awaitility.Awaitility.await;

@RunWith(VertxUnitRunner.class)
public class CancelledJobExecutionConsumerVerticleTest extends KafkaTest {

  private static final String TENANT_ID = "diku";

  private CancelledJobsIdsCache cancelledJobsIdsCache;
  private String verticleDeploymentId;

  @Before
  public void setUp(TestContext context) {
    cancelledJobsIdsCache = new CancelledJobsIdsCache();
    deployVerticle(cancelledJobsIdsCache).onComplete(context.asyncAssertSuccess());
  }

  @After
  public void tearDown(TestContext context) {
    undeployVerticle().onComplete(context.asyncAssertSuccess());
  }

  @Test
  @SuppressWarnings("squid:S2699")
  public void shouldReadAndPutMultipleJobIdsToCache() throws ExecutionException, InterruptedException {
    List<String> ids = generateJobIds(100);

    sendJobIdsToKafka(ids);

    await().atMost(ofSeconds(3)).until(() -> ids.stream()
      .allMatch(id -> cancelledJobsIdsCache.contains(UUID.fromString(id))));
  }

  @Test
  @SuppressWarnings("squid:S2699")
  public void shouldReadAllEventsFromTopicIfVerticleWasRestarted(TestContext context)
    throws ExecutionException, InterruptedException {

    List<String> idsBatch1 = generateJobIds(100);
    sendJobIdsToKafka(idsBatch1);
    await().atMost(ofSeconds(3)).until(() -> idsBatch1.stream()
      .allMatch(id -> cancelledJobsIdsCache.contains(UUID.fromString(id))));

    // stop currently deployed verticle
    Async async = context.async();
    undeployVerticle().onComplete(context.asyncAssertSuccess(v -> async.complete()));

    async.await(3000);
    List<String> idsBatch2 = generateJobIds(200);
    sendJobIdsToKafka(idsBatch2);

    // redeploy the verticle
    Async async2 = context.async();
    cancelledJobsIdsCache = new CancelledJobsIdsCache();
    deployVerticle(cancelledJobsIdsCache).onComplete(context.asyncAssertSuccess(v -> async2.complete()));

    async2.await(3000);
    // verify that the verticle has read all events
    // including previously consumed events and newly produced events
    await().atMost(ofSeconds(3)).until(() -> idsBatch1.stream()
      .allMatch(id -> cancelledJobsIdsCache.contains(UUID.fromString(id))));
    await().atMost(ofSeconds(3)).until(() -> idsBatch2.stream()
      .allMatch(id -> cancelledJobsIdsCache.contains(UUID.fromString(id))));
  }

  private Future<String> deployVerticle(CancelledJobsIdsCache cancelledJobsIdsCache) {
    CompletableFuture<String> future = new CompletableFuture<>();
    vertxAssistant.deployVerticle(
      () -> new CancelledJobExecutionConsumerVerticle(cancelledJobsIdsCache),
      CancelledJobExecutionConsumerVerticle.class.getName(),
      getDeploymentOptions().getConfig().getMap(),
      1,
      future
    );

    return Future.fromCompletionStage(future)
      .onSuccess(deploymentId -> verticleDeploymentId = deploymentId);
  }

  private Future<Void> undeployVerticle() {
    Promise<Void> promise = Promise.promise();
    vertxAssistant.getVertx().undeploy(verticleDeploymentId, promise);

    return promise.future();
  }

  private DeploymentOptions getDeploymentOptions() {
    DeploymentOptions options = new DeploymentOptions(deploymentOptions);
    options.getConfig().put(TEST_MODE_PARAM, true);
    return options;
  }

  private List<String> generateJobIds(int idsNumber) {
    return Stream.iterate(0, i -> i < idsNumber, i -> ++i)
      .map(i -> UUID.randomUUID().toString())
      .toList();
  }

  private void sendJobIdsToKafka(List<String> ids) throws ExecutionException, InterruptedException {
    for (String id : ids) {
      Event event = new Event().withEventPayload(id);
      sendEvent(Map.of(), TENANT_ID, DI_JOB_CANCELLED.value(), "1", Json.encode(event));
    }
  }

}
