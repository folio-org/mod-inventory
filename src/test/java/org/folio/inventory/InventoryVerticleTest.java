package org.folio.inventory;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;

import io.vertx.core.DeploymentOptions;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.core.http.HttpClient;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.core.http.PoolOptions;
import io.vertx.core.json.JsonObject;
import java.net.ServerSocket;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class InventoryVerticleTest {
  private static final String POOL_SIZE_PROPERTY = "inventory.httpClient.maxPoolSize";

  private Vertx vertx;
  private String previousPoolSize;

  @Before
  public void setUp() {
    previousPoolSize = System.getProperty(POOL_SIZE_PROPERTY);
    System.clearProperty(POOL_SIZE_PROPERTY);
    vertx = Vertx.vertx(new VertxOptions().setEventLoopPoolSize(2));
  }

  @After
  public void tearDown() throws Exception {
    if (previousPoolSize == null) {
      System.clearProperty(POOL_SIZE_PROPERTY);
    } else {
      System.setProperty(POOL_SIZE_PROPERTY, previousPoolSize);
    }
    vertx.close().toCompletionStage().toCompletableFuture().get(10, SECONDS);
  }

  @Test
  public void preservesDefaultPoolSize() throws Exception {
    assumeTrue("Default pool size requires HTTP_CLIENT_MAX_POOL_SIZE to be unset",
      System.getenv("HTTP_CLIENT_MAX_POOL_SIZE") == null);
    assertConcurrentStorageRequests(5);
  }

  @Test
  public void allowsMoreThanFiveStorageRequests() throws Exception {
    System.setProperty(POOL_SIZE_PROPERTY, "7");
    assertConcurrentStorageRequests(7);
  }

  @Test
  public void rejectsNonPositivePoolSize() {
    System.setProperty(POOL_SIZE_PROPERTY, "0");
    ExecutionException error = assertThrows(ExecutionException.class,
      () -> deployInventory(0, "http://127.0.0.1:1").toCompletionStage()
        .toCompletableFuture().get(10, SECONDS));
    assertTrue(error.getCause() instanceof IllegalArgumentException);
  }

  @Test
  public void rejectsNonNumericPoolSize() {
    System.setProperty(POOL_SIZE_PROPERTY, "invalid");
    ExecutionException error = assertThrows(ExecutionException.class,
      () -> deployInventory(0, "http://127.0.0.1:1").toCompletionStage()
        .toCompletableFuture().get(10, SECONDS));
    assertTrue(error.getCause() instanceof NumberFormatException);
  }

  void assertConcurrentStorageRequests(int poolSize) throws Exception {
    AtomicInteger received = new AtomicInteger();
    AtomicBoolean holdResponses = new AtomicBoolean(true);
    var waitingResponses = new ConcurrentLinkedQueue<HttpServerResponse>();
    CountDownLatch poolFilled = new CountDownLatch(poolSize);
    var storage = vertx.createHttpServer().requestHandler(request -> {
      if (holdResponses.get()) {
        waitingResponses.add(request.response());
      } else {
        request.response().setStatusCode(404).end();
      }
      received.incrementAndGet();
      poolFilled.countDown();
    }).listen(0, "127.0.0.1").toCompletionStage().toCompletableFuture().get(10, SECONDS);

    int inventoryPort;
    try (ServerSocket socket = new ServerSocket(0)) {
      inventoryPort = socket.getLocalPort();
    }
    String storageUrl = "http://127.0.0.1:" + storage.actualPort();
    deployInventory(inventoryPort, storageUrl).toCompletionStage()
      .toCompletableFuture().get(10, SECONDS);

    HttpClient caller = vertx.createHttpClient(new PoolOptions().setHttp1MaxSize(poolSize + 1));
    List<Future<Integer>> responses = new ArrayList<>();
    for (int i = 0; i <= poolSize; i++) {
      responses.add(caller.request(HttpMethod.GET, inventoryPort, "127.0.0.1",
          "/inventory/instances/" + UUID.randomUUID())
        .compose(request -> request.putHeader("X-Okapi-Tenant", "test_tenant")
          .putHeader("X-Okapi-Url", storageUrl).send())
        .compose(response -> response.body().map(response.statusCode())));
    }

    boolean filled = poolFilled.await(10, SECONDS);
    assertTrue("Expected " + poolSize + " concurrent storage requests, received " + received.get(), filled);
    CompletableFuture<Integer> heldCount = new CompletableFuture<>();
    vertx.setTimer(200, ignored -> {
      heldCount.complete(received.get());
      holdResponses.set(false);
      waitingResponses.forEach(response -> response.setStatusCode(404).end());
    });
    assertEquals(poolSize, heldCount.get(10, SECONDS).intValue());
    Future.all(responses).toCompletionStage().toCompletableFuture().get(10, SECONDS);
    for (Future<Integer> response : responses) {
      assertEquals(Integer.valueOf(404), response.result());
    }
  }

  private Future<String> deployInventory(int port, String storageUrl) {
    return vertx.deployVerticle(new InventoryVerticle(), new DeploymentOptions().setConfig(
      new JsonObject().put("port", port).put("storage.type", "external")
        .put("storage.location", storageUrl)));
  }
}
