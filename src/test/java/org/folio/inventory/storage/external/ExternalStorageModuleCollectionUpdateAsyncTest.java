package org.folio.inventory.storage.external;

import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.eventbus.ReplyException;
import io.vertx.core.http.HttpClient;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.http.HttpServer;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.folio.HoldingsRecord;
import org.folio.rest.tools.utils.NetworkUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.Objects;
import java.util.UUID;

@RunWith(VertxUnitRunner.class)
public class ExternalStorageModuleCollectionUpdateAsyncTest {

  private Vertx vertx;
  private HttpServer server;
  private HttpClient client;
  private final int port = NetworkUtils.nextFreePort();

  private static final String TENANT_ID = "test_tenant";
  private static final String TOKEN = "test_token";
  private static final String USER_ID = "test_user";
  private static final String REQUEST_ID = "test_req_id";

  @Before
  public void setUp(TestContext context) {
    vertx = Vertx.vertx();
    client = vertx.createHttpClient();
  }

  @After
  public void tearDown(TestContext context) {
    Async async = context.async();
    Future.succeededFuture()
      .compose(v -> server != null ? server.close() : Future.succeededFuture())
      .compose(v -> client != null ? client.close() : Future.succeededFuture())
      .compose(v -> vertx != null ? vertx.close() : Future.succeededFuture())
      .onComplete(ar -> {
        if (ar.failed()) {
          context.fail(ar.cause());
        }
        async.complete();
      });
  }

  @Test
  public void updateAsync_succeedsOn204(TestContext context) {
    final Async async = context.async();

    // Arrange
    String holdingId = UUID.randomUUID().toString();
    String instanceId = UUID.randomUUID().toString();

    startServer(req -> {
      if (req.method() == HttpMethod.PUT && req.path().endsWith(holdingId)) {
        req.bodyHandler(body -> {
          JsonObject json = body.toJsonObject();
          context.assertEquals(holdingId, json.getString("id"));
          context.assertEquals(instanceId, json.getString("instanceId"));
          req.response().setStatusCode(204).end();
        });
      } else {
        req.response().setStatusCode(404).end("Path not found: " + req.path());
      }
    })
      .compose(v -> {
        ExternalStorageModuleHoldingsRecordCollection collection = new ExternalStorageModuleHoldingsRecordCollection(
          baseUrl(), TENANT_ID, TOKEN, USER_ID, REQUEST_ID, client);

        HoldingsRecord record = new HoldingsRecord()
          .withId(holdingId)
          .withInstanceId(instanceId)
          .withPermanentLocationId(UUID.randomUUID().toString());

        return collection.updateAsync(record);
      })
      .onComplete(ar -> {
        // Assert
        context.verify(v -> {
          context.assertTrue(ar.succeeded(), "Future should have succeeded, but failed with: " + ar.cause());
          context.assertNull(ar.result(), "Result should be null for a 204 response");
        });
        async.complete();
      });
  }

  @Test
  public void updateAsync_failsOnNon204(TestContext context) {
    Async async = context.async();
    String holdingId = UUID.randomUUID().toString();

    startServer(req -> {
      if (req.method() == HttpMethod.PUT && req.path().contains(holdingId)) {
        req.response().setStatusCode(400).end("bad request");
      } else {
        req.response().setStatusCode(404).end();
      }
    }).onSuccess(v -> {
      // Arrange
      ExternalStorageModuleHoldingsRecordCollection collection = new ExternalStorageModuleHoldingsRecordCollection(
        baseUrl(), TENANT_ID, TOKEN, USER_ID, REQUEST_ID, client);
      HoldingsRecord record = new HoldingsRecord().withId(holdingId);

      // Act
      Future<HoldingsRecord> fut = collection.updateAsync(record);

      // Assert
      fut.onComplete(ar -> {
        context.verify(v2 -> {
          context.assertTrue(ar.failed(), "Future should fail");
          Throwable cause = ar.cause();
          context.assertNotNull(cause);
          context.assertTrue(cause instanceof ReplyException, "Cause should be ReplyException");

          ReplyException re = null;
          if (cause instanceof ReplyException) {
            re = (ReplyException) cause;
          }
          context.assertEquals(400, Objects.requireNonNull(re).failureCode(), "Failure code should be 400");
          context.assertTrue(re.getMessage().contains("Expected 204"), "Message should contain expected text");
        });
        async.complete();
      });
    }).onFailure(context::fail);
  }

  private Future<Object> startServer(io.vertx.core.Handler<io.vertx.core.http.HttpServerRequest> handler) {
    if (server != null) {
      return Future.succeededFuture();
    }
    server = vertx.createHttpServer().requestHandler(handler);
    return server.listen(port)
      .mapEmpty()
      .onFailure(throwable -> server = null);
  }

  private String baseUrl() {
    return "http://localhost:" + port + "/records";
  }
}
