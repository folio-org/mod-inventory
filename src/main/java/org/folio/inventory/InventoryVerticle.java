package org.folio.inventory;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.http.HttpServer;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.Router;
import org.folio.inventory.common.WebRequestDiagnostics;
import org.folio.inventory.domain.ingest.IngestMessageProcessor;
import org.folio.inventory.resources.Instances;
import org.folio.inventory.resources.Items;
import org.folio.inventory.resources.ingest.ModsIngestion;
import org.folio.inventory.storage.Storage;

public class InventoryVerticle extends AbstractVerticle {
  private HttpServer server;

  @Override
  public void start(Future<Void> started) {
    Router router = Router.router(vertx);

    server = vertx.createHttpServer();

    JsonObject config = vertx.getOrCreateContext().config();

    System.out.print("Received Config");

    config.fieldNames().stream().forEach(key -> {
      System.out.println(String.format("%s:%s", key, config.getValue(key).toString()));
    });

    Storage storage = Storage.basedUpon(vertx, config);

    new IngestMessageProcessor(storage).register(vertx.eventBus());

    router.route().handler(WebRequestDiagnostics::outputDiagnostics);

    new ModsIngestion(storage).register(router);
    new Items(storage).register(router);
    new Instances(storage).register(router);

    Handler<AsyncResult<HttpServer>> onHttpServerStart = result -> {
      if (result.succeeded()) {
        System.out.println(String.format("Listening on %s", server.actualPort()));
        started.complete();
      } else {
        started.fail(result.cause());
      }
    };

    server.requestHandler(router::accept)
      .listen(config.getInteger("port"), onHttpServerStart);
  }

  @Override
  public void stop(Future<Void> stopped) {
    System.out.println("Stopping inventory module");
    server.close(result -> {
      if (result.succeeded()) {
        System.out.println("Inventory module stopped");
        stopped.complete();
      } else {
        stopped.fail(result.cause());
      }
    });
  }
}
