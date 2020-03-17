package org.folio.inventory;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.http.HttpClient;
import io.vertx.core.http.HttpServer;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.ext.web.Router;
import org.folio.inventory.common.WebRequestDiagnostics;
import org.folio.inventory.domain.ingest.IngestMessageProcessor;
import org.folio.inventory.resources.EventHandlers;
import org.folio.inventory.resources.Instances;
import org.folio.inventory.resources.InstancesBatch;
import org.folio.inventory.resources.IsbnUtilsApi;
import org.folio.inventory.resources.Items;
import org.folio.inventory.resources.TenantApi;
import org.folio.inventory.resources.ingest.ModsIngestion;
import org.folio.inventory.storage.Storage;

import java.lang.invoke.MethodHandles;

public class InventoryVerticle extends AbstractVerticle {
  private HttpServer server;

  @Override
  public void start(Future<Void> started) {
    Logging.initialiseFormat();

    final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    Router router = Router.router(vertx);

    server = vertx.createHttpServer();

    JsonObject config = vertx.getOrCreateContext().config();

    log.info("Received Config");

    config.fieldNames().stream().forEach(key ->
      log.info(String.format("%s:%s", key, config.getValue(key).toString())));

    HttpClient client = vertx.createHttpClient();

    Storage storage = Storage.basedUpon(vertx, config, client);

    new IngestMessageProcessor(storage).register(vertx.eventBus());

    router.route().handler(WebRequestDiagnostics::outputDiagnostics);

    new ModsIngestion(storage, client).register(router);
    new Items(storage, client).register(router);
    new Instances(storage, client).register(router);
    new InstancesBatch(storage, client).register(router);
    new IsbnUtilsApi().register(router);
    new TenantApi().register(router);
    new EventHandlers(storage).register(router);

    Handler<AsyncResult<HttpServer>> onHttpServerStart = result -> {
      if (result.succeeded()) {
        log.info(String.format("Listening on %s", server.actualPort()));
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
    final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    log.info("Stopping inventory module");
    server.close(result -> {
      if (result.succeeded()) {
        log.info("Inventory module stopped");
        stopped.complete();
      } else {
        stopped.fail(result.cause());
      }
    });
  }
}
