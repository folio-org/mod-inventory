package org.folio.inventory;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.Promise;
import io.vertx.core.http.HttpClient;
import io.vertx.core.http.HttpServer;
import io.vertx.core.json.JsonObject;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import io.vertx.ext.web.Router;
import org.folio.inventory.common.WebRequestDiagnostics;
import org.folio.inventory.domain.ingest.IngestMessageProcessor;
import org.folio.inventory.resources.Holdings;
import org.folio.inventory.resources.Instances;
import org.folio.inventory.resources.InstancesBatch;
import org.folio.inventory.resources.InventoryConfigApi;
import org.folio.inventory.resources.IsbnUtilsApi;
import org.folio.inventory.resources.Items;
import org.folio.inventory.resources.ItemsByHoldingsRecordId;
import org.folio.inventory.resources.MoveApi;
import org.folio.inventory.resources.ingest.ModsIngestion;
import org.folio.inventory.storage.Storage;

import java.lang.invoke.MethodHandles;

public class InventoryVerticle extends AbstractVerticle {
  private HttpServer server;

  @Override
  public void start(Promise<Void> started) {
    Logging.initialiseFormat();

    final Logger log = LogManager.getLogger(MethodHandles.lookup().lookupClass());

    Router router = Router.router(vertx);

    server = vertx.createHttpServer();

    JsonObject config = vertx.getOrCreateContext().config();

    log.info("Received Config");

    config.fieldNames().forEach(key ->
      log.info(String.format("%s:%s", key, config.getValue(key).toString())));

    HttpClient client = vertx.createHttpClient();

    Storage storage = Storage.basedUpon(vertx, config, client);

    new IngestMessageProcessor(storage).register(vertx.eventBus());

    router.route().handler(WebRequestDiagnostics::outputDiagnostics);

    new ModsIngestion(storage, client).register(router);
    new Items(storage, client).register(router);
    new MoveApi(storage, client).register(router);
    new Instances(storage, client).register(router);
    new Holdings(storage).register(router);
    new InstancesBatch(storage, client).register(router);
    new IsbnUtilsApi().register(router);
    new ItemsByHoldingsRecordId(storage, client).register(router);
    new InventoryConfigApi().register(router);

    Handler<AsyncResult<HttpServer>> onHttpServerStart = result -> {
      if (result.succeeded()) {
        log.info(String.format("Listening on %s", server.actualPort()));
        started.complete();
      } else {
        started.fail(result.cause());
      }
    };

    server.requestHandler(router)
      .listen(config.getInteger("port"), onHttpServerStart);
  }

  @Override
  public void stop(Promise<Void> stopped) {
    final Logger log = LogManager.getLogger(MethodHandles.lookup().lookupClass());

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
