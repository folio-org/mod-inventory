package org.folio.inventory;

import java.lang.invoke.MethodHandles;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.inventory.common.WebRequestDiagnostics;
import org.folio.inventory.common.dao.PostgresClientFactory;
import org.folio.inventory.consortium.cache.ConsortiumDataCache;
import org.folio.inventory.consortium.services.ConsortiumService;
import org.folio.inventory.consortium.services.ConsortiumServiceImpl;
import org.folio.inventory.dataimport.services.SnapshotService;
import org.folio.inventory.resources.*;
import org.folio.inventory.storage.Storage;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.Promise;
import io.vertx.core.http.HttpClient;
import io.vertx.core.http.HttpServer;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.Router;

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

    Storage storage = Storage.basedUpon(config, client);

    router.route().handler(WebRequestDiagnostics::outputDiagnostics);

    ConsortiumDataCache consortiumDataCache = new ConsortiumDataCache(vertx, client);
    ConsortiumService consortiumService = new ConsortiumServiceImpl(client, consortiumDataCache);
    SnapshotService snapshotService = new SnapshotService(client);

    new AdminApi().register(router);
    new Items(storage, client).register(router);
    new MoveApi(storage, client, consortiumService).register(router);
    new Instances(storage, client, consortiumService).register(router);
    new Holdings(storage, client).register(router);
    new InstancesBatch(storage, client, consortiumService).register(router);
    new IsbnUtilsApi().register(router);
    new ItemsByHoldingsRecordId(storage, client).register(router);
    new InventoryConfigApi().register(router);
    new TenantApi().register(router);
    new UpdateOwnershipApi(storage, client, consortiumService, snapshotService, new InventoryClientFactoryImpl()).register(router);
    new TenantItems(client).register(router);

    Handler<AsyncResult<HttpServer>> onHttpServerStart = result -> {
      if (result.succeeded()) {
        log.info("Listening on {}", server.actualPort());
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

    PostgresClientFactory.closeAll();

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
