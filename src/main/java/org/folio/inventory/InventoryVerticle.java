package org.folio.inventory;

import java.lang.invoke.MethodHandles;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.inventory.common.WebRequestDiagnostics;
import org.folio.inventory.common.dao.PostgresClientFactory;
import org.folio.inventory.consortium.cache.ConsortiumDataCache;
import org.folio.inventory.consortium.services.ConsortiumService;
import org.folio.inventory.consortium.services.ConsortiumServiceImpl;
import org.folio.inventory.dataimport.services.SnapshotService;
import org.folio.inventory.resources.AdminApi;
import org.folio.inventory.resources.Holdings;
import org.folio.inventory.resources.Instances;
import org.folio.inventory.resources.InstancesBatch;
import org.folio.inventory.resources.InventoryClientFactoryImpl;
import org.folio.inventory.resources.InventoryConfigApi;
import org.folio.inventory.resources.IsbnUtilsApi;
import org.folio.inventory.resources.Items;
import org.folio.inventory.resources.ItemsByHoldingsRecordId;
import org.folio.inventory.resources.MoveApi;
import org.folio.inventory.resources.TenantApi;
import org.folio.inventory.resources.TenantItems;
import org.folio.inventory.resources.UpdateOwnershipApi;
import org.folio.inventory.storage.Storage;

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

    server.requestHandler(router)
      .listen(config.getInteger("port"))
      .onSuccess(httpServer -> {
        log.info("Listening on {}", httpServer.actualPort());
        started.complete();
      })
      .onFailure(started::fail);
  }

  @Override
  public void stop(Promise<Void> stopped) {
    final Logger log = LogManager.getLogger(MethodHandles.lookup().lookupClass());
    log.info("Stopping inventory module...");
    Future<Void> dbCloseFuture = PostgresClientFactory.closeAll();
    Future<Void> serverCloseFuture = server.close();
    Future.all(dbCloseFuture, serverCloseFuture)
      .mapEmpty()
      .onComplete(ar -> {
        if (ar.succeeded()) {
          log.info("Inventory module and all resources stopped successfully.");
          stopped.complete();
        } else {
          log.error("Failed to stop inventory module cleanly", ar.cause());
          stopped.fail(ar.cause());
        }
      });
  }
}
