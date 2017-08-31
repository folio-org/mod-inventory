package org.folio.inventory

import io.vertx.core.AbstractVerticle
import io.vertx.core.Future
import io.vertx.core.http.HttpServer
import io.vertx.ext.web.Router
import org.folio.inventory.common.WebRequestDiagnostics
import org.folio.inventory.domain.ingest.IngestMessageProcessor
import org.folio.inventory.resources.Instances
import org.folio.inventory.resources.Items
import org.folio.inventory.resources.ingest.ModsIngestion
import org.folio.inventory.storage.Storage

class InventoryVerticle extends AbstractVerticle {

  private HttpServer server

  @Override
  void start(Future started) {
    def router = Router.router(vertx)

    server = vertx.createHttpServer()

    Map<String, Object> config = vertx.getOrCreateContext().config().map

    println("Received Config")
    config.each { println("${it.key}:${it.value}") }

    def storage = Storage.basedUpon(vertx, config)

    new IngestMessageProcessor(storage).register(vertx.eventBus())

    router.route().handler(WebRequestDiagnostics.&outputDiagnostics)

    new ModsIngestion(storage).register(router)
    new Items(storage).register(router)
    new Instances(storage).register(router)

    def onHttpServerStart = { result ->
      if (result.succeeded()) {
        println "Listening on ${server.actualPort()}"
        started.complete();
      } else {
        started.fail(result.cause());
      }
    }

    server.requestHandler(router.&accept).listen(config.port, onHttpServerStart)
  }

  @Override
  void stop(Future stopped) {
    println "Stopping inventory module"
    server.close({ result ->
      if (result.succeeded()) {
        println "Stopped listening on ${server.actualPort()}"
        stopped.complete();
      } else {
        stopped.fail(result.cause());
      }
    })
  }
}
