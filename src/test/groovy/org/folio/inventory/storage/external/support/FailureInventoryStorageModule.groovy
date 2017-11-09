package org.folio.inventory.storage.external.support

import io.vertx.core.AbstractVerticle
import io.vertx.core.Future
import io.vertx.core.http.HttpServer
import io.vertx.ext.web.Router
import io.vertx.ext.web.RoutingContext
import org.folio.inventory.common.WebRequestDiagnostics
import org.folio.inventory.support.http.server.ClientErrorResponse
import org.folio.inventory.support.http.server.ServerErrorResponse

class FailureInventoryStorageModule extends AbstractVerticle {
  private static final int PORT_TO_USE = 9493
  private static final String address = "http://localhost:${PORT_TO_USE}"

  private HttpServer server;

  static String getServerErrorAddress() {
    address + "/server-error"
  }

  static String getBadRequestAddress() {
    address + "/bad-request"
  }

  @Override
  void start(Future deployed) {
    server = vertx.createHttpServer()

    def router = Router.router(vertx)

    server.requestHandler(router.&accept)
      .listen(PORT_TO_USE,
      { result ->
        if (result.succeeded()) {
          deployed.complete();
        } else {
          deployed.fail(result.cause());
        }
      })

    router.route().handler(WebRequestDiagnostics.&outputDiagnostics)

    router.route("/server-error/item-storage/items/*").handler(this.&serverError)
    router.route("/server-error/instance-storage/instances/*").handler(this.&serverError)
    router.route("/bad-request/item-storage/items/*").handler(this.&badRequest)
    router.route("/bad-request/instance-storage/instances/*").handler(this.&badRequest)
  }

  @Override
  public void stop(Future stopped) {
    println "Stopping failing storage module"
    server.close({ result ->
      if (result.succeeded()) {
        println "Stopped listening on ${server.actualPort()}"
        stopped.complete();
      } else {
        stopped.fail(result.cause());
      }
    });
  }

  private void serverError(RoutingContext routingContext) {
    ServerErrorResponse.internalError(routingContext.response(), "Server Error")
  }

  private void badRequest(RoutingContext routingContext) {
    ClientErrorResponse.badRequest(routingContext.response(), "Bad Request")
  }
}
