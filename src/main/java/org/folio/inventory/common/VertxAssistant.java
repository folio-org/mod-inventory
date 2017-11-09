package org.folio.inventory.common

import io.vertx.core.DeploymentOptions
import io.vertx.core.Vertx
import io.vertx.core.json.JsonObject

import java.util.concurrent.CompletableFuture
import java.util.function.Function

class VertxAssistant {

  private Vertx vertx

  def useVertx(Closure closure) {
    closure(vertx)
  }

  def <T> T createUsingVertx(Function<Vertx, T> function) {
    function.apply(vertx)
  }

  void start() {
    if(vertx == null) {
      this.vertx = Vertx.vertx()
    }
  }

  void stop() {
    def stopped = new CompletableFuture()

    stop(stopped)

    stopped.join()
  }

  void stop(CompletableFuture stopped) {

    if (vertx != null) {
      vertx.close({ res ->
        if (res.succeeded()) {
          stopped.complete(null);
        } else {
          stopped.completeExceptionally(res.cause());
        }
      })

      stopped.thenAccept({ vertx == null })
    }
  }

  void deployGroovyVerticle(String verticleClass,
                            Map<String, Object> config,
                            CompletableFuture<String> deployed) {

    def startTime = System.currentTimeMillis()

    def options = new DeploymentOptions()

    options.config = new JsonObject(config)
    options.worker = true

    vertx.deployVerticle("groovy:" + verticleClass,
      options,
      { res ->
        if (res.succeeded()) {
          def elapsedTime = System.currentTimeMillis() - startTime
          println("${verticleClass} deployed in ${elapsedTime} milliseconds")
          deployed.complete(res.result());
        } else {
          deployed.completeExceptionally(res.cause());
        }
      });
  }

  void deployVerticle(String verticleClass,
                            Map<String, Object> config,
                            CompletableFuture<String> deployed) {

    def startTime = System.currentTimeMillis()

    def options = new DeploymentOptions()

    options.config = new JsonObject(config)
    options.worker = true

    vertx.deployVerticle(verticleClass,
      options,
      { res ->
        if (res.succeeded()) {
          def elapsedTime = System.currentTimeMillis() - startTime
          println("${verticleClass} deployed in ${elapsedTime} milliseconds")
          deployed.complete(res.result());
        } else {
          deployed.completeExceptionally(res.cause());
        }
      });
  }


  void undeployVerticle(String deploymentId, CompletableFuture undeployed) {

    vertx.undeploy(deploymentId, { res ->
      if (res.succeeded()) {
        undeployed.complete();
      } else {
        undeployed.completeExceptionally(res.cause());
      }
    });
  }
}
