package org.folio.inventory.common;

import io.vertx.core.DeploymentOptions;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import java.util.function.Function;

public class VertxAssistant {
  private Vertx vertx;

  public void useVertx(Consumer<Vertx> action) {
    action.accept(vertx);
  }

  public <T> T createUsingVertx(Function<Vertx, T> function) {
    return function.apply(vertx);
  }

  public void start() {
    if (this.vertx == null) {
      this.vertx = Vertx.vertx();
    }
  }

  public void stop() {
    CompletableFuture stopped = new CompletableFuture();

    stop(stopped);

    stopped.join();
  }

  public void stop(final CompletableFuture stopped) {

    if (vertx != null) {
      vertx.close(res -> {
          if (res.succeeded()) {
            stopped.complete(null);
          } else {
            stopped.completeExceptionally(res.cause());
          }
        }
      );
    }
  }

  public void deployGroovyVerticle(String verticleClass,
                             Map<String, Object> config,
                             CompletableFuture<String> deployed) {

    long startTime = System.currentTimeMillis();

    DeploymentOptions options = new DeploymentOptions();

    options.setConfig(new JsonObject(config));
    options.setWorker(true);

    vertx.deployVerticle("groovy:" + verticleClass, options, result -> {
      if (result.succeeded()) {
        long elapsedTime = System.currentTimeMillis() - startTime;

        String.format("%s deployed in %s milliseconds", verticleClass, elapsedTime);

        deployed.complete(result.result());
      } else {
        deployed.completeExceptionally(result.cause());
      }
    });
  }

  public void deployVerticle(String verticleClass,
                             Map<String, Object> config,
                             CompletableFuture<String> deployed) {

    long startTime = System.currentTimeMillis();

    DeploymentOptions options = new DeploymentOptions();

    options.setConfig(new JsonObject(config));
    options.setWorker(true);

    vertx.deployVerticle(verticleClass, options, result -> {
      if (result.succeeded()) {
        long elapsedTime = System.currentTimeMillis() - startTime;

        String.format("%s deployed in %s milliseconds", verticleClass, elapsedTime);

        deployed.complete(result.result());
      } else {
        deployed.completeExceptionally(result.cause());
      }
    });
  }

  public void undeployVerticle(String deploymentId,
                               CompletableFuture<Void> undeployed) {

    vertx.undeploy(deploymentId, result -> {
      if (result.succeeded()) {
        undeployed.complete(null);
      } else {
        undeployed.completeExceptionally(result.cause());
      }
    });
  }
}
