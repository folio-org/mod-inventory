package org.folio.inventory.common;

import io.vertx.core.DeploymentOptions;
import io.vertx.core.ThreadingModel;
import io.vertx.core.Verticle;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.lang.invoke.MethodHandles;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.function.Supplier;

public class VertxAssistant {
  private static final Logger log = LogManager.getLogger(MethodHandles.lookup().lookupClass());

  private Vertx vertx;

  public <T> T createUsingVertx(Function<Vertx, T> function) {
    return function.apply(vertx);
  }

  public void start() {
    if (this.vertx == null) {
      this.vertx = Vertx.vertx();
    }
  }

  public Vertx getVertx() {
    return vertx;
  }

  public void stop() {
    CompletableFuture<Void> stopped = new CompletableFuture<>();

    stop(stopped);

    stopped.join();
  }

  public void stop(final CompletableFuture<Void> stopped) {

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

  public void deployVerticle(String verticleClass,
                             Map<String, Object> config,
                             CompletableFuture<String> deployed) {
    deployVerticle(verticleClass, config, 1, deployed);
  }

  public void deployVerticle(String verticleClass,
                             Map<String, Object> config,
                             int verticleInstancesNumber,
                             CompletableFuture<String> deployed) {
    long startTime = System.currentTimeMillis();

    DeploymentOptions options = new DeploymentOptions();

    options.setConfig(new JsonObject(config));
    options.setWorker(true);
    options.setInstances(verticleInstancesNumber);

    vertx.deployVerticle(verticleClass, options, result -> {
      if (result.succeeded()) {
        long elapsedTime = System.currentTimeMillis() - startTime;

        log.info(String.format(
          "%s deployed in %s milliseconds", verticleClass, elapsedTime));

        deployed.complete(result.result());
      } else {
        deployed.completeExceptionally(result.cause());
      }
    });

  }

  public void deployVerticle(Supplier<Verticle> verticleSupplier,
                             String verticleClass,
                             Map<String, Object> config,
                             int verticleInstancesNumber,
                             CompletableFuture<String> deployed) {
    long startTime = System.currentTimeMillis();

    DeploymentOptions options = new DeploymentOptions()
      .setConfig(new JsonObject(config))
      .setThreadingModel(ThreadingModel.WORKER)
      .setInstances(verticleInstancesNumber);

    vertx.deployVerticle(verticleSupplier, options, result -> {
      if (result.succeeded()) {
        long elapsedTime = System.currentTimeMillis() - startTime;

        log.info("{} deployed in {} milliseconds", verticleClass, elapsedTime);

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

  public CompletableFuture<Void> undeployVerticle(String deploymentId) {
    CompletableFuture<Void> future = new CompletableFuture<>();
    undeployVerticle(deploymentId, future);
    return future;
  }
}
