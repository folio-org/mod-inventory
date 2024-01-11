package org.folio.inventory.common;

import io.vertx.core.AsyncResult;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Verticle;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.lang.invoke.MethodHandles;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

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

  public void deployVerticle(Verticle verticle,
                             Map<String, Object> config,
                             CompletableFuture<String> deployed) {
    deployVerticle(verticle, config, 1, deployed);
  }

  public void deployVerticle(String verticleClass,
                             Map<String, Object> config,
                             int verticleInstancesNumber,
                             CompletableFuture<String> deployed) {
    long startTime = System.currentTimeMillis();
    DeploymentOptions options = getDeploymentOptions(config, verticleInstancesNumber);

    vertx.deployVerticle(verticleClass, options, result -> processDeployResult(verticleClass, deployed, startTime, result));
  }

  public void deployVerticle(Verticle verticle,
                             Map<String, Object> config,
                             int verticleInstancesNumber,
                             CompletableFuture<String> deployed) {
    long startTime = System.currentTimeMillis();
    DeploymentOptions options = getDeploymentOptions(config, verticleInstancesNumber);

    vertx.deployVerticle(verticle, options, result -> processDeployResult(verticle.getClass().getName(), deployed, startTime, result));
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

  private void processDeployResult(String className, CompletableFuture<String> deployed, long startTime, AsyncResult<String> result) {
    if (result.succeeded()) {
      long elapsedTime = System.currentTimeMillis() - startTime;

      log.info(String.format(
        "%s deployed in %s milliseconds", className, elapsedTime));

      deployed.complete(result.result());
    } else {
      deployed.completeExceptionally(result.cause());
    }
  }

  private static DeploymentOptions getDeploymentOptions(Map<String, Object> config, int verticleInstancesNumber) {
    DeploymentOptions options = new DeploymentOptions();

    options.setConfig(new JsonObject(config));
    options.setWorker(true);
    options.setInstances(verticleInstancesNumber);
    return options;
  }
}
