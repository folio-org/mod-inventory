package org.folio.inventory.storage.external;

import io.vertx.core.Vertx;
import org.folio.inventory.common.VertxAssistant;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import support.fakes.FakeOkapi;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;

@RunWith(Suite.class)
@Suite.SuiteClasses({
  ExternalItemCollectionExamples.class,
  ExternalInstanceCollectionExamples.class
})
public class ExternalStorageSuite {
  private static final VertxAssistant vertxAssistant = new VertxAssistant();
  private static String storageModuleDeploymentId;

  public static <T> T useVertx(Function<Vertx, T> action) {
    return vertxAssistant.createUsingVertx(action);
  }

  public static String getStorageAddress() {
    if (!useFakeStorageModule()) {
      return System.getProperty("inventory.storage.address");
    } else {
      return FakeOkapi.getAddress();
    }
  }

  @BeforeClass
  public static void beforeAll()
    throws InterruptedException, ExecutionException, TimeoutException {
    vertxAssistant.start();

    if (useFakeStorageModule()) {
      System.out.println("Starting Fake Storage Module");

      CompletableFuture<String> deployed = new CompletableFuture<>();

      vertxAssistant.deployVerticle(FakeOkapi.class.getName(), new HashMap<>(),
        deployed);

      storageModuleDeploymentId = deployed.get(20000, TimeUnit.MILLISECONDS);
    }
  }

  private static boolean useFakeStorageModule() {
    return System.getProperty("inventory.storage.use", "fake").equals("fake");
  }

  @AfterClass
  public static void afterAll()
    throws InterruptedException, ExecutionException, TimeoutException {

    if (useFakeStorageModule()) {
      CompletableFuture<Void> undeployed = new CompletableFuture<>();

      vertxAssistant.undeployVerticle(storageModuleDeploymentId, undeployed);

      undeployed.get(20000, TimeUnit.MILLISECONDS);
    }

    vertxAssistant.stop();
  }
}
