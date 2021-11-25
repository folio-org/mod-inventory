package org.folio.inventory.storage.external;

import static api.ApiTestSuite.USER_ID;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.HashMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;

import org.folio.inventory.common.VertxAssistant;
import org.folio.inventory.support.http.client.OkapiHttpClient;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

import io.vertx.core.Vertx;
import io.vertx.ext.web.client.WebClient;
import support.fakes.FakeOkapi;

@RunWith(Suite.class)
@Suite.SuiteClasses({
  ExternalItemCollectionExamples.class,
  ExternalInstanceCollectionExamples.class,
  ExternalStorageModuleHoldingsRecordCollectionExamples.class,
  ExternalStorageModuleAuthorityRecordCollectionExamples.class,
  ReferenceRecordClientExamples.class
})
public class ExternalStorageSuite {
  static final String TENANT_ID = "test_tenant";
  static final String TENANT_TOKEN = "eyJhbGciOiJIUzUxMiJ9.eyJzdWIiOiJhZG1pbiIsInRlbmFudCI6ImRlbW9fdGVuYW50In0.29VPjLI6fLJzxQW0UhQ0jsvAn8xHz501zyXAxRflXfJ9wuDzT8TDf-V75PjzD7fe2kHjSV2dzRXbstt3BTtXIQ";

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

  public static OkapiHttpClient createOkapiHttpClient()
    throws MalformedURLException {

    return new OkapiHttpClient(
      WebClient.wrap(vertxAssistant.createUsingVertx(Vertx::createHttpClient)),
      new URL(getStorageAddress()), TENANT_ID, TENANT_TOKEN, USER_ID, "1234",
      it -> System.out.println(String.format("Request failed: %s", it.toString())));
  }

}
