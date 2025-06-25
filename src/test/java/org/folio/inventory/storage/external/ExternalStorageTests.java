package org.folio.inventory.storage.external;

import static api.ApiTestSuite.USER_ID;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

import java.net.URL;
import java.util.HashMap;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

import org.folio.inventory.common.VertxAssistant;
import org.folio.inventory.support.http.client.OkapiHttpClient;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import io.vertx.core.http.HttpClient;
import lombok.SneakyThrows;
import support.fakes.FakeOkapi;

public abstract class ExternalStorageTests {
  static final String TENANT_ID = "test_tenant";
  static final String TENANT_TOKEN = "eyJhbGciOiJIUzUxMiJ9.eyJzdWIiOiJhZG1pbiIsInRlbmFudCI6ImRlbW9fdGVuYW50In0.29VPjLI6fLJzxQW0UhQ0jsvAn8xHz501zyXAxRflXfJ9wuDzT8TDf-V75PjzD7fe2kHjSV2dzRXbstt3BTtXIQ";

  private static VertxAssistant vertxAssistant;

  private static String storageModuleDeploymentId;

  protected static String getStorageAddress() {
    return FakeOkapi.getAddress();
  }

  @BeforeClass
  @SneakyThrows
  public static void beforeAll() {
    vertxAssistant = new VertxAssistant();
    vertxAssistant.start();

    final var deployed = new CompletableFuture<String>();

    vertxAssistant.deployVerticle(FakeOkapi.class.getName(), new HashMap<>(),
      deployed);

    storageModuleDeploymentId = deployed.get(20000, MILLISECONDS);
  }

  @AfterClass
  @SneakyThrows
  public static void afterAll() {
    final var undeployed = new CompletableFuture<Void>();

    vertxAssistant.undeployVerticle(storageModuleDeploymentId, undeployed);

    undeployed.get(20000, MILLISECONDS);

    vertxAssistant.stop();
  }

  protected static <T> T useHttpClient(Function<HttpClient, T> action) {
    return vertxAssistant.createUsingVertx(
      vertx -> action.apply(vertx.createHttpClient()));
  }

  @SneakyThrows
  protected static OkapiHttpClient createOkapiHttpClient() {
    return new OkapiHttpClient(vertxAssistant.getVertx(),
      new URL(getStorageAddress()), TENANT_ID, TENANT_TOKEN, USER_ID, "1234",
      it -> System.out.printf("Request failed: %s%n", it.toString()));
  }
}
