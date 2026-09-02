package org.folio.inventory.storage.external;

import static api.ApiTestSuite.USER_ID;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

import io.vertx.core.http.HttpClient;
import java.net.URI;
import java.util.HashMap;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import lombok.SneakyThrows;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.inventory.common.VertxAssistant;
import org.folio.inventory.support.http.client.OkapiHttpClient;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import support.fakes.FakeOkapi;

public abstract class AbstractExternalStorageTest {

  static final String TENANT_ID = "test_tenant";
  static final String TENANT_TOKEN =
    "eyJhbGciOiJIUzUxMiJ9.eyJzdWIiOiJhZG1pbiIsInRlbmFudCI6ImRlbW9fdGVuYW50In0.29VPjLI6fLJzxQW0UhQ0jsvAn8xHz501zyXAxRflXfJ9wuDzT8TDf-V75PjzD7fe2kHjSV2dzRXbstt3BTtXIQ";

  private static final Logger LOGGER = LogManager.getLogger(AbstractExternalStorageTest.class);

  private static VertxAssistant vertxAssistant;

  private static String storageModuleDeploymentId;

  @BeforeAll
  @SneakyThrows
  public static void beforeAll() {
    vertxAssistant = new VertxAssistant();
    vertxAssistant.start();

    final var deployed = new CompletableFuture<String>();

    vertxAssistant.deployVerticle(FakeOkapi.class.getName(), new HashMap<>(), deployed);

    storageModuleDeploymentId = deployed.get(20000, MILLISECONDS);
  }

  @AfterAll
  @SneakyThrows
  public static void afterAll() {
    final var undeployed = new CompletableFuture<Void>();

    vertxAssistant.undeployVerticle(storageModuleDeploymentId, undeployed);

    undeployed.get(20000, MILLISECONDS);

    vertxAssistant.stop();
  }

  protected static String getStorageAddress() {
    return FakeOkapi.getADDRESS();
  }

  protected static <T> T useHttpClient(Function<HttpClient, T> action) {
    return vertxAssistant.createUsingVertx(vertx -> action.apply(vertx.createHttpClient()));
  }

  @SneakyThrows
  protected static OkapiHttpClient createOkapiHttpClient() {
    return new OkapiHttpClient(vertxAssistant.getVertx(),
      new URI(getStorageAddress()).toURL(), TENANT_ID, TENANT_TOKEN, USER_ID, "1234",
      it -> LOGGER.error("Request failed.", it));
  }
}
