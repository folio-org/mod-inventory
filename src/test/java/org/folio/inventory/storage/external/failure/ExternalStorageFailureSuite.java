package org.folio.inventory.storage.external.failure;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.any;
import static com.github.tomakehurst.wiremock.client.WireMock.urlPathMatching;

import java.util.HashMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;

import org.folio.inventory.common.VertxAssistant;
import org.folio.inventory.storage.external.support.FailureInventoryStorageModule;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

import com.github.tomakehurst.wiremock.junit.WireMockRule;

import io.vertx.core.Vertx;

@RunWith(Suite.class)
@Suite.SuiteClasses({
  ExternalItemCollectionServerErrorExamples.class,
  ExternalItemCollectionBadRequestExamples.class,
  ExternalInstanceCollectionServerErrorExamples.class,
  ExternalInstanceCollectionBadRequestExamples.class,
  ExternalAuthorityCollectionBadRequestExamples.class,
  ExternalAuthorityCollectionServerErrorExamples.class
})
public class ExternalStorageFailureSuite {
  private static final VertxAssistant vertxAssistant = new VertxAssistant();
  private static String storageModuleDeploymentId;

  @ClassRule
  @Rule
  public static WireMockRule wireMockServer = new WireMockRule();

  public static <T> T createUsing(Function<Vertx, T> function) {
    return vertxAssistant.createUsingVertx(function);
  }

  public static String getServerErrorStorageAddress() {
    return wireMockServer.url("/server-error");
  }

  public static String getBadRequestStorageAddress() {
    return FailureInventoryStorageModule.getBadRequestAddress();
  }

  @BeforeClass
  public static void beforeAll()
    throws InterruptedException, ExecutionException, TimeoutException {

    vertxAssistant.start();

    final var errorResponse = aResponse()
      .withStatus(500)
      .withBody("Server Error")
      .withHeader("Content-Type", "text/plain");

    wireMockServer.stubFor(any(urlPathMatching("/server-error/item-storage/items"))
      .willReturn(errorResponse));

    wireMockServer.stubFor(any(urlPathMatching("/server-error/item-storage/items/[a-z0-9/-]*"))
      .willReturn(errorResponse));

    wireMockServer.stubFor(any(urlPathMatching("/server-error/instance-storage/instances"))
      .willReturn(errorResponse));

    wireMockServer.stubFor(any(urlPathMatching("/server-error/instance-storage/instances/[a-z0-9/-]*"))
      .willReturn(errorResponse));

    wireMockServer.stubFor(any(urlPathMatching("/server-error/authority-storage/authorities"))
      .willReturn(errorResponse));

    wireMockServer.stubFor(any(urlPathMatching("/server-error/authority-storage/authorities/[a-z0-9/-]*"))
      .willReturn(errorResponse));

    System.out.println("Starting Failing Storage Module");

    CompletableFuture<String> deployed = new CompletableFuture<>();

    vertxAssistant.deployVerticle(
      FailureInventoryStorageModule.class.getName(),
      new HashMap<>(),
      deployed);

    storageModuleDeploymentId = deployed.get(20000, TimeUnit.MILLISECONDS);
  }

  @AfterClass()
  public static void afterAll()
    throws InterruptedException, ExecutionException, TimeoutException {

    CompletableFuture<Void> undeployed = new CompletableFuture<>();

    vertxAssistant.undeployVerticle(storageModuleDeploymentId, undeployed);

    undeployed.get(20000, TimeUnit.MILLISECONDS);

    vertxAssistant.stop();
  }
}
