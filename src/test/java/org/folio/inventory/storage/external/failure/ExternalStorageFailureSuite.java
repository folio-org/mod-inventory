package org.folio.inventory.storage.external.failure;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.any;
import static com.github.tomakehurst.wiremock.client.WireMock.urlPathMatching;

import java.util.function.Function;

import org.folio.inventory.common.VertxAssistant;
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
  ExternalInstanceCollectionServerErrorExamples.class,
  ExternalInstanceCollectionBadRequestExamples.class,
  ExternalAuthorityCollectionBadRequestExamples.class,
  ExternalAuthorityCollectionServerErrorExamples.class
})
public class ExternalStorageFailureSuite {
  private static final VertxAssistant vertxAssistant = new VertxAssistant();

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
    return wireMockServer.url("/bad-request");
  }

  @BeforeClass
  public static void beforeAll() {
    vertxAssistant.start();

    final var errorResponse = aResponse()
      .withStatus(500)
      .withBody("Server Error")
      .withHeader("Content-Type", "text/plain");

    wireMockServer.stubFor(any(urlPathMatching("/server-error/instance-storage/instances"))
      .willReturn(errorResponse));

    wireMockServer.stubFor(any(urlPathMatching("/server-error/instance-storage/instances/[a-z0-9/-]*"))
      .willReturn(errorResponse));

    wireMockServer.stubFor(any(urlPathMatching("/server-error/authority-storage/authorities"))
      .willReturn(errorResponse));

    wireMockServer.stubFor(any(urlPathMatching("/server-error/authority-storage/authorities/[a-z0-9/-]*"))
      .willReturn(errorResponse));

    final var badRequestResponse = aResponse()
      .withStatus(400)
      .withBody("Bad Request")
      .withHeader("Content-Type", "text/plain");

    wireMockServer.stubFor(any(urlPathMatching("/bad-request/instance-storage/instances"))
      .willReturn(badRequestResponse));

    wireMockServer.stubFor(any(urlPathMatching("/bad-request/instance-storage/instances/[a-z0-9/-]*"))
      .willReturn(badRequestResponse));

    wireMockServer.stubFor(any(urlPathMatching("/bad-request/authority-storage/authorities"))
      .willReturn(badRequestResponse));

    wireMockServer.stubFor(any(urlPathMatching("/bad-request/authority-storage/authorities/[a-z0-9/-]*"))
      .willReturn(badRequestResponse));
  }

  @AfterClass()
  public static void afterAll() {
    vertxAssistant.stop();
  }
}
