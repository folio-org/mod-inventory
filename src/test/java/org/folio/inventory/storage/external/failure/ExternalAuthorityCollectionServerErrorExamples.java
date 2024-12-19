package org.folio.inventory.storage.external.failure;

import static api.ApiTestSuite.REQUEST_ID;
import static api.ApiTestSuite.USER_ID;
import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.any;
import static com.github.tomakehurst.wiremock.client.WireMock.urlPathMatching;
import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.wireMockConfig;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.fail;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import org.folio.Authority;
import org.folio.inventory.common.VertxAssistant;
import org.folio.inventory.common.api.request.PagingParameters;
import org.folio.inventory.common.domain.Failure;
import org.folio.inventory.domain.AuthorityRecordCollection;
import org.folio.inventory.storage.external.ExternalStorageCollections;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import com.github.tomakehurst.wiremock.client.ResponseDefinitionBuilder;
import com.github.tomakehurst.wiremock.junit5.WireMockExtension;
import com.github.tomakehurst.wiremock.matching.UrlPathPattern;

import lombok.SneakyThrows;

class ExternalAuthorityCollectionServerErrorExamples {
  private static final VertxAssistant vertxAssistant = new VertxAssistant();

  @RegisterExtension
  static WireMockExtension wireMockServer = WireMockExtension.newInstance()
    .options(wireMockConfig().dynamicPort())
    .build();

  @BeforeAll
  public static void beforeAll() {
    vertxAssistant.start();
  }

  @AfterAll
  public static void afterAll() {
    vertxAssistant.stop();
  }

  @Test
  @SneakyThrows
  void serverErrorWhenCreatingAnAuthorityTriggersFailureCallback() {
    wireMockServer.stubFor(any(collectionRoot())
      .willReturn(serverErrorResponse()));

    AuthorityRecordCollection collection = createCollection();

    CompletableFuture<Failure> failureCalled = new CompletableFuture<>();

    collection.add(createAuthority(),
      success -> fail("Completion callback should not be called"),
      failureCalled::complete);

    Failure failure = failureCalled.get(1000, TimeUnit.MILLISECONDS);

    assertServerError(failure);
  }

  @Test
  @SneakyThrows
  void serverErrorWhenUpdatingAnAuthorityTriggersFailureCallback() {
    wireMockServer.stubFor(any(individualItem())
      .willReturn(serverErrorResponse()));

    AuthorityRecordCollection collection = createCollection();

    CompletableFuture<Failure> failureCalled = new CompletableFuture<>();

    collection.update(createAuthority(),
      success -> fail("Completion callback should not be called"),
      failureCalled::complete);

    Failure failure = failureCalled.get(1000, TimeUnit.MILLISECONDS);

    assertServerError(failure);
  }

  @Test
  @SneakyThrows
  void serverErrorWhenGettingAllAuthoritiesTriggersFailureCallback() {
    wireMockServer.stubFor(any(collectionRoot())
      .willReturn(serverErrorResponse()));

    AuthorityRecordCollection collection = createCollection();

    CompletableFuture<Failure> failureCalled = new CompletableFuture<>();

    collection.findAll(PagingParameters.defaults(),
      success -> fail("Completion callback should not be called"),
      failureCalled::complete);

    Failure failure = failureCalled.get(1000, TimeUnit.MILLISECONDS);

    assertServerError(failure);
  }

  @Test
  @SneakyThrows
  void serverErrorWhenGettingAnAuthorityRecordByIdTriggersFailureCallback() {
    wireMockServer.stubFor(any(individualItem())
      .willReturn(serverErrorResponse()));

    AuthorityRecordCollection collection = createCollection();

    CompletableFuture<Failure> failureCalled = new CompletableFuture<>();

    collection.findById(UUID.randomUUID().toString(),
      success -> fail("Completion callback should not be called"),
      failureCalled::complete);

    Failure failure = failureCalled.get(1000, TimeUnit.MILLISECONDS);

    assertServerError(failure);
  }

  @Test
  @SneakyThrows
  void serverErrorWhenDeletingAnAuthorityByIdTriggersFailureCallback() {
    wireMockServer.stubFor(any(individualItem())
      .willReturn(serverErrorResponse()));

    AuthorityRecordCollection collection = createCollection();

    CompletableFuture<Failure> failureCalled = new CompletableFuture<>();

    collection.delete(UUID.randomUUID().toString(),
      success -> fail("Completion callback should not be called"),
      failureCalled::complete);

    Failure failure = failureCalled.get(1000, TimeUnit.MILLISECONDS);

    assertServerError(failure);
  }

  @Test
  @SneakyThrows
  void serverErrorWhenDeletingAllAuthoritiesTriggersFailureCallback() {
    wireMockServer.stubFor(any(collectionRoot())
      .willReturn(serverErrorResponse()));

    AuthorityRecordCollection collection = createCollection();

    CompletableFuture<Failure> failureCalled = new CompletableFuture<>();

    collection.empty(
      success -> fail("Completion callback should not be called"),
      failureCalled::complete);

    Failure failure = failureCalled.get(1000, TimeUnit.MILLISECONDS);

    assertServerError(failure);
  }

  @Test
  @SneakyThrows
  void serverErrorWhenFindingAuthoritiesTriggersFailureCallback() {
    wireMockServer.stubFor(any(collectionRoot())
      .willReturn(serverErrorResponse()));

    AuthorityRecordCollection collection = createCollection();

    CompletableFuture<Failure> failureCalled = new CompletableFuture<>();

    collection.findByCql("title=\"*Small Angry*\"",
      new PagingParameters(10, 0),
      success -> fail("Completion callback should not be called"),
      failureCalled::complete);

    Failure failure = failureCalled.get(1000, TimeUnit.MILLISECONDS);

    assertServerError(failure);
  }

  private Authority createAuthority() {
    return new Authority()
      .withId(UUID.randomUUID().toString());
  }

  private AuthorityRecordCollection createCollection() {
    return vertxAssistant.createUsingVertx(
        it -> new ExternalStorageCollections(
          wireMockServer.baseUrl(), it.createHttpClient()))
      .getAuthorityCollection("test_tenant", "", USER_ID, REQUEST_ID);
  }

  private void assertServerError(Failure failure) {
    assertThat(failure.getReason(), is("Server Error"));
    assertThat(failure.getStatusCode(), is(500));
  }

  private ResponseDefinitionBuilder serverErrorResponse() {
    return aResponse()
      .withStatus(500)
      .withBody("Server Error")
      .withHeader("Content-Type", "text/plain");
  }

  private UrlPathPattern collectionRoot() {
    return urlPathMatching("/authority-storage/authorities");
  }

  private UrlPathPattern individualItem() {
    return urlPathMatching("/authority-storage/authorities/[a-z0-9/-]*");
  }
}
