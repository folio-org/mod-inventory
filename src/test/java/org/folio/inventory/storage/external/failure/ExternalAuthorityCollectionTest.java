package org.folio.inventory.storage.external.failure;

import static api.ApiTestSuite.REQUEST_ID;
import static api.ApiTestSuite.USER_ID;
import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.any;
import static com.github.tomakehurst.wiremock.client.WireMock.urlPathMatching;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.fail;

import com.github.tomakehurst.wiremock.client.ResponseDefinitionBuilder;
import com.github.tomakehurst.wiremock.matching.UrlPathPattern;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import lombok.SneakyThrows;
import org.folio.Authority;
import org.folio.dataimport.testsupport.rest.BaseWireMockTest;
import org.folio.inventory.common.VertxAssistant;
import org.folio.inventory.common.api.request.PagingParameters;
import org.folio.inventory.common.domain.Failure;
import org.folio.inventory.domain.AuthorityRecordCollection;
import org.folio.inventory.storage.external.ExternalStorageCollections;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

class ExternalAuthorityCollectionTest extends BaseWireMockTest {

  private static final VertxAssistant vertxAssistant = new VertxAssistant();

  @BeforeAll
  static void beforeAll() {
    vertxAssistant.start();
  }

  @AfterAll
  static void afterAll() {
    vertxAssistant.stop();
  }

  @Test
  @SneakyThrows
  void badRequestWhenCreatingAnAuthorityTriggersFailureCallback() {
    WIRE_MOCK.stubFor(any(collectionRoot())
      .willReturn(badRequestResponse()));

    AuthorityRecordCollection collection = createCollection();

    CompletableFuture<Failure> failureCalled = new CompletableFuture<>();

    collection.add(createAuthority(),
      success -> fail("Completion callback should not be called"),
      failureCalled::complete);

    Failure failure = failureCalled.get(1000, TimeUnit.MILLISECONDS);

    assertBadRequest(failure);
  }

  @Test
  @SneakyThrows
  void badRequestWhenUpdatingAnAuthorityTriggersFailureCallback() {
    WIRE_MOCK.stubFor(any(individualItem())
      .willReturn(badRequestResponse()));

    AuthorityRecordCollection collection = createCollection();

    CompletableFuture<Failure> failureCalled = new CompletableFuture<>();

    collection.update(createAuthority(),
      success -> fail("Completion callback should not be called"),
      failureCalled::complete);

    Failure failure = failureCalled.get(1000, TimeUnit.MILLISECONDS);

    assertBadRequest(failure);
  }

  @Test
  @SneakyThrows
  void badRequestWhenGettingAllAuthoritiesTriggersFailureCallback() {
    WIRE_MOCK.stubFor(any(collectionRoot())
      .willReturn(badRequestResponse()));

    AuthorityRecordCollection collection = createCollection();

    CompletableFuture<Failure> failureCalled = new CompletableFuture<>();

    collection.findAll(PagingParameters.defaults(),
      success -> fail("Completion callback should not be called"),
      failureCalled::complete);

    Failure failure = failureCalled.get(1000, TimeUnit.MILLISECONDS);

    assertBadRequest(failure);
  }

  @Test
  @SneakyThrows
  void badRequestWhenGettingAnAuthorityRecordByIdTriggersFailureCallback() {
    WIRE_MOCK.stubFor(any(individualItem())
      .willReturn(badRequestResponse()));

    AuthorityRecordCollection collection = createCollection();

    CompletableFuture<Failure> failureCalled = new CompletableFuture<>();

    collection.findById(UUID.randomUUID().toString(),
      success -> fail("Completion callback should not be called"),
      failureCalled::complete);

    Failure failure = failureCalled.get(1000, TimeUnit.MILLISECONDS);

    assertBadRequest(failure);
  }

  @Test
  @SneakyThrows
  void badRequestWhenDeletingAnAuthorityByIdTriggersFailureCallback() {
    WIRE_MOCK.stubFor(any(individualItem())
      .willReturn(badRequestResponse()));

    AuthorityRecordCollection collection = createCollection();

    CompletableFuture<Failure> failureCalled = new CompletableFuture<>();

    collection.delete(UUID.randomUUID().toString(),
      success -> fail("Completion callback should not be called"),
      failureCalled::complete);

    Failure failure = failureCalled.get(1000, TimeUnit.MILLISECONDS);

    assertBadRequest(failure);
  }

  @Test
  @SneakyThrows
  void badRequestWhenDeletingAllAuthoritiesTriggersFailureCallback() {
    WIRE_MOCK.stubFor(any(collectionRoot())
      .willReturn(badRequestResponse()));

    AuthorityRecordCollection collection = createCollection();

    CompletableFuture<Failure> failureCalled = new CompletableFuture<>();

    collection.empty(
      success -> fail("Completion callback should not be called"),
      failureCalled::complete);

    Failure failure = failureCalled.get(1000, TimeUnit.MILLISECONDS);

    assertBadRequest(failure);
  }

  @Test
  @SneakyThrows
  void badRequestWhenFindingAuthoritiesTriggersFailureCallback() {
    WIRE_MOCK.stubFor(any(collectionRoot())
      .willReturn(badRequestResponse()));

    AuthorityRecordCollection collection = createCollection();

    CompletableFuture<Failure> failureCalled = new CompletableFuture<>();

    collection.findByCql("title=\"*Small Angry*\"",
      new PagingParameters(10, 0),
      success -> fail("Completion callback should not be called"),
      failureCalled::complete);

    Failure failure = failureCalled.get(1000, TimeUnit.MILLISECONDS);

    assertBadRequest(failure);
  }

  @Test
  @SneakyThrows
  void serverErrorWhenCreatingAnAuthorityTriggersFailureCallback() {
    WIRE_MOCK.stubFor(any(collectionRoot())
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
    WIRE_MOCK.stubFor(any(individualItem())
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
    WIRE_MOCK.stubFor(any(collectionRoot())
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
    WIRE_MOCK.stubFor(any(individualItem())
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
    WIRE_MOCK.stubFor(any(individualItem())
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
    WIRE_MOCK.stubFor(any(collectionRoot())
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
    WIRE_MOCK.stubFor(any(collectionRoot())
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
        it -> new ExternalStorageCollections(WIRE_MOCK.baseUrl(), it.createHttpClient()))
      .getAuthorityCollection("test_tenant", "", USER_ID, REQUEST_ID);
  }

  private void assertBadRequest(Failure failure) {
    assertThat(failure.getReason(), is("Bad Request"));
    assertThat(failure.getStatusCode(), is(400));
  }

  private void assertServerError(Failure failure) {
    assertThat(failure.getReason(), is("Server Error"));
    assertThat(failure.getStatusCode(), is(500));
  }

  private ResponseDefinitionBuilder badRequestResponse() {
    return aResponse()
      .withStatus(400)
      .withBody("Bad Request")
      .withHeader("Content-Type", "text/plain");
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
