package org.folio.inventory.storage.external.failure;

import static api.ApiTestSuite.REQUEST_ID;
import static api.ApiTestSuite.USER_ID;
import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.any;
import static com.github.tomakehurst.wiremock.client.WireMock.urlPathMatching;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.fail;

import com.github.tomakehurst.wiremock.client.ResponseDefinitionBuilder;
import com.github.tomakehurst.wiremock.matching.UrlPathPattern;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import lombok.SneakyThrows;
import org.folio.dataimport.testsupport.rest.BaseWireMockTest;
import org.folio.inventory.common.VertxAssistant;
import org.folio.inventory.common.api.request.PagingParameters;
import org.folio.inventory.common.domain.Failure;
import org.folio.inventory.domain.instances.Instance;
import org.folio.inventory.domain.instances.InstanceCollection;
import org.folio.inventory.storage.external.ExternalStorageCollections;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

class ExternalInstanceCollectionTest extends BaseWireMockTest {

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
  void badRequestWhenCreatingAnInstanceTriggersFailureCallback() {
    WIRE_MOCK.stubFor(any(collectionRoot())
      .willReturn(badRequestResponse()));

    InstanceCollection collection = createCollection();

    CompletableFuture<Failure> failureCalled = new CompletableFuture<>();

    collection.add(createInstance(),
      success -> fail("Completion callback should not be called"),
      failureCalled::complete);

    Failure failure = failureCalled.get(1000, MILLISECONDS);

    assertBadRequest(failure);
  }

  @Test
  @SneakyThrows
  void badRequestWhenUpdatingAnInstanceTriggersFailureCallback() {
    WIRE_MOCK.stubFor(any(individualInstance())
      .willReturn(badRequestResponse()));

    InstanceCollection collection = createCollection();

    CompletableFuture<Failure> failureCalled = new CompletableFuture<>();

    collection.update(createInstance(),
      success -> fail("Completion callback should not be called"),
      failureCalled::complete);

    Failure failure = failureCalled.get(1000, MILLISECONDS);

    assertBadRequest(failure);
  }

  @Test
  @SneakyThrows
  void badRequestWhenGettingAllInstancesTriggersFailureCallback() {
    WIRE_MOCK.stubFor(any(collectionRoot())
      .willReturn(badRequestResponse()));

    InstanceCollection collection = createCollection();

    CompletableFuture<Failure> failureCalled = new CompletableFuture<>();

    collection.findAll(PagingParameters.defaults(),
      success -> fail("Completion callback should not be called"),
      failureCalled::complete);

    Failure failure = failureCalled.get(1000, MILLISECONDS);

    assertBadRequest(failure);
  }

  @Test
  @SneakyThrows
  void badRequestWhenGettingAnInstanceByIdTriggersFailureCallback() {
    WIRE_MOCK.stubFor(any(individualInstance())
      .willReturn(badRequestResponse()));

    InstanceCollection collection = createCollection();

    CompletableFuture<Failure> failureCalled = new CompletableFuture<>();

    collection.findById(UUID.randomUUID().toString(),
      success -> fail("Completion callback should not be called"),
      failureCalled::complete);

    Failure failure = failureCalled.get(1000, MILLISECONDS);

    assertBadRequest(failure);
  }

  @Test
  @SneakyThrows
  void badRequestWhenDeletingAnInstanceByIdTriggersFailureCallback() {
    WIRE_MOCK.stubFor(any(individualInstance())
      .willReturn(badRequestResponse()));

    InstanceCollection collection = createCollection();

    CompletableFuture<Failure> failureCalled = new CompletableFuture<>();

    collection.delete(UUID.randomUUID().toString(),
      success -> fail("Completion callback should not be called"),
      failureCalled::complete);

    Failure failure = failureCalled.get(1000, MILLISECONDS);

    assertBadRequest(failure);
  }

  @Test
  @SneakyThrows
  void badRequestWhenDeletingAllInstancesTriggersFailureCallback() {
    WIRE_MOCK.stubFor(any(collectionRoot())
      .willReturn(badRequestResponse()));

    InstanceCollection collection = createCollection();

    CompletableFuture<Failure> failureCalled = new CompletableFuture<>();

    collection.empty(
      success -> fail("Completion callback should not be called"),
      failureCalled::complete);

    Failure failure = failureCalled.get(1000, MILLISECONDS);

    assertBadRequest(failure);
  }

  @Test
  @SneakyThrows
  void badRequestWhenFindingItemsTriggersFailureCallback() {
    WIRE_MOCK.stubFor(any(collectionRoot())
      .willReturn(badRequestResponse()));

    InstanceCollection collection = createCollection();

    CompletableFuture<Failure> failureCalled = new CompletableFuture<>();

    collection.findByCql("title=\"*Small Angry*\"",
      new PagingParameters(10, 0),
      success -> fail("Completion callback should not be called"),
      failureCalled::complete);

    Failure failure = failureCalled.get(1000, MILLISECONDS);

    assertBadRequest(failure);
  }

  @Test
  @SneakyThrows
  void serverErrorWhenCreatingAnInstanceTriggersFailureCallback() {
    WIRE_MOCK.stubFor(any(collectionRoot())
      .willReturn(serverErrorResponse()));

    InstanceCollection collection = createCollection();

    CompletableFuture<Failure> failureCalled = new CompletableFuture<>();

    collection.add(createInstance(),
      success -> fail("Completion callback should not be called"),
      failureCalled::complete);

    Failure failure = failureCalled.get(1000, MILLISECONDS);

    assertServerError(failure);
  }

  @Test
  @SneakyThrows
  void serverErrorWhenUpdatingAnInstanceTriggersFailureCallback() {
    WIRE_MOCK.stubFor(any(individualInstance())
      .willReturn(serverErrorResponse()));

    InstanceCollection collection = createCollection();

    CompletableFuture<Failure> failureCalled = new CompletableFuture<>();

    collection.update(createInstance(),
      success -> fail("Completion callback should not be called"),
      failureCalled::complete);

    Failure failure = failureCalled.get(1000, MILLISECONDS);

    assertServerError(failure);
  }

  @Test
  @SneakyThrows
  void serverErrorWhenGettingAllInstancesTriggersFailureCallback() {
    WIRE_MOCK.stubFor(any(collectionRoot())
      .willReturn(serverErrorResponse()));

    InstanceCollection collection = createCollection();

    CompletableFuture<Failure> failureCalled = new CompletableFuture<>();

    collection.findAll(PagingParameters.defaults(),
      success -> fail("Completion callback should not be called"),
      failureCalled::complete);

    Failure failure = failureCalled.get(1000, MILLISECONDS);

    assertServerError(failure);
  }

  @Test
  @SneakyThrows
  void serverErrorWhenGettingAnInstanceByIdTriggersFailureCallback() {
    WIRE_MOCK.stubFor(any(individualInstance())
      .willReturn(serverErrorResponse()));

    InstanceCollection collection = createCollection();

    CompletableFuture<Failure> failureCalled = new CompletableFuture<>();

    collection.findById(UUID.randomUUID().toString(),
      success -> fail("Completion callback should not be called"),
      failureCalled::complete);

    Failure failure = failureCalled.get(1000, MILLISECONDS);

    assertServerError(failure);
  }

  @Test
  @SneakyThrows
  void serverErrorWhenDeletingAnInstanceByIdTriggersFailureCallback() {
    WIRE_MOCK.stubFor(any(individualInstance())
      .willReturn(serverErrorResponse()));

    InstanceCollection collection = createCollection();

    CompletableFuture<Failure> failureCalled = new CompletableFuture<>();

    collection.delete(UUID.randomUUID().toString(),
      success -> fail("Completion callback should not be called"),
      failureCalled::complete);

    Failure failure = failureCalled.get(1000, MILLISECONDS);

    assertServerError(failure);
  }

  @Test
  @SneakyThrows
  void serverErrorWhenDeletingAllInstancesTriggersFailureCallback() {
    WIRE_MOCK.stubFor(any(collectionRoot())
      .willReturn(serverErrorResponse()));

    InstanceCollection collection = createCollection();

    CompletableFuture<Failure> failureCalled = new CompletableFuture<>();

    collection.empty(
      success -> fail("Completion callback should not be called"),
      failureCalled::complete);

    Failure failure = failureCalled.get(1000, MILLISECONDS);

    assertServerError(failure);
  }

  @Test
  @SneakyThrows
  void serverErrorWhenFindingItemsTriggersFailureCallback() {
    WIRE_MOCK.stubFor(any(collectionRoot())
      .willReturn(serverErrorResponse()));

    InstanceCollection collection = createCollection();

    CompletableFuture<Failure> failureCalled = new CompletableFuture<>();

    collection.findByCql("title=\"*Small Angry*\"",
      new PagingParameters(10, 0),
      success -> fail("Completion callback should not be called"),
      failureCalled::complete);

    Failure failure = failureCalled.get(1000, MILLISECONDS);

    assertServerError(failure);
  }

  private static Instance createInstance() {
    return new Instance(
      UUID.randomUUID().toString(),
      4,
      null,
      null, "Nod", UUID.randomUUID().toString());
  }

  private InstanceCollection createCollection() {
    return vertxAssistant.createUsingVertx(
        it -> new ExternalStorageCollections(
          WIRE_MOCK.baseUrl(),
          it.createHttpClient()))
      .getInstanceCollection("test_tenant", "", USER_ID, REQUEST_ID);
  }

  private void assertBadRequest(Failure failure) {
    assertThat(failure.reason(), is("Bad Request"));
    assertThat(failure.statusCode(), is(400));
  }

  private void assertServerError(Failure failure) {
    assertThat(failure.reason(), is("Server Error"));
    assertThat(failure.statusCode(), is(500));
  }

  private ResponseDefinitionBuilder serverErrorResponse() {
    return aResponse()
      .withStatus(500)
      .withBody("Server Error")
      .withHeader("Content-Type", "text/plain");
  }

  private ResponseDefinitionBuilder badRequestResponse() {
    return aResponse()
      .withStatus(400)
      .withBody("Bad Request")
      .withHeader("Content-Type", "text/plain");
  }

  private UrlPathPattern collectionRoot() {
    return urlPathMatching("/instance-storage/instances");
  }

  private UrlPathPattern individualInstance() {
    return urlPathMatching("/instance-storage/instances/[a-z0-9/-]*");
  }
}
