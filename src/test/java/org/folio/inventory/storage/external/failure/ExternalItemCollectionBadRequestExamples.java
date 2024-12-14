package org.folio.inventory.storage.external.failure;

import static api.ApiTestSuite.REQUEST_ID;
import static api.ApiTestSuite.USER_ID;
import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.any;
import static com.github.tomakehurst.wiremock.client.WireMock.urlPathMatching;
import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.wireMockConfig;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.fail;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import org.folio.inventory.common.VertxAssistant;
import org.folio.inventory.common.api.request.PagingParameters;
import org.folio.inventory.common.domain.Failure;
import org.folio.inventory.domain.items.Item;
import org.folio.inventory.domain.items.ItemCollection;
import org.folio.inventory.domain.items.ItemStatusName;
import org.folio.inventory.domain.items.Status;
import org.folio.inventory.storage.external.ExternalStorageCollections;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import com.github.tomakehurst.wiremock.client.ResponseDefinitionBuilder;
import com.github.tomakehurst.wiremock.junit5.WireMockExtension;
import com.github.tomakehurst.wiremock.matching.UrlPathPattern;

import lombok.SneakyThrows;

public class ExternalItemCollectionBadRequestExamples {
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
  void badRequestWhenCreatingAnItemTriggersFailureCallback() {
    wireMockServer.stubFor(any(collectionRoot())
      .willReturn(badRequestResponse()));

    ItemCollection collection = createCollection();

    CompletableFuture<Failure> failureCalled = new CompletableFuture<>();

    collection.add(createItem(),
      success -> fail("Completion callback should not be called"),
      failureCalled::complete);

    Failure failure = failureCalled.get(1000, MILLISECONDS);

    assertBadRequest(failure);
  }

  @Test
  @SneakyThrows
  void badRequestWhenUpdatingAnItemTriggersFailureCallback() {
    wireMockServer.stubFor(any(individualItem())
      .willReturn(badRequestResponse()));

    ItemCollection collection = createCollection();

    CompletableFuture<Failure> failureCalled = new CompletableFuture<>();

    collection.update(createItem(),
      success -> fail("Completion callback should not be called"),
      failureCalled::complete);

    Failure failure = failureCalled.get(1000, MILLISECONDS);

    assertBadRequest(failure);
  }

  @Test
  @SneakyThrows
  void badRequestWhenGettingAllItemsTriggersFailureCallback() {
    wireMockServer.stubFor(any(collectionRoot())
      .willReturn(badRequestResponse()));

    ItemCollection collection = createCollection();

    CompletableFuture<Failure> failureCalled = new CompletableFuture<>();

    collection.findAll(PagingParameters.defaults(),
      success -> fail("Completion callback should not be called"),
      failureCalled::complete);

    Failure failure = failureCalled.get(1000, MILLISECONDS);

    assertBadRequest(failure);
  }

  @Test
  @SneakyThrows
  void badRequestWhenGettingAnItemByIdTriggersFailureCallback() {
    wireMockServer.stubFor(any(individualItem())
      .willReturn(badRequestResponse()));

    ItemCollection collection = createCollection();

    CompletableFuture<Failure> failureCalled = new CompletableFuture<>();

    collection.findById(UUID.randomUUID().toString(),
      success -> fail("Completion callback should not be called"),
      failureCalled::complete);

    Failure failure = failureCalled.get(1000, MILLISECONDS);

    assertBadRequest(failure);
  }

  @Test
  @SneakyThrows
  void badRequestWhenDeletingAnItemByIdTriggersFailureCallback() {
    wireMockServer.stubFor(any(individualItem())
      .willReturn(badRequestResponse()));

    ItemCollection collection = createCollection();

    CompletableFuture<Failure> failureCalled = new CompletableFuture<>();

    collection.delete(UUID.randomUUID().toString(),
      success -> fail("Completion callback should not be called"),
      failureCalled::complete);

    Failure failure = failureCalled.get(1000, MILLISECONDS);

    assertBadRequest(failure);
  }

  @Test
  @SneakyThrows
  void badRequestWhenDeletingAllItemsTriggersFailureCallback() {
    wireMockServer.stubFor(any(collectionRoot())
      .willReturn(badRequestResponse()));

    ItemCollection collection = createCollection();

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
    wireMockServer.stubFor(any(collectionRoot())
      .willReturn(badRequestResponse()));

    ItemCollection collection = createCollection();

    CompletableFuture<Failure> failureCalled = new CompletableFuture<>();

    collection.findByCql("title=\"*Small Angry*\"",
      new PagingParameters(10, 0),
      success -> fail("Completion callback should not be called"),
      failureCalled::complete);

    Failure failure = failureCalled.get(1000, MILLISECONDS);

    assertBadRequest(failure);
  }

  private static Item createItem() {
    return new Item(null,
      null,
      null,
      new Status(ItemStatusName.CHECKED_OUT), UUID.randomUUID().toString(),
      UUID.randomUUID().toString(),  null)
      .withBarcode(UUID.randomUUID().toString())
      .withEnumeration("6575467847")
      .withPermanentLocationId(UUID.randomUUID().toString())
      .withTemporaryLocationId(UUID.randomUUID().toString());
  }

  private ItemCollection createCollection() {
    return vertxAssistant.createUsingVertx(
      it -> new ExternalStorageCollections(
        wireMockServer.baseUrl(),
        it.createHttpClient()))
      .getItemCollection("test_tenant", "", USER_ID, REQUEST_ID);
  }

  private void assertBadRequest(Failure failure) {
    assertThat(failure.getReason(), is("Bad Request"));
    assertThat(failure.getStatusCode(), is(400));
  }

  private ResponseDefinitionBuilder badRequestResponse() {
    return aResponse()
      .withStatus(400)
      .withBody("Bad Request")
      .withHeader("Content-Type", "text/plain");
  }

  private UrlPathPattern collectionRoot() {
    return urlPathMatching("/item-storage/items");
  }

  private UrlPathPattern individualItem() {
    return urlPathMatching("/item-storage/items/[a-z0-9/-]*");
  }
}
