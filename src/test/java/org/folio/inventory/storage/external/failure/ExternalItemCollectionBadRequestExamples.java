package org.folio.inventory.storage.external.failure;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.any;
import static com.github.tomakehurst.wiremock.client.WireMock.urlPathMatching;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.fail;

import java.io.UnsupportedEncodingException;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.folio.inventory.common.VertxAssistant;
import org.folio.inventory.common.api.request.PagingParameters;
import org.folio.inventory.common.domain.Failure;
import org.folio.inventory.domain.CollectionProvider;
import org.folio.inventory.domain.items.Item;
import org.folio.inventory.domain.items.ItemCollection;
import org.folio.inventory.domain.items.ItemStatusName;
import org.folio.inventory.domain.items.Status;
import org.folio.inventory.storage.external.ExternalStorageCollections;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import com.github.tomakehurst.wiremock.junit.WireMockRule;

public class ExternalItemCollectionBadRequestExamples {
  private static final VertxAssistant vertxAssistant = new VertxAssistant();
  private static CollectionProvider collectionProvider;

  @ClassRule
  public static WireMockRule wireMockServer = new WireMockRule();

  @BeforeClass
  public static void beforeAll() {
    vertxAssistant.start();

    collectionProvider = vertxAssistant.createUsingVertx(
      it -> new ExternalStorageCollections(it,
        wireMockServer.url("bad-request"),
        it.createHttpClient()));

    final var badRequestResponse = aResponse()
      .withStatus(400)
      .withBody("Bad Request")
      .withHeader("Content-Type", "text/plain");

    wireMockServer.stubFor(any(urlPathMatching("/bad-request/item-storage/items"))
      .willReturn(badRequestResponse));

    wireMockServer.stubFor(any(urlPathMatching("/bad-request/item-storage/items/[a-z0-9/-]*"))
      .willReturn(badRequestResponse));
  }

  @AfterClass
  public static void afterAll() {
    vertxAssistant.stop();
  }

  @Test
  public void badRequestWhenCreatingAnItemTriggersFailureCallback()
    throws InterruptedException, ExecutionException, TimeoutException {

    ItemCollection collection = createCollection();

    CompletableFuture<Failure> failureCalled = new CompletableFuture<>();

    collection.add(createItem(),
      success -> fail("Completion callback should not be called"),
      failureCalled::complete);

    Failure failure = failureCalled.get(1000, TimeUnit.MILLISECONDS);

    assertBadRequest(failure);
  }

  @Test
  public void badRequestWhenUpdatingAnItemTriggersFailureCallback()
    throws InterruptedException, ExecutionException, TimeoutException {

    ItemCollection collection = createCollection();

    CompletableFuture<Failure> failureCalled = new CompletableFuture<>();

    collection.update(createItem(),
      success -> fail("Completion callback should not be called"),
      failureCalled::complete);

    Failure failure = failureCalled.get(1000, TimeUnit.MILLISECONDS);

    assertBadRequest(failure);
  }

  @Test
  public void badRequestWhenGettingAllItemsTriggersFailureCallback()
    throws InterruptedException, ExecutionException, TimeoutException {

    ItemCollection collection = createCollection();

    CompletableFuture<Failure> failureCalled = new CompletableFuture<>();

    collection.findAll(PagingParameters.defaults(),
      success -> fail("Completion callback should not be called"),
      failureCalled::complete);

    Failure failure = failureCalled.get(1000, TimeUnit.MILLISECONDS);

    assertBadRequest(failure);
  }

  @Test
  public void badRequestWhenGettingAnItemByIdTriggersFailureCallback()
    throws InterruptedException, ExecutionException, TimeoutException {

    ItemCollection collection = createCollection();

    CompletableFuture<Failure> failureCalled = new CompletableFuture<>();

    collection.findById(UUID.randomUUID().toString(),
      success -> fail("Completion callback should not be called"),
      failureCalled::complete);

    Failure failure = failureCalled.get(1000, TimeUnit.MILLISECONDS);

    assertBadRequest(failure);
  }

  @Test
  public void badRequestWhenDeletingAnItemByIdTriggersFailureCallback()
    throws InterruptedException, ExecutionException, TimeoutException {

    ItemCollection collection = createCollection();

    CompletableFuture<Failure> failureCalled = new CompletableFuture<>();

    collection.delete(UUID.randomUUID().toString(),
      success -> fail("Completion callback should not be called"),
      failureCalled::complete);

    Failure failure = failureCalled.get(1000, TimeUnit.MILLISECONDS);

    assertBadRequest(failure);
  }

  @Test
  public void badRequestWhenDeletingAllItemsTriggersFailureCallback()
    throws InterruptedException, ExecutionException, TimeoutException {

    ItemCollection collection = createCollection();

    CompletableFuture<Failure> failureCalled = new CompletableFuture<>();

    collection.empty(
      success -> fail("Completion callback should not be called"),
      failureCalled::complete);

    Failure failure = failureCalled.get(1000, TimeUnit.MILLISECONDS);

    assertBadRequest(failure);
  }

  @Test
  public void badRequestWhenFindingItemsTriggersFailureCallback()
    throws InterruptedException,
    ExecutionException,
    TimeoutException,
    UnsupportedEncodingException {

    ItemCollection collection = createCollection();

    CompletableFuture<Failure> failureCalled = new CompletableFuture<>();

    collection.findByCql("title=\"*Small Angry*\"",
      new PagingParameters(10, 0),
      success -> fail("Completion callback should not be called"),
      failureCalled::complete);

    Failure failure = failureCalled.get(1000, TimeUnit.MILLISECONDS);

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
    return collectionProvider.getItemCollection("test_tenant", "");
  }

  private void assertBadRequest(Failure failure) {
    assertThat(failure.getReason(), is("Bad Request"));
    assertThat(failure.getStatusCode(), is(400));
  }
}
