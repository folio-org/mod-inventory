package org.folio.inventory.storage.external.failure;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.fail;

import java.io.UnsupportedEncodingException;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.folio.inventory.common.api.request.PagingParameters;
import org.folio.inventory.common.domain.Failure;
import org.folio.inventory.domain.CollectionProvider;
import org.folio.inventory.domain.items.Item;
import org.folio.inventory.domain.items.ItemCollection;
import org.folio.inventory.domain.items.ItemStatusName;
import org.folio.inventory.domain.items.Status;
import org.folio.inventory.storage.external.ExternalStorageCollections;
import org.junit.Test;

public class ExternalItemCollectionBadRequestExamples {

  private final CollectionProvider collectionProvider;

  public ExternalItemCollectionBadRequestExamples() {
    collectionProvider = ExternalStorageFailureSuite.createUsing(
      it -> new ExternalStorageCollections(it,
        ExternalStorageFailureSuite.getBadRequestStorageAddress(),
        it.createHttpClient()));
  }

  @Test
  public void serverErrorWhenCreatingAnItemTriggersFailureCallback()
    throws InterruptedException, ExecutionException, TimeoutException {

    ItemCollection collection = createCollection();

    CompletableFuture<Failure> failureCalled = new CompletableFuture<>();

    collection.add(createItem(),
      success -> fail("Completion callback should not be called"),
      failureCalled::complete);

    Failure failure = failureCalled.get(1000, TimeUnit.MILLISECONDS);

    check(failure);
  }

  @Test
  public void serverErrorWhenUpdatingAnItemTriggersFailureCallback()
    throws InterruptedException, ExecutionException, TimeoutException {

    ItemCollection collection = createCollection();

    CompletableFuture<Failure> failureCalled = new CompletableFuture<>();

    collection.update(createItem(),
      success -> fail("Completion callback should not be called"),
      failureCalled::complete);

    Failure failure = failureCalled.get(1000, TimeUnit.MILLISECONDS);

    check(failure);
  }

  @Test
  public void serverErrorWhenGettingAllItemsTriggersFailureCallback()
    throws InterruptedException, ExecutionException, TimeoutException {

    ItemCollection collection = createCollection();

    CompletableFuture<Failure> failureCalled = new CompletableFuture<>();

    collection.findAll(PagingParameters.defaults(),
      success -> fail("Completion callback should not be called"),
      failureCalled::complete);

    Failure failure = failureCalled.get(1000, TimeUnit.MILLISECONDS);

    check(failure);
  }

  @Test
  public void serverErrorWhenGettingAnItemByIdTriggersFailureCallback()
    throws InterruptedException, ExecutionException, TimeoutException {

    ItemCollection collection = createCollection();

    CompletableFuture<Failure> failureCalled = new CompletableFuture<>();

    collection.findById(UUID.randomUUID().toString(),
      success -> fail("Completion callback should not be called"),
      failureCalled::complete);

    Failure failure = failureCalled.get(1000, TimeUnit.MILLISECONDS);

    check(failure);
  }

  @Test
  public void serverErrorWhenDeletingAnItemByIdTriggersFailureCallback()
    throws InterruptedException, ExecutionException, TimeoutException {

    ItemCollection collection = createCollection();

    CompletableFuture<Failure> failureCalled = new CompletableFuture<>();

    collection.delete(UUID.randomUUID().toString(),
      success -> fail("Completion callback should not be called"),
      failureCalled::complete);

    Failure failure = failureCalled.get(1000, TimeUnit.MILLISECONDS);

    check(failure);
  }

  @Test
  public void serverErrorWhenDeletingAllItemsTriggersFailureCallback()
    throws InterruptedException, ExecutionException, TimeoutException {

    ItemCollection collection = createCollection();

    CompletableFuture<Failure> failureCalled = new CompletableFuture<>();

    collection.empty(
      success -> fail("Completion callback should not be called"),
      failureCalled::complete);

    Failure failure = failureCalled.get(1000, TimeUnit.MILLISECONDS);

    check(failure);
  }

  @Test
  public void serverErrorWhenFindingItemsTriggersFailureCallback()
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

    check(failure);
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

  private void check(Failure failure) {
    assertThat(failure.getReason(), is("Bad Request"));
    assertThat(failure.getStatusCode(), is(400));
  }
}
