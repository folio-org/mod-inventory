package org.folio.inventory.storage.external.failure

import org.folio.inventory.common.api.request.PagingParameters
import org.folio.inventory.common.domain.Failure
import org.folio.inventory.common.domain.Success
import org.folio.inventory.domain.CollectionProvider
import org.folio.inventory.domain.Item
import org.folio.inventory.domain.ItemCollection
import org.junit.Test

import java.util.concurrent.CompletableFuture
import java.util.concurrent.TimeUnit

import static org.junit.Assert.fail

abstract class ExternalItemCollectionFailureExamples {

  protected final CollectionProvider collectionProvider

  def ExternalItemCollectionFailureExamples(CollectionProvider collectionProvider) {
    this.collectionProvider = collectionProvider
  }

  @Test
  void serverErrorWhenCreatingAnItemTriggersFailureCallback() {
    def collection = createCollection()

    def failureCalled = new CompletableFuture<Failure>()

    collection.add(createItem(),
      { Success success -> fail("Completion callback should not be called") },
      { Failure failure -> failureCalled.complete(failure) })

    def failure = failureCalled.get(1000, TimeUnit.MILLISECONDS)

    check(failure)
  }

  @Test
  void serverErrorWhenUpdatingAnItemTriggersFailureCallback() {
    def collection = createCollection()

    def failureCalled = new CompletableFuture<Failure>()

    collection.update(createItem(),
      { Success success -> fail("Completion callback should not be called") },
      { Failure failure -> failureCalled.complete(failure) })

    def failure = failureCalled.get(1000, TimeUnit.MILLISECONDS)

    check(failure)
  }

  @Test
  void serverErrorWhenGettingAllItemsTriggersFailureCallback() {
    def collection = createCollection()

    def failureCalled = new CompletableFuture<Failure>()

    collection.findAll(PagingParameters.defaults(),
      { Success success -> fail("Results callback should not be called") },
      { Failure failure -> failureCalled.complete(failure) })

    def failure = failureCalled.get(1000, TimeUnit.MILLISECONDS)

    check(failure)
  }

  @Test
  void serverErrorWhenGettingAnItemByIdTriggersFailureCallback() {
    def collection = createCollection()

    def failureCalled = new CompletableFuture<Failure>()

    collection.findById(UUID.randomUUID().toString(),
      { Success success -> fail("Results callback should not be called") },
      { Failure failure -> failureCalled.complete(failure) })

    def failure = failureCalled.get(1000, TimeUnit.MILLISECONDS)

    check(failure)
  }

  @Test
  void serverErrorWhenDeletingAnItemByIdTriggersFailureCallback() {
    def collection = createCollection()

    def failureCalled = new CompletableFuture<Failure>()

    collection.delete(UUID.randomUUID().toString(),
      { Success success -> fail("Completion callback should not be called") },
      { Failure failure -> failureCalled.complete(failure) })

    def failure = failureCalled.get(1000, TimeUnit.MILLISECONDS)

    check(failure)
  }

  @Test
  void serverErrorWhenDeletingAllItemsTriggersFailureCallback() {
    def collection = createCollection()

    def failureCalled = new CompletableFuture<Failure>()

    collection.empty(
      { Success success -> fail("Completion callback should not be called") },
      { Failure failure -> failureCalled.complete(failure) })

    def failure = failureCalled.get(1000, TimeUnit.MILLISECONDS)

    check(failure)
  }

  @Test
  void serverErrorWhenFindingItemsTriggersFailureCallback() {
    def collection = createCollection()

    def failureCalled = new CompletableFuture<Failure>()

    collection.findByCql("title=\"*Small Angry*\"",
      new PagingParameters(10, 0),
      { Success success -> fail("Success callback should not be called") },
      { Failure failure -> failureCalled.complete(failure) })

    def failure = failureCalled.get(1000, TimeUnit.MILLISECONDS)

    check(failure)
  }

  protected abstract check(Failure failure)

  protected Item createItem() {
    new Item(null, UUID.randomUUID().toString(), "Nod", "6575467847",
      "${UUID.randomUUID()}", "${UUID.randomUUID()}",
      "${UUID.randomUUID()}","${UUID.randomUUID()}","${UUID.randomUUID()}", null)
  }

  private ItemCollection createCollection() {
    collectionProvider.getItemCollection("test_tenant", "")
  }
}
