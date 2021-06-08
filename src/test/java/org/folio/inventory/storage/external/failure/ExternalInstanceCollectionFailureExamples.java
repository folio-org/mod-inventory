package org.folio.inventory.storage.external.failure;

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
import org.folio.inventory.domain.instances.Instance;
import org.folio.inventory.domain.instances.InstanceCollection;
import org.junit.Test;

public abstract class ExternalInstanceCollectionFailureExamples {

  private final CollectionProvider collectionProvider;

  ExternalInstanceCollectionFailureExamples(CollectionProvider collectionProvider) {
    this.collectionProvider = collectionProvider;
  }

  @Test
  public void serverErrorWhenCreatingAnInstanceTriggersFailureCallback()
    throws InterruptedException, ExecutionException, TimeoutException {

    InstanceCollection collection = createCollection();

    CompletableFuture<Failure> failureCalled = new CompletableFuture<>();

    collection.add(createInstance(),
      success -> fail("Completion callback should not be called"),
      failureCalled::complete);

    Failure failure = failureCalled.get(1000, TimeUnit.MILLISECONDS);

    check(failure);
  }

  @Test
  public void serverErrorWhenUpdatingAnInstanceTriggersFailureCallback()
    throws InterruptedException, ExecutionException, TimeoutException {

    InstanceCollection collection = createCollection();

    CompletableFuture<Failure> failureCalled = new CompletableFuture<>();

    collection.update(createInstance(),
      success -> fail("Completion callback should not be called"),
      failureCalled::complete);

    Failure failure = failureCalled.get(1000, TimeUnit.MILLISECONDS);

    check(failure);
  }

  @Test
  public void serverErrorWhenGettingAllInstancesTriggersFailureCallback()
    throws InterruptedException, ExecutionException, TimeoutException {

    InstanceCollection collection = createCollection();

    CompletableFuture<Failure> failureCalled = new CompletableFuture<>();

    collection.findAll(PagingParameters.defaults(),
      success -> fail("Completion callback should not be called"),
      failureCalled::complete);

    Failure failure = failureCalled.get(1000, TimeUnit.MILLISECONDS);

    check(failure);
  }

  @Test
  public void serverErrorWhenGettingAnInstanceByIdTriggersFailureCallback()
    throws InterruptedException, ExecutionException, TimeoutException {

    InstanceCollection collection = createCollection();

    CompletableFuture<Failure> failureCalled = new CompletableFuture<>();

    collection.findById(UUID.randomUUID().toString(),
      success -> fail("Completion callback should not be called"),
      failureCalled::complete);

    Failure failure = failureCalled.get(1000, TimeUnit.MILLISECONDS);

    check(failure);
  }

  @Test
  public void serverErrorWhenDeletingAnInstanceByIdTriggersFailureCallback()
    throws InterruptedException, ExecutionException, TimeoutException {

    InstanceCollection collection = createCollection();

    CompletableFuture<Failure> failureCalled = new CompletableFuture<>();

    collection.delete(UUID.randomUUID().toString(),
      success -> fail("Completion callback should not be called"),
      failureCalled::complete);

    Failure failure = failureCalled.get(1000, TimeUnit.MILLISECONDS);

    check(failure);
  }

  @Test
  public void serverErrorWhenDeletingAllInstancesTriggersFailureCallback()
    throws InterruptedException, ExecutionException, TimeoutException {

    InstanceCollection collection = createCollection();

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

    InstanceCollection collection = createCollection();

    CompletableFuture<Failure> failureCalled = new CompletableFuture<>();

    collection.findByCql("title=\"*Small Angry*\"",
      new PagingParameters(10, 0),
      success -> fail("Completion callback should not be called"),
      failureCalled::complete);

    Failure failure = failureCalled.get(1000, TimeUnit.MILLISECONDS);

    check(failure);
  }

  protected abstract void check(Failure failure);

  private static Instance createInstance() {
    return new Instance(
            UUID.randomUUID().toString(),
            "4",
            null,
            null, "Nod", UUID.randomUUID().toString());
  }

  private InstanceCollection createCollection() {
    return collectionProvider.getInstanceCollection("test_tenant", "");
  }
}
