package org.folio.inventory.storage.external.failure;

import static org.junit.Assert.fail;

import java.io.UnsupportedEncodingException;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.folio.Authority;
import org.folio.inventory.common.api.request.PagingParameters;
import org.folio.inventory.common.domain.Failure;
import org.folio.inventory.domain.AuthorityRecordCollection;
import org.folio.inventory.domain.CollectionProvider;
import org.junit.Test;

public abstract class ExternalAuthorityCollectionFailureExamples {

  private final CollectionProvider collectionProvider;

  ExternalAuthorityCollectionFailureExamples(CollectionProvider collectionProvider) {
    this.collectionProvider = collectionProvider;
  }

  @Test
  public void serverErrorWhenCreatingAnInstanceTriggersFailureCallback() throws InterruptedException, ExecutionException, TimeoutException {

    AuthorityRecordCollection collection = createCollection();

    CompletableFuture<Failure> failureCalled = new CompletableFuture<>();

    collection.add(createAuthority(),
        success -> fail("Completion callback should not be called"),
        failureCalled::complete);

    Failure failure = failureCalled.get(1000, TimeUnit.MILLISECONDS);

    check(failure);
  }

  @Test
  public void serverErrorWhenUpdatingAnInstanceTriggersFailureCallback()
      throws InterruptedException, ExecutionException, TimeoutException {

    AuthorityRecordCollection collection = createCollection();

    CompletableFuture<Failure> failureCalled = new CompletableFuture<>();

    collection.update(createAuthority(),
        success -> fail("Completion callback should not be called"),
        failureCalled::complete);

    Failure failure = failureCalled.get(1000, TimeUnit.MILLISECONDS);

    check(failure);
  }

  @Test
  public void serverErrorWhenGettingAllInstancesTriggersFailureCallback()
      throws InterruptedException, ExecutionException, TimeoutException {

    AuthorityRecordCollection collection = createCollection();

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

    AuthorityRecordCollection collection = createCollection();

    CompletableFuture<Failure> failureCalled = new CompletableFuture<>();

    collection.findById(UUID.randomUUID().toString(),
        success -> fail("Completion callback should not be called"),
        failureCalled::complete);

    Failure failure = failureCalled.get(1000, TimeUnit.MILLISECONDS);

    check(failure);
  }

  @Test
  public void serverErrorWhenDeletingAnInstanceByIdTriggersFailureCallback() throws InterruptedException, ExecutionException, TimeoutException {

    AuthorityRecordCollection collection = createCollection();

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

    AuthorityRecordCollection collection = createCollection();

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

    AuthorityRecordCollection collection = createCollection();

    CompletableFuture<Failure> failureCalled = new CompletableFuture<>();

    collection.findByCql("title=\"*Small Angry*\"",
        new PagingParameters(10, 0),
        success -> fail("Completion callback should not be called"),
        failureCalled::complete);

    Failure failure = failureCalled.get(1000, TimeUnit.MILLISECONDS);

    check(failure);
  }

  protected abstract void check(Failure failure);

  protected static Authority createAuthority() {
    return new Authority()
        .withId(UUID.randomUUID().toString());
  }

  protected AuthorityRecordCollection createCollection() {
    return collectionProvider.getAuthorityCollection("test_tenant", "");
  }
}
