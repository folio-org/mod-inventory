package org.folio.inventory.domain;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

import io.vertx.core.Future;
import org.folio.inventory.common.api.request.PagingParameters;
import org.folio.inventory.common.domain.Failure;
import org.folio.inventory.common.domain.MultipleRecords;
import org.folio.inventory.common.domain.Success;
import org.folio.inventory.exceptions.InternalServerErrorException;

public interface AsynchronousCollection<T> {
  void empty(String cqlQuery,
             Consumer<Success<Void>> completionCallback,
             Consumer<Failure> failureCallback);

  default void empty(Consumer<Success<Void>> completionCallback,
                     Consumer<Failure> failureCallback) {
    empty("cql.allRecords=1", completionCallback, failureCallback);
  }

  void add(T item,
           Consumer<Success<T>> resultCallback,
           Consumer<Failure> failureCallback);

  default CompletableFuture<T> add(T item) {
    final CompletableFuture<T> future = new CompletableFuture<>();

    add(item, success -> future.complete(success.getResult()),
      failure -> future.completeExceptionally(
        new InternalServerErrorException(failure)));

    return future;
  }

  void findById(String id,
                Consumer<Success<T>> resultCallback,
                Consumer<Failure> failureCallback);

  default CompletableFuture<T> findById(String id) {
    final CompletableFuture<T> future = new CompletableFuture<>();

    findById(id, success -> future.complete(success.getResult()),
      failure -> future.completeExceptionally(new InternalServerErrorException(failure.getReason())));

    return future;
  }

  void findAll(PagingParameters pagingParameters,
               Consumer<Success<MultipleRecords<T>>> resultsCallback,
               Consumer<Failure> failureCallback);

  void delete(String id,
              Consumer<Success<Void>> completionCallback,
              Consumer<Failure> failureCallback);

  void update(T item,
              Consumer<Success<Void>> completionCallback,
              Consumer<Failure> failureCallback);

  Future<T> updateAsync(final T item);

  default CompletableFuture<T> update(final T item) {
    final CompletableFuture<T> future = new CompletableFuture<>();

    update(item, success -> future.complete(item),
      failure -> future.completeExceptionally(new InternalServerErrorException(failure)));

    return future;
  }

  default void addBatch(List<T> items,
                        Consumer<Success<BatchResult<T>>> resultCallback,
                        Consumer<Failure> failureCallback) {
    throw new UnsupportedOperationException();
  }
}
