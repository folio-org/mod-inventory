package org.folio.inventory.common;

import org.folio.inventory.common.domain.Failure;
import org.folio.inventory.common.domain.Success;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;

public class FutureAssistance {
  public static <T> T getOnCompletion(CompletableFuture<T> future)
    throws InterruptedException, ExecutionException, TimeoutException {

    return future.get(2000, TimeUnit.MILLISECONDS);
  }

  public static void waitForCompletion(CompletableFuture future)
    throws InterruptedException, ExecutionException, TimeoutException {

    future.get(2000, TimeUnit.MILLISECONDS);
  }

  public static Consumer<Success<Void>> complete(final CompletableFuture<Success<Void>> future) {
    return v -> future.complete(null);
  }

  public static <T> Consumer<Success<T>> succeed(final CompletableFuture<T> future) {
    return success -> future.complete(success.getResult());
  }

  public static Consumer<Failure> fail(final CompletableFuture future) {
    return failure -> future.completeExceptionally(new Exception(failure.getReason()));
  }

  public static <T> CompletableFuture<Void> allOf(
    List<CompletableFuture<T>> allFutures) {

    return CompletableFuture.allOf(allFutures.toArray(new CompletableFuture<?>[] { }));
  }
}
