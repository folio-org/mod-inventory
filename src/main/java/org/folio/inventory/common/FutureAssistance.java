package org.folio.inventory.common;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;
import lombok.experimental.UtilityClass;
import org.folio.inventory.common.domain.Failure;
import org.folio.inventory.common.domain.Success;

@UtilityClass
public class FutureAssistance {

  public static <T> T getOnCompletion(CompletableFuture<T> future)
    throws InterruptedException, ExecutionException, TimeoutException {

    return future.get(2000, TimeUnit.MILLISECONDS);
  }

  public static <T> T getOnCompletion(Consumer<CompletableFuture<T>> task)
    throws InterruptedException, ExecutionException, TimeoutException {

    CompletableFuture<T> future = new CompletableFuture<>();
    task.accept(future);
    return getOnCompletion(future);
  }

  public static <T> void waitForCompletion(CompletableFuture<T> future)
    throws InterruptedException, ExecutionException, TimeoutException {

    future.get(2000, TimeUnit.MILLISECONDS);
  }

  public static <T> void waitForCompletion(Consumer<CompletableFuture<T>> task)
    throws InterruptedException, ExecutionException, TimeoutException {

    CompletableFuture<T> future = new CompletableFuture<>();
    task.accept(future);
    waitForCompletion(future);
  }

  public static <T> Consumer<Success<T>> succeed(final CompletableFuture<T> future) {
    return success -> future.complete(success.result());
  }

  public static Consumer<Failure> fail(final CompletableFuture<?> future) {
    return failure -> future.completeExceptionally(new Exception(failure.reason()));
  }

  public static <T> CompletableFuture<Void> allOf(
    List<CompletableFuture<T>> allFutures) {

    return CompletableFuture.allOf(allFutures.toArray(new CompletableFuture<?>[] { }));
  }
}
