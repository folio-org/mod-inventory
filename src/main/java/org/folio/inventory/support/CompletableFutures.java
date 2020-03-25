package org.folio.inventory.support;

import java.util.concurrent.CompletableFuture;

public class CompletableFutures {

  public static <T> CompletableFuture<T> failedFuture(Throwable cause) {
    final CompletableFuture<T> future = new CompletableFuture<>();

    future.completeExceptionally(cause);

    return future;
  }
}
