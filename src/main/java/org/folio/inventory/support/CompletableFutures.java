package org.folio.inventory.support;

import java.util.concurrent.CompletableFuture;

public final class CompletableFutures {

  private CompletableFutures() {}

  /**
   * Have to be deprecated after migration to java 11.
   */
  public static <T> CompletableFuture<T> failedFuture(Throwable cause) {
    final CompletableFuture<T> future = new CompletableFuture<>();

    future.completeExceptionally(cause);

    return future;
  }
}
