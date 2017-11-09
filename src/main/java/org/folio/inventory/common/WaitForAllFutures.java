package org.folio.inventory.common;

import org.folio.inventory.common.domain.Success;

import java.util.ArrayList;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;

public class WaitForAllFutures<T> {
  private ArrayList<CompletableFuture<T>> allFutures = new ArrayList<CompletableFuture<T>>();

  public Consumer<Void> notifyComplete() {
    CompletableFuture newFuture = new CompletableFuture();

    allFutures.add(newFuture);

    return FutureAssistance.complete(newFuture);
  }

  public Consumer<Success> notifySuccess() {

    CompletableFuture newFuture = new CompletableFuture();

    allFutures.add(newFuture);

    return FutureAssistance.succeed(newFuture);
  }

  public void waitForCompletion()
    throws InterruptedException, ExecutionException, TimeoutException {

    CompletableFuture.allOf(allFutures.toArray(new CompletableFuture<?>[] { }))
      .get(5000, TimeUnit.MILLISECONDS);
  }
}
