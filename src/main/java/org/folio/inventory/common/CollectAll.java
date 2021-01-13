package org.folio.inventory.common;

import org.folio.inventory.common.domain.Success;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import java.util.stream.Collectors;

public class CollectAll<T> {
  private final ArrayList<CompletableFuture<T>> allFutures = new ArrayList<>();

  public Consumer<Success<T>> receive() {
    CompletableFuture newFuture = new CompletableFuture();

    allFutures.add(newFuture);

    return FutureAssistance.succeed(newFuture);
  }

  public void collect(Consumer<List<T>> action) {
    CompletableFuture.allOf(allFutures.toArray(new CompletableFuture<?>[] { }))
      .thenAccept(v -> action.accept(allFutures.stream()
        .map(CompletableFuture::join)
        .collect(Collectors.toList())));
  }
}
