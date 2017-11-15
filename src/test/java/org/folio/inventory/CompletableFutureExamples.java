package org.folio.inventory;

import org.junit.Test;

import java.util.concurrent.CompletableFuture;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

public class CompletableFutureExamples {

  @Test
  public void canReactToException() {
    CompletableFuture<Integer> future = new CompletableFuture<>();
    Exception expectedException = new Exception("");

    CompletableFuture<Integer> exceptionally = future.exceptionally(t -> 17);

    future.completeExceptionally(expectedException);

    Integer join = exceptionally.join();

    assertThat(join, is(17));
  }
}
