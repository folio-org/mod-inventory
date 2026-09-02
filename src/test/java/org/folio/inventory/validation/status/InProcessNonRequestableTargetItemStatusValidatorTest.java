package org.folio.inventory.validation.status;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.concurrent.TimeUnit;
import lombok.SneakyThrows;
import org.folio.inventory.domain.items.Item;
import org.folio.inventory.domain.items.ItemStatusName;
import org.folio.inventory.domain.items.Status;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class InProcessNonRequestableTargetItemStatusValidatorTest {

  private final InProcessNonRequestableTargetItemStatusValidator validator
    = new InProcessNonRequestableTargetItemStatusValidator();

  @SneakyThrows
  @ParameterizedTest
  @ValueSource(strings = {
    "Aged to lost",
    "Available",
    "Awaiting pickup",
    "Awaiting delivery",
    "Checked out",
    "Claimed returned",
    "Declared lost",
    "In transit",
    "Intellectual item",
    "Long missing",
    "Lost and paid",
    "Missing",
    "On order",
    "Order closed",
    "Paged",
    "Restricted",
    "Unavailable",
    "Unknown",
    "Withdrawn"
  })
  void itemCanBeMarkedAsInProcessNonRequestableWhenInAcceptableSourceStatus(String sourceStatus) {
    final var item = new Item(null, null, null, new Status(ItemStatusName.forName(sourceStatus)), null, null, null);
    final var validationFuture = validator.refuseItemWhenNotInAcceptableSourceStatus(item);

    validationFuture.get(1, TimeUnit.SECONDS);

    // Validator responds with a successful future when valid
    assertThat(validationFuture.isDone()).isTrue();
  }

  @ParameterizedTest
  @ValueSource(strings = {
    "In process",
    "In process (non-requestable)"
  })
  void itemCannotBeMarkedAsInProcessNonRequestableWhenNotInAcceptableSourceStatus(String sourceStatus) {
    final var item = new Item(null, null, null, new Status(ItemStatusName.forName(sourceStatus)), null, null, null);
    final var validationFuture = validator.refuseItemWhenNotInAcceptableSourceStatus(item);

    Exception e = assertThrows(
      Exception.class, () -> validationFuture.get(1, TimeUnit.SECONDS)
    );

    assertThat(e.getCause().getMessage()).isEqualTo("Item is not allowed to be marked as In process (non-requestable)");
  }
}
