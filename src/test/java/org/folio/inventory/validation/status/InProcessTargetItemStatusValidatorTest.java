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

class InProcessTargetItemStatusValidatorTest {

  private final InProcessTargetItemStatusValidator validator = new InProcessTargetItemStatusValidator();

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
    "In process (non-requestable)",
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
  void itemCanBeMarkedAsInProcessWhenInAcceptableSourceStatus(String sourceStatus) {
    var status = new Status(ItemStatusName.forName(sourceStatus));
    final var item = new Item(null, null, null, status, null, null, null);
    final var validationFuture = validator.refuseItemWhenNotInAcceptableSourceStatus(item);

    validationFuture.get(1, TimeUnit.SECONDS);

    // Validator responds with a successful future when valid
    assertThat(validationFuture.isDone()).isTrue();
  }

  @ParameterizedTest
  @ValueSource(strings = {
    "In process"
  })
  void itemCannotBeMarkedAsInProcessWhenNotInAcceptableSourceStatus(String sourceStatus) {
    var status = new Status(ItemStatusName.forName(sourceStatus));
    final var item = new Item(null, null, null, status, null, null, null);
    final var validationFuture = validator.refuseItemWhenNotInAcceptableSourceStatus(item);

    Exception e = assertThrows(
      Exception.class, () -> validationFuture.get(1, TimeUnit.SECONDS)
    );

    assertThat(e.getCause().getMessage()).isEqualTo("Item is not allowed to be marked as In process");
  }
}
