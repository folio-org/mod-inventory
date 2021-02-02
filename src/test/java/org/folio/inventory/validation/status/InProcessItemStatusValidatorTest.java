package org.folio.inventory.validation.status;

import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import lombok.SneakyThrows;
import org.folio.inventory.domain.items.Item;
import org.folio.inventory.domain.items.ItemStatusName;
import org.folio.inventory.domain.items.Status;
import org.folio.inventory.validation.status.TargetItemStatusValidators;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.folio.inventory.domain.items.ItemStatusName.IN_PROCESS;
import static org.junit.Assert.assertThrows;

@RunWith(JUnitParamsRunner.class)
public class InProcessItemStatusValidatorTest {
  private static TargetItemStatusValidators validators;

  @BeforeClass
  public static void setUp() throws Exception {
    validators = new TargetItemStatusValidators();
  }

  @SneakyThrows
  @Parameters({
    "Available",
    "Awaiting pickup",
    "Awaiting delivery",
    "In process (non-requestable)",
    "In transit",
    "Intellectual item",
    "Long missing",
    "Lost and paid",
    "Missing",
    "Order closed",
    "Paged",
    "Restricted",
    "Unavailable",
    "Unknown",
    "Withdrawn"
  })
  @Test
  public void itemCanBeMarkedAsInProcessWhenInAcceptableSourceStatus(String sourceStatus) {
    final var targetValidator = validators.getValidator(IN_PROCESS);
    final var item = new Item(null, null, new Status(ItemStatusName.forName(sourceStatus)), null, null, null);
    final var validationFuture = targetValidator.refuseItemWhenNotInAcceptableSourceStatus(item);

    validationFuture.get(1, TimeUnit.SECONDS);

    // Validator responds with a successful future when valid
    assertThat(validationFuture.isDone()).isTrue();
  }

  @Parameters({
    "Aged to lost",
    "Checked out",
    "Claimed returned",
    "Declared lost",
    "In process",
    "On order"
  })
  @Test
  public void itemCannotBeMarkedAsInProcessWhenNotInAcceptableSourceStatus(String sourceStatus) {
    final var targetValidator = validators.getValidator(IN_PROCESS);
    final var item = new Item(null, null, new Status(ItemStatusName.forName(sourceStatus)), null, null, null);
    final var validationFuture = targetValidator.refuseItemWhenNotInAcceptableSourceStatus(item);

    Exception e = assertThrows(
      Exception.class,() -> validationFuture.get(1,TimeUnit.SECONDS)
    );

    assertThat(e.getCause().getMessage()).isEqualTo("Item is not allowed to be marked as In process");
  }
}
