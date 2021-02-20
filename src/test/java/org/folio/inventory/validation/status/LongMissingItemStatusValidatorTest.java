package org.folio.inventory.validation.status;

import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import lombok.SneakyThrows;
import org.folio.inventory.domain.items.Item;
import org.folio.inventory.domain.items.ItemStatusName;
import org.folio.inventory.domain.items.Status;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.folio.inventory.domain.items.ItemStatusName.LONG_MISSING;
import static org.junit.Assert.assertThrows;

@RunWith(JUnitParamsRunner.class)
public class LongMissingItemStatusValidatorTest {
  private static TargetItemStatusValidators validator;

  @BeforeClass
  public static void setUp() throws Exception {
    validator = new TargetItemStatusValidators();
  }

  @SneakyThrows
  @Parameters({
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
  @Test
  public void itemCanBeMarkedAsLongMissingWhenInAcceptableSourceStatus(String sourceStatus) {
    final var targetValidator = validator.getValidator(LONG_MISSING);

    final var item = new Item(null, null, new Status(ItemStatusName.forName(sourceStatus)), null, null, null);

    final var validationFuture = targetValidator.refuseItemWhenNotInAcceptableSourceStatus(item);

    validationFuture.get(1, TimeUnit.SECONDS);

    // Validator responds with a successful future when valid
    assertThat(validationFuture.isDone()).isTrue();
  }

  @Parameters({
    "In process",
    "Long missing"
  })
  @Test
  public void itemCannotBeMarkedAsLongMissingWhenNotInAcceptableSourceStatus(String sourceStatus) {
    final var targetValidator = validator.getValidator(LONG_MISSING);
    final var item = new Item(null, null, new Status(ItemStatusName.forName(sourceStatus)), null, null, null);
    final var validationFuture = targetValidator.refuseItemWhenNotInAcceptableSourceStatus(item);

    Exception e = assertThrows(
      Exception.class,() -> validationFuture.get(1,TimeUnit.SECONDS)
    );

    assertThat(e.getCause().getMessage()).isEqualTo("Item is not allowed to be marked as Long missing");
  }
}
