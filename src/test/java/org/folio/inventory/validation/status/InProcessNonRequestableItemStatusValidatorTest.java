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
import static org.folio.inventory.domain.items.ItemStatusName.IN_PROCESS_NON_REQUESTABLE;
import static org.junit.Assert.assertThrows;

@RunWith(JUnitParamsRunner.class)
public class InProcessNonRequestableItemStatusValidatorTest {
  private static TargetItemStatusValidators validators;

  @BeforeClass
  public static void setUp() throws Exception {
    validators = new TargetItemStatusValidators();
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
  @Test
  public void itemCanBeMarkedAsInProcessNonRequestableWhenInAcceptableSourceStatus(String sourceStatus) {
    final var targetValidator = validators.getValidator(IN_PROCESS_NON_REQUESTABLE);

    final var item = new Item(null, null, null, new Status(ItemStatusName.forName(sourceStatus)), null, null, null);

    final var validationFuture = targetValidator.refuseItemWhenNotInAcceptableSourceStatus(item);

    validationFuture.get(1, TimeUnit.SECONDS);

    // Validator responds with a successful future when valid
    assertThat(validationFuture.isDone()).isTrue();
  }

  @Parameters({
    "In process",
    "In process (non-requestable)"
  })
  @Test
  public void itemCannotBeMarkedAsInProcessNonRequestableWhenNotInAcceptableSourceStatus(String sourceStatus) {
    final var targetValidator = validators.getValidator(IN_PROCESS_NON_REQUESTABLE);
    final var item = new Item(null, null, null, new Status(ItemStatusName.forName(sourceStatus)), null, null, null);
    final var validationFuture = targetValidator.refuseItemWhenNotInAcceptableSourceStatus(item);

    Exception e = assertThrows(
      Exception.class,() -> validationFuture.get(1,TimeUnit.SECONDS)
    );

    assertThat(e.getCause().getMessage()).isEqualTo("Item is not allowed to be marked as In process (non-requestable)");
  }
}
