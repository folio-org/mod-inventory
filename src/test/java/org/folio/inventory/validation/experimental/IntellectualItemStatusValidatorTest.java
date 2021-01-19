package org.folio.inventory.validation.experimental;

import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import lombok.SneakyThrows;
import org.folio.inventory.domain.items.Item;
import org.folio.inventory.domain.items.ItemStatusName;
import org.folio.inventory.domain.items.Status;
import org.folio.inventory.exceptions.UnprocessableEntityException;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.folio.inventory.domain.items.ItemStatusName.INTELLECTUAL_ITEM;

@RunWith(JUnitParamsRunner.class)
public class IntellectualItemStatusValidatorTest {
  private static TargetItemStatusValidators validator;

  @BeforeClass
  public static void setUp() throws Exception {
    validator = new TargetItemStatusValidators();
  }

  @SneakyThrows
  @Parameters({
    "Available",
    "Awaiting delivery",
    "Awaiting pickup",
    "In transit",
    "Lost and paid",
    "Missing",
    "Order closed",
    "Paged",
    "Withdrawn"
  })
  @Test
  public void itemCanBeMarkedAsIntellectualItemWhenInAcceptableSourceStatus(String sourceStatus) {
    final var targetValidator = validator.getValidator(INTELLECTUAL_ITEM);
    assertThat(targetValidator).isNotNull();

    final var item = new Item(null, null, new Status(ItemStatusName.forName(sourceStatus)), null, null, null);
    assertThat(item).isNotNull();

    final var validationFuture = targetValidator.itemHasAllowedStatusToMark(item);
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
    "In process (non-requestable)",
    "Intellectual item",
    "Long missing",
    "On order",
    "Restricted",
    "Unavailable",
    "Unknown"
  })
  @Test
  public void itemCanNotBeMarkedAsAsIntellectualItemWhenInAcceptableSourceStatus(String sourceStatus) {
    final var targetValidator = validator.getValidator(INTELLECTUAL_ITEM);
    final var item = new Item(null, null, new Status(ItemStatusName.forName(sourceStatus)), null, null, null);
    final var validationFuture = targetValidator.itemHasAllowedStatusToMark(item);

    try {
      validationFuture.get(1, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      e.printStackTrace();
    } catch (ExecutionException e) {
      e.printStackTrace();
    } catch (TimeoutException e) {
      e.printStackTrace();
    } catch (UnprocessableEntityException e) {
      assertThat(e).hasMessage("Item is not allowed to be marked as:\"Intellectual item\"");
    }
  }
}
