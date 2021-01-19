package org.folio.inventory.validation.experimental;

import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import lombok.SneakyThrows;
import org.folio.inventory.domain.items.Item;
import org.folio.inventory.domain.items.ItemStatusName;
import org.folio.inventory.domain.items.Status;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.folio.inventory.domain.items.ItemStatusName.IN_PROCESS;
import static org.folio.inventory.domain.items.ItemStatusName.forName;

@RunWith(JUnitParamsRunner.class)
public class InProcessItemStatusValidatorTest {
  private static TargetItemStatusValidator validator;

  @BeforeClass
  public static void setUp() throws Exception {
    validator = new TargetItemStatusValidator();
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
  public void itemCanBeMarkedAsInProcessWhenInAcceptableSourceStatus(String sourceStatus) {
    final var targetValidator = validator.getValidator(IN_PROCESS);
    final var item = new Item(null, null, new Status(ItemStatusName.forName(sourceStatus)), null, null, null);
    final var validationFuture = targetValidator.itemHasAllowedStatusToMark(item);

    validationFuture.get(1, TimeUnit.SECONDS);

    // Validator responds with a successful future when valid
    assertThat(validationFuture.isDone()).isTrue();
  }

  @AfterClass
  public static void tearDown() {
//    assertFalse(true);
  }
}
