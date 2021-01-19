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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.folio.inventory.domain.items.ItemStatusName.IN_PROCESS;
import static org.folio.inventory.domain.items.ItemStatusName.forName;

@RunWith(JUnitParamsRunner.class)
public class TargetItemStatusValidatorsTest {
  private static TargetItemStatusValidators validator;

  @BeforeClass
  public static void setUp() throws Exception {
    validator = new TargetItemStatusValidators();
  }

  @Parameters(
    {
      "In process",
      "In process (non-requestable)",
      "Long missing"
    }
  )
  @Test
  public void targetStatusAllowMarkAs(String statusName) {
    final var targetItemStatusName = forName(statusName);
    final var statusValidator = validator.getValidator(targetItemStatusName);
    assertThat(statusValidator).isNotNull();
    final var allowedStatuses = statusValidator.getAllStatusesAllowedToMark();
    System.out.println("Allowed statuses for:"+statusValidator.getItemStatusName());
    allowedStatuses.stream().forEach(x -> {
      System.out.println("\t"+x);
      Item item = new Item(null, null, new Status(x), null, null, null);
      assertThat(statusValidator.isItemAllowedToMark(item)).isTrue();
    });

    List<ItemStatusName> disallowedStatuses = new ArrayList<>();
    Collections.addAll(disallowedStatuses,ItemStatusName.values());
    disallowedStatuses.removeAll(allowedStatuses);
    Collections.sort(disallowedStatuses);
    System.out.println("Disallowed statuses for:"+statusValidator.getItemStatusName());
    disallowedStatuses.stream().forEach(x -> {
      System.out.println("\t\""+x+"\",");
      Item item = new Item(null, null, new Status(x), null, null, null);
      assertThat(statusValidator.isItemAllowedToMark(item)).isFalse();
    });
  }

  @SneakyThrows
  @Parameters({
    "Available",
    "In transit",
    "Awaiting pickup",
    "Missing",
    "Withdrawn",
    "Lost and paid",
    "Paged",
    "Awaiting delivery",
    "Order closed"
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
