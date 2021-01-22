package org.folio.inventory.validation.experimental;

import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import org.folio.inventory.domain.items.Item;
import org.folio.inventory.domain.items.ItemStatusName;

import org.folio.inventory.domain.items.Status;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.folio.inventory.domain.items.ItemStatusName.forName;
// This class testing transitions itself and is the helper for generation of
// allowed and disallowed statuses parameters lists for tests
@Ignore
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
      "Intellectual item",
      "Long missing",
      "Missing",
      "Restricted",
      "Unavailable",
      "Unknown"
    }
  )
  @Test
  public void targetStatusAllowMarkAs(String statusName) {
    final var targetItemStatusName = forName(statusName);
    final var statusValidator = validator.getValidator(targetItemStatusName);
    assertThat(statusValidator).isNotNull();
    List<ItemStatusName> allowedStatuses = new ArrayList<>(statusValidator.getAllStatusesAllowedToMark());
    Collections.sort(allowedStatuses);
    assertThat(statusValidator.getItemStatusName().value()).isEqualTo(statusName);
    System.out.println("Allowed statuses for:"+statusValidator.getItemStatusName());
    allowedStatuses.stream().forEach(x -> {
      System.out.println("\t\""+x+"\",");
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
}
