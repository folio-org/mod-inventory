package org.folio.inventory.validation.status;

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

// This class testing transitions itself, development process support tests and is the helper for generation of
// allowed and disallowed statuses parameters lists for tests
@Ignore
@RunWith(JUnitParamsRunner.class)
public class TargetItemStatusValidatorsTest {
  private static TargetItemStatusValidators targetItemStatusValidators;

  @BeforeClass
  public static void setUp() throws Exception {
    targetItemStatusValidators = new TargetItemStatusValidators();
  }

  //@Ignore
  @Parameters(
    {
      "In process",
      "In process (non-requestable)",
      "Intellectual item",
      "Long missing",
      "Missing",
      "Restricted",
      "Unavailable",
      "Unknown",
      "Withdrawn"
    }
  )
  @Test
  public void targetStatusAllowMarkAs(String statusName) {
    final var targetItemStatusName = forName(statusName);
    final var statusValidator = targetItemStatusValidators.getValidator(targetItemStatusName);
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

//  @Parameters({
//    "In process",
//    "In process (non-requestable)",
//    "Intellectual item",
//    "Missing",
//    "Restricted",
//    "Unavailable",
//    "Unknown",
//    "Withdrawn"
//  })
//  @Test
//  public void modinv_366_transitions_scenario_1(String targetItemStatus) {
//    final var initialItemStatusName = ItemStatusName.LONG_MISSING;
//    var validator = targetItemStatusValidators.getValidator(ItemStatusName.forName(targetItemStatus));
//    Item item = new Item(null, null, new Status(initialItemStatusName), null, null, null);
//    System.out.println("Transition "+initialItemStatusName+" -> "+targetItemStatus+" is "+ (validator.isItemAllowedToMark(item) ? "Allowed" : "Disallowed"));
//    assertThat(validator.isItemAllowedToMark(item)).isTrue();
//  }

  @Parameters({
      "In process",
      "In process (non-requestable)",
      "Intellectual item",
//      "Long missing",
      "Missing",
      "Restricted",
      "Unavailable",
      "Unknown",
      "Withdrawn"
    })
  @Test
  public void modinv_366_transitions_scenario_1(String targetItemStatus) {
    final var initialItemStatusName = ItemStatusName.LONG_MISSING;
    var validator = targetItemStatusValidators.getValidator(ItemStatusName.forName(targetItemStatus));
    Item item = new Item(null, null, new Status(initialItemStatusName), null, null, null);
    System.out.println("Transition "+initialItemStatusName+" -> "+targetItemStatus+" is "+ (validator.isItemAllowedToMark(item) ? "Allowed" : "Disallowed"));
    assertThat(validator.isItemAllowedToMark(item)).isTrue();
  }

  @Parameters({
    "In process",
//    "In process (non-requestable)",
    "Intellectual item",
    "Long missing",
    "Missing",
    "Restricted",
    "Unavailable",
    "Unknown",
    "Withdrawn"
  })
  @Test
  public void modinv_366_transitions_scenario_2(String targetItemStatus) {
    final var initialItemStatusName = ItemStatusName.IN_PROCESS_NON_REQUESTABLE;
    var validator = targetItemStatusValidators.getValidator(ItemStatusName.forName(targetItemStatus));
    Item item = new Item(null, null, new Status(initialItemStatusName), null, null, null);
    System.out.println("Transition "+initialItemStatusName+" -> "+targetItemStatus+" is "+ (validator.isItemAllowedToMark(item) ? "Allowed" : "Disallowed"));
    assertThat(validator.isItemAllowedToMark(item)).isTrue();
  }

  @Parameters({
    "In process",
    "In process (non-requestable)",
    "Intellectual item",
    "Long missing",
    "Missing",
    "Restricted",
//    "Unavailable",
    "Unknown",
    "Withdrawn"
  })
  @Test
  public void modinv_366_transitions_scenario_3(String targetItemStatus) {
    final var initialItemStatusName = ItemStatusName.UNAVAILABLE;
    var validator = targetItemStatusValidators.getValidator(ItemStatusName.forName(targetItemStatus));
    Item item = new Item(null, null, new Status(initialItemStatusName), null, null, null);
    System.out.println("Transition "+initialItemStatusName+" -> "+targetItemStatus+" is "+ (validator.isItemAllowedToMark(item) ? "Allowed" : "Disallowed"));
    assertThat(validator.isItemAllowedToMark(item)).isTrue();
  }

  @Parameters({
    "In process",
    "In process (non-requestable)",
    "Intellectual item",
    "Long missing",
    "Missing",
    "Restricted",
    "Unavailable",
//    "Unknown",
    "Withdrawn"
  })
  @Test
  public void modinv_366_transitions_scenario_4(String targetItemStatus) {
    final var initialItemStatusName = ItemStatusName.UNKNOWN;
    var validator = targetItemStatusValidators.getValidator(ItemStatusName.forName(targetItemStatus));
    Item item = new Item(null, null, new Status(initialItemStatusName), null, null, null);
    System.out.println("Transition "+initialItemStatusName+" -> "+targetItemStatus+" is "+ (validator.isItemAllowedToMark(item) ? "Allowed" : "Disallowed"));
    assertThat(validator.isItemAllowedToMark(item)).isTrue();
  }

  @Parameters({
    "In process",
    "In process (non-requestable)",
//    "Intellectual item",
    "Long missing",
    "Missing",
    "Restricted",
    "Unavailable",
    "Unknown",
    "Withdrawn"
  })
  @Test
  public void modinv_366_transitions_scenario_5(String targetItemStatus) {
    final var initialItemStatusName = ItemStatusName.INTELLECTUAL_ITEM;
    var validator = targetItemStatusValidators.getValidator(ItemStatusName.forName(targetItemStatus));
    Item item = new Item(null, null, new Status(initialItemStatusName), null, null, null);
    System.out.println("Transition "+initialItemStatusName+" -> "+targetItemStatus+" is "+ (validator.isItemAllowedToMark(item) ? "Allowed" : "Disallowed"));
    assertThat(validator.isItemAllowedToMark(item)).isTrue();
  }

  @Parameters({
    "In process",
    "In process (non-requestable)",
    "Intellectual item",
    "Long missing",
    "Missing",
//    "Restricted",
    "Unavailable",
    "Unknown",
    "Withdrawn"
  })
  @Test
  public void modinv_366_transitions_scenario_6(String targetItemStatus) {
    final var initialItemStatusName = ItemStatusName.RESTRICTED;
    var validator = targetItemStatusValidators.getValidator(ItemStatusName.forName(targetItemStatus));
    Item item = new Item(null, null, new Status(initialItemStatusName), null, null, null);
    System.out.println("Transition "+initialItemStatusName+" -> "+targetItemStatus+" is "+ (validator.isItemAllowedToMark(item) ? "Allowed" : "Disallowed"));
    assertThat(validator.isItemAllowedToMark(item)).isTrue();
  }

  @Parameters({
    "In process",
//    "In process (non-requestable)",
    "Intellectual item",
    "Long missing",
    "Missing",
    "Restricted",
    "Unavailable",
    "Unknown",
    "Withdrawn"
  })
  @Test
  public void uiin_1305_transitions_scenario_2(String targetItemStatus) {
    final var initialItemStatusName = ItemStatusName.IN_PROCESS_NON_REQUESTABLE;
    var validator = targetItemStatusValidators.getValidator(ItemStatusName.forName(targetItemStatus));
    Item item = new Item(null, null, new Status(initialItemStatusName), null, null, null);
    System.out.println("Transition "+initialItemStatusName+" -> "+targetItemStatus+" is "+ (validator.isItemAllowedToMark(item) ? "Allowed" : "Disallowed"));
    assertThat(validator.isItemAllowedToMark(item)).isTrue();
  }

  @Parameters({
    "In process",
    "In process (non-requestable)",
    "Intellectual item",
    "Long missing",
    "Missing",
    "Restricted",
//    "Unavailable",
    "Unknown",
    "Withdrawn"
  })
  @Test
  public void uiin_1305_transitions_scenario_3(String targetItemStatus) {
    final var initialItemStatusName = ItemStatusName.UNAVAILABLE;
    var validator = targetItemStatusValidators.getValidator(ItemStatusName.forName(targetItemStatus));
    Item item = new Item(null, null, new Status(initialItemStatusName), null, null, null);
    System.out.println("Transition "+initialItemStatusName+" -> "+targetItemStatus+" is "+ (validator.isItemAllowedToMark(item) ? "Allowed" : "Disallowed"));
    assertThat(validator.isItemAllowedToMark(item)).isTrue();
  }

  @Parameters({
    "In process",
    "In process (non-requestable)",
    "Intellectual item",
    "Long missing",
    "Missing",
    "Restricted",
    "Unavailable",
//    "Unknown",
    "Withdrawn"
  })
  @Test
  public void uiin_1305_transitions_scenario_4(String targetItemStatus) {
    final var initialItemStatusName = ItemStatusName.UNKNOWN;
    var validator = targetItemStatusValidators.getValidator(ItemStatusName.forName(targetItemStatus));
    Item item = new Item(null, null, new Status(initialItemStatusName), null, null, null);
    System.out.println("Transition "+initialItemStatusName+" -> "+targetItemStatus+" is "+ (validator.isItemAllowedToMark(item) ? "Allowed" : "Disallowed"));
    assertThat(validator.isItemAllowedToMark(item)).isTrue();
  }

  @Parameters({
    "In process",
    "In process (non-requestable)",
//    "Intellectual item",
    "Long missing",
    "Missing",
    "Restricted",
    "Unavailable",
    "Unknown",
    "Withdrawn"
  })
  @Test
  public void uiin_1305_transitions_scenario_5(String targetItemStatus) {
    final var initialItemStatusName = ItemStatusName.INTELLECTUAL_ITEM;
    var validator = targetItemStatusValidators.getValidator(ItemStatusName.forName(targetItemStatus));
    Item item = new Item(null, null, new Status(initialItemStatusName), null, null, null);
    System.out.println("Transition "+initialItemStatusName+" -> "+targetItemStatus+" is "+ (validator.isItemAllowedToMark(item) ? "Allowed" : "Disallowed"));
    assertThat(validator.isItemAllowedToMark(item)).isTrue();
  }

  @Parameters({
    "In process",
    "In process (non-requestable)",
    "Intellectual item",
    "Long missing",
    "Missing",
//    "Restricted",
    "Unavailable",
    "Unknown",
    "Withdrawn"
  })
  @Test
  public void uiin_1305_transitions_scenario_6(String targetItemStatus) {
    final var initialItemStatusName = ItemStatusName.RESTRICTED;
    var validator = targetItemStatusValidators.getValidator(ItemStatusName.forName(targetItemStatus));
    Item item = new Item(null, null, new Status(initialItemStatusName), null, null, null);
    System.out.println("Transition "+initialItemStatusName+" -> "+targetItemStatus+" is "+ (validator.isItemAllowedToMark(item) ? "Allowed" : "Disallowed"));
    assertThat(validator.isItemAllowedToMark(item)).isTrue();
  }
}
