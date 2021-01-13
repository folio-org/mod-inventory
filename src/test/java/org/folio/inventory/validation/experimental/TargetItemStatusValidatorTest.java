package org.folio.inventory.validation.experimental;

import org.assertj.core.util.Arrays;
import org.folio.inventory.domain.items.Item;
import org.folio.inventory.domain.items.ItemStatusName;

import org.folio.inventory.domain.items.Status;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import static org.junit.Assert.*;

public class TargetItemStatusValidatorTest {
  private static TargetItemStatusValidator validator;

  @BeforeClass
  public static void setUp() throws Exception {
    validator = new TargetItemStatusValidator();
  }

  @Test
  public void inProcesAllowMarkAs() {
    ItemStatusName targetItemStatusName = ItemStatusName.IN_PROCESS;
    final AbstractTargetValidator statusValidator = validator.getValidator(targetItemStatusName);

    Set<ItemStatusName> allowedStatuses = statusValidator.getAllStatusesAllowedToMark();
    System.out.println("Allowed statuses for:"+statusValidator.getItemStatusName());
    allowedStatuses.stream().forEach(x -> {
      System.out.println("\t"+x);
      Item item = new Item(null, null, new Status(x), null, null, null);
      assertTrue(statusValidator.isItemAllowedToMark(item));
    });

    Set<ItemStatusName> disallowedStatuses = new HashSet<>();
    Collections.addAll(disallowedStatuses,ItemStatusName.values());
    disallowedStatuses.removeAll(allowedStatuses);
    System.out.println("Disallowed statuses for:"+statusValidator.getItemStatusName());
    disallowedStatuses.stream().forEach(x -> {
      System.out.println("\t"+x);
      Item item = new Item(null, null, new Status(x), null, null, null);
      assertFalse(statusValidator.isItemAllowedToMark(item));
    });
  }

  @Test
  public void inProcessNonRequestableAllowMarkAs() {
    ItemStatusName targetItemStatusName = ItemStatusName.IN_PROCESS_NON_REQUESTABLE;
    final AbstractTargetValidator statusValidator = validator.getValidator(targetItemStatusName);

    Set<ItemStatusName> allowedStatuses = statusValidator.getAllStatusesAllowedToMark();
    System.out.println("Allowed statuses for:"+statusValidator.getItemStatusName());
    allowedStatuses.stream().forEach(x -> {
      System.out.println("\t"+x);
      Item item = new Item(null, null, new Status(x), null, null, null);
      assertTrue(statusValidator.isItemAllowedToMark(item));
    });

    Set<ItemStatusName> disallowedStatuses = new HashSet<>();
    Collections.addAll(disallowedStatuses,ItemStatusName.values());
    disallowedStatuses.removeAll(allowedStatuses);
    System.out.println("Disallowed statuses for:"+statusValidator.getItemStatusName());
    disallowedStatuses.stream().forEach(x -> {
      System.out.println("\t"+x);
      Item item = new Item(null, null, new Status(x), null, null, null);
      assertFalse(statusValidator.isItemAllowedToMark(item));
    });

  }


  @AfterClass
  public static void tearDown() {
//    assertFalse(true);
  }

}
