package org.folio.inventory.domain.items;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public enum ItemStatusName {
  AGED_TO_LOST("Aged to lost"),
  AVAILABLE("Available"),
  AWAITING_PICKUP("Awaiting pickup"),
  AWAITING_DELIVERY("Awaiting delivery"),
  CHECKED_OUT("Checked out"),
  CLAIMED_RETURNED("Claimed returned"),
  DECLARED_LOST("Declared lost"),
  IN_PROCESS("In process"),
  IN_PROCESS_NON_REQUESTABLE("In process (non-requestable)"),
  IN_TRANSIT("In transit"),
  INTELLECTUAL_ITEM("Intellectual item"),
  LONG_MISSING("Long missing"),
  LOST_AND_PAID("Lost and paid"),
  MISSING("Missing"),
  ON_ORDER("On order"),
  ORDER_CLOSED("Order closed"),
  PAGED("Paged"),
  RESTRICTED("Restricted"),
  UNAVAILABLE("Unavailable"),
  UNKNOWN("Unknown"),
  WITHDRAWN("Withdrawn");



  private static final Map<String, ItemStatusName> VALUE_TO_INSTANCE_MAP = initValueToInstanceMap();
  private final String value;

  ItemStatusName(String value) {
    this.value = value;
  }

  @Override
  public String toString() {
    return this.value;
  }

  public String value() {
    return this.value;
  }

  public static ItemStatusName forName(String value) {
    return VALUE_TO_INSTANCE_MAP.get(value);
  }

  public static boolean isStatusCorrect(String value) {
    return VALUE_TO_INSTANCE_MAP.containsKey(value);
  }

  private static Map<String, ItemStatusName> initValueToInstanceMap() {
    final Map<String, ItemStatusName> instances = new HashMap<>();

    Arrays.stream(values())
      .forEach(itemStatusName -> instances.put(itemStatusName.value(), itemStatusName));

    return Collections.unmodifiableMap(instances);
  }
}
