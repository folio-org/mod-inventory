package org.folio.inventory.domain.items;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public enum ItemStatusName {
  AVAILABLE("Available"),
  AWAITING_PICKUP("Awaiting pickup"),
  AWAITING_DELIVERY("Awaiting delivery"),
  CHECKED_OUT("Checked out"),
  IN_PROCESS("In process"),
  IN_TRANSIT("In transit"),
  MISSING("Missing"),
  ON_ORDER("On order"),
  PAGED("Paged"),
  WITHDRAWN("Withdrawn"),
  DECLARED_LOST("Declared lost");

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
