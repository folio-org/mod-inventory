package org.folio.inventory.resources;

import com.google.common.collect.Sets;

import java.util.Set;

/**
 * Stores configuration properties for Inventory entities: Instances, Items, Holdings
 */
public class InventoryConfiguration {
  protected static final Set<String> BLOCKED_FIELDS = Sets.newHashSet(
    "hrid",
    "source",
    "discoverySuppress",
    "staffSuppress",
    "previouslyHeld",
    "statusId",
    "clickable-add-statistical-code");

  private InventoryConfiguration() {
    throw new UnsupportedOperationException("Instance of the " + this.getClass().getName() + " + can not be created, because it is utility class");
  }
}
