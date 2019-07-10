package org.folio.inventory.config;

import com.google.common.collect.Sets;

import java.util.Set;

/**
 * Stores configuration properties for Inventory entities: Instances, Items, Holdings
 */
public class InventoryConfigurationImpl implements InventoryConfiguration {
  private static final Set<String> INSTANCE_BLOCKED_FIELDS = Sets.newHashSet(
    "hrid",
    "source",
    "discoverySuppress",
    "staffSuppress",
    "previouslyHeld",
    "statusId",
    "clickable-add-statistical-code");

  public InventoryConfigurationImpl() {
  }

  public Set<String> getInstanceBlockedFields() {
    return INSTANCE_BLOCKED_FIELDS;
  }
}
