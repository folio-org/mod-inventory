package org.folio.inventory.resources;

import com.google.common.collect.Sets;

import java.util.Set;

/**
 * Stores configuration properties for Instances, Items, Holdings
 */
public class InventoryConfiguration {
  public static final Set<String> BLOCKED_FIELDS = Sets.newHashSet(
    "hrid",
    "source",
    "discoverySuppress",
    "staffSuppress",
    "previouslyHeld",
    "statusId",
    "clickable-add-statistical-code");
}
