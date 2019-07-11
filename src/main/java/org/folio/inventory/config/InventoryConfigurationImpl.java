package org.folio.inventory.config;

import com.google.common.collect.Sets;

import java.util.Set;

/**
 * Stores configuration properties for Inventory entities: Instances, Items, Holdings
 */
public class InventoryConfigurationImpl implements InventoryConfiguration {
  private static final Set<String> INSTANCE_BLOCKED_FIELDS = Sets.newHashSet(
    "discoverySuppress",
    "previouslyHeld",
    "statusId",
    "hrid",
    "staffSuppress",
    "source",
    "alternativeTitles",
    "series",
    "identifiers",
    "contributors",
    "publication",
    "editions",
    "physicalDescriptions",
    "instanceFormatIds",
    "languages",
    "publicationFrequency",
    "publicationRange",
    "notes",
    "electronicAccess",
    "subjects",
    "classifications",
    "parentInstances",
    "childInstances");

  public InventoryConfigurationImpl() {
  }

  public Set<String> getInstanceBlockedFields() {
    return INSTANCE_BLOCKED_FIELDS;
  }
}
