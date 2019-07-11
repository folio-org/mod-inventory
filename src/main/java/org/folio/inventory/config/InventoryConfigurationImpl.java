package org.folio.inventory.config;

import com.google.common.collect.Sets;

import java.util.Set;

import static org.folio.inventory.domain.instances.Instance.*;

/**
 * Stores configuration properties for Inventory entities: Instances, Items, Holdings
 */
public class InventoryConfigurationImpl implements InventoryConfiguration {
  private static final Set<String> INSTANCE_BLOCKED_FIELDS = Sets.newHashSet(
    DISCOVERY_SUPPRESS_KEY,
    PREVIOUSLY_HELD_KEY,
    STATUS_ID_KEY,
    HRID_KEY,
    STAFF_SUPPRESS_KEY,
    SOURCE_KEY,
    ALTERNATIVE_TITLES_KEY,
    SERIES_KEY,
    IDENTIFIERS_KEY,
    CONTRIBUTORS_KEY,
    EDITIONS_KEY,
    PHYSICAL_DESCRIPTIONS_KEY,
    INSTANCE_FORMAT_IDS_KEY,
    LANGUAGES_KEY,
    PUBLICATION_KEY,
    PUBLICATION_FREQUENCY_KEY,
    PUBLICATION_RANGE_KEY,
    NOTES_KEY,
    ELECTRONIC_ACCESS_KEY,
    SUBJECTS_KEY,
    CLASSIFICATIONS_KEY,
    PARENT_INSTANCES_KEY,
    CHILD_INSTANCES_KEY);

  public InventoryConfigurationImpl() {
  }

  public Set<String> getInstanceBlockedFields() {
    return INSTANCE_BLOCKED_FIELDS;
  }
}
