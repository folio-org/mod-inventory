package org.folio.inventory.config;

import java.util.Set;

public interface InventoryConfiguration {

  /**
   * Provides set of blocked fields for Inventory Instance
   * @return set
   */
  Set<String> getInstanceBlockedFields();
}
