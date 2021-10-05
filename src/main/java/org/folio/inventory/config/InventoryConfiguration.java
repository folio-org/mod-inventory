package org.folio.inventory.config;

import java.util.Set;

public interface InventoryConfiguration {
  /**
   * Provides set of blocked fields for Inventory Instance
   * @return Set of String field codes
   */
  Set<String> getInstanceBlockedFields();

  /**
   * Provides set of blocked fields for Inventory Holdings record
   * @return Set of String field codes
   */
  Set<String> getHoldingsBlockedFields();
}
