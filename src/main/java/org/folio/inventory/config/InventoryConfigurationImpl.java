package org.folio.inventory.config;

import com.google.common.collect.Sets;
import org.folio.inventory.domain.instances.Instance;

import java.util.Set;

/**
 * Stores configuration properties for Inventory entities: Instances, Items, Holdings
 */
public class InventoryConfigurationImpl implements InventoryConfiguration {
  private static final Set<String> INSTANCE_BLOCKED_FIELDS = Sets.newHashSet(
    Instance.HRID_KEY,
    Instance.SOURCE_KEY,
    Instance.ALTERNATIVE_TITLES_KEY,
    Instance.SERIES_KEY,
    Instance.IDENTIFIERS_KEY,
    Instance.CONTRIBUTORS_KEY,
    Instance.EDITIONS_KEY,
    Instance.PHYSICAL_DESCRIPTIONS_KEY,
    Instance.INSTANCE_FORMAT_IDS_KEY,
    Instance.LANGUAGES_KEY,
    Instance.PUBLICATION_KEY,
    Instance.PUBLICATION_FREQUENCY_KEY,
    Instance.PUBLICATION_RANGE_KEY,
    Instance.NOTES_KEY,
    Instance.ELECTRONIC_ACCESS_KEY,
    Instance.SUBJECTS_KEY,
    Instance.CLASSIFICATIONS_KEY,
    Instance.TITLE_KEY,
    Instance.INDEX_TITLE_KEY,
    Instance.INSTANCE_TYPE_ID_KEY,
    Instance.MODE_OF_ISSUANCE_ID_KEY,
    Instance.PRECEDING_TITLES_KEY,
    Instance.SUCCEEDING_TITLES_KEY
    );

  private static final Set<String> HOLDINGS_BLOCKED_FIELDS = Sets.newHashSet(
    "formerIds",
    "holdingsTypeId",
    "permanentLocationId",
    "shelvingTitle",
    "copyNumber",
    "callNumberTypeId",
    "callNumberPrefix",
    "callNumber",
    "callNumberSuffix",
    "holdingsStatements",
    "holdingsStatementsForIndexes",
    "holdingsStatementsForSupplements",
    "notes",
    "electronicAccess"
  );

  public InventoryConfigurationImpl() {
  }

  public Set<String> getInstanceBlockedFields() {
    return INSTANCE_BLOCKED_FIELDS;
  }

  public Set<String> getHoldingsBlockedFields() {
    return HOLDINGS_BLOCKED_FIELDS;
  }
}
