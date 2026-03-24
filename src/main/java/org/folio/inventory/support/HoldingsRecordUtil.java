package org.folio.inventory.support;

import io.vertx.core.json.JsonObject;
import java.util.HashMap;
import java.util.Map;

public final class HoldingsRecordUtil {

  private static final String STATISTICAL_CODE_IDS_PROPERTY = "statisticalCodeIds";
  private static final String ADMINISTRATIVE_NOTES_PROPERTY = "administrativeNotes";
  private static final String TEMPORARY_LOCATION_ID_PROPERTY = "temporaryLocationId";
  private static final String ACQUISITION_FORMAT_PROPERTY = "acquisitionFormat";
  private static final String ACQUISITION_METHOD_PROPERTY = "acquisitionMethod";
  private static final String RECEIPT_STATUS_PROPERTY = "receiptStatus";
  private static final String ILL_POLICY_ID_PROPERTY = "illPolicyId";
  private static final String RETENTION_POLICY_PROPERTY = "retentionPolicy";
  private static final String DIGITIZATION_POLICY_PROPERTY = "digitizationPolicy";
  private static final String NUMBER_OF_ITEMS_PROPERTY = "numberOfItems";
  private static final String ADDITIONAL_CALL_NUMBERS_PROPERTY = "additionalCallNumbers";
  private static final String RECEIVING_HISTORY_PROPERTY = "receivingHistory";
  private static final String DISCOVERY_SUPPRESS_PROPERTY = "discoverySuppress";

  private static final String[] FIELDS_TO_PRESERVE = {
    STATISTICAL_CODE_IDS_PROPERTY,
    ADMINISTRATIVE_NOTES_PROPERTY,
    ADDITIONAL_CALL_NUMBERS_PROPERTY,
    RECEIVING_HISTORY_PROPERTY,
    DISCOVERY_SUPPRESS_PROPERTY,
    TEMPORARY_LOCATION_ID_PROPERTY,
    ACQUISITION_FORMAT_PROPERTY,
    ACQUISITION_METHOD_PROPERTY,
    RECEIPT_STATUS_PROPERTY,
    ILL_POLICY_ID_PROPERTY,
    RETENTION_POLICY_PROPERTY,
    DIGITIZATION_POLICY_PROPERTY,
    NUMBER_OF_ITEMS_PROPERTY
  };

  private HoldingsRecordUtil() {
    throw new UnsupportedOperationException("Cannot instantiate utility class");
  }

  /**
   * Merges holdings records by combining the source holdings record into the target one,
   * while preserving the statistical code IDs, administrative notes, and other specific fields from the target record.
   *
   * @param target the target holdings record
   * @param source the source holdings record
   * @return the merged holdings record
   */
  public static JsonObject mergeHoldingsRecords(JsonObject target, JsonObject source) {
    Map<String, Object> preservedValues = new HashMap<>();
    
    for (String field : FIELDS_TO_PRESERVE) {
      preservedValues.put(field, target.getValue(field));
    }
    
    var mergedInstanceAsJson = target.mergeIn(source, true);
    
    for (Map.Entry<String, Object> entry : preservedValues.entrySet()) {
      mergedInstanceAsJson.put(entry.getKey(), entry.getValue());
    }
    
    return mergedInstanceAsJson;
  }
}
