package org.folio.inventory.dataimport.util;

import io.vertx.core.json.JsonObject;

public final class HoldingsRecordUtil {

  private static final String STATISTICAL_CODE_IDS_PROPERTY = "statisticalCodeIds";
  private static final String ADMINISTRATIVE_NOTES_PROPERTY = "administrativeNotes";

  private HoldingsRecordUtil() {
    throw new UnsupportedOperationException("Cannot instantiate utility class");
  }

  /**
   * Merges holdings records by combining the source holdings record into the target one,
   * while preserving the statistical code IDs and administrative notes from the target record.
   *
   * @param target the target holdings record
   * @param source the source holdings record
   * @return the merged holdings record
   */
  public static JsonObject mergeHoldingsRecords(JsonObject target, JsonObject source) {
    var statisticalCodeIds = target.getJsonArray(STATISTICAL_CODE_IDS_PROPERTY);
    var administrativeNotes = target.getJsonArray(ADMINISTRATIVE_NOTES_PROPERTY);
    var mergedInstanceAsJson = target.mergeIn(source, true);
    mergedInstanceAsJson.put(STATISTICAL_CODE_IDS_PROPERTY, statisticalCodeIds);
    mergedInstanceAsJson.put(ADMINISTRATIVE_NOTES_PROPERTY, administrativeNotes);
    return mergedInstanceAsJson;
  }
}
