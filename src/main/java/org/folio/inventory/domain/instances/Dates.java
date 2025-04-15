package org.folio.inventory.domain.instances;

import io.vertx.core.json.JsonObject;
import lombok.AllArgsConstructor;
import lombok.Getter;

import static org.apache.commons.lang3.ObjectUtils.anyNotNull;
import static org.folio.inventory.domain.instances.Instance.DATES_KEY;
import static org.folio.inventory.domain.instances.Instance.JSON_FOR_STORAGE_KEY;
import static org.folio.inventory.support.JsonHelper.includeIfPresent;

@Getter
@AllArgsConstructor
public class Dates {
  // JSON property names
  public static final String DATE_TYPE_ID_KEY = "dateTypeId";
  public static final String DATE1_KEY = "date1";
  public static final String DATE2_KEY = "date2";

  public final String dateTypeId;
  public final String date1;
  public final String date2;

  public static JsonObject datesToJson(Dates dates) {
    if (dates == null || (dates.getDate1() == null && dates.getDate2() == null && dates.getDateTypeId() == null)) {
      return null;
    }
    var json = new JsonObject();
    includeIfPresent(json, DATE_TYPE_ID_KEY, dates.getDateTypeId());
    includeIfPresent(json, DATE1_KEY, dates.getDate1());
    includeIfPresent(json, DATE2_KEY, dates.getDate2());
    return json;
  }

  public static JsonObject retrieveDatesFromJson(JsonObject targetInstance) {
    JsonObject dates = targetInstance.getJsonObject(DATES_KEY);
    if (dates == null) {
      JsonObject jsonForStorage = targetInstance.getJsonObject(JSON_FOR_STORAGE_KEY);
      if (jsonForStorage != null) {
        dates = jsonForStorage.getJsonObject(DATES_KEY);
      }
    }
    return dates;
  }

  public static Dates convertToDates(JsonObject datesAsJson) {
    if (datesAsJson == null) {
      return null;
    }

    var dateTypeId = datesAsJson.getString(DATE_TYPE_ID_KEY);
    var date1 = datesAsJson.getString(DATE1_KEY);
    var date2 = datesAsJson.getString(DATE2_KEY);
    return anyNotNull(dateTypeId, date1, date2) ? new Dates(dateTypeId, date1, date2) : null;
  }
}
