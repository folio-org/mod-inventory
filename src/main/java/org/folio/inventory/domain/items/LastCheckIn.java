
package org.folio.inventory.domain.items;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import io.vertx.core.json.JsonObject;
import org.joda.time.DateTime;
import org.joda.time.format.ISODateTimeFormat;

@JsonIgnoreProperties(ignoreUnknown = true)
public class LastCheckIn {

  private final DateTime dateTime;
  private final String servicePointId;
  private final String staffMemberId;

  public LastCheckIn(DateTime dateTime, String servicePointId, String staffMemberId) {
    this.dateTime = dateTime;
    this.servicePointId = servicePointId;
    this.staffMemberId = staffMemberId;
  }

  public static LastCheckIn from(JsonObject representation) {
    if (representation==null) {
      return null;
    }

    String dateTime = representation.getString("dateTime");
    return new LastCheckIn(
      dateTime != null ? DateTime.parse(dateTime): null,
      representation.getString("servicePointId"),
      representation.getString("staffMemberId")
    );
  }

  public JsonObject toJson() {
    JsonObject entries = new JsonObject();
    entries.put("servicePointId", servicePointId);
    entries.put("staffMemberId", staffMemberId);
    entries.put("dateTime", ISODateTimeFormat.dateTime().print(this.dateTime));
    return entries;
  }
}
