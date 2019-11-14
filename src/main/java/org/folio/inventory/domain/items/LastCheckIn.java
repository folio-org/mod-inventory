
package org.folio.inventory.domain.items;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyDescription;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import io.vertx.core.json.JsonObject;
import java.util.UUID;
import org.joda.time.DateTime;
import org.joda.time.format.ISODateTimeFormat;

/**
 * Information about when an item was last scanned in the Inventory app.
 *
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({ "dateTime", "servicePointId", "staffMemberId" })
public class LastCheckIn {

  /**
   * Date and time of the last check in of the item.
   *
   */
  @JsonProperty("dateTime")
  @JsonPropertyDescription("Date and time of the last check in of the item.")
  private final DateTime dateTime;
  /**
   * A universally unique identifier (UUID), this is a 128-bit number used to identify a record and is shown in hex with dashes, for
   * example 6312d172-f0cf-40f6-b27d-9fa8feaf332f; the UUID version must be from 1-5; see https://dev.folio.org/guides/uuids/
   *
   */
  @JsonProperty("servicePointId")
  @JsonPropertyDescription("A universally unique identifier (UUID), this is a 128-bit number used to identify a record and is shown in hex with dashes, for example 6312d172-f0cf-40f6-b27d-9fa8feaf332f; the UUID version must be from 1-5; see https://dev.folio.org/guides/uuids/")
  private final String servicePointId;
  /**
   * A universally unique identifier (UUID), this is a 128-bit number used to identify a record and is shown in hex with dashes, for
   * example 6312d172-f0cf-40f6-b27d-9fa8feaf332f; the UUID version must be from 1-5; see https://dev.folio.org/guides/uuids/
   *
   */
  @JsonProperty("staffMemberId")
  @JsonPropertyDescription("A universally unique identifier (UUID), this is a 128-bit number used to identify a record and is shown in hex with dashes, for example 6312d172-f0cf-40f6-b27d-9fa8feaf332f; the UUID version must be from 1-5; see https://dev.folio.org/guides/uuids/")
  private final String staffMemberId;

  public LastCheckIn(DateTime dateTime, String servicePointId, String staffMemberId) {
    this.dateTime = dateTime;
    this.servicePointId = servicePointId;
    this.staffMemberId = staffMemberId;
  }
  /**
   * Date and time of the last check in of the item.
   *
   */
  @JsonProperty("dateTime")
  public DateTime getDateTime() {
    return dateTime;
  }

  @JsonProperty("servicePointId")
  public String getServicePointId() {
    return servicePointId;
  }

  public static LastCheckIn from(JsonObject representation) {
    if (representation==null) {
      return null;
    }
    String dateTime = representation.getString("dateTime");
    return new LastCheckIn(
      dateTime!=null ? DateTime.parse(dateTime):null,
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
