
package org.folio.inventory.domain.items;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyDescription;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;
import java.util.Date;
import org.joda.time.format.ISODateTimeFormat;

/**
 * Information about when an item was last scanned in the Inventory app.
 *
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder( {
    "dateTime",
    "servicePointId",
    "staffMemberId"
})
public class LastCheckIn {

    public static LastCheckIn from(JsonObject representation) {
        return representation != null ?
            representation.mapTo(LastCheckIn.class) : null;
    }

    public JsonObject toJson(){
        JsonObject entries = new JsonObject(Json.encode(this));
        entries.put("dateTime",
            ISODateTimeFormat.dateTime().print(this.dateTime.getTime()));
        return entries;
    }

    /**
     * Date and time of the last check in of the item.
     *
     */
    @JsonProperty("dateTime")
    @JsonPropertyDescription("Date and time of the last check in of the item.")
    private Date dateTime;
    /**
     * A universally unique identifier (UUID), this is a 128-bit number used to identify a record and is shown in hex with dashes, for example 6312d172-f0cf-40f6-b27d-9fa8feaf332f; the UUID version must be from 1-5; see https://dev.folio.org/guides/uuids/
     *
     */
    @JsonProperty("servicePointId")
    @JsonPropertyDescription("A universally unique identifier (UUID), this is a 128-bit number used to identify a record and is shown in hex with dashes, for example 6312d172-f0cf-40f6-b27d-9fa8feaf332f; the UUID version must be from 1-5; see https://dev.folio.org/guides/uuids/")
    private String servicePointId;
    /**
     * A universally unique identifier (UUID), this is a 128-bit number used to identify a record and is shown in hex with dashes, for example 6312d172-f0cf-40f6-b27d-9fa8feaf332f; the UUID version must be from 1-5; see https://dev.folio.org/guides/uuids/
     *
     */
    @JsonProperty("staffMemberId")
    @JsonPropertyDescription("A universally unique identifier (UUID), this is a 128-bit number used to identify a record and is shown in hex with dashes, for example 6312d172-f0cf-40f6-b27d-9fa8feaf332f; the UUID version must be from 1-5; see https://dev.folio.org/guides/uuids/")
    private String staffMemberId;

    /**
     * Date and time of the last check in of the item.
     *
     */
    @JsonProperty("dateTime")
    public Date getDateTime() {
        return dateTime;
    }

    /**
     * Date and time of the last check in of the item.
     *
     */
    @JsonProperty("dateTime")
    public void setDateTime(Date dateTime) {
        this.dateTime = dateTime;
    }

    public LastCheckIn withDateTime(Date dateTime) {
        this.dateTime = dateTime;
        return this;
    }

    /**
     * A universally unique identifier (UUID), this is a 128-bit number used to identify a record and is shown in hex with dashes, for example 6312d172-f0cf-40f6-b27d-9fa8feaf332f; the UUID version must be from 1-5; see https://dev.folio.org/guides/uuids/
     *
     */
    @JsonProperty("servicePointId")
    public String getServicePointId() {
        return servicePointId;
    }

    /**
     * A universally unique identifier (UUID), this is a 128-bit number used to identify a record and is shown in hex with dashes, for example 6312d172-f0cf-40f6-b27d-9fa8feaf332f; the UUID version must be from 1-5; see https://dev.folio.org/guides/uuids/
     *
     */
    @JsonProperty("servicePointId")
    public void setServicePointId(String servicePointId) {
        this.servicePointId = servicePointId;
    }

    public LastCheckIn withServicePointId(String servicePointId) {
        this.servicePointId = servicePointId;
        return this;
    }

    /**
     * A universally unique identifier (UUID), this is a 128-bit number used to identify a record and is shown in hex with dashes, for example 6312d172-f0cf-40f6-b27d-9fa8feaf332f; the UUID version must be from 1-5; see https://dev.folio.org/guides/uuids/
     *
     */
    @JsonProperty("staffMemberId")
    public String getStaffMemberId() {
        return staffMemberId;
    }

    /**
     * A universally unique identifier (UUID), this is a 128-bit number used to identify a record and is shown in hex with dashes, for example 6312d172-f0cf-40f6-b27d-9fa8feaf332f; the UUID version must be from 1-5; see https://dev.folio.org/guides/uuids/
     *
     */
    @JsonProperty("staffMemberId")
    public void setStaffMemberId(String staffMemberId) {
        this.staffMemberId = staffMemberId;
    }

    public LastCheckIn withStaffMemberId(String staffMemberId) {
        this.staffMemberId = staffMemberId;
        return this;
    }

}
