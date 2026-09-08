package org.folio.inventory.domain.view.request;

import io.vertx.core.json.JsonObject;
import java.util.Optional;
import org.joda.time.DateTime;

public class Request {
  private final JsonObject representation;

  public Request(JsonObject representation) {
    this.representation = representation;
  }

  public String getId() {
    return representation.getString("id");
  }

  public DateTime getHoldShelfExpirationDate() {
    return Optional.ofNullable(representation.getString("holdShelfExpirationDate"))
      .map(Object::toString)
      .map(DateTime::parse)
      .orElse(null);
  }

  public RequestStatus getStatus() {
    return RequestStatus.of(representation.getString("status"));
  }

  public void setStatus(RequestStatus newStatus) {
    representation.put("status", newStatus.getValue());
  }

  public JsonObject toJson() {
    return representation;
  }
}
