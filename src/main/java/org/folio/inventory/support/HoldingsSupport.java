package org.folio.inventory.support;

import io.vertx.core.json.JsonObject;
import org.folio.inventory.domain.Item;

import java.util.Collection;
import java.util.Optional;

public class HoldingsSupport {
  private HoldingsSupport() { }

  public static String determineEffectiveLocationIdForItem(JsonObject holding, Item item) {
    String effectiveLocationId = null;
    if (item.temporaryLocationId != null) {
      effectiveLocationId = item.temporaryLocationId;
    } else if (item.permanentLocationId != null) {
      effectiveLocationId = item.permanentLocationId;
    } else if (holding != null && holding.containsKey("temporaryLocationId")) {
      effectiveLocationId = holding.getString("temporaryLocationId");
    } else if (holding != null && holding.containsKey("permanentLocationId")) {
      effectiveLocationId = holding.getString("permanentLocationId");
    }
    return effectiveLocationId;
  }

  public static Optional<JsonObject> holdingForItem(
    Item item,
    Collection<JsonObject> holdings) {

    String holdingsRecordId = item.holdingId;

    return holdings.stream()
      .filter(holding -> holding.getString("id").equals(holdingsRecordId))
      .findFirst();
  }

  public static Optional<JsonObject> instanceForHolding(
    JsonObject holding,
    Collection<JsonObject> instances) {

    if(holding == null || !holding.containsKey("instanceId")) {
      return Optional.empty();
    }

    String instanceId = holding.getString("instanceId");

    return instances.stream()
      .filter(instance -> instance.getString("id").equals(instanceId))
      .findFirst();
  }
}
