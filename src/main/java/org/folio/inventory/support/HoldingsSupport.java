package org.folio.inventory.support;

import io.vertx.core.json.JsonObject;
import org.folio.inventory.domain.Item;

import java.util.Collection;
import java.util.Optional;

public class HoldingsSupport {
  private HoldingsSupport() { }

  public static String determineEffectiveLocationIdForItem(JsonObject holding) {
    if(holding != null && holding.containsKey("permanentLocationId")) {
      return holding.getString("permanentLocationId");
    }
    else {
      return null;
    }
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
