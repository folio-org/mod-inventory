package org.folio.inventory.support;

import io.vertx.core.json.JsonObject;
import org.folio.inventory.domain.Item;

import java.util.Collection;
import java.util.Optional;

public class HoldingsSupport {
  public static String determinePermanentLocationIdForItem(Item item, JsonObject holding) {
    if(holding != null && holding.containsKey("permanentLocationId")) {
      return holding.getString("permanentLocationId");
    }
    else {
      return item.permanentLocationId;
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
}
