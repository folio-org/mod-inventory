package org.folio.inventory.support;

import static org.folio.inventory.domain.items.ItemsMove.ITEM_IDS;
import static org.folio.inventory.domain.items.ItemsMove.TO_HOLDINGS_RECORD_ID;
import static org.folio.inventory.support.JsonArrayHelper.toListOfStrings;

import org.folio.inventory.domain.items.ItemsMove;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

public final class MoveUtil {

  private MoveUtil() {
  }

  public static ItemsMove jsonToItemsMove(JsonObject itemRequest) {
    return new ItemsMove().withToHoldingsRecordId(itemRequest.getString(TO_HOLDINGS_RECORD_ID))
      .withItemIds(toListOfStrings(itemRequest.getJsonArray(ItemsMove.ITEM_IDS)));
  }

  public static JsonObject itemsMoveMapToJson(ItemsMove itemsMove) {
    JsonObject itemsMoveJson = new JsonObject();
    itemsMoveJson.put(TO_HOLDINGS_RECORD_ID, itemsMove.getToHoldingsRecordId());
    itemsMoveJson.put(ITEM_IDS, new JsonArray(itemsMove.getItemIds()));
    return itemsMoveJson;
  }
}
