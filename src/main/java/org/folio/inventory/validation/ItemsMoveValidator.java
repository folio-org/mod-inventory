package org.folio.inventory.validation;

import static org.folio.inventory.resources.MoveApi.ITEM_IDS;
import static org.folio.inventory.resources.MoveApi.TO_HOLDINGS_RECORD_ID;

import java.util.List;
import java.util.Optional;

import org.apache.commons.lang3.StringUtils;
import org.folio.inventory.support.JsonArrayHelper;
import org.folio.inventory.support.JsonHelper;
import org.folio.inventory.support.http.server.ValidationError;

import io.vertx.core.json.JsonObject;

public class ItemsMoveValidator {

  private ItemsMoveValidator() { }

  public static Optional<ValidationError> itemsMoveHasRequiredFields(JsonObject itemsMoveRequest) {

    final String toHoldingsRecordId = JsonHelper.getString(itemsMoveRequest, TO_HOLDINGS_RECORD_ID);

    if (StringUtils.isBlank(toHoldingsRecordId)) {
      return Optional.of(new ValidationError("toHoldingsRecordId is a required field", "toHoldingsRecordId", null));
    }

    List<String> itemIds = JsonArrayHelper.toListOfStrings(itemsMoveRequest.getJsonArray(ITEM_IDS));

    if (itemIds.isEmpty()) {
      return Optional.of(new ValidationError("Item ids aren't specified", "itemIds", null));
    }

    return Optional.empty();
  }
}
