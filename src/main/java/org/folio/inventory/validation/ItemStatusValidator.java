package org.folio.inventory.validation;


import java.util.Optional;

import org.apache.commons.lang3.StringUtils;
import org.folio.inventory.domain.items.ItemStatusName;
import org.folio.inventory.support.http.server.ValidationError;

import io.vertx.core.json.JsonObject;

public final class ItemStatusValidator {

  private ItemStatusValidator() {}

  public static Optional<ValidationError> itemHasNoOrKnownStatus(JsonObject itemRequest) {
    if (!itemRequest.containsKey("status")) {
      return Optional.empty();
    }

    final String itemStatusName = itemRequest.getJsonObject("status").getString("name");
    if (StringUtils.isBlank(itemStatusName) || ItemStatusName.hasKnownValue(itemStatusName)) {
      return Optional.empty();
    }

    return Optional.of(new ValidationError(
      "Undefined status specified",
      "status.name",
      itemStatusName
    ));
  }
}
