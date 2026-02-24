package org.folio.inventory.validation;


import java.util.Optional;

import org.apache.commons.lang3.StringUtils;
import org.folio.inventory.domain.items.ItemStatusName;
import org.folio.inventory.support.JsonHelper;
import org.folio.inventory.support.http.server.ValidationError;

import io.vertx.core.json.JsonObject;

public final class ItemStatusValidator {
  private ItemStatusValidator() {}

  public static Optional<ValidationError> itemHasCorrectStatus(JsonObject itemRequest) {
    final String statusName = JsonHelper.getNestedProperty(itemRequest, "status", "name");

    if (StringUtils.isBlank(statusName)) {
      return Optional.of(
        new ValidationError("Status is a required field", "status", null));
    }

    if (!ItemStatusName.isStatusCorrect(statusName)) {
      return Optional.of(
        new ValidationError("Undefined status specified", "status.name", statusName));
    }

    return Optional.empty();
  }

  public static Optional<ValidationError> checkStatusIfPresent(JsonObject patchRequest) {
    return patchRequest.containsKey("status")
      ? itemHasCorrectStatus(patchRequest)
      : Optional.empty();
  }
}
