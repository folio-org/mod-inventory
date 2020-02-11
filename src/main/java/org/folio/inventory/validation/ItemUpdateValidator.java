package org.folio.inventory.validation;

import java.util.Objects;
import java.util.Optional;

import org.folio.inventory.domain.items.Item;
import org.folio.inventory.domain.items.ItemStatusName;
import org.folio.inventory.support.http.server.ValidationError;

import io.vertx.ext.web.RoutingContext;

public final class ItemUpdateValidator {

  private ItemUpdateValidator() {}

  /**
   * Validation logic for {@link org.folio.inventory.resources.Items#update(RoutingContext)}
   * method.
   *
   * @param oldItem - The item from DB.
   * @param newItem - The item from request.
   * @return An optional with validation error, if any.
   */
  public static Optional<ValidationError> validateItemUpdate(Item oldItem, Item newItem) {
    ValidationError validationError = null;

    if (!Objects.equals(newItem.getHrid(), oldItem.getHrid())) {
      validationError = new ValidationError(
        "HRID can not be updated", "hrid", newItem.getHrid());
    }

    if (claimedReturnedItemMarkedMissing(oldItem, newItem)) {
      validationError = new ValidationError(
        "Claimed returned item cannot be marked as missing",
        "status.name", ItemStatusName.MISSING.value());
    }

    return Optional.ofNullable(validationError);
  }

  private static boolean claimedReturnedItemMarkedMissing(Item oldItem, Item newItem) {
    return oldItem.getStatus().getName() == ItemStatusName.CLAIMED_RETURNED
      && newItem.getStatus().getName() == ItemStatusName.MISSING;
  }
}
