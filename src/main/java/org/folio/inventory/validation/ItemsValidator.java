package org.folio.inventory.validation;

import static io.vertx.core.Future.failedFuture;
import static io.vertx.core.Future.succeededFuture;

import java.util.Objects;

import org.folio.inventory.domain.items.Item;
import org.folio.inventory.domain.items.ItemStatusName;
import org.folio.inventory.support.http.server.ValidationError;
import org.folio.inventory.validation.exceptions.NotFoundException;
import org.folio.inventory.validation.exceptions.UnprocessableEntityException;

import io.vertx.core.Future;

public final class ItemsValidator {

  private ItemsValidator() {}

  public static Future<Item> itemNotFound(Item oldItem) {
    return oldItem == null
      ? failedFuture(new NotFoundException("No item found"))
      : succeededFuture(oldItem);
  }

  public static Future<Item> claimedReturnedMarkedAsMissing(Item oldItem, Item newItem) {
    if (isClaimedReturnedItemMarkedMissing(oldItem, newItem)) {
      final ValidationError validationError = new ValidationError(
        "Claimed returned item cannot be marked as missing",
        "status.name", ItemStatusName.MISSING.value());

      return failedFuture(new UnprocessableEntityException(validationError));
    }

    return succeededFuture(oldItem);
  }

  public static Future<Item> hridChanged(Item oldItem, Item newItem) {
    if (!Objects.equals(newItem.getHrid(), oldItem.getHrid())) {
      final ValidationError validationError = new ValidationError(
        "HRID can not be updated", "hrid", newItem.getHrid());

      return failedFuture(new UnprocessableEntityException(validationError));
    }

    return succeededFuture(oldItem);
  }

  private static boolean isClaimedReturnedItemMarkedMissing(Item oldItem, Item newItem) {
    return oldItem.getStatus().getName() == ItemStatusName.CLAIMED_RETURNED
      && newItem.getStatus().getName() == ItemStatusName.MISSING;
  }
}
