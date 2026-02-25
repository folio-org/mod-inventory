package org.folio.inventory.validation;


import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.folio.inventory.domain.items.Item.BARCODE_KEY;
import static org.folio.inventory.domain.items.Item.HRID_KEY;
import static org.folio.inventory.domain.items.Item.STATUS_KEY;
import static org.folio.inventory.support.CompletableFutures.failedFuture;

import io.vertx.core.json.JsonObject;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;

import org.folio.inventory.domain.items.Item;
import org.folio.inventory.domain.items.ItemStatusName;
import org.folio.inventory.exceptions.NotFoundException;
import org.folio.inventory.exceptions.UnprocessableEntityException;
import org.folio.inventory.support.http.server.ValidationError;

public final class ItemsValidator {
  private ItemsValidator() {}

  public static CompletableFuture<Item> refuseWhenItemNotFound(Item oldItem) {
    return oldItem == null
      ? failedFuture(new NotFoundException("No item found"))
      : completedFuture(oldItem);
  }

  public static CompletableFuture<Item> claimedReturnedMarkedAsMissing(Item oldItem, Item newItem) {
    if (isClaimedReturnedItemMarkedMissing(oldItem.getStatus().getName(), newItem.getStatus().getName())) {
      final ValidationError validationError = new ValidationError(
        "Claimed returned item cannot be marked as missing",
        "status.name", ItemStatusName.MISSING.value());

      return failedFuture(new UnprocessableEntityException(validationError));
    }

    return completedFuture(oldItem);
  }

  public static CompletableFuture<Item> claimedReturnedMarkedAsMissing(Item oldItem, JsonObject patchRequest) {
    if (patchRequest.containsKey(STATUS_KEY)) {
      var newStatusName = patchRequest.getJsonObject(STATUS_KEY).getString("name");
      if (isClaimedReturnedItemMarkedMissing(oldItem.getStatus().getName(),
        ItemStatusName.forName(newStatusName))) {
        final ValidationError validationError = new ValidationError(
          "Claimed returned item cannot be marked as missing",
          "status.name", ItemStatusName.MISSING.value());

        return failedFuture(new UnprocessableEntityException(validationError));
      }
    }
    return completedFuture(oldItem);
  }

  public static CompletableFuture<Item> hridChanged(Item oldItem, Item newItem) {
    if (!Objects.equals(newItem.getHrid(), oldItem.getHrid())) {
      final ValidationError validationError = new ValidationError(
        "HRID can not be updated", HRID_KEY, newItem.getHrid());

      return failedFuture(new UnprocessableEntityException(validationError));
    }

    return completedFuture(oldItem);
  }

  public static CompletableFuture<Item> hridChanged(Item oldItem, JsonObject patchRequest) {
    if (patchRequest.containsKey(HRID_KEY)
      && !Objects.equals(patchRequest.getString(HRID_KEY), oldItem.getHrid())) {
      final ValidationError validationError = new ValidationError(
        "HRID can not be updated", HRID_KEY, patchRequest.getString(HRID_KEY));

      return failedFuture(new UnprocessableEntityException(validationError));
    }

    return completedFuture(oldItem);
  }

  public static CompletableFuture<Item> barcodeChanged(Item oldItem, Item newItem) {
    if (!Objects.equals(newItem.getBarcode(), oldItem.getBarcode())) {
      final ValidationError validationError = new ValidationError(
        "Barcode can not be patched", "barcode", newItem.getBarcode());

      return failedFuture(new UnprocessableEntityException(validationError));
    }

    return completedFuture(oldItem);
  }

  public static CompletableFuture<Item> barcodeChanged(Item oldItem, JsonObject patchRequest) {
    if (patchRequest.containsKey(BARCODE_KEY)
      && !Objects.equals(patchRequest.getString(BARCODE_KEY), oldItem.getBarcode())) {
      final ValidationError validationError = new ValidationError(
        "Barcode can not be patched", BARCODE_KEY, patchRequest.getString(BARCODE_KEY));

      return failedFuture(new UnprocessableEntityException(validationError));
    }

    return completedFuture(oldItem);
  }

  private static boolean isClaimedReturnedItemMarkedMissing(ItemStatusName oldStatus, ItemStatusName newStatus) {
    return ItemStatusName.CLAIMED_RETURNED == oldStatus
      && ItemStatusName.MISSING == newStatus;
  }
}
