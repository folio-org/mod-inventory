package org.folio.inventory.validation;


import static java.util.concurrent.CompletableFuture.completedFuture;
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
    if (patchRequest.containsKey("status")) {
      var newStatusName = patchRequest.getJsonObject("status").getString("name");
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
        "HRID can not be updated", "hrid", newItem.getHrid());

      return failedFuture(new UnprocessableEntityException(validationError));
    }

    return completedFuture(oldItem);
  }

  public static CompletableFuture<Item> hridChanged(Item oldItem, JsonObject patchRequest) {
    if (patchRequest.containsKey("hrid")
      && !Objects.equals(patchRequest.getString("hrid"), oldItem.getHrid())) {
      final ValidationError validationError = new ValidationError(
        "HRID can not be updated", "hrid", patchRequest.getString("hrid"));

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
    if (patchRequest.containsKey("barcode")
      && !Objects.equals(patchRequest.getString("barcode"), oldItem.getBarcode())) {
      final ValidationError validationError = new ValidationError(
        "Barcode can not be patched", "barcode", patchRequest.getString("barcode"));

      return failedFuture(new UnprocessableEntityException(validationError));
    }

    return completedFuture(oldItem);
  }

  private static boolean isClaimedReturnedItemMarkedMissing(ItemStatusName oldStatus, ItemStatusName newStatus) {
    return ItemStatusName.CLAIMED_RETURNED == oldStatus
      && ItemStatusName.MISSING == newStatus;
  }
}
