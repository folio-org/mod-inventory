package org.folio.inventory.validation;


import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.folio.inventory.support.CompletableFutures.failedFuture;

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
    if (isClaimedReturnedItemMarkedMissing(oldItem, newItem)) {
      final ValidationError validationError = new ValidationError(
        "Claimed returned item cannot be marked as missing",
        "status.name", ItemStatusName.MISSING.value());

      return failedFuture(new UnprocessableEntityException(validationError));
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

  public static CompletableFuture<Item> barcodeChanged(Item oldItem, Item newItem) {
    if (!Objects.equals(newItem.getBarcode(), oldItem.getBarcode())) {
      final ValidationError validationError = new ValidationError(
        "Barcode can not be patched", "barcode", newItem.getBarcode());

      return failedFuture(new UnprocessableEntityException(validationError));
    }

    return completedFuture(oldItem);
  }

  private static boolean isClaimedReturnedItemMarkedMissing(Item oldItem, Item newItem) {
    return oldItem.getStatus().getName() == ItemStatusName.CLAIMED_RETURNED
      && newItem.getStatus().getName() == ItemStatusName.MISSING;
  }
}
