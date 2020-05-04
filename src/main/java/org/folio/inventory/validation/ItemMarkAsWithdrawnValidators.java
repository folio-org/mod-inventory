package org.folio.inventory.validation;

import java.util.EnumSet;
import java.util.concurrent.CompletableFuture;

import org.folio.inventory.domain.items.Item;
import org.folio.inventory.domain.items.ItemStatusName;
import org.folio.inventory.exceptions.NotFoundException;
import org.folio.inventory.exceptions.UnprocessableEntityException;
import org.folio.inventory.support.CompletableFutures;
import org.folio.inventory.support.http.server.ValidationError;

public final class ItemMarkAsWithdrawnValidators {
  private static final EnumSet<ItemStatusName> ALLOWED_STATUS_TO_MARK_WITHDRAWN =
    EnumSet.of(
      ItemStatusName.AVAILABLE,
      ItemStatusName.IN_TRANSIT,
      ItemStatusName.IN_PROCESS,
      ItemStatusName.AWAITING_PICKUP,
      ItemStatusName.AWAITING_DELIVERY,
      ItemStatusName.MISSING,
      ItemStatusName.PAGED);

  private ItemMarkAsWithdrawnValidators() {}

  public static CompletableFuture<Item> itemIsFound(Item itemResult) {
    return itemResult != null
      ? CompletableFuture.completedFuture(itemResult)
      : CompletableFutures.failedFuture(new NotFoundException("Item not found"));
  }

  public static CompletableFuture<Item> itemHasAllowedStatusToMarkAsWithdrawn(Item item) {
    if (ALLOWED_STATUS_TO_MARK_WITHDRAWN.contains(item.getStatus().getName())) {
      return CompletableFuture.completedFuture(item);
    }

    return CompletableFutures.failedFuture(new UnprocessableEntityException(
      new ValidationError("Item is not allowed to be marked as Withdrawn",
        "status.name", item.getStatus().getName().value())));
  }
}
