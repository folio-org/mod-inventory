package org.folio.inventory.validation;

import org.folio.inventory.domain.items.Item;
import org.folio.inventory.domain.items.ItemStatusName;
import org.folio.inventory.exceptions.UnprocessableEntityException;
import org.folio.inventory.support.http.server.ValidationError;

import java.util.Set;
import java.util.concurrent.CompletableFuture;

import static java.util.Set.of;
import static org.folio.inventory.support.CompletableFutures.failedFuture;

public final class MarkAsUnknownValidators {
  private static final Set<ItemStatusName> ALLOWED_STATUS_TO_MARK_UNKNOWN = of(
    ItemStatusName.AVAILABLE,
    ItemStatusName.AWAITING_DELIVERY,
    ItemStatusName.AWAITING_PICKUP,
    ItemStatusName.IN_TRANSIT,
    ItemStatusName.LOST_AND_PAID,
    ItemStatusName.MISSING,
    ItemStatusName.ORDER_CLOSED,
    ItemStatusName.PAGED,
    ItemStatusName.WITHDRAWN);

  private MarkAsUnknownValidators() {}

  public static CompletableFuture<Item> itemHasAllowedStatusToMarkAsUnknown(Item item) {
   if (ALLOWED_STATUS_TO_MARK_UNKNOWN.contains(item.getStatus().getName())) {
      return CompletableFuture.completedFuture(item);
    }

    return failedFuture(new UnprocessableEntityException(
      new ValidationError("Item is not allowed to be marked as Unknown",
        "status.name", item.getStatus().getName().value())));
  }
}
