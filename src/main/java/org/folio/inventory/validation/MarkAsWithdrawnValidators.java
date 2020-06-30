package org.folio.inventory.validation;

import static java.util.EnumSet.of;
import static org.folio.inventory.support.CompletableFutures.failedFuture;

import java.util.EnumSet;
import java.util.concurrent.CompletableFuture;

import org.folio.inventory.domain.items.Item;
import org.folio.inventory.domain.items.ItemStatusName;
import org.folio.inventory.exceptions.UnprocessableEntityException;
import org.folio.inventory.support.http.server.ValidationError;

public final class MarkAsWithdrawnValidators {
  private static final EnumSet<ItemStatusName> ALLOWED_STATUS_TO_MARK_WITHDRAWN = of(
    ItemStatusName.AVAILABLE,
    ItemStatusName.IN_TRANSIT,
    ItemStatusName.IN_PROCESS,
    ItemStatusName.AWAITING_PICKUP,
    ItemStatusName.AWAITING_DELIVERY,
    ItemStatusName.MISSING,
    ItemStatusName.PAGED);

  private MarkAsWithdrawnValidators() {}

  public static CompletableFuture<Item> itemHasAllowedStatusToMarkAsWithdrawn(Item item) {
    if (ALLOWED_STATUS_TO_MARK_WITHDRAWN.contains(item.getStatus().getName())) {
      return CompletableFuture.completedFuture(item);
    }

    return failedFuture(new UnprocessableEntityException(
      new ValidationError("Item is not allowed to be marked as Withdrawn",
        "status.name", item.getStatus().getName().value())));
  }
}
