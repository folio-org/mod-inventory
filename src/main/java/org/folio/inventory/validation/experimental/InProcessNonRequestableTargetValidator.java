package org.folio.inventory.validation.experimental;

import org.folio.inventory.domain.items.Item;
import org.folio.inventory.domain.items.ItemStatusName;
import org.folio.inventory.exceptions.UnprocessableEntityException;
import org.folio.inventory.support.http.server.ValidationError;

import java.util.Set;
import java.util.concurrent.CompletableFuture;

import static java.util.Set.of;
import static org.folio.inventory.support.CompletableFutures.failedFuture;

public class InProcessNonRequestableTargetValidator implements TargetValidatorInterface {
  public static final ItemStatusName statusName = ItemStatusName.IN_PROCESS_NON_REQUESTABLE;
  private static final Set<ItemStatusName> ALLOWED_STATUS_TO_MARK = of(
    ItemStatusName.AVAILABLE,
    ItemStatusName.AWAITING_DELIVERY,
    ItemStatusName.AWAITING_PICKUP,
    ItemStatusName.IN_TRANSIT,
    ItemStatusName.LOST_AND_PAID,
    ItemStatusName.MISSING,
    ItemStatusName.ORDER_CLOSED,
    ItemStatusName.PAGED,
    ItemStatusName.WITHDRAWN
  );

  @Override
  public ItemStatusName getStatusName() {
    return statusName;
  }

  @Override
  public CompletableFuture<Item> itemHasAllowedStatusToMark(Item item) {
    if (ALLOWED_STATUS_TO_MARK.contains(item.getStatus().getName())) {
      return CompletableFuture.completedFuture(item);
    }

    return failedFuture(new UnprocessableEntityException(
      new ValidationError("Item is not allowed to be marked as:\""+statusName.value()+"\"",
        "status.name", item.getStatus().getName().value())));
  }
}
