package org.folio.inventory.validation.experimental;

import org.folio.inventory.domain.items.Item;
import org.folio.inventory.domain.items.ItemStatusName;
import org.folio.inventory.exceptions.UnprocessableEntityException;
import org.folio.inventory.support.http.server.ValidationError;

import java.util.Set;
import java.util.concurrent.CompletableFuture;

import static org.folio.inventory.support.CompletableFutures.failedFuture;

public abstract class AbstractTargetValidator {
  private ItemStatusName statusName = ItemStatusName.IN_PROCESS;
  private Set<ItemStatusName> ALLOWED_STATUS_TO_MARK;

  public AbstractTargetValidator(ItemStatusName statusName, Set<ItemStatusName> ALLOWED_STATUS_TO_MARK) {
    this.statusName = statusName;
    this.ALLOWED_STATUS_TO_MARK = ALLOWED_STATUS_TO_MARK;
  }

  public CompletableFuture<Item> itemHasAllowedStatusToMark(Item item) {
    if (isItemAllowedToMark(item)) {
      return CompletableFuture.completedFuture(item);
    }

    return failedFuture(new UnprocessableEntityException(
      new ValidationError("Item is not allowed to be marked as:\"" + getStatusName() + "\"",
        "status.name", item.getStatus().getName().value())));
  }

  public ItemStatusName getStatusName() {
    return statusName;
  }

  public boolean isItemAllowedToMark(Item item) {
    return ALLOWED_STATUS_TO_MARK.contains(item.getStatus().getName());
  }

  public Set<ItemStatusName> getAllStatusesAllowedToMark() {
    return ALLOWED_STATUS_TO_MARK;
  }

  public ItemStatusName getItemStatusName() {
    return statusName;
  }

}
