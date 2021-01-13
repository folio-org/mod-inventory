package org.folio.inventory.validation.experimental;

import org.folio.inventory.domain.items.Item;
import org.folio.inventory.domain.items.ItemStatusName;
import org.folio.inventory.exceptions.UnprocessableEntityException;
import org.folio.inventory.support.http.server.ValidationError;

import java.util.Set;
import java.util.concurrent.CompletableFuture;

import static org.folio.inventory.support.CompletableFutures.failedFuture;

public abstract class AbstractTargetValidator {
  private ItemStatusName itemStatusName;
  private Set<ItemStatusName> allowedStatusToMark;

  protected AbstractTargetValidator(ItemStatusName itemStatusName, Set<ItemStatusName> allowedStatusToMark) {
    this.itemStatusName = itemStatusName;
    this.allowedStatusToMark = allowedStatusToMark;
  }

  public CompletableFuture<Item> itemHasAllowedStatusToMark(Item item) {
    if (isItemAllowedToMark(item)) {
      return CompletableFuture.completedFuture(item);
    }

    return failedFuture(new UnprocessableEntityException(
      new ValidationError("Item is not allowed to be marked as:\"" + getItemStatusName() + "\"",
        "status.name", item.getStatus().getName().value())));
  }

  public boolean isItemAllowedToMark(Item item) {
    return allowedStatusToMark.contains(item.getStatus().getName());
  }

  public Set<ItemStatusName> getAllStatusesAllowedToMark() {
    return allowedStatusToMark;
  }

  public ItemStatusName getItemStatusName() {
    return itemStatusName;
  }

}
