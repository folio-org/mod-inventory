package org.folio.inventory.validation.status;

import org.folio.inventory.domain.items.Item;
import org.folio.inventory.domain.items.ItemStatusName;
import org.folio.inventory.exceptions.UnprocessableEntityException;
import org.folio.inventory.support.http.server.ValidationError;

import java.util.Set;
import java.util.concurrent.CompletableFuture;

import static org.folio.inventory.support.CompletableFutures.failedFuture;

public abstract class AbstractTargetItemStatusValidator {
  private ItemStatusName itemStatusName;
  private Set<ItemStatusName> allowedStatusToMark;

  protected AbstractTargetItemStatusValidator(ItemStatusName itemStatusName, Set<ItemStatusName> allowedSourceStatuses) {
    this.itemStatusName = itemStatusName;
    this.allowedStatusToMark = allowedSourceStatuses;
  }

  public CompletableFuture<Item> refuseItemWhenNotInAcceptableSourceStatus(Item item) {
    if (isItemAllowedToMark(item)) {
      return CompletableFuture.completedFuture(item);
    }

    return failedFuture(new UnprocessableEntityException(
      new ValidationError("Item is not allowed to be marked as " + getItemStatusName(),
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
