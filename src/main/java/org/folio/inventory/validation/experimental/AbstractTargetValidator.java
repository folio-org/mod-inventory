package org.folio.inventory.validation.experimental;

import org.folio.inventory.domain.items.Item;
import org.folio.inventory.exceptions.UnprocessableEntityException;
import org.folio.inventory.support.http.server.ValidationError;

import java.util.concurrent.CompletableFuture;

import static org.folio.inventory.support.CompletableFutures.failedFuture;

public abstract class AbstractTargetValidator implements TargetItemStatusValidatorInterface {

  @Override
  public CompletableFuture<Item> itemHasAllowedStatusToMark(Item item) {
    if (isItemAllowedToMark(item)) {
      return CompletableFuture.completedFuture(item);
    }

    return failedFuture(new UnprocessableEntityException(
      new ValidationError("Item is not allowed to be marked as:\""+getStatusName()+"\"",
        "status.name", item.getStatus().getName().value())));
  }

}
