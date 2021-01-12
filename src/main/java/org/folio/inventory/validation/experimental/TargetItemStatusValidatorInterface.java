package org.folio.inventory.validation.experimental;

import org.folio.inventory.domain.items.Item;
import org.folio.inventory.domain.items.ItemStatusName;

import java.util.Set;
import java.util.concurrent.CompletableFuture;

public interface TargetItemStatusValidatorInterface {
  ItemStatusName getStatusName();
  CompletableFuture<Item> itemHasAllowedStatusToMark(Item item);
  boolean isItemAllowedToMark(Item item);
  Set<ItemStatusName> getAllStatusesAllowedToMark();
}
