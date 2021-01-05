package org.folio.inventory.validation.experimental;

import org.folio.inventory.domain.items.Item;
import org.folio.inventory.domain.items.ItemStatusName;

import java.util.concurrent.CompletableFuture;

public interface TargetValidatorInterface {
  ItemStatusName getStatusName();
  CompletableFuture<Item> itemHasAllowedStatusToMark(Item item);
}
