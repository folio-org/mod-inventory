package org.folio.inventory.domain.items;

import org.folio.inventory.domain.AsynchronousCollection;
import org.folio.inventory.domain.SearchableCollection;

public interface ItemCollection extends AsynchronousCollection<Item>, SearchableCollection<Item> {
}
