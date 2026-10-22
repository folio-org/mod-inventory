package org.folio.inventory.services;

import io.vertx.core.json.JsonObject;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.stream.IntStream;
import org.folio.inventory.domain.items.Item;
import org.folio.inventory.exceptions.ExternalResourceFetchException;
import org.folio.inventory.storage.external.CollectionResourceClient;
import org.folio.inventory.storage.external.CqlQuery;
import org.folio.inventory.support.ItemUtil;
import org.folio.inventory.support.MoveApiUtil;

/**
 * Calculates the {@code order} of items that are placed into a holdings record.
 *
 * <p>When an item has no order, storage takes the value from the {@code item_order_tracker} counter, which is
 * never decremented when items leave a holdings record and therefore produces inflated values after items are
 * moved back and forth. Sending an explicit order, continuing after the highest order currently used in the
 * target holdings record, bypasses that counter.
 *
 * <p>Note: reading the highest order is not atomic with the subsequent update, so two concurrent moves into the
 * same holdings record may be assigned the same order values. This is an accepted limitation.
 */
public class ItemOrderService {
  private final CollectionResourceClient itemsStorageClient;

  public ItemOrderService(CollectionResourceClient itemsStorageClient) {
    this.itemsStorageClient = itemsStorageClient;
  }

  /**
   * Fetches the highest numeric {@code order} among items of the given holdings record using a single
   * limit-1 query. The {@code /number} CQL modifier is required: the storage index on {@code order} is
   * untyped, so without it values would be sorted as text ("9" before "10").
   *
   * @param holdingsRecordId the id of the holdings record
   * @return the highest order, or 0 when the holdings record has no items (or the top item has no order)
   */
  public CompletableFuture<Integer> fetchMaxOrder(String holdingsRecordId) {
    var cql = CqlQuery.exactMatch("holdingsRecordId", holdingsRecordId) + " sortBy order/number/sort.descending";
    return itemsStorageClient.getMany(cql, 1, 0)
      .thenApply(response -> {
        if (response.statusCode() != 200) {
          throw new ExternalResourceFetchException(response);
        }
        var topItems = response.getJson().getJsonArray(MoveApiUtil.ITEMS_PROPERTY);
        if (topItems == null || topItems.isEmpty()) {
          return 0;
        }
        return Optional.ofNullable(topItems.getJsonObject(0).getInteger(Item.ORDER_KEY)).orElse(0);
      });
  }

  /**
   * Places the items into the given holdings record and assigns each an explicit order continuing after
   * {@code currentMaxOrder}. Items keep their relative order; items without order go last.
   *
   * @param holdingsRecordId the id of the target holdings record
   * @param storedItems      the items in storage JSON representation
   * @param currentMaxOrder  the highest order currently used in the target holdings record (0 if none)
   * @return a list of Item objects with updated holdingId and order fields
   */
  public List<Item> assignOrder(String holdingsRecordId, List<JsonObject> storedItems, int currentMaxOrder) {
    var sortedItems = storedItems.stream()
      .sorted(Comparator.comparing(json -> json.getInteger(Item.ORDER_KEY),
        Comparator.nullsLast(Comparator.naturalOrder())))
      .toList();

    return IntStream.range(0, sortedItems.size())
      .mapToObj(i -> ItemUtil.fromStoredItemRepresentation(sortedItems.get(i))
        .withHoldingId(holdingsRecordId)
        .withOrder(currentMaxOrder + i + 1))
      .toList();
  }
}
