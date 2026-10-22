package org.folio.inventory.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.tuple;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import org.folio.inventory.domain.items.Item;
import org.folio.inventory.exceptions.ExternalResourceFetchException;
import org.folio.inventory.services.ItemOrderService;
import org.folio.inventory.storage.external.CollectionResourceClient;
import org.folio.inventory.support.http.client.Response;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class ItemOrderServiceTest {
  private static final String HOLDINGS_ID = UUID.randomUUID().toString();

  @Mock
  private CollectionResourceClient itemsStorageClient;
  @InjectMocks
  private ItemOrderService itemOrderService;

  @DisplayName("should query the top item by numeric descending order with limit 1")
  @Test
  void shouldQueryTopItemByNumericDescendingOrder() throws Exception {
    // arrange
    when(itemsStorageClient.getMany(anyString(), anyInt(), anyInt()))
      .thenReturn(CompletableFuture.completedFuture(itemsResponse(200, new JsonObject().put("order", 7))));

    // act
    itemOrderService.fetchMaxOrder(HOLDINGS_ID).get();

    // assert
    verify(itemsStorageClient).getMany(
      eq("holdingsRecordId==\"" + HOLDINGS_ID + "\" sortBy order/number/sort.descending"), eq(1), eq(0));
  }

  @DisplayName("should return order of the top item when holdings record has items")
  @Test
  void shouldReturnOrderOfTopItem_whenHoldingsRecordHasItems() throws Exception {
    // arrange
    when(itemsStorageClient.getMany(anyString(), anyInt(), anyInt()))
      .thenReturn(CompletableFuture.completedFuture(itemsResponse(200, new JsonObject().put("order", 7))));

    // act
    var maxOrder = itemOrderService.fetchMaxOrder(HOLDINGS_ID).get();

    // assert
    assertThat(maxOrder).isEqualTo(7);
  }

  @DisplayName("should return 0 when holdings record has no items")
  @Test
  void shouldReturnZero_whenHoldingsRecordHasNoItems() throws Exception {
    // arrange
    when(itemsStorageClient.getMany(anyString(), anyInt(), anyInt()))
      .thenReturn(CompletableFuture.completedFuture(itemsResponse(200)));

    // act
    var maxOrder = itemOrderService.fetchMaxOrder(HOLDINGS_ID).get();

    // assert
    assertThat(maxOrder).isZero();
  }

  @DisplayName("should return 0 when the top item has no order")
  @Test
  void shouldReturnZero_whenTopItemHasNoOrder() throws Exception {
    // arrange
    when(itemsStorageClient.getMany(anyString(), anyInt(), anyInt()))
      .thenReturn(CompletableFuture.completedFuture(itemsResponse(200, new JsonObject())));

    // act
    var maxOrder = itemOrderService.fetchMaxOrder(HOLDINGS_ID).get();

    // assert
    assertThat(maxOrder).isZero();
  }

  @DisplayName("should fail when storage responds with an error")
  @Test
  void shouldFail_whenStorageRespondsWithError() {
    // arrange
    when(itemsStorageClient.getMany(anyString(), anyInt(), anyInt()))
      .thenReturn(CompletableFuture.completedFuture(new Response(500, "boom", "text/plain", null)));

    // act
    var future = itemOrderService.fetchMaxOrder(HOLDINGS_ID);

    // assert
    assertThatThrownBy(future::get)
      .isInstanceOf(ExecutionException.class)
      .hasCauseInstanceOf(ExternalResourceFetchException.class);
  }

  @DisplayName("should move items to holdings record and number them after current max order")
  @Test
  void shouldNumberItemsAfterCurrentMaxOrder() {
    // arrange
    var stored = List.of(storedItem(5), storedItem(6));

    // act
    var items = itemOrderService.assignOrder(HOLDINGS_ID, stored, 10);

    // assert
    assertThat(items)
      .extracting(Item::getHoldingId, Item::getOrder)
      .containsExactly(
        tuple(HOLDINGS_ID, 11),
        tuple(HOLDINGS_ID, 12));
  }

  @DisplayName("should keep relative order of items and put items without order last")
  @Test
  void shouldKeepRelativeOrderAndPutItemsWithoutOrderLast() {
    // arrange
    var seventh = storedItem(7);
    var withoutOrder = storedItem(null);
    var third = storedItem(3);

    // act
    var items = itemOrderService.assignOrder(HOLDINGS_ID, List.of(seventh, withoutOrder, third), 0);

    // assert
    assertThat(items)
      .extracting(Item::getId, Item::getOrder)
      .containsExactly(
        tuple(third.getString("id"), 1),
        tuple(seventh.getString("id"), 2),
        tuple(withoutOrder.getString("id"), 3));
  }

  @DisplayName("should return empty list when there are no items to move")
  @Test
  void shouldReturnEmptyList_whenNoItemsToMove() {
    // act
    var items = itemOrderService.assignOrder(HOLDINGS_ID, List.of(), 4);

    // assert
    assertThat(items).isEmpty();
  }

  private static JsonObject storedItem(Integer order) {
    return new JsonObject()
      .put("id", UUID.randomUUID().toString())
      .put("holdingsRecordId", UUID.randomUUID().toString())
      .put("status", new JsonObject().put("name", "Available"))
      .put("order", order);
  }

  private static Response itemsResponse(int statusCode, JsonObject... items) {
    var body = new JsonObject()
      .put("items", new JsonArray(List.of(items)))
      .put("totalRecords", items.length);
    return new Response(statusCode, body.encode(), "application/json", null);
  }
}
