package org.folio.inventory.domain.items;

import java.util.ArrayList;
import java.util.List;

public class ItemsMove {

  public static final String TO_HOLDINGS_RECORD_ID = "toHoldingsRecordId";
  public static final String ITEM_IDS = "itemIds";

  private String toHoldingsRecordId;
  private List<String> itemIds = new ArrayList<>();

  public ItemsMove() {
  }

  public String getToHoldingsRecordId() {
    return toHoldingsRecordId;
  }

  public List<String> getItemIds() {
    return itemIds;
  }

  public ItemsMove withToHoldingsRecordId(String toHoldingsRecordId) {
    this.toHoldingsRecordId = toHoldingsRecordId;
    return this;
  }

  public ItemsMove withItemIds(List<String> itemIds) {
    this.itemIds = itemIds;
    return this;
  }
}
