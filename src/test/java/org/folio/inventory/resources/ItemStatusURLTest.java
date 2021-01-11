package org.folio.inventory.resources;

import static org.junit.Assert.assertEquals;

import org.folio.inventory.domain.items.ItemStatusName;
import org.junit.Test;

public class ItemStatusURLTest {
  @Test
  public void canGetItemStatusByFullUrl() {
    assertEquals(ItemStatusName.IN_PROCESS, ItemStatusURL.getItemStatusNameForUrl("http://dummy.net/239853/mark-in-process").get());
  }

  @Test
  public void canGetUrlByItemStatus() {
    assertEquals("/mark-in-process", ItemStatusURL.getUrlForItemStatusName(ItemStatusName.IN_PROCESS).get());
  }

}
