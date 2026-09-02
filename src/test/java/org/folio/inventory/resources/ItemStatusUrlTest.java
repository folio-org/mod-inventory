package org.folio.inventory.resources;

import static org.assertj.core.api.Assertions.assertThat;

import org.folio.inventory.domain.items.ItemStatusName;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

class ItemStatusUrlTest {

  @ParameterizedTest
  @CsvSource({
    "/mark-in-process,In process",
    "/mark-in-process-non-requestable,In process (non-requestable)",
    "/mark-intellectual-item,Intellectual item",
    "/mark-long-missing,Long missing",
    "/mark-missing,Missing"
  })
  void canGetItemStatusByFullUrlAndCanGetUrlByStatusName(String url, String statusName) {
    // Determining the intended target status for a given URL
    String dummyUrl = "http://dummy.net/239853$URL$";
    final var itemStatus = ItemStatusUrl.getItemStatusNameForUrl(dummyUrl.replace("$URL$", url));
    ItemStatusName itemStatusName = ItemStatusName.forName(statusName);
    assertThat(itemStatus).isPresent().contains(itemStatusName);

    // Determining the URL for a given target status
    var itemStatusUrl = ItemStatusUrl.getUrlForItemStatusName(itemStatusName);
    assertThat(itemStatusUrl).isPresent().contains(url);
  }
}
