package org.folio.inventory.resources;

import org.folio.inventory.domain.items.ItemStatusName;
import org.junit.Test;
import org.junit.runner.RunWith;
import junitparams.Parameters;
import junitparams.JUnitParamsRunner;
import static org.assertj.core.api.Assertions.assertThat;

@RunWith(JUnitParamsRunner.class)
public class ItemStatusURLTest {
  @Parameters(
    {
      "/mark-in-process,In process",
      "/mark-in-process-non-requestable,In process (non-requestable)"
    }
  )
  @Test
  public void canGetItemStatusByFullUrlAndCanGetUrlByStatusName(String url, String statusName) {
    String dummyUrl = "http://dummy.net/239853$URL$";
    final var itemStatus= ItemStatusURL.getItemStatusNameForUrl(dummyUrl.replace("$URL$",url));
    assertThat(itemStatus.isPresent()).isTrue();
    ItemStatusName itemStatusName = ItemStatusName.forName(statusName);
    assertThat(itemStatus.get()).isEqualTo(itemStatusName);
    assertThat(ItemStatusURL.getUrlForItemStatusName(itemStatusName).get()).isEqualTo(url);
  }
}
