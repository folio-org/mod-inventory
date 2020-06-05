package org.folio.inventory.support;

import io.vertx.core.json.JsonObject;
import org.folio.inventory.domain.items.Item;
import org.folio.inventory.domain.items.Status;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.util.UUID;

import static org.folio.inventory.domain.items.ItemStatusName.AVAILABLE;
import static org.folio.inventory.support.JsonHelper.getNestedProperty;

@RunWith(JUnit4.class)
public class ItemUtilTest {

  @Test
  public void shouldReturnItemAsMappingResultRepresentation() {
    // given
    Item item = new Item(UUID.randomUUID().toString(), UUID.randomUUID().toString(),
      new Status(AVAILABLE), UUID.randomUUID().toString(), UUID.randomUUID().toString(), null)
      .withTemporaryLoanTypeId(UUID.randomUUID().toString())
      .withPermanentLocationId(UUID.randomUUID().toString())
      .withTemporaryLocationId(UUID.randomUUID().toString());

    // when
    String mappedResult = ItemUtil.mapToMappingResultRepresentation(item);
    JsonObject actualItemJson = new JsonObject(mappedResult);

    // then
    Assert.assertEquals(item.id, actualItemJson.getString("id"));
    Assert.assertEquals(item.getHoldingId(), actualItemJson.getString("holdingsRecordId"));
    Assert.assertEquals(item.getStatus().getName().value(), getNestedProperty(actualItemJson, "status", "name"));
    Assert.assertEquals(item.getMaterialTypeId(), getNestedProperty(actualItemJson, "materialType", "id"));
    Assert.assertEquals(item.getPermanentLoanTypeId(), getNestedProperty(actualItemJson, "permanentLoanType", "id"));
    Assert.assertEquals(item.getTemporaryLoanTypeId(), getNestedProperty(actualItemJson, "temporaryLoanType", "id"));
    Assert.assertEquals(item.getPermanentLocationId(), getNestedProperty(actualItemJson, "permanentLocation", "id"));
    Assert.assertEquals(item.getTemporaryLocationId(), getNestedProperty(actualItemJson, "temporaryLocation", "id"));
  }
}
