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
    Item item = new Item(UUID.randomUUID().toString(), "2", UUID.randomUUID().toString(),
      new Status(AVAILABLE), UUID.randomUUID().toString(), UUID.randomUUID().toString(), null)
      .withTemporaryLoanTypeId(UUID.randomUUID().toString())
      .withPermanentLocationId(UUID.randomUUID().toString())
      .withTemporaryLocationId(UUID.randomUUID().toString());

    // when
    String mappedResult = ItemUtil.mapToMappingResultRepresentation(item);
    JsonObject actualItemJson = new JsonObject(mappedResult);

    // then
    Assert.assertEquals(item.id, actualItemJson.getString("id"));
    Assert.assertEquals(item.getVersion(), actualItemJson.getString("_version"));
    Assert.assertEquals(item.getHoldingId(), actualItemJson.getString("holdingsRecordId"));
    Assert.assertEquals(item.getStatus().getName().value(), getNestedProperty(actualItemJson, "status", "name"));
    Assert.assertEquals(item.getMaterialTypeId(), getNestedProperty(actualItemJson, "materialType", "id"));
    Assert.assertEquals(item.getPermanentLoanTypeId(), getNestedProperty(actualItemJson, "permanentLoanType", "id"));
    Assert.assertEquals(item.getTemporaryLoanTypeId(), getNestedProperty(actualItemJson, "temporaryLoanType", "id"));
    Assert.assertEquals(item.getPermanentLocationId(), getNestedProperty(actualItemJson, "permanentLocation", "id"));
    Assert.assertEquals(item.getTemporaryLocationId(), getNestedProperty(actualItemJson, "temporaryLocation", "id"));
  }

  @Test
  public void shouldMapNestedFieldsAndParseVersionAndPassThroughOtherFields() {
    // given
    JsonObject patchJson = new JsonObject()
      .put("_version", "7")
      .put("materialType", new JsonObject().put("id", "mt-id"))
      .put("permanentLoanType", new JsonObject().put("id", "plt-id"))
      .put("temporaryLoanType", new JsonObject().put("id", "tlt-id"))
      .put("permanentLocation", new JsonObject().put("id", "pl-id"))
      .put("temporaryLocation", new JsonObject().put("id", "tl-id"))
      .put("barcode", "123456")
      .put("notes", "keep-as-is");

    // when
    JsonObject result = ItemUtil.patchToStorageJson(patchJson);

    // then
    Assert.assertEquals(Integer.valueOf(7), result.getInteger("_version"));

    Assert.assertEquals("mt-id", result.getString("materialTypeId"));
    Assert.assertEquals("plt-id", result.getString("permanentLoanTypeId"));
    Assert.assertEquals("tlt-id", result.getString("temporaryLoanTypeId"));
    Assert.assertEquals("pl-id", result.getString("permanentLocationId"));
    Assert.assertEquals("tl-id", result.getString("temporaryLocationId"));

    Assert.assertEquals("123456", result.getString("barcode"));
    Assert.assertEquals("keep-as-is", result.getString("notes"));

    Assert.assertNull(result.getValue("materialType"));
    Assert.assertNull(result.getValue("permanentLoanType"));
    Assert.assertNull(result.getValue("temporaryLoanType"));
    Assert.assertNull(result.getValue("permanentLocation"));
    Assert.assertNull(result.getValue("temporaryLocation"));
  }

  @Test
  public void shouldSetVersionNullWhenPatchVersionIsNull() {
    // given
    JsonObject patchJson = new JsonObject()
      .put("_version", (Object) null);

    // when
    JsonObject result = ItemUtil.patchToStorageJson(patchJson);

    // then
    Assert.assertTrue(result.containsKey("_version"));
    Assert.assertNull(result.getValue("_version"));
  }
}
