package org.folio.inventory.support;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.junit.Assert;
import org.junit.Test;

public class HoldingsRecordUtilTest {

  @Test
  public void mergeHoldingsRecords_shouldPreserveStatisticalCodeIdsFromExisting() {
    // given
    JsonArray existingStatisticalCodeIds = new JsonArray()
      .add("code1")
      .add("code2");

    JsonObject existing = new JsonObject()
      .put("id", "holding-1")
      .put("instanceId", "instance-1")
      .put("permanentLocationId", "loc-1")
      .put("statisticalCodeIds", existingStatisticalCodeIds)
      .put("callNumber", "existing-call-number");

    JsonArray mappedStatisticalCodeIds = new JsonArray()
      .add("code3")
      .add("code4");

    JsonObject mapped = new JsonObject()
      .put("callNumber", null)
      .put("statisticalCodeIds", mappedStatisticalCodeIds)
      .put("holdingsTypeId", "type-1");

    // when
    JsonObject result = HoldingsRecordUtil.mergeHoldingsRecords(existing, mapped);

    // then
    Assert.assertEquals("holding-1", result.getString("id"));
    Assert.assertEquals("instance-1", result.getString("instanceId"));
    Assert.assertEquals("loc-1", result.getString("permanentLocationId"));
    Assert.assertNull(result.getString("callNumber"));
    Assert.assertEquals("type-1", result.getString("holdingsTypeId"));
    Assert.assertEquals(existingStatisticalCodeIds, result.getJsonArray("statisticalCodeIds"));
    Assert.assertNotEquals(mappedStatisticalCodeIds, result.getJsonArray("statisticalCodeIds"));
  }

  @Test
  public void mergeHoldingsRecords_shouldPreserveAdministrativeNotesFromExisting() {
    // given
    JsonArray existingAdministrativeNotes = new JsonArray()
      .add("note1")
      .add("note2");

    JsonObject existing = new JsonObject()
      .put("id", "holding-1")
      .put("administrativeNotes", existingAdministrativeNotes)
      .put("callNumber", "existing-call-number");

    JsonArray mappedAdministrativeNotes = new JsonArray()
      .add("note3")
      .add("note4");

    JsonObject mapped = new JsonObject()
      .put("callNumber", "new-call-number")
      .put("administrativeNotes", mappedAdministrativeNotes)
      .put("holdingsTypeId", "type-1");

    // when
    JsonObject result = HoldingsRecordUtil.mergeHoldingsRecords(existing, mapped);

    // then
    Assert.assertEquals("new-call-number", result.getString("callNumber"));
    Assert.assertEquals("type-1", result.getString("holdingsTypeId"));
    Assert.assertEquals(existingAdministrativeNotes, result.getJsonArray("administrativeNotes"));
    Assert.assertNotEquals(mappedAdministrativeNotes, result.getJsonArray("administrativeNotes"));
  }

  @Test
  public void mergeHoldingsRecords_shouldPreserveBothArraysFromExisting() {
    // given
    JsonArray existingStatisticalCodeIds = new JsonArray()
      .add("code1")
      .add("code2");
    JsonArray existingAdministrativeNotes = new JsonArray()
      .add("note1")
      .add("note2");

    JsonObject existing = new JsonObject()
      .put("id", "holding-1")
      .put("statisticalCodeIds", existingStatisticalCodeIds)
      .put("administrativeNotes", existingAdministrativeNotes)
      .put("permanentLocationId", "loc-1");

    JsonArray mappedStatisticalCodeIds = new JsonArray()
      .add("code3");
    JsonArray mappedAdministrativeNotes = new JsonArray()
      .add("note3");

    JsonObject mapped = new JsonObject()
      .put("statisticalCodeIds", mappedStatisticalCodeIds)
      .put("administrativeNotes", mappedAdministrativeNotes)
      .put("permanentLocationId", "loc-2")
      .put("holdingsTypeId", "type-1");

    // when
    JsonObject result = HoldingsRecordUtil.mergeHoldingsRecords(existing, mapped);

    // then
    Assert.assertEquals("holding-1", result.getString("id"));
    Assert.assertEquals("loc-2", result.getString("permanentLocationId"));
    Assert.assertEquals("type-1", result.getString("holdingsTypeId"));
    Assert.assertEquals(existingStatisticalCodeIds, result.getJsonArray("statisticalCodeIds"));
    Assert.assertEquals(existingAdministrativeNotes, result.getJsonArray("administrativeNotes"));
  }

  @Test
  public void mergeHoldingsRecords_shouldHandleNullStatisticalCodeIdsInExisting() {
    // given
    JsonObject existing = new JsonObject()
      .put("id", "holding-1")
      .put("callNumber", "existing-call-number");

    JsonArray mappedStatisticalCodeIds = new JsonArray()
      .add("code1")
      .add("code2");

    JsonObject mapped = new JsonObject()
      .put("callNumber", "new-call-number")
      .put("statisticalCodeIds", mappedStatisticalCodeIds);

    // when
    JsonObject result = HoldingsRecordUtil.mergeHoldingsRecords(existing, mapped);

    // then
    Assert.assertEquals("new-call-number", result.getString("callNumber"));
    Assert.assertNull(result.getJsonArray("statisticalCodeIds"));
  }

  @Test
  public void mergeHoldingsRecords_shouldHandleNullAdministrativeNotesInExisting() {
    // given
    JsonObject existing = new JsonObject()
      .put("id", "holding-1")
      .put("callNumber", "existing-call-number");

    JsonArray mappedAdministrativeNotes = new JsonArray()
      .add("note1")
      .add("note2");

    JsonObject mapped = new JsonObject()
      .put("callNumber", "new-call-number")
      .put("administrativeNotes", mappedAdministrativeNotes);

    // when
    JsonObject result = HoldingsRecordUtil.mergeHoldingsRecords(existing, mapped);

    // then
    Assert.assertEquals("new-call-number", result.getString("callNumber"));
    Assert.assertNull(result.getJsonArray("administrativeNotes"));
  }

  @Test
  public void mergeHoldingsRecords_shouldMergeNestedObjects() {
    // given
    JsonArray existingStatisticalCodeIds = new JsonArray()
      .add("code1");

    JsonObject existing = new JsonObject()
      .put("id", "holding-1")
      .put("statisticalCodeIds", existingStatisticalCodeIds)
      .put("metadata", new JsonObject()
        .put("createdDate", "2024-01-01")
        .put("createdByUserId", "user-1"));

    JsonObject mapped = new JsonObject()
      .put("callNumber", "new-call-number")
      .put("metadata", new JsonObject()
        .put("updatedDate", "2024-01-02")
        .put("updatedByUserId", "user-2"));

    // when
    JsonObject result = HoldingsRecordUtil.mergeHoldingsRecords(existing, mapped);

    // then
    Assert.assertEquals("holding-1", result.getString("id"));
    Assert.assertEquals("new-call-number", result.getString("callNumber"));
    Assert.assertEquals(existingStatisticalCodeIds, result.getJsonArray("statisticalCodeIds"));

    JsonObject metadata = result.getJsonObject("metadata");
    Assert.assertNotNull(metadata);
    Assert.assertEquals("2024-01-01", metadata.getString("createdDate"));
    Assert.assertEquals("user-1", metadata.getString("createdByUserId"));
    Assert.assertEquals("2024-01-02", metadata.getString("updatedDate"));
    Assert.assertEquals("user-2", metadata.getString("updatedByUserId"));
  }

  @Test
  public void mergeHoldingsRecords_shouldOverrideExistingFieldsWithMappedValues() {
    // given
    JsonObject existing = new JsonObject()
      .put("id", "holding-1")
      .put("callNumber", "old-call-number")
      .put("holdingsTypeId", "old-type")
      .put("permanentLocationId", "old-location");

    JsonObject mapped = new JsonObject()
      .put("callNumber", "new-call-number")
      .put("holdingsTypeId", "new-type")
      .put("permanentLocationId", "new-location");

    // when
    JsonObject result = HoldingsRecordUtil.mergeHoldingsRecords(existing, mapped);

    // then
    Assert.assertEquals("holding-1", result.getString("id"));
    Assert.assertEquals("new-call-number", result.getString("callNumber"));
    Assert.assertEquals("new-type", result.getString("holdingsTypeId"));
    Assert.assertEquals("new-location", result.getString("permanentLocationId"));
  }

  @Test
  public void mergeHoldingsRecords_shouldPreserveFieldsNotInMapped() {
    // given
    JsonObject existing = new JsonObject()
      .put("id", "holding-1")
      .put("instanceId", "instance-1")
      .put("permanentLocationId", "loc-1")
      .put("callNumber", "call-number-1")
      .put("copyNumber", "copy-1");

    JsonObject mapped = new JsonObject()
      .put("callNumber", "new-call-number");

    // when
    JsonObject result = HoldingsRecordUtil.mergeHoldingsRecords(existing, mapped);

    // then
    Assert.assertEquals("holding-1", result.getString("id"));
    Assert.assertEquals("instance-1", result.getString("instanceId"));
    Assert.assertEquals("loc-1", result.getString("permanentLocationId"));
    Assert.assertEquals("new-call-number", result.getString("callNumber"));
    Assert.assertEquals("copy-1", result.getString("copyNumber"));
  }

  @Test
  public void mergeHoldingsRecords_shouldHandleEmptyArraysFromExisting() {
    // given
    JsonArray emptyStatisticalCodeIds = new JsonArray();
    JsonArray emptyAdministrativeNotes = new JsonArray();

    JsonObject existing = new JsonObject()
      .put("id", "holding-1")
      .put("statisticalCodeIds", emptyStatisticalCodeIds)
      .put("administrativeNotes", emptyAdministrativeNotes);

    JsonArray mappedStatisticalCodeIds = new JsonArray()
      .add("code1");
    JsonArray mappedAdministrativeNotes = new JsonArray()
      .add("note1");

    JsonObject mapped = new JsonObject()
      .put("statisticalCodeIds", mappedStatisticalCodeIds)
      .put("administrativeNotes", mappedAdministrativeNotes);

    // when
    JsonObject result = HoldingsRecordUtil.mergeHoldingsRecords(existing, mapped);

    // then
    Assert.assertEquals(emptyStatisticalCodeIds, result.getJsonArray("statisticalCodeIds"));
    Assert.assertEquals(emptyAdministrativeNotes, result.getJsonArray("administrativeNotes"));
    Assert.assertTrue(result.getJsonArray("statisticalCodeIds").isEmpty());
    Assert.assertTrue(result.getJsonArray("administrativeNotes").isEmpty());
  }

  @Test
  public void mergeHoldingsRecords_shouldHandleComplexNestedStructures() {
    // given
    JsonArray existingStatisticalCodeIds = new JsonArray().add("code1");

    JsonObject existing = new JsonObject()
      .put("id", "holding-1")
      .put("statisticalCodeIds", existingStatisticalCodeIds)
      .put("notes", new JsonArray()
        .add(new JsonObject()
          .put("noteType", "type1")
          .put("note", "existing note")));

    JsonObject mapped = new JsonObject()
      .put("callNumber", "new-call-number")
      .put("notes", new JsonArray()
        .add(new JsonObject()
          .put("noteType", "type2")
          .put("note", "new note")));

    // when
    JsonObject result = HoldingsRecordUtil.mergeHoldingsRecords(existing, mapped);

    // then
    Assert.assertEquals("holding-1", result.getString("id"));
    Assert.assertEquals("new-call-number", result.getString("callNumber"));
    Assert.assertEquals(existingStatisticalCodeIds, result.getJsonArray("statisticalCodeIds"));

    JsonArray notes = result.getJsonArray("notes");
    Assert.assertNotNull(notes);
    Assert.assertEquals(1, notes.size());
    JsonObject note = notes.getJsonObject(0);
    Assert.assertEquals("type2", note.getString("noteType"));
    Assert.assertEquals("new note", note.getString("note"));
  }

  @Test
  public void mergeHoldingsRecords_shouldPreserveTemporaryLocationIdFromExisting() {
    // given
    JsonObject existing = new JsonObject()
      .put("id", "holding-1")
      .put("permanentLocationId", "perm-loc-1")
      .put("temporaryLocationId", "temp-loc-1")
      .put("callNumber", "existing-call-number");

    JsonObject mapped = new JsonObject()
      .put("callNumber", "new-call-number")
      .put("temporaryLocationId", "temp-loc-2")
      .put("holdingsTypeId", "type-1");

    // when
    JsonObject result = HoldingsRecordUtil.mergeHoldingsRecords(existing, mapped);

    // then
    Assert.assertEquals("holding-1", result.getString("id"));
    Assert.assertEquals("new-call-number", result.getString("callNumber"));
    Assert.assertEquals("type-1", result.getString("holdingsTypeId"));
    Assert.assertEquals("temp-loc-1", result.getString("temporaryLocationId"));
    Assert.assertNotEquals("temp-loc-2", result.getString("temporaryLocationId"));
  }

  @Test
  public void mergeHoldingsRecords_shouldPreserveAcquisitionFieldsFromExisting() {
    // given
    JsonObject existing = new JsonObject()
      .put("id", "holding-1")
      .put("acquisitionFormat", "format-1")
      .put("acquisitionMethod", "method-1")
      .put("receiptStatus", "status-1")
      .put("callNumber", "existing-call-number");

    JsonObject mapped = new JsonObject()
      .put("callNumber", "new-call-number")
      .put("acquisitionFormat", "format-2")
      .put("acquisitionMethod", "method-2")
      .put("receiptStatus", "status-2");

    // when
    JsonObject result = HoldingsRecordUtil.mergeHoldingsRecords(existing, mapped);

    // then
    Assert.assertEquals("new-call-number", result.getString("callNumber"));
    Assert.assertEquals("format-1", result.getString("acquisitionFormat"));
    Assert.assertEquals("method-1", result.getString("acquisitionMethod"));
    Assert.assertEquals("status-1", result.getString("receiptStatus"));
    Assert.assertNotEquals("format-2", result.getString("acquisitionFormat"));
    Assert.assertNotEquals("method-2", result.getString("acquisitionMethod"));
    Assert.assertNotEquals("status-2", result.getString("receiptStatus"));
  }

  @Test
  public void mergeHoldingsRecords_shouldPreservePolicyFieldsFromExisting() {
    // given
    JsonObject existing = new JsonObject()
      .put("id", "holding-1")
      .put("illPolicyId", "ill-policy-1")
      .put("retentionPolicy", "retention-1")
      .put("digitizationPolicy", "digitization-1")
      .put("callNumber", "existing-call-number");

    JsonObject mapped = new JsonObject()
      .put("callNumber", "new-call-number")
      .put("illPolicyId", "ill-policy-2")
      .put("retentionPolicy", "retention-2")
      .put("digitizationPolicy", "digitization-2");

    // when
    JsonObject result = HoldingsRecordUtil.mergeHoldingsRecords(existing, mapped);

    // then
    Assert.assertEquals("new-call-number", result.getString("callNumber"));
    Assert.assertEquals("ill-policy-1", result.getString("illPolicyId"));
    Assert.assertEquals("retention-1", result.getString("retentionPolicy"));
    Assert.assertEquals("digitization-1", result.getString("digitizationPolicy"));
    Assert.assertNotEquals("ill-policy-2", result.getString("illPolicyId"));
    Assert.assertNotEquals("retention-2", result.getString("retentionPolicy"));
    Assert.assertNotEquals("digitization-2", result.getString("digitizationPolicy"));
  }

  @Test
  public void mergeHoldingsRecords_shouldPreserveNumberOfItemsFromExisting() {
    // given
    JsonObject existing = new JsonObject()
      .put("id", "holding-1")
      .put("numberOfItems", "10")
      .put("callNumber", "existing-call-number");

    JsonObject mapped = new JsonObject()
      .put("callNumber", "new-call-number")
      .put("numberOfItems", "20");

    // when
    JsonObject result = HoldingsRecordUtil.mergeHoldingsRecords(existing, mapped);

    // then
    Assert.assertEquals("new-call-number", result.getString("callNumber"));
    Assert.assertEquals("10", result.getString("numberOfItems"));
    Assert.assertNotEquals("20", result.getString("numberOfItems"));
  }

  @Test
  public void mergeHoldingsRecords_shouldPreserveAllNewFieldsFromExisting() {
    // given
    JsonObject existing = new JsonObject()
      .put("id", "holding-1")
      .put("temporaryLocationId", "temp-loc-1")
      .put("acquisitionFormat", "format-1")
      .put("acquisitionMethod", "method-1")
      .put("receiptStatus", "status-1")
      .put("illPolicyId", "ill-policy-1")
      .put("retentionPolicy", "retention-1")
      .put("digitizationPolicy", "digitization-1")
      .put("numberOfItems", "10")
      .put("permanentLocationId", "perm-loc-1");

    JsonObject mapped = new JsonObject()
      .put("temporaryLocationId", "temp-loc-2")
      .put("acquisitionFormat", "format-2")
      .put("acquisitionMethod", "method-2")
      .put("receiptStatus", "status-2")
      .put("illPolicyId", "ill-policy-2")
      .put("retentionPolicy", "retention-2")
      .put("digitizationPolicy", "digitization-2")
      .put("numberOfItems", "20")
      .put("permanentLocationId", "perm-loc-2")
      .put("holdingsTypeId", "type-1");

    // when
    JsonObject result = HoldingsRecordUtil.mergeHoldingsRecords(existing, mapped);

    // then
    Assert.assertEquals("holding-1", result.getString("id"));
    Assert.assertEquals("perm-loc-2", result.getString("permanentLocationId"));
    Assert.assertEquals("type-1", result.getString("holdingsTypeId"));
    Assert.assertEquals("temp-loc-1", result.getString("temporaryLocationId"));
    Assert.assertEquals("format-1", result.getString("acquisitionFormat"));
    Assert.assertEquals("method-1", result.getString("acquisitionMethod"));
    Assert.assertEquals("status-1", result.getString("receiptStatus"));
    Assert.assertEquals("ill-policy-1", result.getString("illPolicyId"));
    Assert.assertEquals("retention-1", result.getString("retentionPolicy"));
    Assert.assertEquals("digitization-1", result.getString("digitizationPolicy"));
    Assert.assertEquals("10", result.getString("numberOfItems"));
  }

  @Test
  public void mergeHoldingsRecords_shouldHandleNullNewFieldsInExisting() {
    // given
    JsonObject existing = new JsonObject()
      .put("id", "holding-1")
      .put("callNumber", "existing-call-number");

    JsonObject mapped = new JsonObject()
      .put("callNumber", "new-call-number")
      .put("temporaryLocationId", "temp-loc-1")
      .put("acquisitionFormat", "format-1")
      .put("acquisitionMethod", "method-1")
      .put("receiptStatus", "status-1")
      .put("illPolicyId", "ill-policy-1")
      .put("retentionPolicy", "retention-1")
      .put("digitizationPolicy", "digitization-1")
      .put("numberOfItems", "10");

    // when
    JsonObject result = HoldingsRecordUtil.mergeHoldingsRecords(existing, mapped);

    // then
    Assert.assertEquals("new-call-number", result.getString("callNumber"));
    Assert.assertNull(result.getString("temporaryLocationId"));
    Assert.assertNull(result.getString("acquisitionFormat"));
    Assert.assertNull(result.getString("acquisitionMethod"));
    Assert.assertNull(result.getString("receiptStatus"));
    Assert.assertNull(result.getString("illPolicyId"));
    Assert.assertNull(result.getString("retentionPolicy"));
    Assert.assertNull(result.getString("digitizationPolicy"));
    Assert.assertNull(result.getString("numberOfItems"));
  }
}