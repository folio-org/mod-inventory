package org.folio.inventory.support;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.junit.jupiter.api.Test;

class HoldingsRecordUtilTest {

  @Test
  void mergeHoldingsRecords_shouldPreserveStatisticalCodeIdsFromExisting() {
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
    assertEquals("holding-1", result.getString("id"));
    assertEquals("instance-1", result.getString("instanceId"));
    assertEquals("loc-1", result.getString("permanentLocationId"));
    assertNull(result.getString("callNumber"));
    assertEquals("type-1", result.getString("holdingsTypeId"));
    assertEquals(existingStatisticalCodeIds, result.getJsonArray("statisticalCodeIds"));
    assertNotEquals(mappedStatisticalCodeIds, result.getJsonArray("statisticalCodeIds"));
  }

  @Test
  void mergeHoldingsRecords_shouldPreserveAdministrativeNotesFromExisting() {
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
    assertEquals("new-call-number", result.getString("callNumber"));
    assertEquals("type-1", result.getString("holdingsTypeId"));
    assertEquals(existingAdministrativeNotes, result.getJsonArray("administrativeNotes"));
    assertNotEquals(mappedAdministrativeNotes, result.getJsonArray("administrativeNotes"));
  }

  @Test
  void mergeHoldingsRecords_shouldPreserveBothArraysFromExisting() {
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
    assertEquals("holding-1", result.getString("id"));
    assertEquals("loc-2", result.getString("permanentLocationId"));
    assertEquals("type-1", result.getString("holdingsTypeId"));
    assertEquals(existingStatisticalCodeIds, result.getJsonArray("statisticalCodeIds"));
    assertEquals(existingAdministrativeNotes, result.getJsonArray("administrativeNotes"));
  }

  @Test
  void mergeHoldingsRecords_shouldHandleNullStatisticalCodeIdsInExisting() {
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
    assertEquals("new-call-number", result.getString("callNumber"));
    assertNull(result.getJsonArray("statisticalCodeIds"));
  }

  @Test
  void mergeHoldingsRecords_shouldHandleNullAdministrativeNotesInExisting() {
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
    assertEquals("new-call-number", result.getString("callNumber"));
    assertNull(result.getJsonArray("administrativeNotes"));
  }

  @Test
  void mergeHoldingsRecords_shouldMergeNestedObjects() {
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
    assertEquals("holding-1", result.getString("id"));
    assertEquals("new-call-number", result.getString("callNumber"));
    assertEquals(existingStatisticalCodeIds, result.getJsonArray("statisticalCodeIds"));

    JsonObject metadata = result.getJsonObject("metadata");
    assertNotNull(metadata);
    assertEquals("2024-01-01", metadata.getString("createdDate"));
    assertEquals("user-1", metadata.getString("createdByUserId"));
    assertEquals("2024-01-02", metadata.getString("updatedDate"));
    assertEquals("user-2", metadata.getString("updatedByUserId"));
  }

  @Test
  void mergeHoldingsRecords_shouldOverrideExistingFieldsWithMappedValues() {
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
    assertEquals("holding-1", result.getString("id"));
    assertEquals("new-call-number", result.getString("callNumber"));
    assertEquals("new-type", result.getString("holdingsTypeId"));
    assertEquals("new-location", result.getString("permanentLocationId"));
  }

  @Test
  void mergeHoldingsRecords_shouldPreserveFieldsNotInMapped() {
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
    assertEquals("holding-1", result.getString("id"));
    assertEquals("instance-1", result.getString("instanceId"));
    assertEquals("loc-1", result.getString("permanentLocationId"));
    assertEquals("new-call-number", result.getString("callNumber"));
    assertEquals("copy-1", result.getString("copyNumber"));
  }

  @Test
  void mergeHoldingsRecords_shouldHandleEmptyArraysFromExisting() {
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
    assertEquals(emptyStatisticalCodeIds, result.getJsonArray("statisticalCodeIds"));
    assertEquals(emptyAdministrativeNotes, result.getJsonArray("administrativeNotes"));
    assertTrue(result.getJsonArray("statisticalCodeIds").isEmpty());
    assertTrue(result.getJsonArray("administrativeNotes").isEmpty());
  }

  @Test
  void mergeHoldingsRecords_shouldHandleComplexNestedStructures() {
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
    assertEquals("holding-1", result.getString("id"));
    assertEquals("new-call-number", result.getString("callNumber"));
    assertEquals(existingStatisticalCodeIds, result.getJsonArray("statisticalCodeIds"));

    JsonArray notes = result.getJsonArray("notes");
    assertNotNull(notes);
    assertEquals(1, notes.size());
    JsonObject note = notes.getJsonObject(0);
    assertEquals("type2", note.getString("noteType"));
    assertEquals("new note", note.getString("note"));
  }

  @Test
  void mergeHoldingsRecords_shouldPreserveTemporaryLocationIdFromExisting() {
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
    assertEquals("holding-1", result.getString("id"));
    assertEquals("new-call-number", result.getString("callNumber"));
    assertEquals("type-1", result.getString("holdingsTypeId"));
    assertEquals("temp-loc-1", result.getString("temporaryLocationId"));
    assertNotEquals("temp-loc-2", result.getString("temporaryLocationId"));
  }

  @Test
  void mergeHoldingsRecords_shouldPreserveAcquisitionFieldsFromExisting() {
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
    assertEquals("new-call-number", result.getString("callNumber"));
    assertEquals("format-1", result.getString("acquisitionFormat"));
    assertEquals("method-1", result.getString("acquisitionMethod"));
    assertEquals("status-1", result.getString("receiptStatus"));
    assertNotEquals("format-2", result.getString("acquisitionFormat"));
    assertNotEquals("method-2", result.getString("acquisitionMethod"));
    assertNotEquals("status-2", result.getString("receiptStatus"));
  }

  @Test
  void mergeHoldingsRecords_shouldPreservePolicyFieldsFromExisting() {
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
    assertEquals("new-call-number", result.getString("callNumber"));
    assertEquals("ill-policy-1", result.getString("illPolicyId"));
    assertEquals("retention-1", result.getString("retentionPolicy"));
    assertEquals("digitization-1", result.getString("digitizationPolicy"));
    assertNotEquals("ill-policy-2", result.getString("illPolicyId"));
    assertNotEquals("retention-2", result.getString("retentionPolicy"));
    assertNotEquals("digitization-2", result.getString("digitizationPolicy"));
  }

  @Test
  void mergeHoldingsRecords_shouldPreserveNumberOfItemsFromExisting() {
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
    assertEquals("new-call-number", result.getString("callNumber"));
    assertEquals("10", result.getString("numberOfItems"));
    assertNotEquals("20", result.getString("numberOfItems"));
  }

  @Test
  void mergeHoldingsRecords_shouldPreserveAllNewFieldsFromExisting() {
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
    assertEquals("holding-1", result.getString("id"));
    assertEquals("perm-loc-2", result.getString("permanentLocationId"));
    assertEquals("type-1", result.getString("holdingsTypeId"));
    assertEquals("temp-loc-1", result.getString("temporaryLocationId"));
    assertEquals("format-1", result.getString("acquisitionFormat"));
    assertEquals("method-1", result.getString("acquisitionMethod"));
    assertEquals("status-1", result.getString("receiptStatus"));
    assertEquals("ill-policy-1", result.getString("illPolicyId"));
    assertEquals("retention-1", result.getString("retentionPolicy"));
    assertEquals("digitization-1", result.getString("digitizationPolicy"));
    assertEquals("10", result.getString("numberOfItems"));
  }

  @Test
  void mergeHoldingsRecords_shouldHandleNullNewFieldsInExisting() {
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
    assertEquals("new-call-number", result.getString("callNumber"));
    assertNull(result.getString("temporaryLocationId"));
    assertNull(result.getString("acquisitionFormat"));
    assertNull(result.getString("acquisitionMethod"));
    assertNull(result.getString("receiptStatus"));
    assertNull(result.getString("illPolicyId"));
    assertNull(result.getString("retentionPolicy"));
    assertNull(result.getString("digitizationPolicy"));
    assertNull(result.getString("numberOfItems"));
  }
}