package org.folio.inventory.consortium.util;

import static org.folio.inventory.consortium.util.MarcRecordUtil.removeFieldFromMarcRecord;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.UUID;
import org.folio.ParsedRecord;
import org.folio.Record;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.marc4j.MarcJsonReader;
import org.marc4j.MarcReader;
import org.marc4j.MarcStreamWriter;
import org.marc4j.MarcWriter;
import support.TestUtil;

class MarcRecordUtilTest {

  private static final String PARSED_MARC_RECORD_PATH = "src/test/resources/marc/parsedRecordWith9Subfield.json";
  private static final String PARSED_CONTENT_WITHOUT_9_SUBFIELDS =
    "{\"fields\":[{\"001\":\"ybp7406411\"},{\"245\":{\"subfields\":[{\"a\":\"title\"},{\"b\":\"remainder_of_title\"},{\"c\":\"state_of_responsibility\"},{\"f\":\"inclusive_dates\"},{\"g\":\"bulk_dates\"},{\"h\":\"medium\"}],\"ind1\":\" \",\"ind2\":\" \"}},{\"245\":{\"subfields\":[{\"a\":\"title\"},{\"b\":\"remainder_of_title\"},{\"c\":\"state_of_responsibility\"},{\"f\":\"inclusive_dates\"},{\"g\":\"bulk_dates\"},{\"h\":\"medium\"}],\"ind1\":\" \",\"ind2\":\" \"}},{\"248\":{\"subfields\":[{\"a\":\"title\"},{\"b\":\"remainder_of_title\"},{\"c\":\"state_of_responsibility\"},{\"f\":\"inclusive_dates\"},{\"g\":\"bulk_dates\"},{\"h\":\"medium\"},{\"9\":\"e84e4dd4-9d27-4f42-8fda-408d78c7f7ee\"}],\"ind1\":\" \",\"ind2\":\" \"}},{\"700\":{\"subfields\":[{\"a\":\"personal_name_1\"},{\"b\":\"numeration_1\"},{\"9\":\"3f2923d3-6f8e-41a6-94e1-09eaf32872e0\"}],\"ind1\":\" \",\"ind2\":\" \"}},{\"700\":{\"subfields\":[{\"a\":\"personal_name_2\"},{\"b\":\"numeration_2\"}],\"ind1\":\" \",\"ind2\":\" \"}}]}";
  private static final String UUID_1 = "e84e4dd4-9d27-4f42-8fda-408d78c7f7ee";
  private static final String UUID_3 = "7e11b935-2b3a-4e79-8e57-5cde4561a2a8";
  private static final String FIELD_TAG_TO_REMOVE = "001";

  private Record marcRecord;

  @BeforeEach
  void setUp() {
    marcRecord = new Record();
    marcRecord.setParsedRecord(new ParsedRecord());
    ParsedRecord parsedRecord = marcRecord.getParsedRecord();
    JsonObject content = new JsonObject();
    JsonArray fields = new JsonArray();

    fields.add(new JsonObject().put("001", "in00000000001"));
    fields.add(new JsonObject().put("245", "Some Title"));
    fields.add(new JsonObject().put("100", "Main Author"));
    content.put("fields", fields);

    parsedRecord.setContent(content);
    parsedRecord.setFormattedContent(content.encodePrettily());
  }

  @Test
  void shouldRemove9subfieldsThatContainValue() {
    // given
    String recordId = UUID.randomUUID().toString();

    String parsedRecordContent = TestUtil.readFileFromPath(PARSED_MARC_RECORD_PATH);
    ParsedRecord parsedRecord = new ParsedRecord();
    String leader = new JsonObject(parsedRecordContent).getString("leader");
    parsedRecord.setContent(parsedRecordContent);
    Record testRecord = new Record().withId(recordId).withParsedRecord(parsedRecord);
    // when
    MarcRecordUtil.removeSubfieldsThatContainsValues(testRecord, List.of("245", "700"), '9', List.of(UUID_1, UUID_3));
    // then
    JsonObject content = new JsonObject(parsedRecord.getContent().toString());
    JsonArray fields = content.getJsonArray("fields");
    String newLeader = content.getString("leader");
    assertNotEquals(leader, newLeader);
    assertFalse(fields.isEmpty());
    content.remove("leader");
    assertEquals(PARSED_CONTENT_WITHOUT_9_SUBFIELDS, content.encode());
  }

  @Test
  void shouldNotThrowExceptionIfNullMarcRecordDuringRemoveOfSubfield() {
    // given
    String recordId = UUID.randomUUID().toString();

    ParsedRecord parsedRecord = new ParsedRecord();
    parsedRecord.setContent("null");
    Record testRecord = new Record().withId(recordId).withParsedRecord(parsedRecord);
    // when
    assertDoesNotThrow(() -> MarcRecordUtil.removeSubfieldsThatContainsValues(testRecord, List.of("245", "700"), '9',
      List.of(UUID_1, UUID_3)), "Exception thrown");
  }

  @Test
  void removeFieldFromMarcRecord_Remove001Field() {
    Record updatedRecord = removeFieldFromMarcRecord(marcRecord, FIELD_TAG_TO_REMOVE);

    // content is now a marc4j-recalculated JSON String, like every other write path in this codebase -
    // no longer a raw JsonObject, so it must be re-parsed rather than mapped.
    JsonObject content = new JsonObject(updatedRecord.getParsedRecord().getContent().toString());
    JsonArray fields = content.getJsonArray("fields");

    assertEquals(2, fields.size());

    for (int i = 0; i < fields.size(); i++) {
      JsonObject field = fields.getJsonObject(i);
      assertFalse(field.containsKey(FIELD_TAG_TO_REMOVE));
    }
  }

  @Test
  void removeFieldFromMarcRecord_removesAllFieldsSharingTagAndRecalculatesLeader() {
    // given: two "700" fields sharing the same tag, plus other distinct fields. The old implementation's
    // for-loop skipped every other match after a removal shifted the array, so only one of the two "700"s
    // would have been removed.
    String parsedContent =
      "{\"leader\":\"00000nam a2200000 a 4500\",\"fields\":["
      + "{\"001\":\"in00000000001\"},"
      + "{\"700\":{\"subfields\":[{\"a\":\"Author One\"}],\"ind1\":\" \",\"ind2\":\" \"}},"
      + "{\"700\":{\"subfields\":[{\"a\":\"Author Two\"}],\"ind1\":\" \",\"ind2\":\" \"}},"
      + "{\"245\":{\"subfields\":[{\"a\":\"Title\"}],\"ind1\":\" \",\"ind2\":\" \"}}]}";
    ParsedRecord parsedRecord = new ParsedRecord();
    parsedRecord.setContent(parsedContent);
    Record testRecord = new Record().withId(UUID.randomUUID().toString()).withParsedRecord(parsedRecord);

    // when
    removeFieldFromMarcRecord(testRecord, "700");

    // then: both "700" fields are gone; the untagged fields survive untouched
    JsonObject content = new JsonObject(testRecord.getParsedRecord().getContent().toString());
    JsonArray fields = content.getJsonArray("fields");
    assertEquals(2, fields.size());
    for (int i = 0; i < fields.size(); i++) {
      assertFalse(fields.getJsonObject(i).containsKey("700"));
    }

    // and: the leader's record-length prefix is recalculated to reflect the actually-reduced content -
    // verified independently by re-serializing the updated content to ISO 2709 and comparing byte lengths,
    // rather than trusting the same code path that produced it.
    String newLeader = content.getString("leader");
    int declaredLength = Integer.parseInt(newLeader.substring(0, 5));
    int actualLength = reserializeToIso2709Length(testRecord.getParsedRecord().getContent().toString());
    assertEquals(actualLength, declaredLength);
  }

  @Test
  void isSubfieldExistUsesFallbackParser_whenPrimaryParsePathFails() {
    // given: a JSON comment is valid input for Jackson (used by the fallback's normalization) but is
    // rejected outright by marc4j's own hand-rolled JSON parser (used by the primary parse path), forcing
    // computeMarcRecord into its buildMarcReader fallback branch. Before the fix, buildMarcReader fed the
    // whole ParsedRecord wrapper object (not its content) into that fallback parser, so it silently produced
    // an empty record and this would have returned false.
    String contentWithComment =
      "{/*hand-edited*/\"fields\":[{\"245\":{\"subfields\":[{\"9\":\"linked-value\"}],\"ind1\":\" \",\"ind2\":\" \"}}]}";
    var parsedRecord = new ParsedRecord();
    parsedRecord.setContent(contentWithComment);
    var recordForcingFallback = new Record().withId(UUID.randomUUID().toString()).withParsedRecord(parsedRecord);

    // when
    var result = MarcRecordUtil.isSubfieldExist(recordForcingFallback, '9');

    // then
    assertTrue(result);
  }

  @Test
  void removeSubfieldsThatContainsValues_skipsControlFieldTagInsteadOfThrowing() {
    // given: "001" is a control field (a plain string value, not a DataField). removeSubfieldsThatContainsValues
    // has no try/catch of its own, so the old unconditional (DataField) cast's ClassCastException propagated
    // straight to its caller (MarcInstanceSharingHandlerImpl) instead of being treated as "no matching field".
    String parsedRecordContent = TestUtil.readFileFromPath(PARSED_MARC_RECORD_PATH);
    ParsedRecord parsedRecord = new ParsedRecord();
    parsedRecord.setContent(parsedRecordContent);
    Record testRecord = new Record().withId(UUID.randomUUID().toString()).withParsedRecord(parsedRecord);

    // when / then
    assertDoesNotThrow(() -> MarcRecordUtil.removeSubfieldsThatContainsValues(testRecord, List.of("001"), '9',
      List.of(UUID_1)));

    // and: "001" is a control field with no subfields, so nothing was removed
    JsonObject content = new JsonObject(parsedRecord.getContent().toString());
    boolean has001WithOriginalValue = content.getJsonArray("fields").stream()
      .map(JsonObject.class::cast)
      .anyMatch(field -> "ybp7406411".equals(field.getString("001")));
    assertTrue(has001WithOriginalValue);
  }

  private static int reserializeToIso2709Length(String marcJsonContent) {
    MarcReader reader =
      new MarcJsonReader(new ByteArrayInputStream(marcJsonContent.getBytes(StandardCharsets.UTF_8)));
    assertTrue(reader.hasNext());
    org.marc4j.marc.Record marc4jRecord = reader.next();
    try (ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
      MarcWriter writer = new MarcStreamWriter(baos);
      try (AutoCloseable closeWriter = writer::close) {
        writer.write(marc4jRecord);
      }
      return baos.size();
    } catch (Exception e) {
      throw new IllegalStateException(e);
    }
  }

  @Test
  void isSubfieldExistReturnsTrue() {
    var marcJson =
      "{\"leader\":\"00000cam a2200000 a 4500\",\"fields\":[{\"100\":{\"subfields\":[{\"a\":\"John Doe\"},{\"9\":\"test\"}],\"ind1\":\" \",\"ind2\":\" \"}}]}";
    var recordWith9 = new Record();
    var parsedRecord = new ParsedRecord();
    parsedRecord.setContent(marcJson);
    recordWith9.setParsedRecord(parsedRecord);
    var result = MarcRecordUtil.isSubfieldExist(recordWith9, '9');
    assertTrue(result);
  }

  @Test
  void isSubfieldExistReturnsFalse() {
    var marcJson =
      "{\"leader\":\"00000cam a2200000 a 4500\",\"fields\":[{\"100\":{\"subfields\":[{\"a\":\"John Doe\"}],\"ind1\":\" \",\"ind2\":\" \"}}]}";
    var recordWithout9 = new Record();
    var parsedRecord = new ParsedRecord();
    parsedRecord.setContent(marcJson);
    recordWithout9.setParsedRecord(parsedRecord);
    var result = MarcRecordUtil.isSubfieldExist(recordWithout9, '9');
    assertFalse(result);
  }
}
