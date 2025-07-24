package org.folio.inventory.consortium.util;

import static org.folio.inventory.consortium.util.MarcRecordUtil.removeFieldFromMarcRecord;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import java.io.IOException;
import java.util.List;
import java.util.UUID;
import org.folio.ParsedRecord;
import org.folio.Record;
import org.folio.inventory.TestUtil;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

@RunWith(VertxUnitRunner.class)
public class MarcRecordUtilTest {
  private static final String PARSED_MARC_RECORD_PATH = "src/test/resources/marc/parsedRecordWith9Subfield.json";
  private static final String PARSED_CONTENT_WITHOUT_9_SUBFIELDS = "{\"fields\":[{\"001\":\"ybp7406411\"},{\"245\":{\"subfields\":[{\"a\":\"title\"},{\"b\":\"remainder_of_title\"},{\"c\":\"state_of_responsibility\"},{\"f\":\"inclusive_dates\"},{\"g\":\"bulk_dates\"},{\"h\":\"medium\"}],\"ind1\":\" \",\"ind2\":\" \"}},{\"245\":{\"subfields\":[{\"a\":\"title\"},{\"b\":\"remainder_of_title\"},{\"c\":\"state_of_responsibility\"},{\"f\":\"inclusive_dates\"},{\"g\":\"bulk_dates\"},{\"h\":\"medium\"}],\"ind1\":\" \",\"ind2\":\" \"}},{\"248\":{\"subfields\":[{\"a\":\"title\"},{\"b\":\"remainder_of_title\"},{\"c\":\"state_of_responsibility\"},{\"f\":\"inclusive_dates\"},{\"g\":\"bulk_dates\"},{\"h\":\"medium\"},{\"9\":\"e84e4dd4-9d27-4f42-8fda-408d78c7f7ee\"}],\"ind1\":\" \",\"ind2\":\" \"}},{\"700\":{\"subfields\":[{\"a\":\"personal_name_1\"},{\"b\":\"numeration_1\"},{\"9\":\"3f2923d3-6f8e-41a6-94e1-09eaf32872e0\"}],\"ind1\":\" \",\"ind2\":\" \"}},{\"700\":{\"subfields\":[{\"a\":\"personal_name_2\"},{\"b\":\"numeration_2\"}],\"ind1\":\" \",\"ind2\":\" \"}}]}";
  private static final String UUID_1 = "e84e4dd4-9d27-4f42-8fda-408d78c7f7ee";
  private static final String UUID_3 = "7e11b935-2b3a-4e79-8e57-5cde4561a2a8";
  private Record marcRecord;
  private static final String fieldTagToRemove = "001";

  @Before
  public void setUp() {
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
  public void shouldRemove9subfieldsThatContainValue() throws IOException {
    // given
    String recordId = UUID.randomUUID().toString();

    String parsedRecordContent = TestUtil.readFileFromPath(PARSED_MARC_RECORD_PATH);
    ParsedRecord parsedRecord = new ParsedRecord();
    String leader = new JsonObject(parsedRecordContent).getString("leader");
    parsedRecord.setContent(parsedRecordContent);
    Record record = new Record().withId(recordId).withParsedRecord(parsedRecord);
    // when
    MarcRecordUtil.removeSubfieldsThatContainsValues(record, List.of("245", "700"), '9', List.of(UUID_1, UUID_3));
    // then
    JsonObject content = new JsonObject(parsedRecord.getContent().toString());
    JsonArray fields = content.getJsonArray("fields");
    String newLeader = content.getString("leader");
    Assert.assertNotEquals(leader, newLeader);
    Assert.assertFalse(fields.isEmpty());
    content.remove("leader");
    Assert.assertEquals(PARSED_CONTENT_WITHOUT_9_SUBFIELDS, content.encode());
  }

  @Test
  public void shouldNotThrowExceptionIfNullMarcRecordDuringRemoveOfSubfield() throws IOException {
    // given
    String recordId = UUID.randomUUID().toString();

    ParsedRecord parsedRecord = new ParsedRecord();
    parsedRecord.setContent("null");
    Record record = new Record().withId(recordId).withParsedRecord(parsedRecord);
    // when
    try {
      MarcRecordUtil.removeSubfieldsThatContainsValues(record, List.of("245", "700"), '9', List.of(UUID_1, UUID_3));
    } catch (Exception e) {
      Assert.fail("Exception thrown");
    }
  }

  @Test
  public void removeFieldFromMarcRecord_Remove001Field() {
    Record updatedRecord = removeFieldFromMarcRecord(marcRecord, fieldTagToRemove);

    JsonObject content = JsonObject.mapFrom(updatedRecord.getParsedRecord().getContent());
    JsonArray fields = content.getJsonArray("fields");

    assertEquals(2, fields.size());

    for (int i = 0; i < fields.size(); i++) {
      JsonObject field = fields.getJsonObject(i);
      assertFalse(field.containsKey(fieldTagToRemove));
    }
  }

  @Test
  public void isSubfieldExistReturnsTrue() {
    var marcJson = "{\"leader\":\"00000cam a2200000 a 4500\",\"fields\":[{\"100\":{\"subfields\":[{\"a\":\"John Doe\"},{\"9\":\"test\"}],\"ind1\":\" \",\"ind2\":\" \"}}]}";
    var recordWith9 = new Record();
    var parsedRecord = new ParsedRecord();
    parsedRecord.setContent(marcJson);
    recordWith9.setParsedRecord(parsedRecord);
    var result = MarcRecordUtil.isSubfieldExist(recordWith9, '9');
    Assert.assertTrue(result);
  }

  @Test
  public void isSubfieldExistReturnsFalse() {
    var marcJson = "{\"leader\":\"00000cam a2200000 a 4500\",\"fields\":[{\"100\":{\"subfields\":[{\"a\":\"John Doe\"}],\"ind1\":\" \",\"ind2\":\" \"}}]}";
    var recordWithout9 = new Record();
    var parsedRecord = new ParsedRecord();
    parsedRecord.setContent(marcJson);
    recordWithout9.setParsedRecord(parsedRecord);
    var result = MarcRecordUtil.isSubfieldExist(recordWithout9, '9');
    Assert.assertFalse(result);
  }
}
