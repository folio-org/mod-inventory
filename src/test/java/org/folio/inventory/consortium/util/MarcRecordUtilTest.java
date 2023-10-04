package org.folio.inventory.consortium.util;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.folio.inventory.TestUtil;
import org.folio.ParsedRecord;
import org.folio.Record;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.util.List;
import java.util.UUID;

@RunWith(VertxUnitRunner.class)
public class MarcRecordUtilTest {
  private static final String PARSED_MARC_RECORD_PATH = "src/test/resources/marc/parsedRecordWith9Subfield.json";
  private static final String PARSED_CONTENT_WITHOUT_9_SUBFIELDS = "{\"fields\":[{\"001\":\"ybp7406411\"},{\"245\":{\"subfields\":[{\"a\":\"title\"},{\"b\":\"remainder_of_title\"},{\"c\":\"state_of_responsibility\"},{\"f\":\"inclusive_dates\"},{\"g\":\"bulk_dates\"},{\"h\":\"medium\"}],\"ind1\":\" \",\"ind2\":\" \"}},{\"700\":{\"subfields\":[{\"a\":\"personal_name_1\"},{\"b\":\"numeration_1\"},{\"9\":\"3f2923d3-6f8e-41a6-94e1-09eaf32872e0\"}],\"ind1\":\" \",\"ind2\":\" \"}},{\"700\":{\"subfields\":[{\"a\":\"personal_name_2\"},{\"b\":\"numeration_2\"}],\"ind1\":\" \",\"ind2\":\" \"}}]}";
  private static final String UUID_1 = "e84e4dd4-9d27-4f42-8fda-408d78c7f7ee";
  private static final String UUID_2 = "3f2923d3-6f8e-41a6-94e1-09eaf32872e0";
  private static final String UUID_3 = "7e11b935-2b3a-4e79-8e57-5cde4561a2a8";

  @Test
  public void shouldRemove9subfieldsThatContainValue() throws IOException {
    // given
    String recordId = UUID.randomUUID().toString();
    String instanceId = UUID.randomUUID().toString();

    String parsedRecordContent = TestUtil.readFileFromPath(PARSED_MARC_RECORD_PATH);
    ParsedRecord parsedRecord = new ParsedRecord();
    String leader = new JsonObject(parsedRecordContent).getString("leader");
    parsedRecord.setContent(parsedRecordContent);
    Record record = new Record().withId(recordId).withParsedRecord(parsedRecord);
    // when
    boolean removedSubfields = MarcRecordUtil.removeSubfieldsThatContainsValue(record, '9', List.of(UUID_1, UUID_3));
    // then
    Assert.assertTrue(removedSubfields);
    JsonObject content = new JsonObject(parsedRecord.getContent().toString());
    JsonArray fields = content.getJsonArray("fields");
    String newLeader = content.getString("leader");
    Assert.assertNotEquals(leader, newLeader);
    Assert.assertFalse(fields.isEmpty());
    content.remove("leader");
    Assert.assertEquals(content.encode(), PARSED_CONTENT_WITHOUT_9_SUBFIELDS);
  }
}
