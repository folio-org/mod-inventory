package org.folio.inventory.dataimport.util;

import org.junit.Test;
import io.vertx.core.json.JsonObject;
import java.util.Optional;
import org.folio.rest.jaxrs.model.ParsedRecord;
import org.folio.inventory.dataimport.util.ParsedRecordUtil.AdditionalSubfields;
import org.folio.rest.jaxrs.model.Record;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;

public class ParsedRecordUtilTest {

  @Test
  public void shouldNormalizeParsedRecordContent() {
    // given
    String content = "{\"leader\":\"01240cvs a2200397   4500\",\"fields\":[{\"001\":\"value\"}]}";
    ParsedRecord parsedRecord = new ParsedRecord().withContent(content);

    // when
    JsonObject normalizedContent = ParsedRecordUtil.normalize(parsedRecord.getContent());

    // then
    assertNotNull(normalizedContent);
    assertEquals("01240cvs a2200397   4500", normalizedContent.getString("leader"));
  }

  @Test
  public void shouldReturnEmptyOptionalWhenLeaderIsNull() {
    // given
    String content = "{\"fields\":[{\"001\":\"value\"}]}";
    ParsedRecord parsedRecord = new ParsedRecord().withContent(content);

    // when
    Optional<Character> leaderStatus = ParsedRecordUtil.getLeaderStatus(parsedRecord);

    // then
    assertFalse(leaderStatus.isPresent());
  }

  @Test
  public void shouldReturnEmptyOptionalWhenLeaderIsShorterThanExpected() {
    // given
    String content = "{\"leader\":\"short\",\"fields\":[{\"001\":\"value\"}]}";
    ParsedRecord parsedRecord = new ParsedRecord().withContent(content);

    // when
    Optional<Character> leaderStatus = ParsedRecordUtil.getLeaderStatus(parsedRecord);

    // then
    assertFalse(leaderStatus.isPresent());
  }

  @Test
  public void shouldReturnLeaderStatusWhenLeaderIsValid() {
    // given
    String content = "{\"leader\":\"01240cvs a2200397   4500\",\"fields\":[{\"001\":\"value\"}]}";
    ParsedRecord parsedRecord = new ParsedRecord().withContent(content);

    // when
    Optional<Character> leaderStatus = ParsedRecordUtil.getLeaderStatus(parsedRecord);

    // then
    assertTrue(leaderStatus.isPresent());
    assertEquals(Character.valueOf('c'), leaderStatus.get());
  }

  @Test
  public void shouldUpdateLeaderStatusWhenLeaderIsValid() {
    // given
    String content = "{\"leader\":\"01240cvs a2200397   4500\",\"fields\":[{\"001\":\"value\"}]}";
    ParsedRecord parsedRecord = new ParsedRecord().withContent(content);
    Character newStatus = 'b';

    // when
    ParsedRecordUtil.updateLeaderStatus(parsedRecord, newStatus);

    // then
    JsonObject updatedContent = ParsedRecordUtil.normalize(parsedRecord.getContent());
    assertEquals("01240bvs a2200397   4500", updatedContent.getString("leader"));
  }

  @Test
  public void shouldNotUpdateLeaderStatusWhenLeaderIsNull() {
    // given
    String content = "{\"fields\":[{\"001\":\"value\"}]}";
    ParsedRecord parsedRecord = new ParsedRecord().withContent(content);
    Character newStatus = 'b';

    // when
    ParsedRecordUtil.updateLeaderStatus(parsedRecord, newStatus);

    // then
    JsonObject updatedContent = ParsedRecordUtil.normalize(parsedRecord.getContent());
    assertFalse(updatedContent.containsKey("leader"));
  }

  @Test
  public void shouldNotUpdateLeaderStatusWhenLeaderIsShorterThanExpected() {
    // given
    String content = "{\"leader\":\"short\",\"fields\":[{\"001\":\"value\"}]}";
    ParsedRecord parsedRecord = new ParsedRecord().withContent(content);
    Character newStatus = 'b';

    // when
    ParsedRecordUtil.updateLeaderStatus(parsedRecord, newStatus);

    // then
    JsonObject updatedContent = ParsedRecordUtil.normalize(parsedRecord.getContent());
    assertEquals("short", updatedContent.getString("leader"));
  }

  @Test
  public void shouldGetAdditionalSubfieldValue() {
    // given
    String content = "{\"fields\":[{\"999\":{\"ind1\":\"f\",\"ind2\":\"f\",\"subfields\":[{\"h\":\"valueH\"},{\"b\":\"valueB\"}]}}]}";
    ParsedRecord parsedRecord = new ParsedRecord().withContent(content);
    AdditionalSubfields additionalSubfield = AdditionalSubfields.H;

    // when
    String result = ParsedRecordUtil.getAdditionalSubfieldValue(parsedRecord, additionalSubfield);

    // then
    assertEquals("valueH", result);
  }

  @Test
  public void shouldGetAdditionalSubfieldValueWhenFieldNotFound() {
    // given
    String content = "{\"fields\":[{\"999\":{\"ind1\":\"f\",\"ind2\":\"f\",\"subfields\":[{\"b\":\"valueB\"}]}}]}";
    ParsedRecord parsedRecord = new ParsedRecord().withContent(content);
    AdditionalSubfields additionalSubfield = AdditionalSubfields.H;

    // when
    String result = ParsedRecordUtil.getAdditionalSubfieldValue(parsedRecord, additionalSubfield);

    // then
    assertEquals("", result);
  }

  @Test
  public void shouldGetAdditionalSubfieldValueWhenFieldsIsNull() {
    // given
    String content = "{\"fields\":null}";
    ParsedRecord parsedRecord = new ParsedRecord().withContent(content);
    ParsedRecordUtil.AdditionalSubfields additionalSubfield = AdditionalSubfields.H;

    // when
    String result = ParsedRecordUtil.getAdditionalSubfieldValue(parsedRecord, additionalSubfield);

    // then
    assertEquals("", result);
  }

  @Test
  public void shouldReturnControlFieldValueWhenFieldExists() {
    // given
    String content = "{\"leader\":\"01240cvs a2200397   4500\",\"fields\":[{\"001\":\"value001\"},{\"003\":\"value003\"}]}";
    ParsedRecord parsedRecord = new ParsedRecord().withContent(content);
    Record record = new Record().withParsedRecord(parsedRecord);

    // when
    String result = ParsedRecordUtil.getControlFieldValue(record, "001");

    // then
    assertEquals("value001", result);
  }

  @Test
  public void shouldReturnNullWhenFieldDoesNotExist() {
    // given
    String content = "{\"leader\":\"01240cvs a2200397   4500\",\"fields\":[{\"001\":\"value001\"},{\"003\":\"value003\"}]}";
    ParsedRecord parsedRecord = new ParsedRecord().withContent(content);
    Record record = new Record().withParsedRecord(parsedRecord);

    // when
    String result = ParsedRecordUtil.getControlFieldValue(record, "002");

    // then
    assertNull(result);
  }

  @Test
  public void shouldReturnNullWhenParsedRecordIsNull() {
    // given
    Record record = new Record().withParsedRecord(null);

    // when
    String result = ParsedRecordUtil.getControlFieldValue(record, "001");

    // then
    assertNull(result);
  }

  @Test
  public void shouldReturnNullWhenContentIsNull() {
    // given
    ParsedRecord parsedRecord = new ParsedRecord().withContent(null);
    Record record = new Record().withParsedRecord(parsedRecord);

    // when
    String result = ParsedRecordUtil.getControlFieldValue(record, "001");

    // then
    assertNull(result);
  }

  @Test
  public void shouldReturnNullWhenContentIsInvalidJson() {
    // given
    ParsedRecord parsedRecord = new ParsedRecord().withContent("invalid json");
    Record record = new Record().withParsedRecord(parsedRecord);

    // when
    String result = ParsedRecordUtil.getControlFieldValue(record, "001");

    // then
    assertNull(result);
  }
}
