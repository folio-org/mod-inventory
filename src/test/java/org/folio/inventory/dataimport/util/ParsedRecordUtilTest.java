package org.folio.inventory.dataimport.util;

import static org.folio.dataimport.util.marc.MarcConstants.SUBFIELD_A;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.vertx.core.json.JsonObject;
import java.util.Optional;
import org.folio.dataimport.util.marc.MarcContentCodec;
import org.folio.rest.jaxrs.model.ParsedRecord;
import org.folio.rest.jaxrs.model.Record;
import org.junit.jupiter.api.Test;

class ParsedRecordUtilTest {

  @Test
  void shouldReturnEmptyOptionalWhenLeaderIsNull() {
    // given
    String content = "{\"fields\":[{\"001\":\"value\"}]}";
    ParsedRecord parsedRecord = new ParsedRecord().withContent(content);

    // when
    Optional<Character> leaderStatus = ParsedRecordUtil.getLeaderStatus(parsedRecord);

    // then
    assertFalse(leaderStatus.isPresent());
  }

  @Test
  void shouldReturnEmptyOptionalWhenLeaderIsShorterThanExpected() {
    // given
    String content = "{\"leader\":\"short\",\"fields\":[{\"001\":\"value\"}]}";
    ParsedRecord parsedRecord = new ParsedRecord().withContent(content);

    // when
    Optional<Character> leaderStatus = ParsedRecordUtil.getLeaderStatus(parsedRecord);

    // then
    assertFalse(leaderStatus.isPresent());
  }

  @Test
  void shouldReturnLeaderStatusWhenLeaderIsValid() {
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
  void shouldUpdateLeaderStatusWhenLeaderIsValid() {
    // given
    String content = "{\"leader\":\"01240cvs a2200397   4500\",\"fields\":[{\"001\":\"value\"}]}";
    ParsedRecord parsedRecord = new ParsedRecord().withContent(content);
    Character newStatus = 'b';

    // when
    ParsedRecordUtil.updateLeaderStatus(parsedRecord, newStatus);

    // then
    assertThat(parsedRecord.getContent(), instanceOf(String.class));
    JsonObject updatedContent = MarcContentCodec.canonicalizeJson(parsedRecord.getContent());
    assertEquals(2, updatedContent.fieldNames().size());
    assertThat(updatedContent.fieldNames(), containsInAnyOrder("fields", "leader"));
    assertEquals("01240bvs a2200397   4500", updatedContent.getString("leader"));
  }

  @Test
  void shouldNotUpdateLeaderStatusWhenLeaderIsNull() {
    // given
    String content = "{\"fields\":[{\"001\":\"value\"}]}";
    ParsedRecord parsedRecord = new ParsedRecord().withContent(content);
    Character newStatus = 'b';

    // when
    ParsedRecordUtil.updateLeaderStatus(parsedRecord, newStatus);

    // then
    JsonObject updatedContent = MarcContentCodec.canonicalizeJson(parsedRecord.getContent());
    assertFalse(updatedContent.containsKey("leader"));
  }

  @Test
  void shouldNotUpdateLeaderStatusWhenLeaderIsShorterThanExpected() {
    // given
    String content = "{\"leader\":\"short\",\"fields\":[{\"001\":\"value\"}]}";
    ParsedRecord parsedRecord = new ParsedRecord().withContent(content);
    Character newStatus = 'b';

    // when
    ParsedRecordUtil.updateLeaderStatus(parsedRecord, newStatus);

    // then
    JsonObject updatedContent = MarcContentCodec.canonicalizeJson(parsedRecord.getContent());
    assertEquals("short", updatedContent.getString("leader"));
  }

  @Test
  void shouldGetAdditionalSubfieldValue() {
    // given
    String content =
      "{\"fields\":[{\"999\":{\"ind1\":\"f\",\"ind2\":\"f\",\"subfields\":[{\"a\":\"valueH\"},{\"b\":\"valueB\"}]}}]}";
    ParsedRecord parsedRecord = new ParsedRecord().withContent(content);

    // when
    String result = ParsedRecordUtil.getAdditionalSubfieldValue(parsedRecord, SUBFIELD_A);

    // then
    assertEquals("valueH", result);
  }

  @Test
  void shouldGetAdditionalSubfieldValueWhenFieldNotFound() {
    // given
    String content = "{\"fields\":[{\"999\":{\"ind1\":\"f\",\"ind2\":\"f\",\"subfields\":[{\"b\":\"valueB\"}]}}]}";
    ParsedRecord parsedRecord = new ParsedRecord().withContent(content);

    // when
    String result = ParsedRecordUtil.getAdditionalSubfieldValue(parsedRecord, SUBFIELD_A);

    // then
    assertEquals("", result);
  }

  @Test
  void shouldGetAdditionalSubfieldValueWhenFieldsIsNull() {
    // given
    String content = "{\"fields\":null}";
    ParsedRecord parsedRecord = new ParsedRecord().withContent(content);

    // when
    String result = ParsedRecordUtil.getAdditionalSubfieldValue(parsedRecord, SUBFIELD_A);

    // then
    assertEquals("", result);
  }

  @Test
  void shouldReturnControlFieldValueWhenFieldExists() {
    // given
    String content =
      "{\"leader\":\"01240cvs a2200397   4500\",\"fields\":[{\"001\":\"value001\"},{\"003\":\"value003\"}]}";
    ParsedRecord parsedRecord = new ParsedRecord().withContent(content);
    Record srsRecord = new Record().withParsedRecord(parsedRecord);

    // when
    String result = AdditionalFieldsUtil.getValueFromControlledField(srsRecord, "001");

    // then
    assertEquals("value001", result);
  }

  @Test
  void shouldReturnNullWhenFieldDoesNotExist() {
    // given
    String content =
      "{\"leader\":\"01240cvs a2200397   4500\",\"fields\":[{\"001\":\"value001\"},{\"003\":\"value003\"}]}";
    ParsedRecord parsedRecord = new ParsedRecord().withContent(content);
    Record srsRecord = new Record().withParsedRecord(parsedRecord);

    // when
    String result = AdditionalFieldsUtil.getValueFromControlledField(srsRecord, "002");

    // then
    assertNull(result);
  }

  @Test
  void shouldReturnNullWhenParsedRecordIsNull() {
    // given
    Record srsRecord = new Record().withParsedRecord(null);

    // when
    String result = AdditionalFieldsUtil.getValueFromControlledField(srsRecord, "001");

    // then
    assertNull(result);
  }

  @Test
  void shouldReturnNullWhenContentIsNull() {
    // given
    ParsedRecord parsedRecord = new ParsedRecord().withContent(null);
    Record srsRecord = new Record().withParsedRecord(parsedRecord);

    // when
    String result = AdditionalFieldsUtil.getValueFromControlledField(srsRecord, "001");

    // then
    assertNull(result);
  }

  @Test
  void shouldReturnNullWhenContentIsInvalidJson() {
    // given
    ParsedRecord parsedRecord = new ParsedRecord().withContent("invalid json");
    Record srsRecord = new Record().withParsedRecord(parsedRecord);

    // when
    String result = AdditionalFieldsUtil.getValueFromControlledField(srsRecord, "001");

    // then
    assertNull(result);
  }
}
