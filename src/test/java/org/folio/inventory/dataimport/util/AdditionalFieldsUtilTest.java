package org.folio.inventory.dataimport.util;

import static org.folio.inventory.dataimport.util.AdditionalFieldsUtil.INDICATOR_F;
import static org.folio.inventory.dataimport.util.AdditionalFieldsUtil.INVALID_DATA_FIELD_MSG;
import static org.folio.inventory.dataimport.util.AdditionalFieldsUtil.SUBFIELD_I;
import static org.folio.inventory.dataimport.util.AdditionalFieldsUtil.TAG_001;
import static org.folio.inventory.dataimport.util.AdditionalFieldsUtil.TAG_005;
import static org.folio.inventory.dataimport.util.AdditionalFieldsUtil.TAG_999;
import static org.folio.inventory.dataimport.util.AdditionalFieldsUtil.addControlledFieldToMarcRecord;
import static org.folio.inventory.dataimport.util.AdditionalFieldsUtil.addDataFieldToMarcRecord;
import static org.folio.inventory.dataimport.util.AdditionalFieldsUtil.addFieldToMarcRecord;
import static org.folio.inventory.dataimport.util.AdditionalFieldsUtil.dateTime005Formatter;
import static org.folio.inventory.dataimport.util.AdditionalFieldsUtil.getCacheStats;
import static org.folio.inventory.dataimport.util.AdditionalFieldsUtil.getValueFromControlledField;
import static org.folio.inventory.dataimport.util.AdditionalFieldsUtil.getValueFromDataField;
import static org.folio.inventory.dataimport.util.AdditionalFieldsUtil.isFieldExist;
import static org.folio.inventory.dataimport.util.AdditionalFieldsUtil.removeField;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.github.benmanes.caffeine.cache.stats.CacheStats;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import java.io.File;
import java.io.IOException;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.folio.inventory.domain.instances.Instance;
import org.folio.processing.mapping.defaultmapper.processor.parameters.MappingParameters;
import org.folio.rest.jaxrs.model.ExternalIdsHolder;
import org.folio.rest.jaxrs.model.MarcFieldProtectionSetting;
import org.folio.rest.jaxrs.model.ParsedRecord;
import org.folio.rest.jaxrs.model.Record;
import org.hamcrest.MatcherAssert;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.marc4j.MarcException;
import support.TestUtil;

class AdditionalFieldsUtilTest {

  private static final String PARSED_MARC_RECORD_PATH = "src/test/resources/marc/parsedMarcRecord.json";
  private static final String PARSED_RECORD = "src/test/resources/marc/parsedRecord.json";
  private static final String REORDERED_PARSED_RECORD = "src/test/resources/marc/reorderedParsedRecord.json";
  private static final String REORDERING_RESULT_RECORD = "src/test/resources/marc/reorderingResultRecord.json";

  @Test
  void shouldAddInstanceIdSubfield() {
    // given
    String recordId = UUID.randomUUID().toString();
    String instanceId = UUID.randomUUID().toString();

    String parsedRecordContent = TestUtil.readFileFromPath(PARSED_MARC_RECORD_PATH);
    ParsedRecord parsedRecord = new ParsedRecord().withContent(parsedRecordContent);
    String leader = new JsonObject(parsedRecordContent).getString("leader");
    var marcRecord = new Record().withId(recordId).withParsedRecord(parsedRecord);
    // when
    boolean addedSourceRecordId = addFieldToMarcRecord(marcRecord, TAG_999, 's', recordId);
    boolean addedInstanceId = addFieldToMarcRecord(marcRecord, TAG_999, 'i', instanceId);
    // then
    assertTrue(addedSourceRecordId);
    assertTrue(addedInstanceId);
    JsonObject content = new JsonObject(parsedRecord.getContent().toString());
    JsonArray fields = content.getJsonArray("fields");
    String newLeader = content.getString("leader");
    assertNotEquals(leader, newLeader);
    assertFalse(fields.isEmpty());
    int totalFieldsCount = 0;
    for (int i = fields.size(); i-- > 0; ) {
      JsonObject targetField = fields.getJsonObject(i);
      if (targetField.containsKey(TAG_999)) {
        JsonArray subfields = targetField.getJsonObject(TAG_999).getJsonArray("subfields");
        for (int j = subfields.size(); j-- > 0; ) {
          JsonObject targetSubfield = subfields.getJsonObject(j);
          if (targetSubfield.containsKey("i")) {
            String actualInstanceId = (String) targetSubfield.getValue("i");
            assertEquals(instanceId, actualInstanceId);
          }
          if (targetSubfield.containsKey("s")) {
            String actualSourceRecordId = (String) targetSubfield.getValue("s");
            assertEquals(recordId, actualSourceRecordId);
          }
        }
        totalFieldsCount++;
      }
    }
    assertEquals(2, totalFieldsCount);
  }

  @Test
  void shouldNotAddInstanceIdSubfieldIfNoParsedRecordContent() {
    // given
    var marcRecord = new Record();
    String instanceId = UUID.randomUUID().toString();
    // when
    boolean added = addFieldToMarcRecord(marcRecord, TAG_999, 'i', instanceId);
    // then
    assertFalse(added);
    assertNull(marcRecord.getParsedRecord());
  }

  @Test
  void shouldNotAddInstanceIdSubfieldIfNoFieldsInParsedRecordContent() {
    // given
    var marcRecord = new Record();
    String content = StringUtils.EMPTY;
    marcRecord.setParsedRecord(new ParsedRecord().withContent(content));
    String instanceId = UUID.randomUUID().toString();
    // when
    boolean added = addFieldToMarcRecord(marcRecord, TAG_999, 'i', instanceId);
    // then
    assertFalse(added);
    assertNotNull(marcRecord.getParsedRecord());
    assertNotNull(marcRecord.getParsedRecord().getContent());
    assertEquals(content, marcRecord.getParsedRecord().getContent());
  }

  @Test
  void shouldNotAddInstanceIdSubfieldIfCanNotConvertParsedContentToJsonObject() {
    // given
    var marcRecord = new Record();
    String content = "{fields}";
    marcRecord.setParsedRecord(new ParsedRecord().withContent(content));
    String instanceId = UUID.randomUUID().toString();
    // when
    boolean added = addFieldToMarcRecord(marcRecord, TAG_999, 'i', instanceId);
    // then
    assertFalse(added);
    assertNotNull(marcRecord.getParsedRecord());
    assertNotNull(marcRecord.getParsedRecord().getContent());
    assertEquals(content, marcRecord.getParsedRecord().getContent());
  }

  @Test
  void shouldNotAddInstanceIdSubfieldIfContentHasNoFields() {
    // given
    var marcRecord = new Record();
    String content = "{\"leader\":\"01240cas a2200397\"}";
    marcRecord.setParsedRecord(new ParsedRecord().withContent(content));
    String instanceId = UUID.randomUUID().toString();
    // when
    boolean added = addFieldToMarcRecord(marcRecord, TAG_999, 'i', instanceId);
    // then
    assertFalse(added);
    assertNotNull(marcRecord.getParsedRecord());
    assertNotNull(marcRecord.getParsedRecord().getContent());
  }

  @Test
  void shouldNotAddInstanceIdSubfieldIfContentIsNull() {
    // given
    var marcRecord = new Record();
    marcRecord.setParsedRecord(new ParsedRecord().withContent(null));
    String instanceId = UUID.randomUUID().toString();
    // when
    boolean added = addFieldToMarcRecord(marcRecord, TAG_999, 'i', instanceId);
    // then
    assertFalse(added);
    assertNotNull(marcRecord.getParsedRecord());
    assertNull(marcRecord.getParsedRecord().getContent());
  }

  @Test
  void shouldRemoveField() {
    String recordId = UUID.randomUUID().toString();
    String parsedRecordContent = TestUtil.readFileFromPath(PARSED_MARC_RECORD_PATH);
    ParsedRecord parsedRecord = new ParsedRecord();
    String leader = new JsonObject(parsedRecordContent).getString("leader");
    parsedRecord.setContent(parsedRecordContent);
    var marcRecord = new Record().withId(recordId).withParsedRecord(parsedRecord);
    boolean deleted = removeField(marcRecord, "001");
    assertTrue(deleted);
    JsonObject content = new JsonObject(parsedRecord.getContent().toString());
    JsonArray fields = content.getJsonArray("fields");
    String newLeader = content.getString("leader");
    assertNotEquals(leader, newLeader);
    assertFalse(fields.isEmpty());
    boolean contains001Field = IntStream.range(0, fields.size()).mapToObj(fields::getJsonObject)
      .anyMatch(targetField -> targetField.containsKey("001"));
    assertFalse(contains001Field);
  }

  @Test
  void shouldNotAddControlledFieldToMarcRecord() {
    String recordId = UUID.randomUUID().toString();
    String parsedRecordContent = TestUtil.readFileFromPath(PARSED_MARC_RECORD_PATH);
    ParsedRecord parsedRecord = new ParsedRecord().withContent(parsedRecordContent);
    var marcRecord = new Record().withId(recordId).withParsedRecord(parsedRecord);
    boolean added = addControlledFieldToMarcRecord(marcRecord, "002", "", null);
    assertFalse(added);
  }

  @Test
  void shouldAddControlledFieldToMarcRecord() {
    String recordId = UUID.randomUUID().toString();
    String parsedRecordContent = TestUtil.readFileFromPath(PARSED_MARC_RECORD_PATH);
    ParsedRecord parsedRecord = new ParsedRecord().withContent(parsedRecordContent);
    String leader = new JsonObject(parsedRecordContent).getString("leader");
    var marcRecord = new Record().withId(recordId).withParsedRecord(parsedRecord);
    boolean added = addControlledFieldToMarcRecord(
      marcRecord, "002", "test", AdditionalFieldsUtil::addControlledFieldToMarcRecord);
    assertTrue(added);
    JsonObject content = new JsonObject(parsedRecord.getContent().toString());
    JsonArray fields = content.getJsonArray("fields");
    String newLeader = content.getString("leader");
    assertNotEquals(leader, newLeader);
    assertFalse(fields.isEmpty());
    boolean contains002Field = IntStream.range(0, fields.size()).mapToObj(fields::getJsonObject)
      .anyMatch(field -> field.containsKey("002") && field.getString("002").equals("test"));
    assertTrue(contains002Field);
  }

  @Test
  void shouldReplaceControlledFieldInMarcRecord() {
    String recordId = UUID.randomUUID().toString();
    String parsedRecordContent = TestUtil.readFileFromPath(PARSED_MARC_RECORD_PATH);
    ParsedRecord parsedRecord = new ParsedRecord().withContent(parsedRecordContent);
    String leader = new JsonObject(parsedRecordContent).getString("leader");
    var marcRecord = new Record().withId(recordId).withParsedRecord(parsedRecord);
    boolean added = addControlledFieldToMarcRecord(
      marcRecord, "003", "test", AdditionalFieldsUtil::replaceOrAddControlledFieldInMarcRecord);
    assertTrue(added);
    JsonObject content = new JsonObject(parsedRecord.getContent().toString());
    JsonArray fields = content.getJsonArray("fields");
    String newLeader = content.getString("leader");
    assertNotEquals(leader, newLeader);
    assertFalse(fields.isEmpty());
    boolean is003Field = fields.getJsonObject(1).getString("003").equals("test");
    assertTrue(is003Field);
  }

  @Test
  void isFieldsFillingNeededTrue() {
    String instanceId = UUID.randomUUID().toString();
    String instanceHrId = UUID.randomUUID().toString();
    Record srcRecord = new Record().withExternalIdsHolder(
      new ExternalIdsHolder().withInstanceId(instanceId).withInstanceHrid(UUID.randomUUID().toString()));
    Instance instance = new Instance(instanceId, 0, instanceHrId, "", "", "");
    assertTrue(AdditionalFieldsUtil.isFieldsFillingNeeded(srcRecord, instance));

    srcRecord.getExternalIdsHolder().setInstanceHrid(null);
    assertTrue(AdditionalFieldsUtil.isFieldsFillingNeeded(srcRecord, instance));
  }

  @Test
  void isFieldsFillingNeededFalse() {
    String instanceId = UUID.randomUUID().toString();
    String instanceHrId = UUID.randomUUID().toString();
    Record srcRecord = new Record().withExternalIdsHolder(
      new ExternalIdsHolder().withInstanceId(instanceId).withInstanceHrid(instanceHrId));
    Instance instance = new Instance(instanceId, 0, instanceHrId, "", "", "");
    assertFalse(AdditionalFieldsUtil.isFieldsFillingNeeded(srcRecord, instance));

    srcRecord.getExternalIdsHolder().withInstanceId(instanceId);
    instance = new Instance(UUID.randomUUID().toString(), 0, instanceHrId, "", "", "");
    assertFalse(AdditionalFieldsUtil.isFieldsFillingNeeded(srcRecord, instance));

    srcRecord.getExternalIdsHolder().withInstanceId(null).withInstanceHrid(null);
    instance = new Instance(UUID.randomUUID().toString(), 0, instanceHrId, "", "", "");
    assertFalse(AdditionalFieldsUtil.isFieldsFillingNeeded(srcRecord, instance));
  }

  @Test
  void isFieldsFillingNeededForExternalHolderInstanceShouldThrowException() {
    String instanceId = UUID.randomUUID().toString();
    String instanceHrId = UUID.randomUUID().toString();
    Record srcRecord = new Record().withExternalIdsHolder(
      new ExternalIdsHolder().withInstanceId(instanceId).withInstanceHrid(instanceHrId));
    Instance instance = new Instance(null, 0, instanceHrId, "", "", "");
    assertThrows(Exception.class, () -> AdditionalFieldsUtil.isFieldsFillingNeeded(srcRecord, instance));
  }

  @Test
  void shouldAddFieldToMarcRecordInNumericalOrder() {
    // given
    String instanceHrId = UUID.randomUUID().toString();
    String parsedRecordContent = TestUtil.readFileFromPath(PARSED_MARC_RECORD_PATH);
    ParsedRecord parsedRecord = new ParsedRecord().withContent(parsedRecordContent);
    String leader = new JsonObject(parsedRecordContent).getString("leader");
    var marcRecord = new Record().withId(UUID.randomUUID().toString()).withParsedRecord(parsedRecord);
    // when
    boolean added = addDataFieldToMarcRecord(marcRecord, "035", ' ', ' ', 'a', instanceHrId);
    // then
    assertTrue(added);
    JsonObject content = new JsonObject(parsedRecord.getContent().toString());
    JsonArray fields = content.getJsonArray("fields");
    String newLeader = content.getString("leader");
    assertNotEquals(leader, newLeader);
    assertFalse(fields.isEmpty());
    boolean existsNewField = false;
    for (int i = 0; i < fields.size() - 1; i++) {
      JsonObject targetField = fields.getJsonObject(i);
      if (targetField.containsKey("035")) {
        existsNewField = true;
        String currentTag = fields.getJsonObject(i).stream().map(Map.Entry::getKey).findFirst().orElse("");
        String nextTag = fields.getJsonObject(i + 1).stream().map(Map.Entry::getKey).findFirst().orElse("");
        MatcherAssert.assertThat(currentTag, lessThanOrEqualTo(nextTag));
      }
    }
    assertTrue(existsNewField);
  }

  @Test
  void shouldNotSortExistingFieldsWhenAddFieldToToMarcRecord() {
    // given
    String instanceId = "12345";
    String parsedContent =
      "{\"leader\":\"00115nam  22000731a 4500\",\"fields\":[{\"001\":\"ybp7406411\"},{\"507\":{\"subfields\":[{\"a\":\"data\"}],\"ind1\":\" \",\"ind2\":\" \"}},{\"500\":{\"subfields\":[{\"a\":\"data\"}],\"ind1\":\" \",\"ind2\":\" \"}}]}";
    String expectedParsedContent =
      "{\"leader\":\"00113nam  22000731a 4500\",\"fields\":[{\"001\":\"ybp7406411\"},{\"507\":{\"subfields\":[{\"a\":\"data\"}],\"ind1\":\" \",\"ind2\":\" \"}},{\"500\":{\"subfields\":[{\"a\":\"data\"}],\"ind1\":\" \",\"ind2\":\" \"}},{\"999\":{\"subfields\":[{\"i\":\"12345\"}],\"ind1\":\"f\",\"ind2\":\"f\"}}]}";
    ParsedRecord parsedRecord = new ParsedRecord().withContent(parsedContent);
    var marcRecord = new Record().withId(UUID.randomUUID().toString()).withParsedRecord(parsedRecord);
    // when
    boolean added = addDataFieldToMarcRecord(marcRecord, "999", 'f', 'f', 'i', instanceId);
    // then
    assertTrue(added);
    assertEquals(expectedParsedContent, parsedRecord.getContent());
  }

  @Test
  void shouldNotAdd035FieldIf001And003FieldsNotExists() {
    // given
    String parsedContent =
      "{\"leader\":\"00115nam  22000731a 4500\",\"fields\":[{\"003\":\"in001\"},{\"507\":{\"subfields\":[{\"a\":\"data\"}],\"ind1\":\" \",\"ind2\":\" \"}},{\"500\":{\"subfields\":[{\"a\":\"data\"}],\"ind1\":\" \",\"ind2\":\" \"}}]}";
    String expectedParsedContent =
      "{\"leader\":\"00068nam  22000491a 4500\",\"fields\":[{\"507\":{\"subfields\":[{\"a\":\"data\"}],\"ind1\":\" \",\"ind2\":\" \"}},{\"500\":{\"subfields\":[{\"a\":\"data\"}],\"ind1\":\" \",\"ind2\":\" \"}}]}";
    ParsedRecord parsedRecord = new ParsedRecord().withContent(parsedContent);

    var marcRecord = new Record().withId(UUID.randomUUID().toString())
      .withParsedRecord(parsedRecord)
      .withGeneration(0)
      .withState(Record.State.ACTUAL)
      .withExternalIdsHolder(new ExternalIdsHolder().withInstanceId("001").withInstanceHrid("in001"));
    // when
    AdditionalFieldsUtil.move001To035(marcRecord);
    // then
    assertEquals(expectedParsedContent, parsedRecord.getContent());
  }

  @Test
  void shouldAdd035If001NotEqual003() {
    // given
    String parsedContent =
      "{\"leader\":\"00086nam  22000611a 4500\",\"fields\":[{\"001\":\"in001\"},{\"003\":\"ybp7406411\"},{\"507\":{\"subfields\":[{\"a\":\"data\"}],\"ind1\":\" \",\"ind2\":\" \"}},{\"500\":{\"subfields\":[{\"a\":\"data\"}],\"ind1\":\" \",\"ind2\":\" \"}}]}";
    String expectedParsedContent =
      "{\"leader\":\"00120nam  22000731a 4500\",\"fields\":[{\"001\":\"in001\"},{\"035\":{\"subfields\":[{\"a\":\"(ybp7406411)in001\"}],\"ind1\":\" \",\"ind2\":\" \"}},{\"507\":{\"subfields\":[{\"a\":\"data\"}],\"ind1\":\" \",\"ind2\":\" \"}},{\"500\":{\"subfields\":[{\"a\":\"data\"}],\"ind1\":\" \",\"ind2\":\" \"}}]}";
    ParsedRecord parsedRecord = new ParsedRecord().withContent(parsedContent);

    var marcRecord = new Record().withId(UUID.randomUUID().toString())
      .withParsedRecord(parsedRecord)
      .withGeneration(0)
      .withState(Record.State.ACTUAL)
      .withExternalIdsHolder(new ExternalIdsHolder().withInstanceId("001").withInstanceHrid("in001"));
    // when
    AdditionalFieldsUtil.move001To035(marcRecord);
    // then
    assertEquals(expectedParsedContent, parsedRecord.getContent());
  }

  @Test
  void shouldRemovePeriodsAndSpacesAfterNormalization() {
    // given
    var parsedContent = "{\"leader\":\"00120nam  22000731a 4500\",\"fields\":[{\"001\":\"in001\"}," +
                        "{\"035\":{\"subfields\":[{\"a\":\"(OCoLC)on. 607TST .001\"}],\"ind1\":\" \",\"ind2\":\" \"}},"
                        +
                        "{\"500\":{\"subfields\":[{\"a\":\"data\"}],\"ind1\":\" \",\"ind2\":\" \"}}]}";

    var expectedParsedContent = "{\"leader\":\"00098nam  22000611a 4500\",\"fields\":[{\"001\":\"in001\"}," +
                                "{\"035\":{\"subfields\":[{\"a\":\"(OCoLC)607TST001\"}],\"ind1\":\" \",\"ind2\":\" \"}},"
                                +
                                "{\"500\":{\"subfields\":[{\"a\":\"data\"}],\"ind1\":\" \",\"ind2\":\" \"}}]}";
    ParsedRecord parsedRecord = new ParsedRecord().withContent(parsedContent);

    var marcRecord = new Record().withId(UUID.randomUUID().toString())
      .withParsedRecord(parsedRecord)
      .withGeneration(0)
      .withState(Record.State.ACTUAL)
      .withExternalIdsHolder(new ExternalIdsHolder().withInstanceId("001").withInstanceHrid("in001"));
    // when
    AdditionalFieldsUtil.normalize035(marcRecord);
    assertEquals(expectedParsedContent, parsedRecord.getContent());
  }

  @Test
  void shouldPreserveOrderOf035FieldsAfterNormalization() {
    // given
    var parsedContent = "{\"leader\":\"00198cama 22003611a 4500\",\"fields\":[" +
                        "{\"001\":\"10065352\"}," +
                        "{\"005\":\"20220127143948.0\"}," +
                        "{\"008\":\"761216s1853mauch0010eng\"}," +
                        "{\"906\":{\"subfields\":[{\"a\":\"7\"},{\"b\":\"cbc\"},{\"c\":\"oclcrpl\"},{\"d\":\"u\"},{\"e\":\"ncip\"},{\"f\":\"19\"},{\"g\":\"y-gencatlg\"}],\"ind1\":\"\",\"ind2\":\"\"}},"
                        +
                        "{\"035\":{\"subfields\":[{\"9\":\"(DLC)01012052\"}],\"ind1\":\"\",\"ind2\":\"\"}}," +
                        "{\"010\":{\"subfields\":[{\"a\":\"01012052\"}],\"ind1\":\"\",\"ind2\":\"\"}}," +
                        "{\"022\":{\"subfields\":[{\"a\":\"0022-0469\"}],\"ind1\":\"\",\"ind2\":\"\"}}," +
                        "{\"030\":{\"subfields\":[{\"a\":\"0030-0469\"}],\"ind1\":\" \",\"ind2\":\" \"}}," +
                        "{\"035\":{\"subfields\":[{\"a\":\"(OCoLC)on2628488\"}],\"ind1\":\"\",\"ind2\":\"\"}}," +
                        "{\"035\":{\"subfields\":[{\"a\":\"(OCoLC)2628488\"}],\"ind1\":\"\",\"ind2\":\"\"}}," +
                        "{\"035\":{\"subfields\":[{\"a\":\"(OCoLC)00012345\"}],\"ind1\":\"\",\"ind2\":\"\"}}," +
                        "{\"040\":{\"subfields\":[{\"a\":\"DLC\"},{\"b\":\"eng\"},{\"c\":\"O\"},{\"d\":\"O\"},{\"d\":\"DLC\"}],\"ind1\":\"\",\"ind2\":\"\"}}]}";

    var expectedParsedContent = "{\"leader\":\"00372cama 22001571a 4500\",\"fields\":[" +
                                "{\"001\":\"10065352\"}," +
                                "{\"005\":\"20220127143948.0\"}," +
                                "{\"008\":\"761216s1853mauch0010eng\"}," +
                                "{\"906\":{\"subfields\":[{\"a\":\"7\"},{\"b\":\"cbc\"},{\"c\":\"oclcrpl\"},{\"d\":\"u\"},{\"e\":\"ncip\"},{\"f\":\"19\"},{\"g\":\"y-gencatlg\"}],\"ind1\":\" \",\"ind2\":\" \"}},"
                                +
                                "{\"035\":{\"subfields\":[{\"9\":\"(DLC)01012052\"}],\"ind1\":\" \",\"ind2\":\" \"}}," +
                                "{\"010\":{\"subfields\":[{\"a\":\"01012052\"}],\"ind1\":\" \",\"ind2\":\" \"}}," +
                                "{\"022\":{\"subfields\":[{\"a\":\"0022-0469\"}],\"ind1\":\" \",\"ind2\":\" \"}}," +
                                "{\"030\":{\"subfields\":[{\"a\":\"0030-0469\"}],\"ind1\":\" \",\"ind2\":\" \"}}," +
                                "{\"035\":{\"subfields\":[{\"a\":\"(OCoLC)2628488\"}],\"ind1\":\" \",\"ind2\":\" \"}},"
                                +
                                "{\"035\":{\"subfields\":[{\"a\":\"(OCoLC)12345\"}],\"ind1\":\" \",\"ind2\":\" \"}}," +
                                "{\"040\":{\"subfields\":[{\"a\":\"DLC\"},{\"b\":\"eng\"},{\"c\":\"O\"},{\"d\":\"O\"},{\"d\":\"DLC\"}],\"ind1\":\" \",\"ind2\":\" \"}}]}";

    ParsedRecord parsedRecord = new ParsedRecord().withContent(parsedContent);

    var marcRecord = new Record().withId(UUID.randomUUID().toString())
      .withParsedRecord(parsedRecord)
      .withGeneration(0)
      .withState(Record.State.ACTUAL)
      .withExternalIdsHolder(new ExternalIdsHolder().withInstanceId("001").withInstanceHrid("in001"));
    // when
    AdditionalFieldsUtil.normalize035(marcRecord);
    assertEquals(expectedParsedContent, parsedRecord.getContent());
  }

  @Test
  void shouldNotAdd035if001IsNull() {
    // given
    String parsedContent =
      "{\"leader\":\"00086nam  22000611a 4500\",\"fields\":[{\"003\":\"in001\"},{\"507\":{\"subfields\":[{\"a\":\"data\"}],\"ind1\":\" \",\"ind2\":\" \"}},{\"500\":{\"subfields\":[{\"a\":\"data\"}],\"ind1\":\" \",\"ind2\":\" \"}}]}";
    String expectedParsedContent =
      "{\"leader\":\"00068nam  22000491a 4500\",\"fields\":[{\"507\":{\"subfields\":[{\"a\":\"data\"}],\"ind1\":\" \",\"ind2\":\" \"}},{\"500\":{\"subfields\":[{\"a\":\"data\"}],\"ind1\":\" \",\"ind2\":\" \"}}]}";
    ParsedRecord parsedRecord = new ParsedRecord().withContent(parsedContent);

    var marcRecord = new Record().withId(UUID.randomUUID().toString())
      .withParsedRecord(parsedRecord)
      .withGeneration(0)
      .withState(Record.State.ACTUAL)
      .withExternalIdsHolder(new ExternalIdsHolder().withInstanceId("001").withInstanceHrid("in001"));
    // when
    AdditionalFieldsUtil.move001To035(marcRecord);
    // then
    assertEquals(expectedParsedContent, parsedRecord.getContent());
  }

  @Test
  void shouldFill001IfHrIdNotEmpty() {
    // given
    String parsedContent =
      "{\"leader\":\"00118nam  22000731a 4500\",\"fields\":[{\"001\":\"in001\"},{\"003\":\"qwerty\"},{\"035\":{\"subfields\":[{\"a\":\"(NhFolYBP)in001\"}],\"ind1\":\" \",\"ind2\":\" \"}},{\"507\":{\"subfields\":[{\"a\":\"data\"}],\"ind1\":\" \",\"ind2\":\" \"}},{\"500\":{\"subfields\":[{\"a\":\"data\"}],\"ind1\":\" \",\"ind2\":\" \"}}]}";
    String expectedParsedContent =
      "{\"leader\":\"00137nam  22000851a 4500\",\"fields\":[{\"001\":\"in001\"},{\"003\":\"qwerty\"},{\"035\":{\"subfields\":[{\"a\":\"(NhFolYBP)in001\"}],\"ind1\":\" \",\"ind2\":\" \"}},{\"507\":{\"subfields\":[{\"a\":\"data\"}],\"ind1\":\" \",\"ind2\":\" \"}},{\"500\":{\"subfields\":[{\"a\":\"data\"}],\"ind1\":\" \",\"ind2\":\" \"}}]}";
    ParsedRecord parsedRecord = new ParsedRecord();
    parsedRecord.setContent(parsedContent);

    var marcRecord = new Record().withId(UUID.randomUUID().toString())
      .withParsedRecord(parsedRecord)
      .withGeneration(0)
      .withState(Record.State.ACTUAL)
      .withExternalIdsHolder(new ExternalIdsHolder().withInstanceId("001").withInstanceHrid("in001"));
    // when
    AdditionalFieldsUtil.fill001FieldInMarcRecord(marcRecord, "in001");
    // then
    assertEquals(expectedParsedContent, parsedRecord.getContent());
  }

  @Test
  void shouldNotFill001IfHrIdIsNull() {
    // given
    String parsedContent =
      "{\"leader\":\"00118nam  22000731a 4500\",\"fields\":[{\"001\":\"in001\"},{\"003\":\"qwerty\"},{\"035\":{\"subfields\":[{\"a\":\"(NhFolYBP)in001\"}],\"ind1\":\" \",\"ind2\":\" \"}},{\"507\":{\"subfields\":[{\"a\":\"data\"}],\"ind1\":\" \",\"ind2\":\" \"}},{\"500\":{\"subfields\":[{\"a\":\"data\"}],\"ind1\":\" \",\"ind2\":\" \"}}]}";
    String expectedParsedContent =
      "{\"leader\":\"00119nam  22000731a 4500\",\"fields\":[{\"003\":\"qwerty\"},{\"035\":{\"subfields\":[{\"a\":\"(NhFolYBP)in001\"}],\"ind1\":\" \",\"ind2\":\" \"}},{\"507\":{\"subfields\":[{\"a\":\"data\"}],\"ind1\":\" \",\"ind2\":\" \"}},{\"500\":{\"subfields\":[{\"a\":\"data\"}],\"ind1\":\" \",\"ind2\":\" \"}}]}";
    ParsedRecord parsedRecord = new ParsedRecord().withContent(parsedContent);

    var marcRecord = new Record().withId(UUID.randomUUID().toString())
      .withParsedRecord(parsedRecord)
      .withGeneration(0)
      .withState(Record.State.ACTUAL)
      .withExternalIdsHolder(new ExternalIdsHolder().withInstanceId("001").withInstanceHrid("in001"));
    // when
    AdditionalFieldsUtil.fill001FieldInMarcRecord(marcRecord, null);
    // then
    assertEquals(expectedParsedContent, parsedRecord.getContent());
  }

  @Test
  void shouldNotUpdate005Field() {
    String parsedContent =
      "{\"leader\":\"00115nam  22000731a 4500\",\"fields\":[{\"001\":\"in001\"},{\"003\":\"qwerty\"},{\"005\":\"20141107001016.0\"},{\"507\":{\"subfields\":[{\"a\":\"data\"}],\"ind1\":\" \",\"ind2\":\" \"}},{\"500\":{\"subfields\":[{\"a\":\"data\"}],\"ind1\":\" \",\"ind2\":\" \"}}]}";
    var marcRecord = new Record().withId(UUID.randomUUID().toString())
      .withParsedRecord(new ParsedRecord().withContent(parsedContent))
      .withGeneration(0)
      .withState(Record.State.ACTUAL)
      .withExternalIdsHolder(new ExternalIdsHolder().withInstanceId("001").withInstanceHrid("in001"));

    AdditionalFieldsUtil.updateLatestTransactionDate(marcRecord,
      new MappingParameters().withMarcFieldProtectionSettings(
        List.of(new MarcFieldProtectionSetting().withField("*").withData("*"))));

    String actualDate = getValueFromControlledField(marcRecord, TAG_005);
    assertNotNull(actualDate);
    assertEquals("20141107001016.0", actualDate);
  }

  @Test
  void shouldUpdate005Field() {
    String parsedContent =
      "{\"leader\":\"00115nam  22000731a 4500\",\"fields\":[{\"001\":\"in001\"},{\"003\":\"qwerty\"},{\"005\":\"20141107001016.0\"},{\"507\":{\"subfields\":[{\"a\":\"data\"}],\"ind1\":\" \",\"ind2\":\" \"}},{\"500\":{\"subfields\":[{\"a\":\"data\"}],\"ind1\":\" \",\"ind2\":\" \"}}]}";
    var marcRecord = new Record().withId(UUID.randomUUID().toString())
      .withParsedRecord(new ParsedRecord().withContent(parsedContent))
      .withGeneration(0)
      .withState(Record.State.ACTUAL)
      .withExternalIdsHolder(new ExternalIdsHolder().withInstanceId("001").withInstanceHrid("in001"));

    String expectedDate = dateTime005Formatter.format(ZonedDateTime.ofInstant(Instant.now(), ZoneId.systemDefault()));

    AdditionalFieldsUtil.updateLatestTransactionDate(marcRecord, new MappingParameters());

    String actualDate = getValueFromControlledField(marcRecord, TAG_005);
    assertNotNull(actualDate);
    assertEquals(expectedDate.substring(0, 10), actualDate.substring(0, 10));
  }

  @Test
  void shouldReturnValueFromDataField() {
    // given
    var id = UUID.randomUUID().toString();
    var parsedContent = """
      {
          "fields": [
              {"001": "in001"},
              {"999": {
                  "ind1": "f",
                  "ind2": "f",
                  "subfields": [
                      {"i": "%s"}
                  ]
              }}
          ]
      }
      """.formatted(id);
    var marcRecord = new Record().withParsedRecord(new ParsedRecord().withContent(parsedContent));

    // when
    var parsedId = getValueFromDataField(marcRecord, TAG_999, INDICATOR_F, INDICATOR_F, SUBFIELD_I);

    // then
    assertTrue(parsedId.isPresent());
    assertEquals(id, parsedId.get());
  }

  @Test
  void shouldReturnEmptyOptionalFromDataFieldWhenIndicatorsDoNotMatch() {
    var parsedContent = """
      {
          "fields": [
              {"999": {
                  "ind1": " ",
                  "ind2": " ",
                  "subfields": [
                      {"i": "test"}
                  ]
              }}
          ]
      }
      """;
    shouldReturnEmptyOptional(parsedContent);
  }

  @Test
  void shouldReturnEmptyOptionalFromDataFieldWhenSubfieldIsAbsent() {
    var parsedContent = """
      {
          "fields": [
              {"999": {
                  "ind1": "f",
                  "ind2": "f",
                  "subfields": [
                      {"s": "test"}
                  ]
              }}
          ]
      }
      """;
    shouldReturnEmptyOptional(parsedContent);
  }

  @Test
  void shouldReturnEmptyOptionalFromDataFieldWhenTagIsAbsent() {
    var parsedContent = """
      {
          "fields": [
              {"001": "ybp7406411"},
              {"245": {
                  "ind1": "1",
                  "ind2": "0",
                  "subfields": [
                      {"a": "titleValue"}
                  ]
              }}
          ]
      }
      """;
    shouldReturnEmptyOptional(parsedContent);
  }

  @Test
  void shouldThrowIllegalArgumentExceptionOnGetValueFromDataFieldCallWithNonDataFieldTag() {
    // given
    var marcRecord = new Record().withParsedRecord(new ParsedRecord().withContent("{}"));

    // when / then
    var exception = assertThrows(IllegalArgumentException.class,
      () -> getValueFromDataField(marcRecord, TAG_001, INDICATOR_F, INDICATOR_F, SUBFIELD_I));
    assertEquals(INVALID_DATA_FIELD_MSG.formatted(TAG_001), exception.getMessage());
  }

  @Test
  void shouldExistControlField() {
    String parsedRecordContent = TestUtil.readFileFromPath(PARSED_MARC_RECORD_PATH);
    ParsedRecord parsedRecord = new ParsedRecord().withContent(parsedRecordContent);
    var marcRecord = new Record().withId(UUID.randomUUID().toString()).withParsedRecord(parsedRecord);
    assertTrue(isFieldExist(marcRecord, "001", ' ', "ybp7406411"));
  }

  @Test
  void caching() {
    // given
    String parsedRecordContent = TestUtil.readFileFromPath(PARSED_MARC_RECORD_PATH);
    ParsedRecord parsedRecord = new ParsedRecord().withContent(parsedRecordContent);
    var marcRecord = new Record().withId(UUID.randomUUID().toString()).withParsedRecord(parsedRecord);
    String instanceId = UUID.randomUUID().toString();

    CacheStats initialCacheStats = getCacheStats();

    // record with null parsed content
    assertFalse(
      isFieldExist(new Record().withId(UUID.randomUUID().toString()), "035", 'a', instanceId));
    CacheStats cacheStats = getCacheStats().minus(initialCacheStats);
    assertEquals(0, cacheStats.hitCount());
    assertEquals(0, cacheStats.missCount());
    assertEquals(0, cacheStats.loadCount());
    // record with empty parsed content
    assertFalse(
      isFieldExist(new Record().withId(UUID.randomUUID().toString())
        .withParsedRecord(new ParsedRecord().withContent("")), "035", 'a', instanceId));
    cacheStats = getCacheStats().minus(initialCacheStats);
    assertEquals(0, cacheStats.requestCount());
    assertEquals(0, cacheStats.hitCount());
    assertEquals(0, cacheStats.missCount());
    assertEquals(0, cacheStats.loadCount());
    // record with bad parsed content
    assertFalse(
      isFieldExist(new Record().withId(UUID.randomUUID().toString())
        .withParsedRecord(new ParsedRecord().withContent("test")), "035", 'a', instanceId));
    cacheStats = getCacheStats().minus(initialCacheStats);
    assertEquals(1, cacheStats.requestCount());
    assertEquals(0, cacheStats.hitCount());
    assertEquals(1, cacheStats.missCount());
    assertEquals(1, cacheStats.loadCount());
    // does field exists?
    assertFalse(isFieldExist(marcRecord, "035", 'a', instanceId));
    cacheStats = getCacheStats().minus(initialCacheStats);
    assertEquals(2, cacheStats.requestCount());
    assertEquals(0, cacheStats.hitCount());
    assertEquals(2, cacheStats.missCount());
    assertEquals(2, cacheStats.loadCount());
    // update field
    addDataFieldToMarcRecord(marcRecord, "035", ' ', ' ', 'a', instanceId);
    cacheStats = getCacheStats().minus(initialCacheStats);
    assertEquals(3, cacheStats.requestCount());
    assertEquals(1, cacheStats.hitCount());
    assertEquals(2, cacheStats.missCount());
    assertEquals(2, cacheStats.loadCount());
    // verify that field exists
    assertTrue(isFieldExist(marcRecord, "035", 'a', instanceId));
    cacheStats = getCacheStats().minus(initialCacheStats);
    assertEquals(4, cacheStats.requestCount());
    assertEquals(2, cacheStats.hitCount());
    assertEquals(2, cacheStats.missCount());
    assertEquals(2, cacheStats.loadCount());
    // verify that field exists again
    assertTrue(isFieldExist(marcRecord, "035", 'a', instanceId));
    cacheStats = getCacheStats().minus(initialCacheStats);
    assertEquals(5, cacheStats.requestCount());
    assertEquals(3, cacheStats.hitCount());
    assertEquals(2, cacheStats.missCount());
    assertEquals(2, cacheStats.loadCount());
    // remove the field
    assertTrue(removeField(marcRecord, "035"));
    cacheStats = getCacheStats().minus(initialCacheStats);
    assertEquals(6, cacheStats.requestCount());
    assertEquals(4, cacheStats.hitCount());
    assertEquals(2, cacheStats.missCount());
    assertEquals(2, cacheStats.loadCount());
    // get value from controlled field
    assertEquals("ybp7406411", getValueFromControlledField(marcRecord, "001"));
    cacheStats = getCacheStats().minus(initialCacheStats);
    assertEquals(7, cacheStats.requestCount());
    assertEquals(5, cacheStats.hitCount());
    assertEquals(2, cacheStats.missCount());
    assertEquals(2, cacheStats.loadCount());
    // add controlled field to marc record
    assertTrue(addControlledFieldToMarcRecord(marcRecord, "002", "test",
      AdditionalFieldsUtil::addControlledFieldToMarcRecord));
    cacheStats = getCacheStats().minus(initialCacheStats);
    assertEquals(8, cacheStats.requestCount());
    assertEquals(6, cacheStats.hitCount());
    assertEquals(2, cacheStats.missCount());
    assertEquals(2, cacheStats.loadCount());
    // add field to marc record
    assertTrue(addFieldToMarcRecord(marcRecord, TAG_999, 'i', instanceId));
    cacheStats = getCacheStats().minus(initialCacheStats);
    assertEquals(9, cacheStats.requestCount());
    assertEquals(7, cacheStats.hitCount());
    assertEquals(2, cacheStats.missCount());
    assertEquals(2, cacheStats.loadCount());
  }

  @Test
  void shouldRemove003ifHRIDManipulationAlreadyDone() {
    // given
    String parsedContent =
      "{\"leader\":\"00115nam  22000731a 4500\",\"fields\":[{\"001\":\"in001\"},{\"003\":\"qwerty\"},{\"507\":{\"subfields\":[{\"a\":\"data\"}],\"ind1\":\" \",\"ind2\":\" \"}},{\"500\":{\"subfields\":[{\"a\":\"data\"}],\"ind1\":\" \",\"ind2\":\" \"}}]}";
    String expectedParsedContent =
      "{\"leader\":\"00086nam  22000611a 4500\",\"fields\":[{\"001\":\"in001\"},{\"507\":{\"subfields\":[{\"a\":\"data\"}],\"ind1\":\" \",\"ind2\":\" \"}},{\"500\":{\"subfields\":[{\"a\":\"data\"}],\"ind1\":\" \",\"ind2\":\" \"}}]}";
    ParsedRecord parsedRecord = new ParsedRecord();
    parsedRecord.setContent(parsedContent);

    var marcRecord = new Record().withId(UUID.randomUUID().toString())
      .withParsedRecord(parsedRecord)
      .withGeneration(0)
      .withState(Record.State.ACTUAL)
      .withExternalIdsHolder(new ExternalIdsHolder().withInstanceId("001").withInstanceHrid("in001"));

    JsonObject jsonObject = new JsonObject("{\"hrid\":\"in001\"}");
    Pair<Record, JsonObject> pair = Pair.of(marcRecord, jsonObject);
    // when
    AdditionalFieldsUtil.fillHrIdFieldInMarcRecord(pair);
    // then
    assertEquals(expectedParsedContent, parsedRecord.getContent());
  }

  @Test
  void shouldNotProcessRecord() {
    // given
    String parsedContent =
      "{\"leader\":\"00115nam  22000731a 4500\",\"fields\":[{\"001\":\"in001\"},{\"003\":\"qwerty\"},{\"507\":{\"subfields\":[{\"a\":\"data\"}],\"ind1\":\" \",\"ind2\":\" \"}},{\"500\":{\"subfields\":[{\"a\":\"data\"}],\"ind1\":\" \",\"ind2\":\" \"}}]}";
    String expectedParsedContent =
      "{\"leader\":\"00086nam  22000611a 4500\",\"fields\":[{\"001\":\"in001\"},{\"507\":{\"subfields\":[{\"a\":\"data\"}],\"ind1\":\" \",\"ind2\":\" \"}},{\"500\":{\"subfields\":[{\"a\":\"data\"}],\"ind1\":\" \",\"ind2\":\" \"}}]}";
    ParsedRecord parsedRecord = new ParsedRecord();
    parsedRecord.setContent(parsedContent);

    var marcRecord = new Record().withId(UUID.randomUUID().toString())
      .withParsedRecord(parsedRecord)
      .withGeneration(0)
      .withState(Record.State.ACTUAL)
      .withExternalIdsHolder(new ExternalIdsHolder().withInstanceId("001").withInstanceHrid("in001"));

    JsonObject jsonObject = new JsonObject("{\"hrid\":\"in003\"}");
    Pair<Record, JsonObject> pair = Pair.of(marcRecord, jsonObject);
    // when
    AdditionalFieldsUtil.fillHrIdFieldInMarcRecord(pair);
    // then
    assertNotEquals(expectedParsedContent, parsedRecord.getContent());
  }

  @Test
  void shouldNotAdd035AndAdd001FieldsIf001And003FieldsNotExists() {
    // given
    String parsedContent =
      "{\"leader\":\"00116nam  22000732a 4900\",\"fields\":[{\"003\":\"in001\"},{\"507\":{\"subfields\":[{\"a\":\"data\"}],\"ind1\":\" \",\"ind2\":\" \"}},{\"500\":{\"subfields\":[{\"a\":\"data\"}],\"ind1\":\" \",\"ind2\":\" \"}}]}";
    String expectedParsedContent =
      "{\"leader\":\"00086nam  22000612a 4900\",\"fields\":[{\"001\":\"in001\"},{\"507\":{\"subfields\":[{\"a\":\"data\"}],\"ind1\":\" \",\"ind2\":\" \"}},{\"500\":{\"subfields\":[{\"a\":\"data\"}],\"ind1\":\" \",\"ind2\":\" \"}}]}";
    ParsedRecord parsedRecord = new ParsedRecord();
    parsedRecord.setContent(parsedContent);

    var marcRecord = new Record().withId(UUID.randomUUID().toString())
      .withParsedRecord(parsedRecord)
      .withGeneration(0)
      .withState(Record.State.ACTUAL)
      .withExternalIdsHolder(new ExternalIdsHolder().withInstanceId("001").withInstanceHrid("in001"));

    JsonObject jsonObject = new JsonObject("{\"hrid\":\"in001\"}");
    Pair<Record, JsonObject> pair = Pair.of(marcRecord, jsonObject);
    // when
    AdditionalFieldsUtil.fillHrIdFieldInMarcRecord(pair);
    // then
    assertEquals(expectedParsedContent, parsedRecord.getContent());
  }

  @Test
  void shouldRemove035() {
    // given
    String parsedContent =
      "{\"leader\":\"00120nam  22000731a 4500\",\"fields\":[{\"001\":\"in001\"},{\"035\":{\"subfields\":[{\"a\":\"(ybp7406411)in001\"}],\"ind1\":\" \",\"ind2\":\" \"}},{\"507\":{\"subfields\":[{\"a\":\"data\"}],\"ind1\":\" \",\"ind2\":\" \"}},{\"500\":{\"subfields\":[{\"a\":\"data\"}],\"ind1\":\" \",\"ind2\":\" \"}}]}";
    String expectedParsedContent =
      "{\"leader\":\"00086nam  22000611a 4500\",\"fields\":[{\"001\":\"in001\"},{\"507\":{\"subfields\":[{\"a\":\"data\"}],\"ind1\":\" \",\"ind2\":\" \"}},{\"500\":{\"subfields\":[{\"a\":\"data\"}],\"ind1\":\" \",\"ind2\":\" \"}}]}";
    ParsedRecord parsedRecord = new ParsedRecord();
    parsedRecord.setContent(parsedContent);

    var marcRecord = new Record().withId(UUID.randomUUID().toString())
      .withParsedRecord(parsedRecord)
      .withRecordType(Record.RecordType.MARC_BIB)
      .withGeneration(0)
      .withState(Record.State.ACTUAL)
      .withExternalIdsHolder(new ExternalIdsHolder().withInstanceId("001").withInstanceHrid("in001"));

    // when
    AdditionalFieldsUtil.remove035FieldWhenRecordContainsHrId(marcRecord);
    // then
    assertEquals(expectedParsedContent, parsedRecord.getContent());
  }

  @Test
  void shouldReorderMarcRecordFields() throws IOException, MarcException {
    var systemReorderedRecordContent = readFileFromPath(PARSED_RECORD);
    var userOrderRecordContent = readFileFromPath(REORDERED_PARSED_RECORD);
    var expectedOrderRecord = readFileFromPath(REORDERING_RESULT_RECORD);

    var actualOrderRecord =
      AdditionalFieldsUtil.reorderMarcRecordFields(userOrderRecordContent, systemReorderedRecordContent);

    assertNotNull(actualOrderRecord);
    assertEquals(formatContent(expectedOrderRecord), formatContent(actualOrderRecord));
  }

  private void shouldReturnEmptyOptional(String parsedContent) {
    // given
    var marcRecord = new Record().withParsedRecord(new ParsedRecord().withContent(parsedContent));

    // when
    var result = getValueFromDataField(marcRecord, TAG_999, INDICATOR_F, INDICATOR_F, SUBFIELD_I);

    // then
    assertTrue(result.isEmpty());
  }

  private static String readFileFromPath(String path) throws IOException {
    return new String(FileUtils.readFileToByteArray(new File(path)));
  }

  private String formatContent(String content) {
    return content.replaceAll("\\s", "");
  }

  static class OclcFieldNormalizationTest {

    static Stream<Arguments> data() {
      return Stream.of(
        Arguments.of(
          "{\"leader\":\"00120nam  22000731a 4500\",\"fields\":[{\"001\":\"in001\"}," +
          "{\"035\":{\"subfields\":[{\"a\":\"(ybp7406411)in001\"}," +
          "{\"a\":\"(OCoLC)00006475800\"}],\"ind1\":\" \",\"ind2\":\" \"}}," +
          "{\"500\":{\"subfields\":[{\"a\":\"data\"}],\"ind1\":\" \",\"ind2\":\" \"}}]}",

          "{\"leader\":\"00115nam  22000611a 4500\",\"fields\":[{\"001\":\"in001\"}," +
          "{\"035\":{\"subfields\":[{\"a\":\"(ybp7406411)in001\"}," +
          "{\"a\":\"(OCoLC)6475800\"}],\"ind1\":\" \",\"ind2\":\" \"}}," +
          "{\"500\":{\"subfields\":[{\"a\":\"data\"}],\"ind1\":\" \",\"ind2\":\" \"}}]}"
        ),
        Arguments.of(
          "{\"leader\":\"00120nam  22000731a 4500\",\"fields\":[{\"001\":\"in001\"}," +
          "{\"035\":{\"subfields\":[{\"a\":\"(ybp7406411)in001\"}," +
          "{\"a\":\"(OCoLC)tfe0006475800\"} ],\"ind1\":\" \",\"ind2\":\" \"}}," +
          "{\"500\":{\"subfields\":[{\"a\":\"data\"}],\"ind1\":\" \",\"ind2\":\" \"}}]}",

          "{\"leader\":\"00118nam  22000611a 4500\",\"fields\":[{\"001\":\"in001\"}," +
          "{\"035\":{\"subfields\":[{\"a\":\"(ybp7406411)in001\"}," +
          "{\"a\":\"(OCoLC)tfe6475800\"}],\"ind1\":\" \",\"ind2\":\" \"}}," +
          "{\"500\":{\"subfields\":[{\"a\":\"data\"}],\"ind1\":\" \",\"ind2\":\" \"}}]}"
        ),
        Arguments.of(
          "{\"leader\":\"00120nam  22000731a 4500\",\"fields\":[{\"001\":\"in001\"}," +
          "{\"035\":{\"subfields\":[{\"a\":\"(ybp7406411)in001\"}," +
          "{\"a\":\"(OCoLC)00064758\"}," +
          "{\"a\":\"(OCoLC)ocm00064758\"}," +
          "{\"z\":\"(OCoLC)00024758\"} ],\"ind1\":\" \",\"ind2\":\" \"}}," +
          "{\"500\":{\"subfields\":[{\"a\":\"data\"}],\"ind1\":\" \",\"ind2\":\" \"}}]}",

          "{\"leader\":\"00127nam  22000611a 4500\",\"fields\":[{\"001\":\"in001\"}," +
          "{\"035\":{\"subfields\":[{\"a\":\"(ybp7406411)in001\"}," +
          "{\"a\":\"(OCoLC)64758\"},{\"z\":\"(OCoLC)24758\"}],\"ind1\":\" \",\"ind2\":\" \"}}," +
          "{\"500\":{\"subfields\":[{\"a\":\"data\"}],\"ind1\":\" \",\"ind2\":\" \"}}]}"
        ),
        Arguments.of(
          "{\"leader\":\"00120nam  22000731a 4500\",\"fields\":[{\"001\":\"in001\"}," +
          "{\"035\":{\"subfields\":[{\"a\":\"(OCoLC)00064758\"} ],\"ind1\":\" \",\"ind2\":\" \"}}," +
          "{\"035\":{\"subfields\":[{\"a\":\"(OCoLC)ocn000064758\"} ],\"ind1\":\" \",\"ind2\":\" \"}}," +
          "{\"035\":{\"subfields\":[{\"a\":\"(OCoLC)ocm0000064758\"}, {\"z\":\"(OCoLC)11114758\"} ],\"ind1\":\" \"," +
          "\"ind2\":\" \"}}," +
          "{\"500\":{\"subfields\":[{\"a\":\"data\"}],\"ind1\":\" \",\"ind2\":\" \"}}]}",

          "{\"leader\":\"00111nam  22000611a 4500\",\"fields\":[{\"001\":\"in001\"}," +
          "{\"035\":{\"subfields\":[{\"a\":\"(OCoLC)64758\"},{\"z\":\"(OCoLC)11114758\"}],\"ind1\":\" \",\"ind2\":\" \"}},"
          +
          "{\"500\":{\"subfields\":[{\"a\":\"data\"}],\"ind1\":\" \",\"ind2\":\" \"}}]}"
        ),
        Arguments.of(
          "{\"leader\":\"00120nam  22000731a 4500\",\"fields\":[{\"001\":\"in001\"}," +
          "{\"035\":{\"subfields\":[{\"a\":\"(ybp7406411)in001\"}],\"ind1\":\" \",\"ind2\":\" \"}}," +
          "{\"035\":{\"subfields\":[{\"a\":\"(OCoLC)ocn00064758\"}],\"ind1\":\" \",\"ind2\":\" \"}}," +
          "{\"035\":{\"subfields\":[{\"a\":\"(OCoLC)ocm000064758\"}],\"ind1\":\" \",\"ind2\":\" \"}}," +
          "{\"500\":{\"subfields\":[{\"a\":\"data\"}],\"ind1\":\" \",\"ind2\":\" \"}}]}",

          "{\"leader\":\"00128nam  22000731a 4500\",\"fields\":[{\"001\":\"in001\"}," +
          "{\"035\":{\"subfields\":[{\"a\":\"(ybp7406411)in001\"}],\"ind1\":\" \",\"ind2\":\" \"}}," +
          "{\"035\":{\"subfields\":[{\"a\":\"(OCoLC)64758\"}],\"ind1\":\" \",\"ind2\":\" \"}}," +
          "{\"500\":{\"subfields\":[{\"a\":\"data\"}],\"ind1\":\" \",\"ind2\":\" \"}}]}"
        ),
        Arguments.of(
          "{\"leader\":\"00120nam  22000731a 4500\",\"fields\":[{\"001\":\"in001\"}," +
          "{\"035\":{\"subfields\":[{\"a\":\"(OCoLC)ocn607TST001\"}],\"ind1\":\" \",\"ind2\":\" \"}}," +
          "{\"500\":{\"subfields\":[{\"a\":\"data\"}],\"ind1\":\" \",\"ind2\":\" \"}}]}",

          "{\"leader\":\"00098nam  22000611a 4500\",\"fields\":[{\"001\":\"in001\"}," +
          "{\"035\":{\"subfields\":[{\"a\":\"(OCoLC)607TST001\"}],\"ind1\":\" \",\"ind2\":\" \"}}," +
          "{\"500\":{\"subfields\":[{\"a\":\"data\"}],\"ind1\":\" \",\"ind2\":\" \"}}]}"
        ),
        Arguments.of(
          "{\"leader\":\"00120nam  22000731a 4500\",\"fields\":[{\"001\":\"in001\"}," +
          "{\"035\":{\"subfields\":[{\"a\":\"(OCoLC-M)ocn0001234\"}],\"ind1\":\" \",\"ind2\":\" \"}}," +
          "{\"035\":{\"subfields\":[{\"a\":\"(ABC)ocn0001234\"}],\"ind1\":\" \",\"ind2\":\" \"}}," +
          "{\"035\":{\"subfields\":[{\"a\":\"(OCoLC)ocn0001234\"}, {\"a\":\"(OCoLC)ocn1234\"}, {\"b\":\"(OCoLC)ocn1234\"}],\"ind1\":\" \",\"ind2\":\" \"}},"
          +
          "{\"035\":{\"subfields\":[{\"a\":\"(OCoLC)ocm1234\"}],\"ind1\":\" \",\"ind2\":\" \"}}," +
          "{\"035\":{\"subfields\":[{\"a\":\"(OCoLC)ocn00098765\"}],\"ind1\":\" \",\"ind2\":\" \"}}," +
          "{\"035\":{\"subfields\":[{\"a\":\"(OCoLC)ocn0001234\"}],\"ind1\":\" \",\"ind2\":\" \"}}," +
          "{\"500\":{\"subfields\":[{\"a\":\"data\"}],\"ind1\":\" \",\"ind2\":\" \"}}]}",

          "{\"leader\":\"00218nam  22001091a 4500\",\"fields\":[{\"001\":\"in001\"}," +
          "{\"035\":{\"subfields\":[{\"a\":\"(OCoLC-M)ocn0001234\"}],\"ind1\":\" \",\"ind2\":\" \"}}," +
          "{\"035\":{\"subfields\":[{\"a\":\"(ABC)ocn0001234\"}],\"ind1\":\" \",\"ind2\":\" \"}}," +
          "{\"035\":{\"subfields\":[{\"b\":\"(OCoLC)1234\"}],\"ind1\":\" \",\"ind2\":\" \"}}," +
          "{\"035\":{\"subfields\":[{\"a\":\"(OCoLC)98765\"}],\"ind1\":\" \",\"ind2\":\" \"}}," +
          "{\"035\":{\"subfields\":[{\"a\":\"(OCoLC)1234\"}],\"ind1\":\" \",\"ind2\":\" \"}}," +
          "{\"500\":{\"subfields\":[{\"a\":\"data\"}],\"ind1\":\" \",\"ind2\":\" \"}}]}"
        ),
        Arguments.of(
          "{\"leader\":\"00126nam  22000731a 4500\",\"fields\":[{\"001\":\"in001\"}," +
          "{\"035\":{\"subfields\":[{\"a\":\"(OCoLC)1234\"}],\"ind1\":\" \",\"ind2\":\" \"}}," +
          "{\"035\":{\"subfields\":[{\"a\":\"(OCoLC-M)1234456\"}],\"ind1\":\" \",\"ind2\":\" \"}}," +
          "{\"500\":{\"subfields\":[{\"a\":\"data\"}],\"ind1\":\" \",\"ind2\":\" \"}}]}",

          "{\"leader\":\"00126nam  22000731a 4500\",\"fields\":[{\"001\":\"in001\"}," +
          "{\"035\":{\"subfields\":[{\"a\":\"(OCoLC)1234\"}],\"ind1\":\" \",\"ind2\":\" \"}}," +
          "{\"035\":{\"subfields\":[{\"a\":\"(OCoLC-M)1234456\"}],\"ind1\":\" \",\"ind2\":\" \"}}," +
          "{\"500\":{\"subfields\":[{\"a\":\"data\"}],\"ind1\":\" \",\"ind2\":\" \"}}]}"
        ),
        Arguments.of(
          "{\"leader\":\"00120nam  22000731a 4500\",\"fields\":[{\"001\":\"in001\"}," +
          "{\"035\":{\"subfields\":[{\"a\":\"(ybp7406411)in001\"}," +
          "{\"a\":\"   (OCoLC)000012345\"}],\"ind1\":\" \",\"ind2\":\" \"}}," +
          "{\"500\":{\"subfields\":[{\"a\":\"data\"}],\"ind1\":\" \",\"ind2\":\" \"}}]}",

          "{\"leader\":\"00113nam  22000611a 4500\",\"fields\":[{\"001\":\"in001\"}," +
          "{\"035\":{\"subfields\":[{\"a\":\"(ybp7406411)in001\"}," +
          "{\"a\":\"(OCoLC)12345\"}],\"ind1\":\" \",\"ind2\":\" \"}}," +
          "{\"500\":{\"subfields\":[{\"a\":\"data\"}],\"ind1\":\" \",\"ind2\":\" \"}}]}"
        )
      );
    }

    @ParameterizedTest
    @MethodSource("data")
    void shouldNormalizeOCoLCField035(String parsedContent, String expectedParsedContent) {
      // given
      ParsedRecord parsedRecord = new ParsedRecord().withContent(parsedContent);

      var marcRecord = new Record().withId(UUID.randomUUID().toString())
        .withParsedRecord(parsedRecord)
        .withGeneration(0)
        .withState(Record.State.ACTUAL)
        .withExternalIdsHolder(new ExternalIdsHolder().withInstanceId("001").withInstanceHrid("in001"));
      // when
      AdditionalFieldsUtil.normalize035(marcRecord);
      assertEquals(expectedParsedContent, parsedRecord.getContent());
    }
  }
}
