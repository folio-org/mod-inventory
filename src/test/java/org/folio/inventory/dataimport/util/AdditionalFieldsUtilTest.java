package org.folio.inventory.dataimport.util;

import com.github.benmanes.caffeine.cache.stats.CacheStats;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.folio.inventory.TestUtil;
import org.folio.inventory.domain.instances.Instance;
import org.folio.processing.mapping.defaultmapper.processor.parameters.MappingParameters;
import org.folio.rest.jaxrs.model.ExternalIdsHolder;
import org.folio.rest.jaxrs.model.MarcFieldProtectionSetting;
import org.folio.rest.jaxrs.model.ParsedRecord;
import org.folio.rest.jaxrs.model.Record;
import org.hamcrest.MatcherAssert;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.BlockJUnit4ClassRunner;
import org.marc4j.MarcException;

import java.io.File;
import java.io.IOException;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.IntStream;

import static org.folio.inventory.dataimport.util.AdditionalFieldsUtil.TAG_001;
import static org.folio.inventory.dataimport.util.AdditionalFieldsUtil.TAG_005;
import static org.folio.inventory.dataimport.util.AdditionalFieldsUtil.TAG_999;
import static org.folio.inventory.dataimport.util.AdditionalFieldsUtil.addControlledFieldToMarcRecord;
import static org.folio.inventory.dataimport.util.AdditionalFieldsUtil.addDataFieldToMarcRecord;
import static org.folio.inventory.dataimport.util.AdditionalFieldsUtil.addFieldToMarcRecord;
import static org.folio.inventory.dataimport.util.AdditionalFieldsUtil.dateTime005Formatter;
import static org.folio.inventory.dataimport.util.AdditionalFieldsUtil.getCacheStats;
import static org.folio.inventory.dataimport.util.AdditionalFieldsUtil.getValueFromControlledField;
import static org.folio.inventory.dataimport.util.AdditionalFieldsUtil.isFieldExist;
import static org.folio.inventory.dataimport.util.AdditionalFieldsUtil.removeField;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.junit.Assert.*;

@RunWith(BlockJUnit4ClassRunner.class)
public class AdditionalFieldsUtilTest {

  private static final String PARSED_MARC_RECORD_PATH = "src/test/resources/marc/parsedMarcRecord.json";
  private static final String PARSED_RECORD = "src/test/resources/marc/parsedRecord.json";
  private static final String REORDERED_PARSED_RECORD = "src/test/resources/marc/reorderedParsedRecord.json";
  private static final String REORDERING_RESULT_RECORD = "src/test/resources/marc/reorderingResultRecord.json";

  @Test
  public void shouldAddInstanceIdSubfield() throws IOException {
    // given
    String recordId = UUID.randomUUID().toString();
    String instanceId = UUID.randomUUID().toString();

    String parsedRecordContent = TestUtil.readFileFromPath(PARSED_MARC_RECORD_PATH);
    ParsedRecord parsedRecord = new ParsedRecord().withContent(parsedRecordContent);
    String leader = new JsonObject(parsedRecordContent).getString("leader");
    Record record = new Record().withId(recordId).withParsedRecord(parsedRecord);
    // when
    boolean addedSourceRecordId = addFieldToMarcRecord(record, TAG_999, 's', recordId);
    boolean addedInstanceId = addFieldToMarcRecord(record, TAG_999, 'i', instanceId);
    // then
    Assert.assertTrue(addedSourceRecordId);
    Assert.assertTrue(addedInstanceId);
    JsonObject content = new JsonObject(parsedRecord.getContent().toString());
    JsonArray fields = content.getJsonArray("fields");
    String newLeader = content.getString("leader");
    Assert.assertNotEquals(leader, newLeader);
    Assert.assertFalse(fields.isEmpty());
    int totalFieldsCount = 0;
    for (int i = fields.size(); i-- > 0; ) {
      JsonObject targetField = fields.getJsonObject(i);
      if (targetField.containsKey(TAG_999)) {
        JsonArray subfields = targetField.getJsonObject(TAG_999).getJsonArray("subfields");
        for (int j = subfields.size(); j-- > 0; ) {
          JsonObject targetSubfield = subfields.getJsonObject(j);
          if (targetSubfield.containsKey("i")) {
            String actualInstanceId = (String) targetSubfield.getValue("i");
            Assert.assertEquals(instanceId, actualInstanceId);
          }
          if (targetSubfield.containsKey("s")) {
            String actualSourceRecordId = (String) targetSubfield.getValue("s");
            Assert.assertEquals(recordId, actualSourceRecordId);
          }
        }
        totalFieldsCount++;
      }
    }
    Assert.assertEquals(2, totalFieldsCount);
  }

  @Test
  public void shouldNotAddInstanceIdSubfieldIfNoParsedRecordContent() {
    // given
    Record record = new Record();
    String instanceId = UUID.randomUUID().toString();
    // when
    boolean added = addFieldToMarcRecord(record, TAG_999, 'i', instanceId);
    // then
    Assert.assertFalse(added);
    Assert.assertNull(record.getParsedRecord());
  }

  @Test
  public void shouldNotAddInstanceIdSubfieldIfNoFieldsInParsedRecordContent() {
    // given
    Record record = new Record();
    String content = StringUtils.EMPTY;
    record.setParsedRecord(new ParsedRecord().withContent(content));
    String instanceId = UUID.randomUUID().toString();
    // when
    boolean added = addFieldToMarcRecord(record, TAG_999, 'i', instanceId);
    // then
    Assert.assertFalse(added);
    Assert.assertNotNull(record.getParsedRecord());
    Assert.assertNotNull(record.getParsedRecord().getContent());
    Assert.assertEquals(content, record.getParsedRecord().getContent());
  }

  @Test
  public void shouldNotAddInstanceIdSubfieldIfCanNotConvertParsedContentToJsonObject() {
    // given
    Record record = new Record();
    String content = "{fields}";
    record.setParsedRecord(new ParsedRecord().withContent(content));
    String instanceId = UUID.randomUUID().toString();
    // when
    boolean added = addFieldToMarcRecord(record, TAG_999, 'i', instanceId);
    // then
    Assert.assertFalse(added);
    Assert.assertNotNull(record.getParsedRecord());
    Assert.assertNotNull(record.getParsedRecord().getContent());
    Assert.assertEquals(content, record.getParsedRecord().getContent());
  }

  @Test
  public void shouldNotAddInstanceIdSubfieldIfContentHasNoFields() {
    // given
    Record record = new Record();
    String content = "{\"leader\":\"01240cas a2200397\"}";
    record.setParsedRecord(new ParsedRecord().withContent(content));
    String instanceId = UUID.randomUUID().toString();
    // when
    boolean added = addFieldToMarcRecord(record, TAG_999, 'i', instanceId);
    // then
    Assert.assertFalse(added);
    Assert.assertNotNull(record.getParsedRecord());
    Assert.assertNotNull(record.getParsedRecord().getContent());
  }

  @Test
  public void shouldNotAddInstanceIdSubfieldIfContentIsNull() {
    // given
    Record record = new Record();
    record.setParsedRecord(new ParsedRecord().withContent(null));
    String instanceId = UUID.randomUUID().toString();
    // when
    boolean added = addFieldToMarcRecord(record, TAG_999, 'i', instanceId);
    // then
    Assert.assertFalse(added);
    Assert.assertNotNull(record.getParsedRecord());
    Assert.assertNull(record.getParsedRecord().getContent());
  }

  @Test
  public void shouldRemoveField() throws IOException {
    String recordId = UUID.randomUUID().toString();
    String parsedRecordContent = TestUtil.readFileFromPath(PARSED_MARC_RECORD_PATH);
    ParsedRecord parsedRecord = new ParsedRecord();
    String leader = new JsonObject(parsedRecordContent).getString("leader");
    parsedRecord.setContent(parsedRecordContent);
    Record record = new Record().withId(recordId).withParsedRecord(parsedRecord);
    boolean deleted = removeField(record, "001");
    Assert.assertTrue(deleted);
    JsonObject content = new JsonObject(parsedRecord.getContent().toString());
    JsonArray fields = content.getJsonArray("fields");
    String newLeader = content.getString("leader");
    Assert.assertNotEquals(leader, newLeader);
    Assert.assertFalse(fields.isEmpty());
    boolean contains001Field = IntStream.range(0, fields.size()).mapToObj(fields::getJsonObject)
      .anyMatch(targetField -> targetField.containsKey("001"));
    Assert.assertFalse(contains001Field);
  }

  @Test
  public void shouldNotAddControlledFieldToMarcRecord() throws IOException {
    String recordId = UUID.randomUUID().toString();
    String parsedRecordContent = TestUtil.readFileFromPath(PARSED_MARC_RECORD_PATH);
    ParsedRecord parsedRecord = new ParsedRecord().withContent(parsedRecordContent);
    Record record = new Record().withId(recordId).withParsedRecord(parsedRecord);
    boolean added = addControlledFieldToMarcRecord(record, "002", "", null);
    Assert.assertFalse(added);
  }

  @Test
  public void shouldAddControlledFieldToMarcRecord() throws IOException {
    String recordId = UUID.randomUUID().toString();
    String parsedRecordContent = TestUtil.readFileFromPath(PARSED_MARC_RECORD_PATH);
    ParsedRecord parsedRecord = new ParsedRecord().withContent(parsedRecordContent);
    String leader = new JsonObject(parsedRecordContent).getString("leader");
    Record record = new Record().withId(recordId).withParsedRecord(parsedRecord);
    boolean added = addControlledFieldToMarcRecord(
      record, "002", "test", AdditionalFieldsUtil::addControlledFieldToMarcRecord);
    Assert.assertTrue(added);
    JsonObject content = new JsonObject(parsedRecord.getContent().toString());
    JsonArray fields = content.getJsonArray("fields");
    String newLeader = content.getString("leader");
    Assert.assertNotEquals(leader, newLeader);
    Assert.assertFalse(fields.isEmpty());
    boolean contains002Field = IntStream.range(0, fields.size()).mapToObj(fields::getJsonObject)
      .anyMatch(field -> field.containsKey("002") && field.getString("002").equals("test"));
    Assert.assertTrue(contains002Field);
  }

  @Test
  public void shouldReplaceControlledFieldInMarcRecord() throws IOException {
    String recordId = UUID.randomUUID().toString();
    String parsedRecordContent = TestUtil.readFileFromPath(PARSED_MARC_RECORD_PATH);
    ParsedRecord parsedRecord = new ParsedRecord().withContent(parsedRecordContent);
    String leader = new JsonObject(parsedRecordContent).getString("leader");
    Record record = new Record().withId(recordId).withParsedRecord(parsedRecord);
    boolean added = addControlledFieldToMarcRecord(
      record, "003", "test", AdditionalFieldsUtil::replaceOrAddControlledFieldInMarcRecord);
    Assert.assertTrue(added);
    JsonObject content = new JsonObject(parsedRecord.getContent().toString());
    JsonArray fields = content.getJsonArray("fields");
    String newLeader = content.getString("leader");
    Assert.assertNotEquals(leader, newLeader);
    Assert.assertFalse(fields.isEmpty());
    boolean is003Field = fields.getJsonObject(1).getString("003").equals("test");
    Assert.assertTrue(is003Field);
  }

  @Test
  public void isFieldsFillingNeededTrue() {
    String instanceId = UUID.randomUUID().toString();
    String instanceHrId = UUID.randomUUID().toString();
    Record srcRecord = new Record().withExternalIdsHolder(new ExternalIdsHolder().withInstanceId(instanceId).withInstanceHrid(UUID.randomUUID().toString()));
    Instance instance = new Instance(instanceId, "0", instanceHrId, "", "", "");
    Assert.assertTrue(AdditionalFieldsUtil.isFieldsFillingNeeded(srcRecord, instance));

    srcRecord.getExternalIdsHolder().setInstanceHrid(null);
    Assert.assertTrue(AdditionalFieldsUtil.isFieldsFillingNeeded(srcRecord, instance));
  }

  @Test
  public void isFieldsFillingNeededFalse() {
    String instanceId = UUID.randomUUID().toString();
    String instanceHrId = UUID.randomUUID().toString();
    Record srcRecord = new Record().withExternalIdsHolder(new ExternalIdsHolder().withInstanceId(instanceId).withInstanceHrid(instanceHrId));
    Instance instance = new Instance(instanceId, "0", instanceHrId, "", "", "");
    assertFalse(AdditionalFieldsUtil.isFieldsFillingNeeded(srcRecord, instance));

    srcRecord.getExternalIdsHolder().withInstanceId(instanceId);
    instance = new Instance(UUID.randomUUID().toString(), "0", instanceHrId, "", "", "");
    assertFalse(AdditionalFieldsUtil.isFieldsFillingNeeded(srcRecord, instance));

    srcRecord.getExternalIdsHolder().withInstanceId(null).withInstanceHrid(null);
    instance = new Instance(UUID.randomUUID().toString(), "0", instanceHrId, "", "", "");
    assertFalse(AdditionalFieldsUtil.isFieldsFillingNeeded(srcRecord, instance));
  }

  @Test(expected = Exception.class)
  public void isFieldsFillingNeededForExternalHolderInstanceShouldThrowException() {
    String instanceId = UUID.randomUUID().toString();
    String instanceHrId = UUID.randomUUID().toString();
    Record srcRecord = new Record().withExternalIdsHolder(new ExternalIdsHolder().withInstanceId(instanceId).withInstanceHrid(instanceHrId));
    Instance instance = new Instance(null, "0", instanceHrId, "", "", "");
    AdditionalFieldsUtil.isFieldsFillingNeeded(srcRecord, instance);
  }

  @Test
  public void shouldAddFieldToMarcRecordInNumericalOrder() throws IOException {
    // given
    String instanceHrId = UUID.randomUUID().toString();
    String parsedRecordContent = TestUtil.readFileFromPath(PARSED_MARC_RECORD_PATH);
    ParsedRecord parsedRecord = new ParsedRecord().withContent(parsedRecordContent);
    String leader = new JsonObject(parsedRecordContent).getString("leader");
    Record record = new Record().withId(UUID.randomUUID().toString()).withParsedRecord(parsedRecord);
    // when
    boolean added = addDataFieldToMarcRecord(record, "035", ' ', ' ', 'a', instanceHrId);
    // then
    Assert.assertTrue(added);
    JsonObject content = new JsonObject(parsedRecord.getContent().toString());
    JsonArray fields = content.getJsonArray("fields");
    String newLeader = content.getString("leader");
    Assert.assertNotEquals(leader, newLeader);
    Assert.assertFalse(fields.isEmpty());
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
    Assert.assertTrue(existsNewField);
  }

  @Test
  public void shouldNotSortExistingFieldsWhenAddFieldToToMarcRecord() {
    // given
    String instanceId = "12345";
    String parsedContent = "{\"leader\":\"00115nam  22000731a 4500\",\"fields\":[{\"001\":\"ybp7406411\"},{\"507\":{\"subfields\":[{\"a\":\"data\"}],\"ind1\":\" \",\"ind2\":\" \"}},{\"500\":{\"subfields\":[{\"a\":\"data\"}],\"ind1\":\" \",\"ind2\":\" \"}}]}";
    String expectedParsedContent = "{\"leader\":\"00113nam  22000731a 4500\",\"fields\":[{\"001\":\"ybp7406411\"},{\"507\":{\"subfields\":[{\"a\":\"data\"}],\"ind1\":\" \",\"ind2\":\" \"}},{\"500\":{\"subfields\":[{\"a\":\"data\"}],\"ind1\":\" \",\"ind2\":\" \"}},{\"999\":{\"subfields\":[{\"i\":\"12345\"}],\"ind1\":\"f\",\"ind2\":\"f\"}}]}";
    ParsedRecord parsedRecord = new ParsedRecord().withContent(parsedContent);
    Record record = new Record().withId(UUID.randomUUID().toString()).withParsedRecord(parsedRecord);
    // when
    boolean added = addDataFieldToMarcRecord(record, "999", 'f', 'f', 'i', instanceId);
    // then
    Assert.assertTrue(added);
    Assert.assertEquals(expectedParsedContent, parsedRecord.getContent());
  }

  @Test
  public void shouldNotAdd035FieldIf001And003FieldsNotExists() {
    // given
    String parsedContent = "{\"leader\":\"00115nam  22000731a 4500\",\"fields\":[{\"003\":\"in001\"},{\"507\":{\"subfields\":[{\"a\":\"data\"}],\"ind1\":\" \",\"ind2\":\" \"}},{\"500\":{\"subfields\":[{\"a\":\"data\"}],\"ind1\":\" \",\"ind2\":\" \"}}]}";
    String expectedParsedContent = "{\"leader\":\"00068nam  22000491a 4500\",\"fields\":[{\"507\":{\"subfields\":[{\"a\":\"data\"}],\"ind1\":\" \",\"ind2\":\" \"}},{\"500\":{\"subfields\":[{\"a\":\"data\"}],\"ind1\":\" \",\"ind2\":\" \"}}]}";
    ParsedRecord parsedRecord = new ParsedRecord().withContent(parsedContent);

    Record record = new Record().withId(UUID.randomUUID().toString())
      .withParsedRecord(parsedRecord)
      .withGeneration(0)
      .withState(Record.State.ACTUAL)
      .withExternalIdsHolder(new ExternalIdsHolder().withInstanceId("001").withInstanceHrid("in001"));
    // when
    AdditionalFieldsUtil.move001To035(record);
    // then
    Assert.assertEquals(expectedParsedContent, parsedRecord.getContent());
  }

  @Test
  public void shouldAdd035If001NotEqual003() {
    // given
    String parsedContent = "{\"leader\":\"00086nam  22000611a 4500\",\"fields\":[{\"001\":\"in001\"},{\"003\":\"ybp7406411\"},{\"507\":{\"subfields\":[{\"a\":\"data\"}],\"ind1\":\" \",\"ind2\":\" \"}},{\"500\":{\"subfields\":[{\"a\":\"data\"}],\"ind1\":\" \",\"ind2\":\" \"}}]}";
    String expectedParsedContent = "{\"leader\":\"00120nam  22000731a 4500\",\"fields\":[{\"001\":\"in001\"},{\"035\":{\"subfields\":[{\"a\":\"(ybp7406411)in001\"}],\"ind1\":\" \",\"ind2\":\" \"}},{\"507\":{\"subfields\":[{\"a\":\"data\"}],\"ind1\":\" \",\"ind2\":\" \"}},{\"500\":{\"subfields\":[{\"a\":\"data\"}],\"ind1\":\" \",\"ind2\":\" \"}}]}";
    ParsedRecord parsedRecord = new ParsedRecord().withContent(parsedContent);

    Record record = new Record().withId(UUID.randomUUID().toString())
      .withParsedRecord(parsedRecord)
      .withGeneration(0)
      .withState(Record.State.ACTUAL)
      .withExternalIdsHolder(new ExternalIdsHolder().withInstanceId("001").withInstanceHrid("in001"));
    // when
    AdditionalFieldsUtil.move001To035(record);
    // then
    Assert.assertEquals(expectedParsedContent, parsedRecord.getContent());
  }

  @Test
  public void shouldNotAdd035if001IsNull() {
    // given
    String parsedContent = "{\"leader\":\"00086nam  22000611a 4500\",\"fields\":[{\"003\":\"in001\"},{\"507\":{\"subfields\":[{\"a\":\"data\"}],\"ind1\":\" \",\"ind2\":\" \"}},{\"500\":{\"subfields\":[{\"a\":\"data\"}],\"ind1\":\" \",\"ind2\":\" \"}}]}";
    String expectedParsedContent = "{\"leader\":\"00068nam  22000491a 4500\",\"fields\":[{\"507\":{\"subfields\":[{\"a\":\"data\"}],\"ind1\":\" \",\"ind2\":\" \"}},{\"500\":{\"subfields\":[{\"a\":\"data\"}],\"ind1\":\" \",\"ind2\":\" \"}}]}";
    ParsedRecord parsedRecord = new ParsedRecord().withContent(parsedContent);

    Record record = new Record().withId(UUID.randomUUID().toString())
      .withParsedRecord(parsedRecord)
      .withGeneration(0)
      .withState(Record.State.ACTUAL)
      .withExternalIdsHolder(new ExternalIdsHolder().withInstanceId("001").withInstanceHrid("in001"));
    // when
    AdditionalFieldsUtil.move001To035(record);
    // then
    Assert.assertEquals(expectedParsedContent, parsedRecord.getContent());
  }

  @Test
  public void shouldFill001IfHrIdNotEmpty() {
    // given
    String parsedContent = "{\"leader\":\"00118nam  22000731a 4500\",\"fields\":[{\"001\":\"in001\"},{\"003\":\"qwerty\"},{\"035\":{\"subfields\":[{\"a\":\"(NhFolYBP)in001\"}],\"ind1\":\" \",\"ind2\":\" \"}},{\"507\":{\"subfields\":[{\"a\":\"data\"}],\"ind1\":\" \",\"ind2\":\" \"}},{\"500\":{\"subfields\":[{\"a\":\"data\"}],\"ind1\":\" \",\"ind2\":\" \"}}]}";
    String expectedParsedContent = "{\"leader\":\"00137nam  22000851a 4500\",\"fields\":[{\"001\":\"in001\"},{\"003\":\"qwerty\"},{\"035\":{\"subfields\":[{\"a\":\"(NhFolYBP)in001\"}],\"ind1\":\" \",\"ind2\":\" \"}},{\"507\":{\"subfields\":[{\"a\":\"data\"}],\"ind1\":\" \",\"ind2\":\" \"}},{\"500\":{\"subfields\":[{\"a\":\"data\"}],\"ind1\":\" \",\"ind2\":\" \"}}]}";
    ParsedRecord parsedRecord = new ParsedRecord();
    parsedRecord.setContent(parsedContent);

    Record record = new Record().withId(UUID.randomUUID().toString())
      .withParsedRecord(parsedRecord)
      .withGeneration(0)
      .withState(Record.State.ACTUAL)
      .withExternalIdsHolder(new ExternalIdsHolder().withInstanceId("001").withInstanceHrid("in001"));
    // when
    AdditionalFieldsUtil.fill001FieldInMarcRecord(record, "in001");
    // then
    Assert.assertEquals(expectedParsedContent, parsedRecord.getContent());
  }

  @Test
  public void shouldNotFill001IfHrIdIsNull() {
    // given
    String parsedContent = "{\"leader\":\"00118nam  22000731a 4500\",\"fields\":[{\"001\":\"in001\"},{\"003\":\"qwerty\"},{\"035\":{\"subfields\":[{\"a\":\"(NhFolYBP)in001\"}],\"ind1\":\" \",\"ind2\":\" \"}},{\"507\":{\"subfields\":[{\"a\":\"data\"}],\"ind1\":\" \",\"ind2\":\" \"}},{\"500\":{\"subfields\":[{\"a\":\"data\"}],\"ind1\":\" \",\"ind2\":\" \"}}]}";
    String expectedParsedContent = "{\"leader\":\"00119nam  22000731a 4500\",\"fields\":[{\"003\":\"qwerty\"},{\"035\":{\"subfields\":[{\"a\":\"(NhFolYBP)in001\"}],\"ind1\":\" \",\"ind2\":\" \"}},{\"507\":{\"subfields\":[{\"a\":\"data\"}],\"ind1\":\" \",\"ind2\":\" \"}},{\"500\":{\"subfields\":[{\"a\":\"data\"}],\"ind1\":\" \",\"ind2\":\" \"}}]}";
    ParsedRecord parsedRecord = new ParsedRecord().withContent(parsedContent);

    Record record = new Record().withId(UUID.randomUUID().toString())
      .withParsedRecord(parsedRecord)
      .withGeneration(0)
      .withState(Record.State.ACTUAL)
      .withExternalIdsHolder(new ExternalIdsHolder().withInstanceId("001").withInstanceHrid("in001"));
    // when
    AdditionalFieldsUtil.fill001FieldInMarcRecord(record, null);
    // then
    Assert.assertEquals(expectedParsedContent, parsedRecord.getContent());
  }

  @Test
  public void shouldNotUpdate005Field() {
    String parsedContent = "{\"leader\":\"00115nam  22000731a 4500\",\"fields\":[{\"001\":\"in001\"},{\"003\":\"qwerty\"},{\"005\":\"20141107001016.0\"},{\"507\":{\"subfields\":[{\"a\":\"data\"}],\"ind1\":\" \",\"ind2\":\" \"}},{\"500\":{\"subfields\":[{\"a\":\"data\"}],\"ind1\":\" \",\"ind2\":\" \"}}]}";
    Record record = new Record().withId(UUID.randomUUID().toString())
      .withParsedRecord(new ParsedRecord().withContent(parsedContent))
      .withGeneration(0)
      .withState(Record.State.ACTUAL)
      .withExternalIdsHolder(new ExternalIdsHolder().withInstanceId("001").withInstanceHrid("in001"));

    AdditionalFieldsUtil.updateLatestTransactionDate(record,
      new MappingParameters().withMarcFieldProtectionSettings(List.of(new MarcFieldProtectionSetting().withField("*").withData("*"))));

    String actualDate = getValueFromControlledField(record, TAG_005);
    assertNotNull(actualDate);
    assertEquals("20141107001016.0", actualDate);
  }

  @Test
  public void shouldUpdate005Field() {
    String parsedContent = "{\"leader\":\"00115nam  22000731a 4500\",\"fields\":[{\"001\":\"in001\"},{\"003\":\"qwerty\"},{\"005\":\"20141107001016.0\"},{\"507\":{\"subfields\":[{\"a\":\"data\"}],\"ind1\":\" \",\"ind2\":\" \"}},{\"500\":{\"subfields\":[{\"a\":\"data\"}],\"ind1\":\" \",\"ind2\":\" \"}}]}";
    Record record = new Record().withId(UUID.randomUUID().toString())
      .withParsedRecord(new ParsedRecord().withContent(parsedContent))
      .withGeneration(0)
      .withState(Record.State.ACTUAL)
      .withExternalIdsHolder(new ExternalIdsHolder().withInstanceId("001").withInstanceHrid("in001"));

    String expectedDate = dateTime005Formatter.format(ZonedDateTime.ofInstant(Instant.now(), ZoneId.systemDefault()));

    AdditionalFieldsUtil.updateLatestTransactionDate(record, new MappingParameters());

    String actualDate = getValueFromControlledField(record, TAG_005);
    assertNotNull(actualDate);
    assertEquals(expectedDate.substring(0, 10), actualDate.substring(0, 10));
  }

  @Test
  public void shouldReturnFields() {
    // given
    var id = UUID.randomUUID().toString();
    var hrId = "in001";
    var parsedContent = """
      {
          "fields": [
              {"001": "%s"},
              {"999": {
                  "ind1": "f",
                  "ind2": "f",
                  "subfields": [
                      {"i": "%s"}
                  ]
              }}
          ]
      }
      """.formatted(hrId, id);
    var record = new Record().withParsedRecord(new ParsedRecord().withContent(parsedContent));

    // when
    var parsedHrId = AdditionalFieldsUtil.getValue(record, TAG_001, ' ');
    var parsedId = AdditionalFieldsUtil.getValue(record, TAG_999, 'i');
    var missingField = AdditionalFieldsUtil.getValue(record, TAG_005, ' ');

    // then
    assertTrue(parsedHrId.isPresent());
    assertTrue(parsedId.isPresent());
    assertTrue(missingField.isEmpty());

    assertEquals(hrId, parsedHrId.get());
    assertEquals(id, parsedId.get());
  }

  @Test
  public void shouldExistControlField() throws IOException {
    String parsedRecordContent = TestUtil.readFileFromPath(PARSED_MARC_RECORD_PATH);
    ParsedRecord parsedRecord = new ParsedRecord().withContent(parsedRecordContent);
    Record record = new Record().withId(UUID.randomUUID().toString()).withParsedRecord(parsedRecord);
    Assert.assertTrue(isFieldExist(record, "001", ' ', "ybp7406411"));
  }

  @Test
  public void caching() throws IOException {
    // given
    String parsedRecordContent = TestUtil.readFileFromPath(PARSED_MARC_RECORD_PATH);
    ParsedRecord parsedRecord = new ParsedRecord().withContent(parsedRecordContent);
    Record record = new Record().withId(UUID.randomUUID().toString()).withParsedRecord(parsedRecord);
    String instanceId = UUID.randomUUID().toString();

    CacheStats initialCacheStats = getCacheStats();

    // record with null parsed content
    Assert.assertFalse(
      isFieldExist(new Record().withId(UUID.randomUUID().toString()), "035", 'a', instanceId));
    CacheStats cacheStats = getCacheStats().minus(initialCacheStats);
    Assert.assertEquals(0, cacheStats.hitCount());
    Assert.assertEquals(0, cacheStats.missCount());
    Assert.assertEquals(0, cacheStats.loadCount());
    // record with empty parsed content
    Assert.assertFalse(
      isFieldExist(new Record().withId(UUID.randomUUID().toString())
        .withParsedRecord(new ParsedRecord().withContent("")), "035", 'a', instanceId));
    cacheStats = getCacheStats().minus(initialCacheStats);
    Assert.assertEquals(0, cacheStats.requestCount());
    Assert.assertEquals(0, cacheStats.hitCount());
    Assert.assertEquals(0, cacheStats.missCount());
    Assert.assertEquals(0, cacheStats.loadCount());
    // record with bad parsed content
    Assert.assertFalse(
      isFieldExist(new Record().withId(UUID.randomUUID().toString())
        .withParsedRecord(new ParsedRecord().withContent("test")), "035", 'a', instanceId));
    cacheStats = getCacheStats().minus(initialCacheStats);
    Assert.assertEquals(1, cacheStats.requestCount());
    Assert.assertEquals(0, cacheStats.hitCount());
    Assert.assertEquals(1, cacheStats.missCount());
    Assert.assertEquals(1, cacheStats.loadCount());
    // does field exists?
    Assert.assertFalse(isFieldExist(record, "035", 'a', instanceId));
    cacheStats = getCacheStats().minus(initialCacheStats);
    Assert.assertEquals(2, cacheStats.requestCount());
    Assert.assertEquals(0, cacheStats.hitCount());
    Assert.assertEquals(2, cacheStats.missCount());
    Assert.assertEquals(2, cacheStats.loadCount());
    // update field
    addDataFieldToMarcRecord(record, "035", ' ', ' ', 'a', instanceId);
    cacheStats = getCacheStats().minus(initialCacheStats);
    Assert.assertEquals(3, cacheStats.requestCount());
    Assert.assertEquals(1, cacheStats.hitCount());
    Assert.assertEquals(2, cacheStats.missCount());
    Assert.assertEquals(2, cacheStats.loadCount());
    // verify that field exists
    Assert.assertTrue(isFieldExist(record, "035", 'a', instanceId));
    cacheStats = getCacheStats().minus(initialCacheStats);
    Assert.assertEquals(4, cacheStats.requestCount());
    Assert.assertEquals(2, cacheStats.hitCount());
    Assert.assertEquals(2, cacheStats.missCount());
    Assert.assertEquals(2, cacheStats.loadCount());
    // verify that field exists again
    Assert.assertTrue(isFieldExist(record, "035", 'a', instanceId));
    cacheStats = getCacheStats().minus(initialCacheStats);
    Assert.assertEquals(5, cacheStats.requestCount());
    Assert.assertEquals(3, cacheStats.hitCount());
    Assert.assertEquals(2, cacheStats.missCount());
    Assert.assertEquals(2, cacheStats.loadCount());
    // remove the field
    Assert.assertTrue(removeField(record, "035"));
    cacheStats = getCacheStats().minus(initialCacheStats);
    Assert.assertEquals(6, cacheStats.requestCount());
    Assert.assertEquals(4, cacheStats.hitCount());
    Assert.assertEquals(2, cacheStats.missCount());
    Assert.assertEquals(2, cacheStats.loadCount());
    // get value from controlled field
    Assert.assertEquals("ybp7406411", getValueFromControlledField(record, "001"));
    cacheStats = getCacheStats().minus(initialCacheStats);
    Assert.assertEquals(7, cacheStats.requestCount());
    Assert.assertEquals(5, cacheStats.hitCount());
    Assert.assertEquals(2, cacheStats.missCount());
    Assert.assertEquals(2, cacheStats.loadCount());
    // add controlled field to marc record
    Assert.assertTrue(addControlledFieldToMarcRecord(record, "002", "test",
      AdditionalFieldsUtil::addControlledFieldToMarcRecord));
    cacheStats = getCacheStats().minus(initialCacheStats);
    Assert.assertEquals(8, cacheStats.requestCount());
    Assert.assertEquals(6, cacheStats.hitCount());
    Assert.assertEquals(2, cacheStats.missCount());
    Assert.assertEquals(2, cacheStats.loadCount());
    // add field to marc record
    Assert.assertTrue(addFieldToMarcRecord(record, TAG_999, 'i', instanceId));
    cacheStats = getCacheStats().minus(initialCacheStats);
    Assert.assertEquals(9, cacheStats.requestCount());
    Assert.assertEquals(7, cacheStats.hitCount());
    Assert.assertEquals(2, cacheStats.missCount());
    Assert.assertEquals(2, cacheStats.loadCount());
  }

  @Test
  public void shouldRemove003ifHRIDManipulationAlreadyDone() {
    // given
    String parsedContent = "{\"leader\":\"00115nam  22000731a 4500\",\"fields\":[{\"001\":\"in001\"},{\"003\":\"qwerty\"},{\"507\":{\"subfields\":[{\"a\":\"data\"}],\"ind1\":\" \",\"ind2\":\" \"}},{\"500\":{\"subfields\":[{\"a\":\"data\"}],\"ind1\":\" \",\"ind2\":\" \"}}]}";
    String expectedParsedContent = "{\"leader\":\"00086nam  22000611a 4500\",\"fields\":[{\"001\":\"in001\"},{\"507\":{\"subfields\":[{\"a\":\"data\"}],\"ind1\":\" \",\"ind2\":\" \"}},{\"500\":{\"subfields\":[{\"a\":\"data\"}],\"ind1\":\" \",\"ind2\":\" \"}}]}";
    ParsedRecord parsedRecord = new ParsedRecord();
    parsedRecord.setContent(parsedContent);

    Record record = new Record().withId(UUID.randomUUID().toString())
      .withParsedRecord(parsedRecord)
      .withGeneration(0)
      .withState(Record.State.ACTUAL)
      .withExternalIdsHolder(new ExternalIdsHolder().withInstanceId("001").withInstanceHrid("in001"));

    JsonObject jsonObject = new JsonObject("{\"hrid\":\"in001\"}");
    Pair<Record, JsonObject> pair = Pair.of(record, jsonObject);
    // when
    AdditionalFieldsUtil.fillHrIdFieldInMarcRecord(pair);
    // then
    Assert.assertEquals(expectedParsedContent, parsedRecord.getContent());
  }

  @Test
  public void shouldNotProcessRecord() {
    // given
    String parsedContent = "{\"leader\":\"00115nam  22000731a 4500\",\"fields\":[{\"001\":\"in001\"},{\"003\":\"qwerty\"},{\"507\":{\"subfields\":[{\"a\":\"data\"}],\"ind1\":\" \",\"ind2\":\" \"}},{\"500\":{\"subfields\":[{\"a\":\"data\"}],\"ind1\":\" \",\"ind2\":\" \"}}]}";
    String expectedParsedContent = "{\"leader\":\"00086nam  22000611a 4500\",\"fields\":[{\"001\":\"in001\"},{\"507\":{\"subfields\":[{\"a\":\"data\"}],\"ind1\":\" \",\"ind2\":\" \"}},{\"500\":{\"subfields\":[{\"a\":\"data\"}],\"ind1\":\" \",\"ind2\":\" \"}}]}";
    ParsedRecord parsedRecord = new ParsedRecord();
    parsedRecord.setContent(parsedContent);

    Record record = new Record().withId(UUID.randomUUID().toString())
      .withParsedRecord(parsedRecord)
      .withGeneration(0)
      .withState(Record.State.ACTUAL)
      .withExternalIdsHolder(new ExternalIdsHolder().withInstanceId("001").withInstanceHrid("in001"));

    JsonObject jsonObject = new JsonObject("{\"hrid\":\"in003\"}");
    Pair<Record, JsonObject> pair = Pair.of(record, jsonObject);
    // when
    AdditionalFieldsUtil.fillHrIdFieldInMarcRecord(pair);
    // then
    Assert.assertNotEquals(expectedParsedContent, parsedRecord.getContent());
  }

  @Test
  public void shouldNotAdd035AndAdd001FieldsIf001And003FieldsNotExists() {
    // given
    String parsedContent = "{\"leader\":\"00116nam  22000732a 4900\",\"fields\":[{\"003\":\"in001\"},{\"507\":{\"subfields\":[{\"a\":\"data\"}],\"ind1\":\" \",\"ind2\":\" \"}},{\"500\":{\"subfields\":[{\"a\":\"data\"}],\"ind1\":\" \",\"ind2\":\" \"}}]}";
    String expectedParsedContent = "{\"leader\":\"00086nam  22000612a 4900\",\"fields\":[{\"001\":\"in001\"},{\"507\":{\"subfields\":[{\"a\":\"data\"}],\"ind1\":\" \",\"ind2\":\" \"}},{\"500\":{\"subfields\":[{\"a\":\"data\"}],\"ind1\":\" \",\"ind2\":\" \"}}]}";
    ParsedRecord parsedRecord = new ParsedRecord();
    parsedRecord.setContent(parsedContent);

    Record record = new Record().withId(UUID.randomUUID().toString())
      .withParsedRecord(parsedRecord)
      .withGeneration(0)
      .withState(Record.State.ACTUAL)
      .withExternalIdsHolder(new ExternalIdsHolder().withInstanceId("001").withInstanceHrid("in001"));

    JsonObject jsonObject = new JsonObject("{\"hrid\":\"in001\"}");
    Pair<Record, JsonObject> pair = Pair.of(record, jsonObject);
    // when
    AdditionalFieldsUtil.fillHrIdFieldInMarcRecord(pair);
    // then
    Assert.assertEquals(expectedParsedContent, parsedRecord.getContent());
  }

  @Test
  public void shouldRemove035() {
    // given
    String parsedContent = "{\"leader\":\"00120nam  22000731a 4500\",\"fields\":[{\"001\":\"in001\"},{\"035\":{\"subfields\":[{\"a\":\"(ybp7406411)in001\"}],\"ind1\":\" \",\"ind2\":\" \"}},{\"507\":{\"subfields\":[{\"a\":\"data\"}],\"ind1\":\" \",\"ind2\":\" \"}},{\"500\":{\"subfields\":[{\"a\":\"data\"}],\"ind1\":\" \",\"ind2\":\" \"}}]}";
    String expectedParsedContent = "{\"leader\":\"00086nam  22000611a 4500\",\"fields\":[{\"001\":\"in001\"},{\"507\":{\"subfields\":[{\"a\":\"data\"}],\"ind1\":\" \",\"ind2\":\" \"}},{\"500\":{\"subfields\":[{\"a\":\"data\"}],\"ind1\":\" \",\"ind2\":\" \"}}]}";
    ParsedRecord parsedRecord = new ParsedRecord();
    parsedRecord.setContent(parsedContent);

    Record record = new Record().withId(UUID.randomUUID().toString())
      .withParsedRecord(parsedRecord)
      .withRecordType(Record.RecordType.MARC_BIB)
      .withGeneration(0)
      .withState(Record.State.ACTUAL)
      .withExternalIdsHolder(new ExternalIdsHolder().withInstanceId("001").withInstanceHrid("in001"));

    // when
    AdditionalFieldsUtil.remove035FieldWhenRecordContainsHrId(record);
    // then
    Assert.assertEquals(expectedParsedContent, parsedRecord.getContent());
  }

  @Test
  public void shouldReorderMarcRecordFields() throws IOException, MarcException {
    var reorderedRecordContent = readFileFromPath(PARSED_RECORD);
    var sourceRecordContent = readFileFromPath(REORDERED_PARSED_RECORD);
    var reorderingResultRecord = readFileFromPath(REORDERING_RESULT_RECORD);

    var resultContent = AdditionalFieldsUtil.reorderMarcRecordFields(sourceRecordContent, reorderedRecordContent);

    assertNotNull(resultContent);
    assertEquals(formatContent(resultContent), formatContent(reorderingResultRecord));
  }

  private static String readFileFromPath(String path) throws IOException {
    return new String(FileUtils.readFileToByteArray(new File(path)));
  }

  private String formatContent(String content) {
    return content.replaceAll("\\s", "");
  }
}
