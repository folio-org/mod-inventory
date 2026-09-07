package org.folio.inventory.dataimport.util;

import static org.assertj.core.api.Assertions.assertThat;
import static org.folio.dataimport.util.marc.MarcConstants.FIELD_001;
import static org.folio.dataimport.util.marc.MarcConstants.FIELD_005;
import static org.folio.dataimport.util.marc.MarcConstants.FIELD_999;
import static org.folio.dataimport.util.marc.MarcConstants.INDICATOR_F;
import static org.folio.dataimport.util.marc.MarcConstants.SUBFIELD_I;
import static org.folio.inventory.dataimport.util.AdditionalFieldsUtil.DATE_TIME_005_FORMATTER;
import static org.folio.inventory.dataimport.util.AdditionalFieldsUtil.INVALID_DATA_FIELD_MSG;
import static org.folio.inventory.dataimport.util.AdditionalFieldsUtil.addControlledFieldToMarcRecord;
import static org.folio.inventory.dataimport.util.AdditionalFieldsUtil.addDataFieldToMarcRecord;
import static org.folio.inventory.dataimport.util.AdditionalFieldsUtil.addFieldToMarcRecord;
import static org.folio.inventory.dataimport.util.AdditionalFieldsUtil.getCacheStats;
import static org.folio.inventory.dataimport.util.AdditionalFieldsUtil.getValueFromControlledField;
import static org.folio.inventory.dataimport.util.AdditionalFieldsUtil.getValueFromDataField;
import static org.folio.inventory.dataimport.util.AdditionalFieldsUtil.isFieldExist;
import static org.folio.inventory.dataimport.util.AdditionalFieldsUtil.removeField;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import java.io.File;
import java.io.IOException;
import java.time.Clock;
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
import org.folio.dataimport.util.marc.MarcContentCacheStats;
import org.folio.dataimport.util.marc.MarcContentException;
import org.folio.dataimport.util.marc.MarcRecordEditor;
import org.folio.inventory.domain.instances.Instance;
import org.folio.processing.exceptions.EventProcessingException;
import org.folio.processing.mapping.defaultmapper.processor.parameters.MappingParameters;
import org.folio.rest.jaxrs.model.ExternalIdsHolder;
import org.folio.rest.jaxrs.model.MarcFieldProtectionSetting;
import org.folio.rest.jaxrs.model.ParsedRecord;
import org.folio.rest.jaxrs.model.Record;
import org.hamcrest.MatcherAssert;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.marc4j.MarcException;
import support.LogCaptureTestAppender;
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
    boolean addedSourceRecordId = addFieldToMarcRecord(marcRecord, FIELD_999, 's', recordId);
    boolean addedInstanceId = addFieldToMarcRecord(marcRecord, FIELD_999, 'i', instanceId);
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
      if (targetField.containsKey(FIELD_999)) {
        JsonArray subfields = targetField.getJsonObject(FIELD_999).getJsonArray("subfields");
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
    boolean added = addFieldToMarcRecord(marcRecord, FIELD_999, 'i', instanceId);
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
    boolean added = addFieldToMarcRecord(marcRecord, FIELD_999, 'i', instanceId);
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
    boolean added = addFieldToMarcRecord(marcRecord, FIELD_999, 'i', instanceId);
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
    boolean added = addFieldToMarcRecord(marcRecord, FIELD_999, 'i', instanceId);
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
    boolean added = addFieldToMarcRecord(marcRecord, FIELD_999, 'i', instanceId);
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
    boolean added = addControlledFieldToMarcRecord(marcRecord, "002", null, false);
    assertFalse(added);
  }

  @Test
  void shouldAddControlledFieldToMarcRecord() {
    String recordId = UUID.randomUUID().toString();
    String parsedRecordContent = TestUtil.readFileFromPath(PARSED_MARC_RECORD_PATH);
    ParsedRecord parsedRecord = new ParsedRecord().withContent(parsedRecordContent);
    String leader = new JsonObject(parsedRecordContent).getString("leader");
    var marcRecord = new Record().withId(recordId).withParsedRecord(parsedRecord);
    boolean added = addControlledFieldToMarcRecord(marcRecord, "002", "test", false);
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
    boolean added = addControlledFieldToMarcRecord(marcRecord, "003", "test", true);
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
  void isFieldsFillingNeededReturnsFalse_whenInstanceIdIsNull() {
    // given: a null instance id used to NPE inside isValidIdAndHrid's raw id.equals(externalId) call - it now
    // goes through Objects.equals, so a null id/hrid deterministically fails the match instead of throwing.
    String instanceId = UUID.randomUUID().toString();
    String instanceHrId = UUID.randomUUID().toString();
    Record srcRecord = new Record().withExternalIdsHolder(
      new ExternalIdsHolder().withInstanceId(instanceId).withInstanceHrid(instanceHrId));
    Instance instance = new Instance(null, 0, instanceHrId, "", "", "");

    assertFalse(AdditionalFieldsUtil.isFieldsFillingNeeded(srcRecord, instance));
  }

  @Test
  void isFieldsFillingNeededReturnsFalse_whenExternalIdsHolderIsNull() {
    // given: srcRecord with no ExternalIdsHolder at all - previously an unguarded
    // srcRecord.getExternalIdsHolder().getInstanceId() would NPE straight out of this method, with no
    // catch anywhere on this call path (see AbstractInstanceEventHandler.setExternalIds).
    Record srcRecord = new Record();
    Instance instance = new Instance(UUID.randomUUID().toString(), 0, UUID.randomUUID().toString(), "", "", "");

    assertFalse(AdditionalFieldsUtil.isFieldsFillingNeeded(srcRecord, instance));
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
  @SuppressWarnings("checkstyle:MethodLength")
  void shouldNotSortExistingFieldsWhenAddFieldToToMarcRecord() {
    // given
    String instanceId = "12345";
    String parsedContent = """
      {
        "leader": "00115nam  22000731a 4500",
        "fields": [
          {
            "001": "ybp7406411"
          },
          {
            "507": {
              "subfields": [
                {
                  "a": "data"
                }
              ],
              "ind1": " ",
              "ind2": " "
            }
          },
          {
            "500": {
              "subfields": [
                {
                  "a": "data"
                }
              ],
              "ind1": " ",
              "ind2": " "
            }
          }
        ]
      }
      """;
    String expectedParsedContent = """
      {
        "leader": "00113nam  22000731a 4500",
        "fields": [
          {
            "001": "ybp7406411"
          },
          {
            "507": {
              "subfields": [
                {
                  "a": "data"
                }
              ],
              "ind1": " ",
              "ind2": " "
            }
          },
          {
            "500": {
              "subfields": [
                {
                  "a": "data"
                }
              ],
              "ind1": " ",
              "ind2": " "
            }
          },
          {
            "999": {
              "subfields": [
                {
                  "i": "12345"
                }
              ],
              "ind1": "f",
              "ind2": "f"
            }
          }
        ]
      }
      """;
    ParsedRecord parsedRecord = new ParsedRecord().withContent(parsedContent);
    var marcRecord = new Record().withId(UUID.randomUUID().toString()).withParsedRecord(parsedRecord);
    // when
    boolean added = addDataFieldToMarcRecord(marcRecord, "999", 'f', 'f', 'i', instanceId);
    // then
    assertTrue(added);
    assertEquals(expectedParsedContent, parsedRecord.getContent());
  }

  @Test
  @SuppressWarnings("checkstyle:MethodLength")
  void shouldNotAdd035FieldIf001And003FieldsNotExists() {
    // given
    String parsedContent = """
      {
        "leader": "00115nam  22000731a 4500",
        "fields": [
          {
            "003": "in001"
          },
          {
            "507": {
              "subfields": [
                {
                  "a": "data"
                }
              ],
              "ind1": " ",
              "ind2": " "
            }
          },
          {
            "500": {
              "subfields": [
                {
                  "a": "data"
                }
              ],
              "ind1": " ",
              "ind2": " "
            }
          }
        ]
      }
      """;
    String expectedParsedContent = """
      {
        "leader": "00068nam  22000491a 4500",
        "fields": [
          {
            "507": {
              "subfields": [
                {
                  "a": "data"
                }
              ],
              "ind1": " ",
              "ind2": " "
            }
          },
          {
            "500": {
              "subfields": [
                {
                  "a": "data"
                }
              ],
              "ind1": " ",
              "ind2": " "
            }
          }
        ]
      }
      """;
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
  @SuppressWarnings("checkstyle:MethodLength")
  void shouldAdd035If001NotEqual003() {
    // given
    String parsedContent = """
      {
        "leader": "00086nam  22000611a 4500",
        "fields": [
          {
            "001": "in001"
          },
          {
            "003": "ybp7406411"
          },
          {
            "507": {
              "subfields": [
                {
                  "a": "data"
                }
              ],
              "ind1": " ",
              "ind2": " "
            }
          },
          {
            "500": {
              "subfields": [
                {
                  "a": "data"
                }
              ],
              "ind1": " ",
              "ind2": " "
            }
          }
        ]
      }
      """;
    String expectedParsedContent = """
      {
        "leader": "00120nam  22000731a 4500",
        "fields": [
          {
            "001": "in001"
          },
          {
            "035": {
              "subfields": [
                {
                  "a": "(ybp7406411)in001"
                }
              ],
              "ind1": " ",
              "ind2": " "
            }
          },
          {
            "507": {
              "subfields": [
                {
                  "a": "data"
                }
              ],
              "ind1": " ",
              "ind2": " "
            }
          },
          {
            "500": {
              "subfields": [
                {
                  "a": "data"
                }
              ],
              "ind1": " ",
              "ind2": " "
            }
          }
        ]
      }
      """;
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
  @SuppressWarnings("checkstyle:MethodLength")
  void shouldRemovePeriodsAndSpacesAfterNormalization() {
    // given
    var parsedContent = """
      {
        "leader": "00120nam  22000731a 4500",
        "fields": [
          {
            "001": "in001"
          },
          {
            "035": {
              "subfields": [
                {
                  "a": "(OCoLC)on. 607TST .001"
                }
              ],
              "ind1": " ",
              "ind2": " "
            }
          },
          {
            "500": {
              "subfields": [
                {
                  "a": "data"
                }
              ],
              "ind1": " ",
              "ind2": " "
            }
          }
        ]
      }
      """;

    var expectedParsedContent = """
      {
        "leader": "00098nam  22000611a 4500",
        "fields": [
          {
            "001": "in001"
          },
          {
            "035": {
              "subfields": [
                {
                  "a": "(OCoLC)607TST001"
                }
              ],
              "ind1": " ",
              "ind2": " "
            }
          },
          {
            "500": {
              "subfields": [
                {
                  "a": "data"
                }
              ],
              "ind1": " ",
              "ind2": " "
            }
          }
        ]
      }
      """;
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
  @SuppressWarnings("checkstyle:MethodLength")
  void shouldPreserveOrderOf035FieldsAfterNormalization() {
    // given
    var parsedContent = """
      {
        "leader": "00198cama 22003611a 4500",
        "fields": [
          {
            "001": "10065352"
          },
          {
            "005": "20220127143948.0"
          },
          {
            "008": "761216s1853mauch0010eng"
          },
          {
            "906": {
              "subfields": [
                {
                  "a": "7"
                },
                {
                  "b": "cbc"
                },
                {
                  "c": "oclcrpl"
                },
                {
                  "d": "u"
                },
                {
                  "e": "ncip"
                },
                {
                  "f": "19"
                },
                {
                  "g": "y-gencatlg"
                }
              ],
              "ind1": "",
              "ind2": ""
            }
          },
          {
            "035": {
              "subfields": [
                {
                  "9": "(DLC)01012052"
                }
              ],
              "ind1": "",
              "ind2": ""
            }
          },
          {
            "010": {
              "subfields": [
                {
                  "a": "01012052"
                }
              ],
              "ind1": "",
              "ind2": ""
            }
          },
          {
            "022": {
              "subfields": [
                {
                  "a": "0022-0469"
                }
              ],
              "ind1": "",
              "ind2": ""
            }
          },
          {
            "030": {
              "subfields": [
                {
                  "a": "0030-0469"
                }
              ],
              "ind1": " ",
              "ind2": " "
            }
          },
          {
            "035": {
              "subfields": [
                {
                  "a": "(OCoLC)on2628488"
                }
              ],
              "ind1": "",
              "ind2": ""
            }
          },
          {
            "035": {
              "subfields": [
                {
                  "a": "(OCoLC)2628488"
                }
              ],
              "ind1": "",
              "ind2": ""
            }
          },
          {
            "035": {
              "subfields": [
                {
                  "a": "(OCoLC)00012345"
                }
              ],
              "ind1": "",
              "ind2": ""
            }
          },
          {
            "040": {
              "subfields": [
                {
                  "a": "DLC"
                },
                {
                  "b": "eng"
                },
                {
                  "c": "O"
                },
                {
                  "d": "O"
                },
                {
                  "d": "DLC"
                }
              ],
              "ind1": "",
              "ind2": ""
            }
          }
        ]
      }
      """;

    var expectedParsedContent = """
      {
        "leader": "00372cama 22001571a 4500",
        "fields": [
          {
            "001": "10065352"
          },
          {
            "005": "20220127143948.0"
          },
          {
            "008": "761216s1853mauch0010eng"
          },
          {
            "906": {
              "subfields": [
                {
                  "a": "7"
                },
                {
                  "b": "cbc"
                },
                {
                  "c": "oclcrpl"
                },
                {
                  "d": "u"
                },
                {
                  "e": "ncip"
                },
                {
                  "f": "19"
                },
                {
                  "g": "y-gencatlg"
                }
              ],
              "ind1": " ",
              "ind2": " "
            }
          },
          {
            "035": {
              "subfields": [
                {
                  "9": "(DLC)01012052"
                }
              ],
              "ind1": " ",
              "ind2": " "
            }
          },
          {
            "010": {
              "subfields": [
                {
                  "a": "01012052"
                }
              ],
              "ind1": " ",
              "ind2": " "
            }
          },
          {
            "022": {
              "subfields": [
                {
                  "a": "0022-0469"
                }
              ],
              "ind1": " ",
              "ind2": " "
            }
          },
          {
            "030": {
              "subfields": [
                {
                  "a": "0030-0469"
                }
              ],
              "ind1": " ",
              "ind2": " "
            }
          },
          {
            "035": {
              "subfields": [
                {
                  "a": "(OCoLC)2628488"
                }
              ],
              "ind1": " ",
              "ind2": " "
            }
          },
          {
            "035": {
              "subfields": [
                {
                  "a": "(OCoLC)12345"
                }
              ],
              "ind1": " ",
              "ind2": " "
            }
          },
          {
            "040": {
              "subfields": [
                {
                  "a": "DLC"
                },
                {
                  "b": "eng"
                },
                {
                  "c": "O"
                },
                {
                  "d": "O"
                },
                {
                  "d": "DLC"
                }
              ],
              "ind1": " ",
              "ind2": " "
            }
          }
        ]
      }
      """;

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
  @SuppressWarnings("checkstyle:MethodLength")
  void shouldNotAdd035if001IsNull() {
    // given
    String parsedContent = """
      {
        "leader": "00086nam  22000611a 4500",
        "fields": [
          {
            "003": "in001"
          },
          {
            "507": {
              "subfields": [
                {
                  "a": "data"
                }
              ],
              "ind1": " ",
              "ind2": " "
            }
          },
          {
            "500": {
              "subfields": [
                {
                  "a": "data"
                }
              ],
              "ind1": " ",
              "ind2": " "
            }
          }
        ]
      }
      """;
    String expectedParsedContent = """
      {
        "leader": "00068nam  22000491a 4500",
        "fields": [
          {
            "507": {
              "subfields": [
                {
                  "a": "data"
                }
              ],
              "ind1": " ",
              "ind2": " "
            }
          },
          {
            "500": {
              "subfields": [
                {
                  "a": "data"
                }
              ],
              "ind1": " ",
              "ind2": " "
            }
          }
        ]
      }
      """;
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
  @SuppressWarnings("checkstyle:MethodLength")
  void shouldFill001IfHrIdNotEmpty() {
    // given: 001 already holds the target hrid
    String parsedContent = """
      {
        "leader": "00118nam  22000731a 4500",
        "fields": [
          {
            "001": "in001"
          },
          {
            "003": "qwerty"
          },
          {
            "035": {
              "subfields": [
                {
                  "a": "(NhFolYBP)in001"
                }
              ],
              "ind1": " ",
              "ind2": " "
            }
          },
          {
            "507": {
              "subfields": [
                {
                  "a": "data"
                }
              ],
              "ind1": " ",
              "ind2": " "
            }
          },
          {
            "500": {
              "subfields": [
                {
                  "a": "data"
                }
              ],
              "ind1": " ",
              "ind2": " "
            }
          }
        ]
      }
      """;
    ParsedRecord parsedRecord = new ParsedRecord();
    parsedRecord.setContent(parsedContent);

    var marcRecord = new Record().withId(UUID.randomUUID().toString())
      .withParsedRecord(parsedRecord)
      .withGeneration(0)
      .withState(Record.State.ACTUAL)
      .withExternalIdsHolder(new ExternalIdsHolder().withInstanceId("001").withInstanceHrid("in001"));
    // when
    AdditionalFieldsUtil.fill001FieldInMarcRecord(marcRecord, "in001");
    // then: 001 already equals the target hrid, so filling it again is a no-op
    assertEquals(parsedContent, parsedRecord.getContent());
  }

  @Test
  @SuppressWarnings("checkstyle:MethodLength")
  void shouldNotFill001IfHrIdIsNull() {
    // given
    String parsedContent = """
      {
        "leader": "00118nam  22000731a 4500",
        "fields": [
          {
            "001": "in001"
          },
          {
            "003": "qwerty"
          },
          {
            "035": {
              "subfields": [
                {
                  "a": "(NhFolYBP)in001"
                }
              ],
              "ind1": " ",
              "ind2": " "
            }
          },
          {
            "507": {
              "subfields": [
                {
                  "a": "data"
                }
              ],
              "ind1": " ",
              "ind2": " "
            }
          },
          {
            "500": {
              "subfields": [
                {
                  "a": "data"
                }
              ],
              "ind1": " ",
              "ind2": " "
            }
          }
        ]
      }
      """;
    String expectedParsedContent = """
      {
        "leader": "00119nam  22000731a 4500",
        "fields": [
          {
            "003": "qwerty"
          },
          {
            "035": {
              "subfields": [
                {
                  "a": "(NhFolYBP)in001"
                }
              ],
              "ind1": " ",
              "ind2": " "
            }
          },
          {
            "507": {
              "subfields": [
                {
                  "a": "data"
                }
              ],
              "ind1": " ",
              "ind2": " "
            }
          },
          {
            "500": {
              "subfields": [
                {
                  "a": "data"
                }
              ],
              "ind1": " ",
              "ind2": " "
            }
          }
        ]
      }
      """;
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
  @SuppressWarnings("checkstyle:MethodLength")
  void shouldNotUpdate005Field() {
    String parsedContent = """
      {
        "leader": "00115nam  22000731a 4500",
        "fields": [
          {
            "001": "in001"
          },
          {
            "003": "qwerty"
          },
          {
            "005": "20141107001016.0"
          },
          {
            "507": {
              "subfields": [
                {
                  "a": "data"
                }
              ],
              "ind1": " ",
              "ind2": " "
            }
          },
          {
            "500": {
              "subfields": [
                {
                  "a": "data"
                }
              ],
              "ind1": " ",
              "ind2": " "
            }
          }
        ]
      }
      """;
    var marcRecord = new Record().withId(UUID.randomUUID().toString())
      .withParsedRecord(new ParsedRecord().withContent(parsedContent))
      .withGeneration(0)
      .withState(Record.State.ACTUAL)
      .withExternalIdsHolder(new ExternalIdsHolder().withInstanceId("001").withInstanceHrid("in001"));

    AdditionalFieldsUtil.updateLatestTransactionDate(marcRecord,
      new MappingParameters().withMarcFieldProtectionSettings(
        List.of(new MarcFieldProtectionSetting().withField("*").withData("*"))));

    String actualDate = getValueFromControlledField(marcRecord, FIELD_005);
    assertNotNull(actualDate);
    assertEquals("20141107001016.0", actualDate);
  }

  @Test
  @SuppressWarnings("checkstyle:MethodLength")
  void shouldUpdate005Field() {
    String parsedContent = """
      {
        "leader": "00115nam  22000731a 4500",
        "fields": [
          {
            "001": "in001"
          },
          {
            "003": "qwerty"
          },
          {
            "005": "20141107001016.0"
          },
          {
            "507": {
              "subfields": [
                {
                  "a": "data"
                }
              ],
              "ind1": " ",
              "ind2": " "
            }
          },
          {
            "500": {
              "subfields": [
                {
                  "a": "data"
                }
              ],
              "ind1": " ",
              "ind2": " "
            }
          }
        ]
      }
      """;
    var marcRecord = new Record().withId(UUID.randomUUID().toString())
      .withParsedRecord(new ParsedRecord().withContent(parsedContent))
      .withGeneration(0)
      .withState(Record.State.ACTUAL)
      .withExternalIdsHolder(new ExternalIdsHolder().withInstanceId("001").withInstanceHrid("in001"));

    String expectedDate =
      DATE_TIME_005_FORMATTER.format(ZonedDateTime.ofInstant(Instant.now(), ZoneId.systemDefault()));

    AdditionalFieldsUtil.updateLatestTransactionDate(marcRecord, new MappingParameters());

    String actualDate = getValueFromControlledField(marcRecord, FIELD_005);
    assertNotNull(actualDate);
    assertEquals(expectedDate.substring(0, 10), actualDate.substring(0, 10));
  }

  @Test
  @SuppressWarnings("checkstyle:MethodLength")
  void shouldUpdate005FieldWithExactValue_whenUsingFixedClock() {
    // given: an injectable Clock makes the value written to 005 fully deterministic - no more asserting only
    // that a date "looks close enough" to Instant.now() at assertion time.
    String parsedContent = """
      {
        "leader": "00115nam  22000731a 4500",
        "fields": [
          {
            "001": "in001"
          },
          {
            "003": "qwerty"
          },
          {
            "005": "20141107001016.0"
          },
          {
            "507": {
              "subfields": [
                {
                  "a": "data"
                }
              ],
              "ind1": " ",
              "ind2": " "
            }
          },
          {
            "500": {
              "subfields": [
                {
                  "a": "data"
                }
              ],
              "ind1": " ",
              "ind2": " "
            }
          }
        ]
      }
      """;
    var marcRecord = new Record().withId(UUID.randomUUID().toString())
      .withParsedRecord(new ParsedRecord().withContent(parsedContent))
      .withGeneration(0)
      .withState(Record.State.ACTUAL)
      .withExternalIdsHolder(new ExternalIdsHolder().withInstanceId("001").withInstanceHrid("in001"));
    Clock fixedClock = Clock.fixed(Instant.parse("2024-03-15T10:30:45.123Z"), ZoneId.of("UTC"));

    // when
    AdditionalFieldsUtil.updateLatestTransactionDate(marcRecord, new MappingParameters(), fixedClock);

    // then
    String actualDate = getValueFromControlledField(marcRecord, FIELD_005);
    assertEquals("20240315103045.1", actualDate);
  }

  @Test
  void updateLatestTransactionDateThrowsEventProcessingExceptionWithCause_whenAddingControlledFieldFails() {
    // given: a record with no parsed record at all. isField005NeedToUpdate short-circuits to "needs update"
    // when no field protection settings are configured, regardless of content, so this reaches the write path,
    // which fails immediately on addControlledFieldToMarcRecordOrThrow's null-content guard. Before this fix,
    // the EventProcessingException thrown here carried only a plain message - the real cause was already
    // swallowed and logged by addControlledFieldToMarcRecord several stack frames down.
    var recordWithNoParsedRecord = new Record().withId(UUID.randomUUID().toString());

    // when
    var mappingParameters = new MappingParameters();
    var exception = assertThrows(EventProcessingException.class,
      () -> AdditionalFieldsUtil.updateLatestTransactionDate(recordWithNoParsedRecord, mappingParameters));

    // then
    assertNotNull(exception.getCause());
    assertInstanceOf(MarcContentException.class, exception.getCause());
  }

  @Test
  @SuppressWarnings("checkstyle:MethodLength")
  void recalculateLeaderAndParsedRecordLogsOversizedRecordWarning_whenSerializedContentExceedsMarc21LengthLimit() {
    // given: one field with a huge subfield value pushes the total ISO 2709 record length past marc4j's
    // MARC21 99999-byte ceiling. MarcStreamWriter throws MarcException past that limit; before this fix,
    // recalculateLeaderAndParsedRecord's catch-all logged this identically to any other, unrelated failure.
    // The write-back/oversized-record check now lives in MarcRecordEditor.recalculateAndWriteBack (relocated
    // there as part of the Stage 4c extraction), so the warning is emitted under that class's logger.
    String hugeValue = "a".repeat(150_000);
    String parsedContent = "{\"leader\":\"00000nam a2200000 a 4500\",\"fields\":[{\"001\":\"in001\"},"
                           + "{\"999\":{\"subfields\":[{\"a\":\"" + hugeValue
                           + "\"}],\"ind1\":\" \",\"ind2\":\" \"}}]}";
    var marcRecord = new Record().withId(UUID.randomUUID().toString())
      .withParsedRecord(new ParsedRecord().withContent(parsedContent));
    var appender = LogCaptureTestAppender.attachTo(MarcRecordEditor.class);

    try {
      // when
      boolean added = AdditionalFieldsUtil.addControlledFieldToMarcRecord(marcRecord, "002", "test", false);

      // then: return-value contract is unchanged - still a plain false, same as any other write failure
      assertFalse(added);
      assertTrue(appender.getMessages().stream()
        .anyMatch(msg -> msg.contains("exceeds the MARC21 99999-byte length limit")));
    } finally {
      appender.detach();
    }
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
    var parsedId = getValueFromDataField(marcRecord, FIELD_999, INDICATOR_F, INDICATOR_F, SUBFIELD_I);

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
      () -> getValueFromDataField(marcRecord, FIELD_001, INDICATOR_F, INDICATOR_F, SUBFIELD_I));
    assertEquals(INVALID_DATA_FIELD_MSG.formatted(FIELD_001), exception.getMessage());
  }

  @Test
  void shouldExistControlField() {
    String parsedRecordContent = TestUtil.readFileFromPath(PARSED_MARC_RECORD_PATH);
    ParsedRecord parsedRecord = new ParsedRecord().withContent(parsedRecordContent);
    var marcRecord = new Record().withId(UUID.randomUUID().toString()).withParsedRecord(parsedRecord);
    assertTrue(isFieldExist(marcRecord, "001", ' ', "ybp7406411"));
  }

  @Test
  void isFieldExistReturnsFalse_whenValueIsNull() {
    // given: a null value used to reach value.trim() inside the search loop, NPE, and get swallowed by the
    // method's own catch-all into a misleadingly-logged "false" - now it is an explicit, deliberate early return.
    String parsedRecordContent = TestUtil.readFileFromPath(PARSED_MARC_RECORD_PATH);
    var marcRecord = new Record().withId(UUID.randomUUID().toString())
      .withParsedRecord(new ParsedRecord().withContent(parsedRecordContent));

    assertFalse(isFieldExist(marcRecord, "001", ' ', null));
  }

  @Test
  void getValueFromControlledFieldReturnsNull_whenParsedRecordContentIsNull() {
    // given: computeMarcRecord's own guard NPEs on null content before its try block, and that NPE propagates
    // into getValueFromControlledField's catch, whose logging previously called srcRecord.getId() directly.
    var srcRecord = new Record().withId(UUID.randomUUID().toString())
      .withParsedRecord(new ParsedRecord().withContent(null));

    var result = getValueFromControlledField(srcRecord, FIELD_001);

    assertNull(result);
  }

  @Test
  void addFieldToMarcRecordDoesNotThrow_whenFieldTagIsControlField() {
    // given: "001" is a control field in this fixture (a plain string value, no indicators/subfields).
    // getSingleFieldByIndicators used to cast every VariableField to DataField unconditionally, so this call
    // threw a ClassCastException that addFieldToMarcRecord's catch swallowed into "false".
    String parsedRecordContent = TestUtil.readFileFromPath(PARSED_MARC_RECORD_PATH);
    var marcRecord = new Record().withId(UUID.randomUUID().toString())
      .withParsedRecord(new ParsedRecord().withContent(parsedRecordContent));

    // when: no data field with tag "001" is found (the existing "001" is a control field, filtered out by the
    // instanceof guard), so addFieldToMarcRecord deterministically falls through to adding a new one
    boolean added = assertDoesNotThrow(() -> addFieldToMarcRecord(marcRecord, FIELD_001, 'z', "value"));

    // then
    assertTrue(added);
  }

  @Test
  void mutatingRecordViaWritePathMustNotCorruptAnotherRecordSharingSameContentString() {
    // given: two distinct FOLIO Records whose ParsedRecord content is the exact same String instance,
    // which can legitimately happen when a record is copied/cloned before either copy is mutated.
    String sharedContent = TestUtil.readFileFromPath(PARSED_MARC_RECORD_PATH);
    var recordToMutate = new Record().withId(UUID.randomUUID().toString())
      .withParsedRecord(new ParsedRecord().withContent(sharedContent));
    var untouchedRecord = new Record().withId(UUID.randomUUID().toString())
      .withParsedRecord(new ParsedRecord().withContent(sharedContent));
    String newFieldValue = UUID.randomUUID().toString();

    // when: warm the cache by reading recordToMutate, then mutate it via a write path
    assertFalse(isFieldExist(recordToMutate, "035", 'a', newFieldValue));
    assertTrue(addDataFieldToMarcRecord(recordToMutate, "035", ' ', ' ', 'a', newFieldValue));

    // then: untouchedRecord's content string was never changed, so it must not observe the field
    // that was only added to recordToMutate's underlying marc4j Record instance.
    assertFalse(isFieldExist(untouchedRecord, "035", 'a', newFieldValue));
  }

  @Test
  @SuppressWarnings("checkstyle:MethodLength")
  void caching() {
    // given: content carrying a random marker field so its canonical cache key is guaranteed unique to this
    // test run - the cache is now content-addressed (equals/hashCode, not identity), so reusing the shared
    // fixture content verbatim could collide with an entry already warmed by another test in this class.
    JsonObject contentWithUniqueMarker = new JsonObject(TestUtil.readFileFromPath(PARSED_MARC_RECORD_PATH));
    contentWithUniqueMarker.getJsonArray("fields").add(new JsonObject()
      .put("901", new JsonObject()
        .put("subfields", new JsonArray().add(new JsonObject().put("z", UUID.randomUUID().toString())))
        .put("ind1", " ")
        .put("ind2", " ")));
    String parsedRecordContent = contentWithUniqueMarker.encode();
    ParsedRecord parsedRecord = new ParsedRecord().withContent(parsedRecordContent);
    final var marcRecord = new Record().withId(UUID.randomUUID().toString()).withParsedRecord(parsedRecord);
    String instanceId = UUID.randomUUID().toString();

    MarcContentCacheStats initialCacheStats = getCacheStats();

    // record with null parsed content
    assertFalse(
      isFieldExist(new Record().withId(UUID.randomUUID().toString()), "035", 'a', instanceId));
    MarcContentCacheStats cacheStats = getCacheStats().minus(initialCacheStats);
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
    // record with bad (non-JSON) parsed content: canonicalization now fails before the cache is ever
    // consulted (normalizeContent parses through JsonObject up front), so - unlike the old passthrough-key
    // design - this no longer registers as a cache request/miss/load at all.
    assertFalse(
      isFieldExist(new Record().withId(UUID.randomUUID().toString())
        .withParsedRecord(new ParsedRecord().withContent("test")), "035", 'a', instanceId));
    cacheStats = getCacheStats().minus(initialCacheStats);
    assertEquals(0, cacheStats.requestCount());
    assertEquals(0, cacheStats.hitCount());
    assertEquals(0, cacheStats.missCount());
    assertEquals(0, cacheStats.loadCount());
    // does field exists?
    assertFalse(isFieldExist(marcRecord, "035", 'a', instanceId));
    cacheStats = getCacheStats().minus(initialCacheStats);
    assertEquals(1, cacheStats.requestCount());
    assertEquals(0, cacheStats.hitCount());
    assertEquals(1, cacheStats.missCount());
    assertEquals(1, cacheStats.loadCount());
    // update field: this record is re-read from cache (a genuine equals()-based hit, not an identity fluke),
    // mutated, and re-keyed under its new content
    final var contentBeforeUpdate = parsedRecordContent;
    addDataFieldToMarcRecord(marcRecord, "035", ' ', ' ', 'a', instanceId);
    cacheStats = getCacheStats().minus(initialCacheStats);
    assertEquals(2, cacheStats.requestCount());
    assertEquals(1, cacheStats.hitCount());
    assertEquals(1, cacheStats.missCount());
    assertEquals(1, cacheStats.loadCount());
    // verify that field exists
    assertTrue(isFieldExist(marcRecord, "035", 'a', instanceId));
    cacheStats = getCacheStats().minus(initialCacheStats);
    assertEquals(3, cacheStats.requestCount());
    assertEquals(2, cacheStats.hitCount());
    assertEquals(1, cacheStats.missCount());
    assertEquals(1, cacheStats.loadCount());
    // verify that field exists again
    assertTrue(isFieldExist(marcRecord, "035", 'a', instanceId));
    cacheStats = getCacheStats().minus(initialCacheStats);
    assertEquals(4, cacheStats.requestCount());
    assertEquals(3, cacheStats.hitCount());
    assertEquals(1, cacheStats.missCount());
    assertEquals(1, cacheStats.loadCount());
    // remove the field
    assertTrue(removeField(marcRecord, "035"));
    cacheStats = getCacheStats().minus(initialCacheStats);
    assertEquals(5, cacheStats.requestCount());
    assertEquals(4, cacheStats.hitCount());
    assertEquals(1, cacheStats.missCount());
    assertEquals(1, cacheStats.loadCount());
    // get value from controlled field
    assertEquals("ybp7406411", getValueFromControlledField(marcRecord, "001"));
    cacheStats = getCacheStats().minus(initialCacheStats);
    assertEquals(6, cacheStats.requestCount());
    assertEquals(5, cacheStats.hitCount());
    assertEquals(1, cacheStats.missCount());
    assertEquals(1, cacheStats.loadCount());
    // add controlled field to marc record
    assertTrue(addControlledFieldToMarcRecord(marcRecord, "002", "test", false));
    cacheStats = getCacheStats().minus(initialCacheStats);
    assertEquals(7, cacheStats.requestCount());
    assertEquals(6, cacheStats.hitCount());
    assertEquals(1, cacheStats.missCount());
    assertEquals(1, cacheStats.loadCount());
    // add field to marc record
    assertTrue(addFieldToMarcRecord(marcRecord, FIELD_999, 'i', instanceId));
    cacheStats = getCacheStats().minus(initialCacheStats);
    assertEquals(8, cacheStats.requestCount());
    assertEquals(7, cacheStats.hitCount());
    assertEquals(1, cacheStats.missCount());
    assertEquals(1, cacheStats.loadCount());

    // and: the aliasing bug is fixed - a fresh record built from the original (pre-mutation) content string
    // must not observe the "035" field that was only ever added to marcRecord's mutated marc4j Record. Under
    // the old design this old key was left dangling and pointing at the mutated instance; now it was
    // invalidated as part of the very first mutation above, so this lookup is forced to reparse fresh.
    var recordWithStaleContent = new Record().withId(UUID.randomUUID().toString())
      .withParsedRecord(new ParsedRecord().withContent(contentBeforeUpdate));
    assertFalse(isFieldExist(recordWithStaleContent, "035", 'a', instanceId));
    cacheStats = getCacheStats().minus(initialCacheStats);
    assertEquals(9, cacheStats.requestCount());
    assertEquals(7, cacheStats.hitCount());
    assertEquals(2, cacheStats.missCount());
    assertEquals(2, cacheStats.loadCount());
  }

  @Test
  @SuppressWarnings("checkstyle:MethodLength")
  void shouldRemove003ifHridManipulationAlreadyDone() {
    // given
    String parsedContent = """
      {
        "leader": "00115nam  22000731a 4500",
        "fields": [
          {
            "001": "in001"
          },
          {
            "003": "qwerty"
          },
          {
            "507": {
              "subfields": [
                {
                  "a": "data"
                }
              ],
              "ind1": " ",
              "ind2": " "
            }
          },
          {
            "500": {
              "subfields": [
                {
                  "a": "data"
                }
              ],
              "ind1": " ",
              "ind2": " "
            }
          }
        ]
      }
      """;
    String expectedParsedContent = """
      {
        "leader": "00086nam  22000611a 4500",
        "fields": [
          {
            "001": "in001"
          },
          {
            "507": {
              "subfields": [
                {
                  "a": "data"
                }
              ],
              "ind1": " ",
              "ind2": " "
            }
          },
          {
            "500": {
              "subfields": [
                {
                  "a": "data"
                }
              ],
              "ind1": " ",
              "ind2": " "
            }
          }
        ]
      }
      """;
    ParsedRecord parsedRecord = new ParsedRecord();
    parsedRecord.setContent(parsedContent);

    var marcRecord = new Record().withId(UUID.randomUUID().toString())
      .withParsedRecord(parsedRecord)
      .withGeneration(0)
      .withState(Record.State.ACTUAL)
      .withExternalIdsHolder(new ExternalIdsHolder().withInstanceId("001").withInstanceHrid("in001"));

    // when
    AdditionalFieldsUtil.fillHrIdFieldInMarcRecord(marcRecord, "in001");
    // then
    assertEquals(expectedParsedContent, parsedRecord.getContent());
  }

  @Test
  @SuppressWarnings("checkstyle:MethodLength")
  void shouldNotProcessRecord() {
    // given
    String parsedContent = """
      {
        "leader": "00115nam  22000731a 4500",
        "fields": [
          {
            "001": "in001"
          },
          {
            "003": "qwerty"
          },
          {
            "507": {
              "subfields": [
                {
                  "a": "data"
                }
              ],
              "ind1": " ",
              "ind2": " "
            }
          },
          {
            "500": {
              "subfields": [
                {
                  "a": "data"
                }
              ],
              "ind1": " ",
              "ind2": " "
            }
          }
        ]
      }
      """;
    String expectedParsedContent = """
      {
        "leader": "00086nam  22000611a 4500",
        "fields": [
          {
            "001": "in001"
          },
          {
            "507": {
              "subfields": [
                {
                  "a": "data"
                }
              ],
              "ind1": " ",
              "ind2": " "
            }
          },
          {
            "500": {
              "subfields": [
                {
                  "a": "data"
                }
              ],
              "ind1": " ",
              "ind2": " "
            }
          }
        ]
      }
      """;
    ParsedRecord parsedRecord = new ParsedRecord();
    parsedRecord.setContent(parsedContent);

    var marcRecord = new Record().withId(UUID.randomUUID().toString())
      .withParsedRecord(parsedRecord)
      .withGeneration(0)
      .withState(Record.State.ACTUAL)
      .withExternalIdsHolder(new ExternalIdsHolder().withInstanceId("001").withInstanceHrid("in001"));

    // when
    AdditionalFieldsUtil.fillHrIdFieldInMarcRecord(marcRecord, "in003");
    // then
    assertNotEquals(expectedParsedContent, parsedRecord.getContent());
  }

  @Test
  @SuppressWarnings("checkstyle:MethodLength")
  void shouldNotAdd035AndAdd001FieldsIf001And003FieldsNotExists() {
    // given
    String parsedContent = """
      {
        "leader": "00116nam  22000732a 4900",
        "fields": [
          {
            "003": "in001"
          },
          {
            "507": {
              "subfields": [
                {
                  "a": "data"
                }
              ],
              "ind1": " ",
              "ind2": " "
            }
          },
          {
            "500": {
              "subfields": [
                {
                  "a": "data"
                }
              ],
              "ind1": " ",
              "ind2": " "
            }
          }
        ]
      }
      """;
    String expectedParsedContent = """
      {
        "leader": "00086nam  22000612a 4900",
        "fields": [
          {
            "001": "in001"
          },
          {
            "507": {
              "subfields": [
                {
                  "a": "data"
                }
              ],
              "ind1": " ",
              "ind2": " "
            }
          },
          {
            "500": {
              "subfields": [
                {
                  "a": "data"
                }
              ],
              "ind1": " ",
              "ind2": " "
            }
          }
        ]
      }
      """;
    ParsedRecord parsedRecord = new ParsedRecord();
    parsedRecord.setContent(parsedContent);

    var marcRecord = new Record().withId(UUID.randomUUID().toString())
      .withParsedRecord(parsedRecord)
      .withGeneration(0)
      .withState(Record.State.ACTUAL)
      .withExternalIdsHolder(new ExternalIdsHolder().withInstanceId("001").withInstanceHrid("in001"));

    // when
    AdditionalFieldsUtil.fillHrIdFieldInMarcRecord(marcRecord, "in001");
    // then
    assertEquals(expectedParsedContent, parsedRecord.getContent());
  }

  @Test
  @SuppressWarnings("checkstyle:MethodLength")
  void shouldRemove035() {
    // given
    String parsedContent = """
      {
        "leader": "00120nam  22000731a 4500",
        "fields": [
          {
            "001": "in001"
          },
          {
            "035": {
              "subfields": [
                {
                  "a": "(ybp7406411)in001"
                }
              ],
              "ind1": " ",
              "ind2": " "
            }
          },
          {
            "507": {
              "subfields": [
                {
                  "a": "data"
                }
              ],
              "ind1": " ",
              "ind2": " "
            }
          },
          {
            "500": {
              "subfields": [
                {
                  "a": "data"
                }
              ],
              "ind1": " ",
              "ind2": " "
            }
          }
        ]
      }
      """;
    String expectedParsedContent = """
      {
        "leader": "00086nam  22000611a 4500",
        "fields": [
          {
            "001": "in001"
          },
          {
            "507": {
              "subfields": [
                {
                  "a": "data"
                }
              ],
              "ind1": " ",
              "ind2": " "
            }
          },
          {
            "500": {
              "subfields": [
                {
                  "a": "data"
                }
              ],
              "ind1": " ",
              "ind2": " "
            }
          }
        ]
      }
      """;
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
      AdditionalFieldsUtil.reorderMarcRecordFields(userOrderRecordContent, systemReorderedRecordContent,
        UUID.randomUUID().toString());

    assertNotNull(actualOrderRecord);
    assertEquals(formatContent(expectedOrderRecord), formatContent(actualOrderRecord));
  }

  @Test
  @SuppressWarnings("checkstyle:MethodLength")
  void reorderMarcRecordFieldsDoesNotThrow_whenSystemOrderContentHasAnEmptyFieldNode() {
    // given: an empty field node ({}) mixed into the system-reordered content's "fields" array. Before the
    // getTagFromNode guard, node.fieldNames().next() threw NoSuchElementException on this node, which
    // reorderMarcRecordFields' own catch-all then swallowed into the un-reordered systemOrderContent fallback -
    // so this scenario was already "safe" from a crash-propagation standpoint, but only by accident. This test
    // pins the actual (post-guard) behaviour instead of just asserting "doesn't throw".
    var sourceOrderContent = """
      {
        "fields": [
          {
            "245": {
              "subfields": [
                {
                  "a": "Title"
                }
              ],
              "ind1": " ",
              "ind2": " "
            }
          }
        ]
      }
      """;
    var systemOrderContent = """
      {
        "leader": "00000nam a2200000 a 4500",
        "fields": [
          {
            "001": "in001"
          },
          {
      
          },
          {
            "245": {
              "subfields": [
                {
                  "a": "Title"
                }
              ],
              "ind1": " ",
              "ind2": " "
            }
          }
        ]
      }
      """;
    var expectedReorderedContent = """
      {
        "leader": "00000nam a2200000 a 4500",
        "fields": [
          {
            "001": "in001"
          },
          {
            "245": {
              "subfields": [
                {
                  "a": "Title"
                }
              ],
              "ind1": " ",
              "ind2": " "
            }
          }
        ]
      }
      """;

    // when
    var actualReorderedContent = assertDoesNotThrow(() ->
      AdditionalFieldsUtil.reorderMarcRecordFields(sourceOrderContent, systemOrderContent,
        UUID.randomUUID().toString()));

    // then: the empty node carries no tag and no information, so it is dropped rather than corrupting the
    // order of the real fields around it
    assertEquals(formatContent(expectedReorderedContent), formatContent(actualReorderedContent));
  }

  @DisplayName("should produce content identical to the sequential update005/move001To035/normalize035 calls "
               + "when 005 needs updating, 001 is present, and an OCoLC-prefixed 035 exists")
  @Test
  @SuppressWarnings("checkstyle:MethodLength")
  void shouldProduceSameContentAsSequentialCalls_whenStandardManipulationUpdates005Moves001AndNormalizes035() {
    // given: two identical records - a 001 to move to 035, an existing OCoLC-prefixed 035 to normalize, and no
    // field-protection settings, so field 005 also needs updating
    String parsedContent = """
      {
        "leader": "00115nam  22000731a 4500",
        "fields": [
          {
            "001": "in001"
          },
          {
            "003": "qwerty"
          },
          {
            "005": "20141107001016.0"
          },
          {
            "035": {
              "subfields": [
                {
                  "a": "(OCoLC)on. 607TST .001"
                }
              ],
              "ind1": " ",
              "ind2": " "
            }
          },
          {
            "500": {
              "subfields": [
                {
                  "a": "data"
                }
              ],
              "ind1": " ",
              "ind2": " "
            }
          }
        ]
      }
      """;
    var mappingParameters = new MappingParameters();
    Clock fixedClock = Clock.fixed(Instant.parse("2024-03-15T10:30:45.123Z"), ZoneId.of("UTC"));

    var sequentialRecord = buildMarcRecordWithContent(parsedContent);
    var batchedRecord = buildMarcRecordWithContent(parsedContent);

    // when
    AdditionalFieldsUtil.updateLatestTransactionDate(sequentialRecord, mappingParameters, fixedClock);
    AdditionalFieldsUtil.move001To035(sequentialRecord);
    AdditionalFieldsUtil.normalize035(sequentialRecord);

    AdditionalFieldsUtil.executeStandardFieldsManipulation(batchedRecord, mappingParameters, fixedClock);

    // then
    assertThat(batchedRecord.getParsedRecord().getContent())
      .isEqualTo(sequentialRecord.getParsedRecord().getContent());
  }

  @DisplayName("should produce content identical to the sequential update005/move001To035/normalize035 calls "
               + "when 005 is protected, 001 is absent, and no OCoLC-prefixed 035 exists")
  @Test
  @SuppressWarnings("checkstyle:MethodLength")
  void shouldProduceSameContentAsSequentialCalls_whenStandardManipulationSkipsAllThreeSteps() {
    // given: two identical records - no 001 (move001To035 only removes 003), no OCoLC-prefixed 035
    // (normalize035 is a no-op), and a field-protection setting that protects 005 (so 005 is skipped too)
    String parsedContent = """
      {
        "leader": "00115nam  22000731a 4500",
        "fields": [
          {
            "003": "qwerty"
          },
          {
            "005": "20141107001016.0"
          },
          {
            "035": {
              "subfields": [
                {
                  "a": "(NhFolYBP)in001"
                }
              ],
              "ind1": " ",
              "ind2": " "
            }
          },
          {
            "500": {
              "subfields": [
                {
                  "a": "data"
                }
              ],
              "ind1": " ",
              "ind2": " "
            }
          }
        ]
      }
      """;
    var mappingParameters = new MappingParameters().withMarcFieldProtectionSettings(
      List.of(new MarcFieldProtectionSetting().withField(FIELD_005).withData("*")));
    Clock fixedClock = Clock.fixed(Instant.parse("2024-03-15T10:30:45.123Z"), ZoneId.of("UTC"));

    var sequentialRecord = buildMarcRecordWithContent(parsedContent);
    var batchedRecord = buildMarcRecordWithContent(parsedContent);

    // when
    AdditionalFieldsUtil.updateLatestTransactionDate(sequentialRecord, mappingParameters, fixedClock);
    AdditionalFieldsUtil.move001To035(sequentialRecord);
    AdditionalFieldsUtil.normalize035(sequentialRecord);

    AdditionalFieldsUtil.executeStandardFieldsManipulation(batchedRecord, mappingParameters, fixedClock);

    // then
    assertThat(batchedRecord.getParsedRecord().getContent())
      .isEqualTo(sequentialRecord.getParsedRecord().getContent());
  }

  @DisplayName("should produce content identical to the sequential update005/normalize035/remove035WithHrId calls "
               + "when the record is a MARC_BIB and the 035-with-hrid removal actually runs")
  @Test
  @SuppressWarnings("checkstyle:MethodLength")
  void shouldProduceSameContentAsSequentialCalls_whenReplaceManipulationRunsHrIdRemovalOnMarcBib() {
    // given: two identical MARC_BIB records - an OCoLC-prefixed 035 to normalize, and a second 035 whose
    // subfield contains the 001 hrid value, so remove035FieldWhenRecordContainsHrId actually removes it
    String parsedContent = """
      {
        "leadder": "00115nam  22000731a 4500",
        "fields": [
          {
            "001": "in001"
          },
          {
            "005": "20141107001016.0"
          },
          {
            "035": {
              "subfields": [
                {
                  "a": "(ybp7406411)in001"
                }
              ],
              "ind1": " ",
              "ind2": " "
            }
          },
          {
            "035": {
              "subfields": [
                {
                  "a": "(OCoLC)on. 607TST .001"
                }
              ],
              "ind1": " ",
              "ind2": " "
            }
          },
          {
            "500": {
              "subfields": [
                {
                  "a": "data"
                }
              ],
              "ind1": " ",
              "ind2": " "
            }
          }
        ]
      }
      """;
    var mappingParameters = new MappingParameters();
    Clock fixedClock = Clock.fixed(Instant.parse("2024-03-15T10:30:45.123Z"), ZoneId.of("UTC"));

    var sequentialRecord = buildMarcRecordWithContent(parsedContent).withRecordType(Record.RecordType.MARC_BIB);
    var batchedRecord = buildMarcRecordWithContent(parsedContent).withRecordType(Record.RecordType.MARC_BIB);

    // when
    AdditionalFieldsUtil.updateLatestTransactionDate(sequentialRecord, mappingParameters, fixedClock);
    AdditionalFieldsUtil.normalize035(sequentialRecord);
    AdditionalFieldsUtil.remove035FieldWhenRecordContainsHrId(sequentialRecord);

    AdditionalFieldsUtil.executeReplaceFieldsManipulation(batchedRecord, mappingParameters, fixedClock);

    // then
    assertThat(batchedRecord.getParsedRecord().getContent())
      .isEqualTo(sequentialRecord.getParsedRecord().getContent());
  }

  @DisplayName("should produce content identical to the sequential update005/normalize035/remove035WithHrId calls "
               + "when the record is not a MARC_BIB and the 035-with-hrid removal is skipped")
  @Test
  @SuppressWarnings("checkstyle:MethodLength")
  void shouldProduceSameContentAsSequentialCalls_whenReplaceManipulationSkipsHrIdRemovalOnNonMarcBib() {
    // given: same fixture as the MARC_BIB case, but recorded as a MARC_AUTHORITY record, so
    // remove035FieldWhenRecordContainsHrId's MARC_BIB guard skips the 035-with-hrid removal entirely on both sides
    String parsedContent = """
      {
        "leader": "00115nam  22000731a 4500",
        "fields": [
          {
            "001": "in001"
          },
          {
            "005": "20141107001016.0"
          },
          {
            "035": {
              "subfields": [
                {
                  "a": "(ybp7406411)in001"
                }
              ],
              "ind1": " ",
              "ind2": " "
            }
          },
          {
            "035": {
              "subfields": [
                {
                  "a": "(OCoLC)on. 607TST .001"
                }
              ],
              "ind1": " ",
              "ind2": " "
            }
          },
          {
            "500": {
              "subfields": [
                {
                  "a": "data"
                }
              ],
              "ind1": " ",
              "ind2": " "
            }
          }
        ]
      }
      """;
    var mappingParameters = new MappingParameters();
    Clock fixedClock = Clock.fixed(Instant.parse("2024-03-15T10:30:45.123Z"), ZoneId.of("UTC"));

    var sequentialRecord =
      buildMarcRecordWithContent(parsedContent).withRecordType(Record.RecordType.MARC_AUTHORITY);
    var batchedRecord =
      buildMarcRecordWithContent(parsedContent).withRecordType(Record.RecordType.MARC_AUTHORITY);

    // when
    AdditionalFieldsUtil.updateLatestTransactionDate(sequentialRecord, mappingParameters, fixedClock);
    AdditionalFieldsUtil.normalize035(sequentialRecord);
    AdditionalFieldsUtil.remove035FieldWhenRecordContainsHrId(sequentialRecord);

    AdditionalFieldsUtil.executeReplaceFieldsManipulation(batchedRecord, mappingParameters, fixedClock);

    // then
    assertThat(batchedRecord.getParsedRecord().getContent())
      .isEqualTo(sequentialRecord.getParsedRecord().getContent());
  }

  @DisplayName("should throw EventProcessingException when 005 needs updating but the record cannot be parsed")
  @Test
  void executeStandardFieldsManipulationThrowsEventProcessingException_whenRecordCannotBeParsed() {
    // given: a record with no parsed record at all. isField005NeedToUpdate short-circuits to "needs update"
    // when no field protection settings are configured, regardless of content, so executeStandardFieldsManipulation
    // reaches its own null-marcRecord guard and throws rather than silently no-oping - mirroring
    // updateLatestTransactionDateThrowsEventProcessingExceptionWithCause_whenAddingControlledFieldFails's fixture.
    var recordWithNoParsedRecord = new Record().withId(UUID.randomUUID().toString());

    // when
    var clock = Clock.systemDefaultZone();
    var mappingParameters = new MappingParameters();
    var exception = assertThrows(EventProcessingException.class,
      () -> AdditionalFieldsUtil
        .executeStandardFieldsManipulation(recordWithNoParsedRecord, mappingParameters, clock));

    // then
    assertThat(exception.getMessage()).contains(recordWithNoParsedRecord.getId());
  }

  private void shouldReturnEmptyOptional(String parsedContent) {
    // given
    var marcRecord = new Record().withParsedRecord(new ParsedRecord().withContent(parsedContent));

    // when
    var result = getValueFromDataField(marcRecord, FIELD_999, INDICATOR_F, INDICATOR_F, SUBFIELD_I);

    // then
    assertTrue(result.isEmpty());
  }

  private static Record buildMarcRecordWithContent(String parsedContent) {
    return new Record().withId(UUID.randomUUID().toString())
      .withParsedRecord(new ParsedRecord().withContent(parsedContent))
      .withGeneration(0)
      .withState(Record.State.ACTUAL)
      .withExternalIdsHolder(new ExternalIdsHolder().withInstanceId("001").withInstanceHrid("in001"));
  }

  private static String readFileFromPath(String path) throws IOException {
    return new String(FileUtils.readFileToByteArray(new File(path)));
  }

  private String formatContent(String content) {
    return content.replaceAll("\\s", "");
  }

  static class OclcFieldNormalizationTest {

    @SuppressWarnings("checkstyle:MethodLength")
    static Stream<Arguments> data() {
      return Stream.of(
        Arguments.of(
          """
            {
              "leader": "00120nam  22000731a 4500",
              "fields": [
                {
                  "001": "in001"
                },
                {
                  "035": {
                    "subfields": [
                      {
                        "a": "(ybp7406411)in001"
                      },
                      {
                        "a": "(OCoLC)00006475800"
                      }
                    ],
                    "ind1": " ",
                    "ind2": " "
                  }
                },
                {
                  "500": {
                    "subfields": [
                      {
                        "a": "data"
                      }
                    ],
                    "ind1": " ",
                    "ind2": " "
                  }
                }
              ]
            }
            """,
          """
            {
              "leader": "00115nam  22000611a 4500",
              "fields": [
                {
                  "001": "in001"
                },
                {
                  "035": {
                    "subfields": [
                      {
                        "a": "(ybp7406411)in001"
                      },
                      {
                        "a": "(OCoLC)6475800"
                      }
                    ],
                    "ind1": " ",
                    "ind2": " "
                  }
                },
                {
                  "500": {
                    "subfields": [
                      {
                        "a": "data"
                      }
                    ],
                    "ind1": " ",
                    "ind2": " "
                  }
                }
              ]
            }
            """
        ),
        Arguments.of(
          """
            {
              "leader": "00120nam  22000731a 4500",
              "fields": [
                {
                  "001": "in001"
                },
                {
                  "035": {
                    "subfields": [
                      {
                        "a": "(ybp7406411)in001"
                      },
                      {
                        "a": "(OCoLC)tfe0006475800"
                      }
                    ],
                    "ind1": " ",
                    "ind2": " "
                  }
                },
                {
                  "500": {
                    "subfields": [
                      {
                        "a": "data"
                      }
                    ],
                    "ind1": " ",
                    "ind2": " "
                  }
                }
              ]
            }
            """,

          """
            {
              "leader": "00118nam  22000611a 4500",
              "fields": [
                {
                  "001": "in001"
                },
                {
                  "035": {
                    "subfields": [
                      {
                        "a": "(ybp7406411)in001"
                      },
                      {
                        "a": "(OCoLC)tfe6475800"
                      }
                    ],
                    "ind1": " ",
                    "ind2": " "
                  }
                },
                {
                  "500": {
                    "subfields": [
                      {
                        "a": "data"
                      }
                    ],
                    "ind1": " ",
                    "ind2": " "
                  }
                }
              ]
            }
            """
        ),
        Arguments.of(
          """
            {
              "leader": "00120nam  22000731a 4500",
              "fields": [
                {
                  "001": "in001"
                },
                {
                  "035": {
                    "subfields": [
                      {
                        "a": "(ybp7406411)in001"
                      },
                      {
                        "a": "(OCoLC)00064758"
                      },
                      {
                        "a": "(OCoLC)ocm00064758"
                      },
                      {
                        "z": "(OCoLC)00024758"
                      }
                    ],
                    "ind1": " ",
                    "ind2": " "
                  }
                },
                {
                  "500": {
                    "subfields": [
                      {
                        "a": "data"
                      }
                    ],
                    "ind1": " ",
                    "ind2": " "
                  }
                }
              ]
            }
            """,

          """
            {
              "leader": "00127nam  22000611a 4500",
              "fields": [
                {
                  "001": "in001"
                },
                {
                  "035": {
                    "subfields": [
                      {
                        "a": "(ybp7406411)in001"
                      },
                      {
                        "a": "(OCoLC)64758"
                      },
                      {
                        "z": "(OCoLC)24758"
                      }
                    ],
                    "ind1": " ",
                    "ind2": " "
                  }
                },
                {
                  "500": {
                    "subfields": [
                      {
                        "a": "data"
                      }
                    ],
                    "ind1": " ",
                    "ind2": " "
                  }
                }
              ]
            }
            """
        ),
        Arguments.of(
          """
            {
              "leader": "00120nam  22000731a 4500",
              "fields": [
                {
                  "001": "in001"
                },
                {
                  "035": {
                    "subfields": [
                      {
                        "a": "(OCoLC)00064758"
                      }
                    ],
                    "ind1": " ",
                    "ind2": " "
                  }
                },
                {
                  "035": {
                    "subfields": [
                      {
                        "a": "(OCoLC)ocn000064758"
                      }
                    ],
                    "ind1": " ",
                    "ind2": " "
                  }
                },
                {
                  "035": {
                    "subfields": [
                      {
                        "a": "(OCoLC)ocm0000064758"
                      },
                      {
                        "z": "(OCoLC)11114758"
                      }
                    ],
                    "ind1": " ",
                    "ind2": " "
                  }
                },
                {
                  "500": {
                    "subfields": [
                      {
                        "a": "data"
                      }
                    ],
                    "ind1": " ",
                    "ind2": " "
                  }
                }
              ]
            }
            """,

          """
            {
              "leader": "00111nam  22000611a 4500",
              "fields": [
                {
                  "001": "in001"
                },
                {
                  "035": {
                    "subfields": [
                      {
                        "a": "(OCoLC)64758"
                      },
                      {
                        "z": "(OCoLC)11114758"
                      }
                    ],
                    "ind1": " ",
                    "ind2": " "
                  }
                },
                {
                  "500": {
                    "subfields": [
                      {
                        "a": "data"
                      }
                    ],
                    "ind1": " ",
                    "ind2": " "
                  }
                }
              ]
            }
            """
        ),
        Arguments.of(
          """
            {
              "leader": "00120nam  22000731a 4500",
              "fields": [
                {
                  "001": "in001"
                },
                {
                  "035": {
                    "subfields": [
                      {
                        "a": "(ybp7406411)in001"
                      }
                    ],
                    "ind1": " ",
                    "ind2": " "
                  }
                },
                {
                  "035": {
                    "subfields": [
                      {
                        "a": "(OCoLC)ocn00064758"
                      }
                    ],
                    "ind1": " ",
                    "ind2": " "
                  }
                },
                {
                  "035": {
                    "subfields": [
                      {
                        "a": "(OCoLC)ocm000064758"
                      }
                    ],
                    "ind1": " ",
                    "ind2": " "
                  }
                },
                {
                  "500": {
                    "subfields": [
                      {
                        "a": "data"
                      }
                    ],
                    "ind1": " ",
                    "ind2": " "
                  }
                }
              ]
            }
            """,

          """
            {
              "leader": "00128nam  22000731a 4500",
              "fields": [
                {
                  "001": "in001"
                },
                {
                  "035": {
                    "subfields": [
                      {
                        "a": "(ybp7406411)in001"
                      }
                    ],
                    "ind1": " ",
                    "ind2": " "
                  }
                },
                {
                  "035": {
                    "subfields": [
                      {
                        "a": "(OCoLC)64758"
                      }
                    ],
                    "ind1": " ",
                    "ind2": " "
                  }
                },
                {
                  "500": {
                    "subfields": [
                      {
                        "a": "data"
                      }
                    ],
                    "ind1": " ",
                    "ind2": " "
                  }
                }
              ]
            }
            """
        ),
        Arguments.of(
          """
            {
              "leader": "00120nam  22000731a 4500",
              "fields": [
                {
                  "001": "in001"
                },
                {
                  "035": {
                    "subfields": [
                      {
                        "a": "(OCoLC)ocn607TST001"
                      }
                    ],
                    "ind1": " ",
                    "ind2": " "
                  }
                },
                {
                  "500": {
                    "subfields": [
                      {
                        "a": "data"
                      }
                    ],
                    "ind1": " ",
                    "ind2": " "
                  }
                }
              ]
            }
            """,

          """
            {
              "leader": "00098nam  22000611a 4500",
              "fields": [
                {
                  "001": "in001"
                },
                {
                  "035": {
                    "subfields": [
                      {
                        "a": "(OCoLC)607TST001"
                      }
                    ],
                    "ind1": " ",
                    "ind2": " "
                  }
                },
                {
                  "500": {
                    "subfields": [
                      {
                        "a": "data"
                      }
                    ],
                    "ind1": " ",
                    "ind2": " "
                  }
                }
              ]
            }
            """
        ),
        Arguments.of(
          """
            {
              "leader": "00120nam  22000731a 4500",
              "fields": [
                {
                  "001": "in001"
                },
                {
                  "035": {
                    "subfields": [
                      {
                        "a": "(OCoLC-M)ocn0001234"
                      }
                    ],
                    "ind1": " ",
                    "ind2": " "
                  }
                },
                {
                  "035": {
                    "subfields": [
                      {
                        "a": "(ABC)ocn0001234"
                      }
                    ],
                    "ind1": " ",
                    "ind2": " "
                  }
                },
                {
                  "035": {
                    "subfields": [
                      {
                        "a": "(OCoLC)ocn0001234"
                      },
                      {
                        "a": "(OCoLC)ocn1234"
                      },
                      {
                        "b": "(OCoLC)ocn1234"
                      }
                    ],
                    "ind1": " ",
                    "ind2": " "
                  }
                },
                {
                  "035": {
                    "subfields": [
                      {
                        "a": "(OCoLC)ocm1234"
                      }
                    ],
                    "ind1": " ",
                    "ind2": " "
                  }
                },
                {
                  "035": {
                    "subfields": [
                      {
                        "a": "(OCoLC)ocn00098765"
                      }
                    ],
                    "ind1": " ",
                    "ind2": " "
                  }
                },
                {
                  "035": {
                    "subfields": [
                      {
                        "a": "(OCoLC)ocn0001234"
                      }
                    ],
                    "ind1": " ",
                    "ind2": " "
                  }
                },
                {
                  "500": {
                    "subfields": [
                      {
                        "a": "data"
                      }
                    ],
                    "ind1": " ",
                    "ind2": " "
                  }
                }
              ]
            }
            """,

          """
            {
              "leader": "00218nam  22001091a 4500",
              "fields": [
                {
                  "001": "in001"
                },
                {
                  "035": {
                    "subfields": [
                      {
                        "a": "(OCoLC-M)ocn0001234"
                      }
                    ],
                    "ind1": " ",
                    "ind2": " "
                  }
                },
                {
                  "035": {
                    "subfields": [
                      {
                        "a": "(ABC)ocn0001234"
                      }
                    ],
                    "ind1": " ",
                    "ind2": " "
                  }
                },
                {
                  "035": {
                    "subfields": [
                      {
                        "b": "(OCoLC)1234"
                      }
                    ],
                    "ind1": " ",
                    "ind2": " "
                  }
                },
                {
                  "035": {
                    "subfields": [
                      {
                        "a": "(OCoLC)98765"
                      }
                    ],
                    "ind1": " ",
                    "ind2": " "
                  }
                },
                {
                  "035": {
                    "subfields": [
                      {
                        "a": "(OCoLC)1234"
                      }
                    ],
                    "ind1": " ",
                    "ind2": " "
                  }
                },
                {
                  "500": {
                    "subfields": [
                      {
                        "a": "data"
                      }
                    ],
                    "ind1": " ",
                    "ind2": " "
                  }
                }
              ]
            }
            """
        ),
        Arguments.of(
          """
            {
              "leader": "00126nam  22000731a 4500",
              "fields": [
                {
                  "001": "in001"
                },
                {
                  "035": {
                    "subfields": [
                      {
                        "a": "(OCoLC)1234"
                      }
                    ],
                    "ind1": " ",
                    "ind2": " "
                  }
                },
                {
                  "035": {
                    "subfields": [
                      {
                        "a": "(OCoLC-M)1234456"
                      }
                    ],
                    "ind1": " ",
                    "ind2": " "
                  }
                },
                {
                  "500": {
                    "subfields": [
                      {
                        "a": "data"
                      }
                    ],
                    "ind1": " ",
                    "ind2": " "
                  }
                }
              ]
            }
            """,

          """
            {
              "leader": "00126nam  22000731a 4500",
              "fields": [
                {
                  "001": "in001"
                },
                {
                  "035": {
                    "subfields": [
                      {
                        "a": "(OCoLC)1234"
                      }
                    ],
                    "ind1": " ",
                    "ind2": " "
                  }
                },
                {
                  "035": {
                    "subfields": [
                      {
                        "a": "(OCoLC-M)1234456"
                      }
                    ],
                    "ind1": " ",
                    "ind2": " "
                  }
                },
                {
                  "500": {
                    "subfields": [
                      {
                        "a": "data"
                      }
                    ],
                    "ind1": " ",
                    "ind2": " "
                  }
                }
              ]
            }
            """
        ),
        Arguments.of(
          """
            {
              "leader": "00120nam  22000731a 4500",
              "fields": [
                {
                  "001": "in001"
                },
                {
                  "035": {
                    "subfields": [
                      {
                        "a": "(ybp7406411)in001"
                      },
                      {
                        "a": "   (OCoLC)000012345"
                      }
                    ],
                    "ind1": " ",
                    "ind2": " "
                  }
                },
                {
                  "500": {
                    "subfields": [
                      {
                        "a": "data"
                      }
                    ],
                    "ind1": " ",
                    "ind2": " "
                  }
                }
              ]
            }
            """,

          """
            {
              "leader": "00113nam  22000611a 4500",
              "fields": [
                {
                  "001": "in001"
                },
                {
                  "035": {
                    "subfields": [
                      {
                        "a": "(ybp7406411)in001"
                      },
                      {
                        "a": "(OCoLC)12345"
                      }
                    ],
                    "ind1": " ",
                    "ind2": " "
                  }
                },
                {
                  "500": {
                    "subfields": [
                      {
                        "a": "data"
                      }
                    ],
                    "ind1": " ",
                    "ind2": " "
                  }
                }
              ]
            }
            """
        )
      );
    }

    @ParameterizedTest
    @MethodSource("data")
    void shouldNormalizeOcolcField035(String parsedContent, String expectedParsedContent) {
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
