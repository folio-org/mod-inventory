package org.folio.inventory.dataimport.util;

import static java.lang.String.format;
import static org.apache.commons.lang3.StringUtils.isBlank;
import static org.apache.commons.lang3.StringUtils.isNotBlank;
import static org.apache.commons.lang3.StringUtils.isNotEmpty;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.github.benmanes.caffeine.cache.CacheLoader;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import io.vertx.core.json.JsonObject;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.Strings;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.inventory.domain.instances.Instance;
import org.folio.processing.exceptions.EventProcessingException;
import org.folio.processing.mapping.defaultmapper.processor.parameters.MappingParameters;
import org.folio.processing.util.MarcRecordNormalizer;
import org.folio.rest.jaxrs.model.MarcFieldProtectionSetting;
import org.folio.rest.jaxrs.model.ParsedRecord;
import org.folio.rest.jaxrs.model.Record;
import org.marc4j.MarcException;
import org.marc4j.MarcJsonReader;
import org.marc4j.MarcJsonWriter;
import org.marc4j.MarcReader;
import org.marc4j.MarcStreamWriter;
import org.marc4j.MarcWriter;
import org.marc4j.marc.ControlField;
import org.marc4j.marc.DataField;
import org.marc4j.marc.MarcFactory;
import org.marc4j.marc.VariableField;
import org.marc4j.marc.impl.Verifier;

/**
 * Util to work with additional fields
 */
public final class AdditionalFieldsUtil {

  public static final DateTimeFormatter DATE_TIME_005_FORMATTER = DateTimeFormatter.ofPattern("yyyyMMddHHmmss.S");
  public static final String TAG_00X_PREFIX = "00";
  public static final String TAG_005 = "005";
  public static final String TAG_999 = "999";
  public static final String TAG_001 = "001";
  public static final String TAG_035 = "035";
  public static final char TAG_035_SUB = 'a';
  public static final char INDICATOR_F = 'f';
  public static final char SUBFIELD_I = 'i';
  public static final char SUBFIELD_L = 'l';
  public static final String FIELDS = "fields";
  static final String INVALID_DATA_FIELD_MSG = "Field '%s' is not a data field.";
  private static final Logger LOGGER = LogManager.getLogger();
  private static final String TAG_003 = "003";
  private static final char TAG_035_IND = ' ';
  private static final String ANY_STRING = "*";
  private static final CacheLoader<String, org.marc4j.marc.Record> parsedRecordContentCacheLoader;
  private static final LoadingCache<String, org.marc4j.marc.Record> parsedRecordContentCache;
  private static final String OCLC_PREFIX = "(OCoLC)";
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
  private static final int MAX_CACHE_SIZE = 2000;

  static {
    // this function is executed when creating a new item to be saved in the cache.
    // In this case, this is a MARC4J Record
    parsedRecordContentCacheLoader =
      parsedRecordContent -> {
        MarcJsonReader marcJsonReader =
          new MarcJsonReader(
            new ByteArrayInputStream(
              parsedRecordContent.getBytes(StandardCharsets.UTF_8)));
        if (marcJsonReader.hasNext()) {
          return marcJsonReader.next();
        }
        return null;
      };

    parsedRecordContentCache =
      Caffeine.newBuilder()
        .maximumSize(MAX_CACHE_SIZE)
        // strong (equals/hashCode-based) keys: the cache is content-addressed, so structurally-equal
        // parsed content must map to the same entry regardless of String instance identity.
        .recordStats()
        .build(parsedRecordContentCacheLoader);
  }

  private AdditionalFieldsUtil() {
  }

  public static AdditionalFieldsUtilCacheStats getCacheStats() {
    return AdditionalFieldsUtilCacheStats.fromCaffeine(parsedRecordContentCache.stats());
  }

  /**
   * Adds field if it does not exist and a subfield with a value to that field
   *
   * @param recordForUpdate record that needs to be updated
   * @param field           field that should contain new subfield
   * @param subfield        new subfield to add
   * @param value           value of the subfield to add
   * @return true if succeeded, false otherwise
   */
  public static boolean addFieldToMarcRecord(Record recordForUpdate, String field, char subfield, String value) {
    boolean result = false;
    try {
      if (recordForUpdate != null && recordForUpdate.getParsedRecord() != null
          && recordForUpdate.getParsedRecord().getContent() != null) {
        org.marc4j.marc.Record marcRecord = computeMarcRecord(recordForUpdate);
        if (marcRecord != null) {
          MarcFieldEditor.addSubfieldToField(marcRecord, field, subfield, value);
          result = recalculateLeaderAndParsedRecord(recordForUpdate, marcRecord);
        }
      }
    } catch (Exception e) {
      LOGGER.warn("addFieldToMarcRecord:: Failed to add additional subfield {} for field {} to record {}",
        subfield, field, getRecordId(recordForUpdate), e);
    }
    return result;
  }

  /**
   * Updates field 005 for case when this field is not protected.
   *
   * @param recordForUpdate   record to update
   * @param mappingParameters mapping parameters
   */
  public static void updateLatestTransactionDate(Record recordForUpdate, MappingParameters mappingParameters) {
    updateLatestTransactionDate(recordForUpdate, mappingParameters, Clock.systemDefaultZone());
  }

  /**
   * Updates field 005 for case when this field is not protected, using the given {@link Clock} as the source of
   * the current time so the resulting value is deterministic and testable.
   *
   * @param recordForUpdate   record to update
   * @param mappingParameters mapping parameters
   * @param clock             clock used to compute the current date/time written to field 005
   */
  public static void updateLatestTransactionDate(Record recordForUpdate, MappingParameters mappingParameters,
                                                 Clock clock) {
    if (isField005NeedToUpdate(recordForUpdate, mappingParameters)) {
      String date = DATE_TIME_005_FORMATTER.format(ZonedDateTime.now(clock));
      try {
        addControlledFieldToMarcRecordOrThrow(recordForUpdate, TAG_005, date, true);
      } catch (Exception e) {
        throw new EventProcessingException(format("Failed to update field '005' to record with id '%s'",
          recordForUpdate != null ? recordForUpdate.getId() : "null"), e);
      }
    }
  }

  /**
   * Adds new controlled field to marc record
   *
   * @param recordForUpdate record that needs to be updated
   * @param field           tag of controlled field
   * @param value           value of the field to add
   * @param replace         if true, replaces an existing field with the same tag; if false, appends a new field
   * @return true if succeeded, false otherwise
   */
  public static boolean addControlledFieldToMarcRecord(Record recordForUpdate, String field, String value,
                                                       boolean replace) {
    try {
      addControlledFieldToMarcRecordOrThrow(recordForUpdate, field, value, replace);
      return true;
    } catch (Exception e) {
      LOGGER.warn("addControlledFieldToMarcRecord:: Failed to add additional controlled field {} to record {}",
        field, getRecordId(recordForUpdate), e);
      return false;
    }
  }

  /**
   * Throwing core of {@link #addControlledFieldToMarcRecord(Record, String, String, boolean)}. Same behaviour,
   * except failures - null record/parsed-record/content, an unparseable record, or a failed write-back - raise
   * a {@link MarcContentException} carrying the failure detail instead of being swallowed into a boolean.
   * This lets callers that need the real cause (e.g. {@link #updateLatestTransactionDate}) chain it into their
   * own exception rather than losing it.
   */
  private static void addControlledFieldToMarcRecordOrThrow(Record recordForUpdate, String field, String value,
                                                            boolean replace) {
    if (recordForUpdate == null || recordForUpdate.getParsedRecord() == null
        || recordForUpdate.getParsedRecord().getContent() == null) {
      throw new MarcContentException(format(
        "Cannot add controlled field '%s' to record '%s': record, parsed record, or its content is null",
        field, getRecordId(recordForUpdate)));
    }
    org.marc4j.marc.Record marcRecord = computeMarcRecord(recordForUpdate);
    if (marcRecord == null) {
      throw new MarcContentException(format(
        "Cannot add controlled field '%s' to record '%s': failed to parse the parsed record content",
        field, getRecordId(recordForUpdate)));
    }
    MarcFieldEditor.addOrReplaceControlField(marcRecord, field, value, replace);
    if (!recalculateLeaderAndParsedRecord(recordForUpdate, marcRecord)) {
      throw new MarcContentException(format(
        "Cannot add controlled field '%s' to record '%s': failed to recalculate leader and write back the "
        + "parsed record content", field, getRecordId(recordForUpdate)));
    }
  }

  /**
   * Move 001 tag value to 035 tag
   *
   * @param srcRecord input record to modify
   */
  public static void move001To035(Record srcRecord) {
    String valueFrom001 = getValueFromControlledField(srcRecord, TAG_001);
    if (StringUtils.isNotEmpty(valueFrom001)) {
      String valueFrom003 = getValueFromControlledField(srcRecord, TAG_003);
      String new035Value = mergeFieldsFor035(valueFrom003, valueFrom001);
      if (!isFieldExist(srcRecord, TAG_035, TAG_035_SUB, new035Value)) {
        addDataFieldToMarcRecord(srcRecord, TAG_035, TAG_035_IND, TAG_035_IND, TAG_035_SUB, new035Value);
      }
    }
    removeField(srcRecord, TAG_003);
  }

  public static void normalize035(Record srcRecord) {
    org.marc4j.marc.Record marcRecord = computeMarcRecord(srcRecord);
    if (marcRecord != null && has035SubfieldWithOclcPrefix(marcRecord)) {
      MarcRecordNormalizer.normalize035Field(marcRecord);
      recalculateLeaderAndParsedRecord(srcRecord, marcRecord);
    }
  }

  public static void fill001FieldInMarcRecord(Record marcRecord, String hrId) {
    String valueFrom001 = getValueFromControlledField(marcRecord, TAG_001);
    if (!Strings.CS.equals(hrId, valueFrom001)) {
      removeField(marcRecord, TAG_001);
      if (StringUtils.isNotEmpty(hrId)) {
        addControlledFieldToMarcRecord(marcRecord, TAG_001, hrId, false);
      }
    }
  }

  /**
   * Read value from controlled field in marc record
   *
   * @param srcRecord marc record
   * @param tag       tag to read
   * @return value from field
   */
  public static String getValueFromControlledField(Record srcRecord, String tag) {
    try {
      org.marc4j.marc.Record marcRecord = computeMarcRecord(srcRecord);
      if (marcRecord != null) {
        return MarcFieldEditor.getControlFieldValue(marcRecord, tag);
      }
    } catch (Exception e) {
      LOGGER.warn("getValueFromControlledField:: Failed to read controlled field {} from record {}", tag,
        getRecordId(srcRecord), e);
      return null;
    }
    return null;
  }

  /**
   * Reads the value of a subfield from the first matching data field in a
   * MARC record, identified by {@code tag} and indicator values.
   *
   * @param srcRecord record containing the parsed MARC content to retrieve data
   * @param tag       three-character MARC tag of the data field (must not be
   *                  a control field tag, i.e. must not start with "00")
   * @param ind1      first indicator
   * @param ind2      second indicator
   * @param subfield  subfield code whose data value should be returned
   * @return {@link Optional} containing the data of the first matching
   *   subfield, or an empty {@link Optional} if no matching field or
   *   subfield is found
   * @throws IllegalArgumentException if {@code tag} identifies a control
   *                                  instead of a data field
   */
  public static Optional<String> getValueFromDataField(Record srcRecord, String tag, char ind1, char ind2,
                                                       char subfield) {
    checkForControlField(tag);

    return Optional.ofNullable(computeMarcRecord(srcRecord))
      .map(marcRecord -> MarcFieldEditor.getDataFieldSubfieldValue(marcRecord, tag, ind1, ind2, subfield));
  }

  /**
   * Reads the value of a subfield from the first matching data field in a
   * MARC record, identified by {@code tag} only, disregarding indicator values.
   *
   * @param srcRecord record containing the parsed MARC content to retrieve data
   * @param tag       three-character MARC tag of the data field (must not be
   *                  a control field tag, i.e. must not start with "00")
   * @param subfield  subfield code whose data value should be returned
   * @return {@link Optional} containing the data of the first matching
   *   subfield in any data field with the given tag, or an empty
   *   {@link Optional} if no matching field or subfield is found
   * @throws IllegalArgumentException if {@code tag} identifies a control
   *                                  instead of a data field
   */
  public static Optional<String> getValueFromDataField(Record srcRecord, String tag, char subfield) {
    checkForControlField(tag);

    return Optional.ofNullable(computeMarcRecord(srcRecord))
      .map(marcRecord -> MarcFieldEditor.getDataFieldSubfieldValue(marcRecord, tag, subfield));
  }

  /**
   * Remove field from marc record
   *
   * @param recordForUpdate record that needs to be updated
   * @param fieldName       tag of the field
   * @param subfield        subfield of the field
   * @param value           value of the field
   * @return true if succeeded, false otherwise
   */
  public static boolean removeField(Record recordForUpdate, String fieldName, char subfield, String value) {
    boolean isFieldRemoveSucceed = false;
    try {
      if (recordForUpdate != null && recordForUpdate.getParsedRecord() != null
          && recordForUpdate.getParsedRecord().getContent() != null) {
        org.marc4j.marc.Record marcRecord = computeMarcRecord(recordForUpdate);
        if (marcRecord != null) {
          if (StringUtils.isEmpty(value)) {
            isFieldRemoveSucceed = MarcFieldEditor.removeFirstField(marcRecord, fieldName);
          } else {
            isFieldRemoveSucceed = MarcFieldEditor.removeFieldWithSubfieldValue(marcRecord, fieldName, subfield,
              value);
          }

          if (isFieldRemoveSucceed) {
            isFieldRemoveSucceed = recalculateLeaderAndParsedRecord(recordForUpdate, marcRecord);
          }
        }
      }
    } catch (Exception e) {
      LOGGER.warn("removeField:: Failed to remove controlled field {} from record {}",
        fieldName, getRecordId(recordForUpdate), e);
    }
    return isFieldRemoveSucceed;
  }

  /**
   * remove field from marc record
   *
   * @param recordForUpdate record that needs to be updated
   * @param field           tag of the field
   * @return true if succeeded, false otherwise
   */
  public static boolean removeField(Record recordForUpdate, String field) {
    return removeField(recordForUpdate, field, '\0', null);
  }

  /**
   * Check if record should be filled with specific fields.
   *
   * @param srcRecord - source record.
   * @param instance  - instance.
   * @return - true if filling needed.
   */
  public static boolean isFieldsFillingNeeded(Record srcRecord, Instance instance) {
    var externalIdsHolder = srcRecord != null ? srcRecord.getExternalIdsHolder() : null;
    if (externalIdsHolder == null) {
      return false;
    }
    return isValidIdAndHrid(instance.getId(), instance.getHrid(),
      externalIdsHolder.getInstanceId(), externalIdsHolder.getInstanceHrid());
  }

  /**
   * Adds new data field to marc record
   *
   * @param recordForUpdate record that needs to be updated
   * @param tag             tag of data field
   * @param value           value of the field to add
   * @return true if succeeded, false otherwise
   */
  public static boolean addDataFieldToMarcRecord(Record recordForUpdate, String tag, char ind1, char ind2,
                                                 char subfield, String value) {
    boolean result = false;
    try {
      if (recordForUpdate != null && recordForUpdate.getParsedRecord() != null
          && recordForUpdate.getParsedRecord().getContent() != null) {
        MarcFactory factory = MarcFactory.newInstance();
        org.marc4j.marc.Record marcRecord = computeMarcRecord(recordForUpdate);
        if (marcRecord != null) {
          DataField dataField = factory.newDataField(tag, ind1, ind2);
          dataField.addSubfield(factory.newSubfield(subfield, value));
          MarcFieldEditor.addDataFieldInOrder(marcRecord, dataField);
          result = recalculateLeaderAndParsedRecord(recordForUpdate, marcRecord);
        }
      }
    } catch (Exception e) {
      LOGGER.warn("addDataFieldToMarcRecord:: Failed to add additional data field {} to record {}",
        tag, getRecordId(recordForUpdate), e);
    }
    return result;
  }

  public static String mergeFieldsFor035(String valueFrom003, String valueFrom001) {
    if (isBlank(valueFrom003)) {
      return valueFrom001;
    }
    return "(" + valueFrom003 + ")" + valueFrom001;
  }

  /**
   * Check if data field with the same value exist
   *
   * @param recordForUpdate record that needs to be updated
   * @param tag             tag of data field
   * @param value           value of the field to add
   * @return true if exist
   */
  public static boolean isFieldExist(Record recordForUpdate, String tag, char subfield, String value) {
    if (value == null) {
      // nothing to match against - deliberately "not found", rather than letting the trim() below
      // NPE and get masked as a caught-and-logged "error during the search" false.
      return false;
    }
    try {
      org.marc4j.marc.Record marcRecord = computeMarcRecord(recordForUpdate);
      if (marcRecord != null) {
        return MarcFieldEditor.fieldExists(marcRecord, tag, subfield, value);
      }
    } catch (Exception e) {
      LOGGER.warn("isFieldExist:: Error during the search a field in the record", e);
      return false;
    }
    return false;
  }

  public static void remove035FieldWhenRecordContainsHrId(Record srcRecord) {
    if (Record.RecordType.MARC_BIB.equals(srcRecord.getRecordType())) {
      String hrid = getValueFromControlledField(srcRecord, TAG_001);
      remove035WithActualHrId(srcRecord, hrid);
    }
  }

  public static void remove035WithActualHrId(Record srcRecord, String actualHrId) {
    removeField(srcRecord, TAG_035, TAG_035_SUB, actualHrId);
  }

  /**
   * Move original marc hrId to 035 tag and assign created by inventory hrId into 001 tag
   *
   * @param srcRecord record to update
   * @param hrid      hrid to assign into the 001 tag
   */
  public static void fillHrIdFieldInMarcRecord(Record srcRecord, String hrid) {
    String valueFrom001 = getValueFromControlledField(srcRecord, TAG_001);
    if (!Strings.CS.equals(hrid, valueFrom001)) {
      if (StringUtils.isNotEmpty(valueFrom001)) {
        String originalHrIdPrefix = getValueFromControlledField(srcRecord, TAG_003);
        String originalHrId = mergeFieldsFor035(originalHrIdPrefix, valueFrom001);
        if (!isFieldExist(srcRecord, TAG_035, TAG_035_SUB, originalHrId)) {
          addDataFieldToMarcRecord(srcRecord, TAG_035, TAG_035_IND, TAG_035_IND, TAG_035_SUB, originalHrId);
        }
      }
      removeField(srcRecord, TAG_001);
      if (StringUtils.isNotEmpty(hrid)) {
        addControlledFieldToMarcRecord(srcRecord, TAG_001, hrid, false);
      }
    } else {
      remove035WithActualHrId(srcRecord, hrid);
    }
    removeField(srcRecord, TAG_003);
  }

  /**
   * Take field values from system modified record content while preserving incoming record content`s field order.
   * Put system fields (001, 005) first, regardless of incoming record fields order.
   *
   * @param sourceOrderContent content with incoming record fields order
   * @param systemOrderContent system modified record content with reordered fields
   * @param recordId           id of the record being reordered, for diagnostics if reordering fails
   * @return MARC record parsed content with desired fields order
   */
  public static String reorderMarcRecordFields(String sourceOrderContent, String systemOrderContent,
                                               String recordId) {
    try {
      var parsedContent = OBJECT_MAPPER.readTree(systemOrderContent);
      var fieldsArrayNode = (ArrayNode) parsedContent.path(FIELDS);

      var nodes = toNodeList(fieldsArrayNode);
      var nodes00X = removeAndGetNodesByTagPrefix(nodes, TAG_00X_PREFIX);
      var sourceOrderTags = getSourceFields(sourceOrderContent);
      var reorderedFields = OBJECT_MAPPER.createArrayNode();

      var node001 = removeAndGetNodeByTag(nodes00X, TAG_001);
      if (node001 != null && !node001.isEmpty()) {
        reorderedFields.add(node001);
      }

      var node005 = removeAndGetNodeByTag(nodes00X, TAG_005);
      if (node005 != null && !node005.isEmpty()) {
        reorderedFields.add(node005);
      }

      for (var tag : sourceOrderTags) {
        var nodeTag = tag;
        //loop will add system generated fields that are absent in initial record, preserving their order, f.e. 035
        do {
          var node = tag.startsWith(TAG_00X_PREFIX)
                     ? removeAndGetNodeByTag(nodes00X, tag)
                     : removeFirstNode(nodes);
          if (node != null && !node.isEmpty()) {
            nodeTag = getTagFromNode(node);
            reorderedFields.add(node);
          }
        } while (!tag.equals(nodeTag) && !nodes.isEmpty());
      }

      reorderedFields.addAll(nodes);

      ((ObjectNode) parsedContent).set(FIELDS, reorderedFields);
      return parsedContent.toString();
    } catch (Exception e) {
      LOGGER.error("reorderMarcRecordFields:: Failed to reorder Marc record fields for record '{}', falling back "
        + "to the un-reordered system field order: {}", recordId, e.getMessage(), e);
      return systemOrderContent;
    }
  }

  private static JsonNode removeFirstNode(List<JsonNode> nodes) {
    return nodes.isEmpty() ? null : nodes.removeFirst();
  }

  private static boolean has035SubfieldWithOclcPrefix(org.marc4j.marc.Record marcRecord) {
    return marcRecord.getVariableFields(TAG_035).stream()
      .filter(DataField.class::isInstance)
      .map(DataField.class::cast)
      .flatMap(dataField -> dataField.getSubfields().stream())
      .anyMatch(sf -> sf.getData() != null && sf.getData().trim().startsWith(OCLC_PREFIX));
  }

  /**
   * Recalculates the leader (via a stream-writer round trip) and rewrites the parsed record content for
   * {@code recordForUpdate}, then refreshes the cache entry for the new content.
   *
   * @param recordForUpdate record whose parsed record content should be replaced
   * @param marcRecord      mutated marc4j record to serialize
   * @return true if the leader was recalculated and the record content was updated, false if an error occurred
   */
  private static boolean recalculateLeaderAndParsedRecord(Record recordForUpdate, org.marc4j.marc.Record marcRecord) {
    try (ByteArrayOutputStream os = new ByteArrayOutputStream()) {
      // marcRecord has already been mutated in place by the caller. The cache entry keyed by the record's
      // current (pre-mutation) content string now points at an object whose fields no longer match that key -
      // invalidate it before anyone else can observe the stale mapping, and before we re-key it below.
      String staleContentKey = normalizeContent(recordForUpdate.getParsedRecord().getContent());
      parsedRecordContentCache.invalidate(staleContentKey);
      MarcWriter streamWriter = new MarcStreamWriter(new ByteArrayOutputStream());
      MarcWriter jsonWriter = new MarcJsonWriter(os);
      try (AutoCloseable closeStreamWriter = streamWriter::close; AutoCloseable closeJsonWriter = jsonWriter::close) {
        // use stream writer to recalculate leader
        streamWriter.write(marcRecord);
        jsonWriter.write(marcRecord);

        String parsedContentString = new JsonObject(os.toString()).encode();
        // save parsed content string to cache then set it on the record
        parsedRecordContentCache.put(parsedContentString, marcRecord);
        recordForUpdate.setParsedRecord(recordForUpdate.getParsedRecord().withContent(parsedContentString));
        return true;
      }
    } catch (Exception e) {
      if (isOversizedRecordException(e)) {
        LOGGER.warn("recalculateLeaderAndParsedRecord:: Record {} exceeds the MARC21 99999-byte length limit "
          + "and cannot be serialized", recordForUpdate.getId(), e);
      } else {
        LOGGER.warn("recalculateLeaderAndParsedRecord:: Failed to recalculate leader and parsed record for "
          + "record: {}", recordForUpdate.getId(), e);
      }
      return false;
    }
  }

  /**
   * Detects marc4j's oversized-record failure - {@code MarcStreamWriter} refuses to write a record whose
   * ISO 2709 serialization would exceed the MARC21 99999-byte record-length limit. Checking the exception type
   * first, then the message, keeps this from misclassifying unrelated {@link MarcException}s (e.g. an oversized
   * individual field, which marc4j reports with a different message) as this specific, actionable condition.
   */
  private static boolean isOversizedRecordException(Exception e) {
    return e instanceof MarcException && e.getMessage() != null && e.getMessage().contains("99999 bytes");
  }

  private static MarcReader buildMarcReader(Record srcRecord) {
    String content = normalizeContent(srcRecord.getParsedRecord().getContent());
    return new MarcJsonReader(new ByteArrayInputStream(content.getBytes(StandardCharsets.UTF_8)));
  }

  /**
   * Checks whether field 005 needs to be updated or this field is protected.
   *
   * @param srcRecord         record to check
   * @param mappingParameters mapping parameters
   * @return true for case when field 005 have to updated
   */
  private static boolean isField005NeedToUpdate(Record srcRecord, MappingParameters mappingParameters) {
    boolean needToUpdate = true;
    List<MarcFieldProtectionSetting> fieldProtectionSettings = mappingParameters.getMarcFieldProtectionSettings();
    if (CollectionUtils.isNotEmpty(fieldProtectionSettings)) {
      org.marc4j.marc.Record marcRecord = computeMarcRecord(srcRecord);
      if (marcRecord != null) {
        List<VariableField> variableFields = marcRecord.getVariableFields(TAG_005);
        if (!variableFields.isEmpty()) {
          VariableField field = variableFields.getFirst();
          needToUpdate = isNotProtected(fieldProtectionSettings, (ControlField) field);
        }
      }
    }
    return needToUpdate;
  }

  /**
   * Checks is the control field is protected or not.
   *
   * @param fieldProtectionSettings List of MarcFieldProtectionSettings
   * @param field                   Control field that is being checked
   * @return true for case when control field isn't protected
   */
  private static boolean isNotProtected(List<MarcFieldProtectionSetting> fieldProtectionSettings, ControlField field) {
    return fieldProtectionSettings.stream()
      .filter(setting -> setting.getField().equals(ANY_STRING) || setting.getField().equals(field.getTag()))
      .noneMatch(setting -> setting.getData().equals(ANY_STRING) || setting.getData().equals(field.getData()));
  }

  private static void checkForControlField(String tag) {
    if (Verifier.isControlField(tag)) {
      String msg = INVALID_DATA_FIELD_MSG.formatted(tag);
      LOGGER.warn("getValueFromDataField:: {}", msg);
      throw new IllegalArgumentException(msg);
    }
  }

  private static org.marc4j.marc.Record computeMarcRecord(Record srcRecord) {
    if (srcRecord != null && srcRecord.getParsedRecord() != null && isNotBlank(
      srcRecord.getParsedRecord().getContent().toString())) {
      try {
        var content = normalizeContent(srcRecord.getParsedRecord().getContent());
        return parsedRecordContentCache.get(content);
      } catch (Exception e) {
        LOGGER.warn("computeMarcRecord:: Error during the transformation to marc record", e);
        try {
          MarcReader reader = buildMarcReader(srcRecord);
          if (reader.hasNext()) {
            return reader.next();
          }
        } catch (Exception ex) {
          LOGGER.warn("computeMarcRecord:: Error during the building of MarcReader", ex);
        }
        return null;
      }
    }
    return null;
  }

  /**
   * Canonicalizes parsed record content (as held by {@link ParsedRecord#getContent()}, either a {@link String} or a
   * structured type such as {@link JsonObject}/{@code Map}) into a single canonical JSON string, so that content
   * differing only in whitespace or key order produces the same cache key.
   *
   * @param content parsed record content
   * @return canonicalized content string
   */
  private static String normalizeContent(Object content) {
    return (content instanceof String contentStr ? new JsonObject(contentStr) : JsonObject.mapFrom(content)).encode();
  }

  private static boolean isValidIdAndHrid(String id, String hrid, String externalId, String externalHrid) {
    return isNotEmpty(externalId) && (Objects.equals(id, externalId) && !Objects.equals(hrid, externalHrid));
  }

  private static String getRecordId(Record srcRecord) {
    return srcRecord != null ? srcRecord.getId() : "";
  }

  private static List<JsonNode> toNodeList(ArrayNode fieldsArrayNode) {
    var nodes = new LinkedList<JsonNode>();
    for (var node : fieldsArrayNode) {
      nodes.add(node);
    }
    return nodes;
  }

  private static JsonNode removeAndGetNodeByTag(List<JsonNode> nodes, String tag) {
    var toRemove = nodes.stream()
      .filter(node -> getTagFromNode(node).equals(tag))
      .findFirst();
    toRemove.ifPresent(nodes::remove);
    return toRemove.orElse(null);
  }

  private static List<JsonNode> removeAndGetNodesByTagPrefix(List<JsonNode> nodes, String prefix) {
    var startsWithNodes = new LinkedList<JsonNode>();
    for (JsonNode node : nodes) {
      var nodeTag = getTagFromNode(node);
      if (nodeTag.startsWith(prefix)) {
        startsWithNodes.add(node);
      }
    }

    nodes.removeAll(startsWithNodes);
    return startsWithNodes;
  }

  private static String getTagFromNode(JsonNode node) {
    // an empty field node ({}) has no tag and must never structurally match a real tag lookup, so callers
    // (removeAndGetNodeByTag's equality check, removeAndGetNodesByTagPrefix's startsWith check, and the
    // getSourceFields loop) all correctly treat "" as "never matches" for well-formed input.
    var fieldNames = node.fieldNames();
    return fieldNames.hasNext() ? fieldNames.next() : "";
  }

  private static List<String> getSourceFields(String source) {
    var sourceFields = new ArrayList<String>();
    var remainingFields = new ArrayList<String>();
    var has001 = false;
    try {
      var sourceJson = OBJECT_MAPPER.readTree(source);
      var fieldsNode = sourceJson.get(FIELDS);

      for (JsonNode fieldNode : fieldsNode) {
        var tag = getTagFromNode(fieldNode);
        if (tag.equals(TAG_001)) {
          sourceFields.addFirst(tag);
          has001 = true;
        } else if (tag.equals(TAG_005)) {
          if (!has001) {
            sourceFields.addFirst(tag);
          } else {
            sourceFields.add(1, tag);
          }
        } else {
          remainingFields.add(tag);
        }
      }
      sourceFields.addAll(remainingFields);
    } catch (Exception e) {
      LOGGER.error("An error occurred while parsing source JSON: {}", e.getMessage(), e);
    }
    return sourceFields;
  }
}
