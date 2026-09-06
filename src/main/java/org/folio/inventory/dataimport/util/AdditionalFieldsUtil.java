package org.folio.inventory.dataimport.util;

import static java.lang.String.format;
import static org.apache.commons.lang3.StringUtils.isBlank;
import static org.apache.commons.lang3.StringUtils.isNotEmpty;

import java.time.Clock;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
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
import org.folio.rest.jaxrs.model.Record;
import org.marc4j.marc.ControlField;
import org.marc4j.marc.DataField;
import org.marc4j.marc.MarcFactory;
import org.marc4j.marc.VariableField;

/**
 * Util to work with additional fields.
 *
 * <p>Thin facade over {@link MarcRecordEditor} (the shared parse -&gt; mutate -&gt; write-back logic, including
 * the parsed-record-content cache) via a {@link JaxrsRecordHolder} adapter, plus the bits of logic that cannot
 * move into the FOLIO-type-agnostic {@link MarcRecordEditor}: {@link #isFieldsFillingNeeded} (needs
 * {@link Instance}), {@link #updateLatestTransactionDate} (needs {@link MappingParameters}/{@link Clock}),
 * {@link #normalize035} (needs {@link MarcRecordNormalizer}), and the pure compositions
 * ({@link #move001To035}, {@link #fill001FieldInMarcRecord}, {@link #fillHrIdFieldInMarcRecord},
 * {@link #remove035WithActualHrId}) that just call the named operations below in sequence.
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
  private static final String OCLC_PREFIX = "(OCoLC)";

  private AdditionalFieldsUtil() {
  }

  public static AdditionalFieldsUtilCacheStats getCacheStats() {
    return MarcRecordEditor.getCacheStats();
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
    return MarcRecordEditor.addFieldToMarcRecord(new JaxrsRecordHolder(recordForUpdate), field, subfield, value);
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
        MarcRecordEditor.addControlledFieldToMarcRecordOrThrow(new JaxrsRecordHolder(recordForUpdate), TAG_005,
          date, true);
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
    return MarcRecordEditor.addControlledFieldToMarcRecord(new JaxrsRecordHolder(recordForUpdate), field, value,
      replace);
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
    JaxrsRecordHolder holder = new JaxrsRecordHolder(srcRecord);
    org.marc4j.marc.Record marcRecord = MarcRecordEditor.computeMarcRecord(holder);
    if (marcRecord != null && has035SubfieldWithOclcPrefix(marcRecord)) {
      MarcRecordNormalizer.normalize035Field(marcRecord);
      MarcRecordEditor.recalculateAndWriteBack(holder, marcRecord);
    }
  }

  /**
   * Runs the update-005 / move-001-to-035 / normalize-035 sequence used by instance create/replace/ingress
   * handlers as a single parse -&gt; mutate -&gt; write-back, instead of three independent round trips through the
   * parsed-record-content cache. Equivalent to calling {@link #updateLatestTransactionDate(Record,
   * MappingParameters, Clock)}, {@link #move001To035(Record)}, and {@link #normalize035(Record)} in sequence.
   *
   * @param targetRecord      record to update
   * @param mappingParameters mapping parameters (for the 005 field-protection check)
   * @param clock             clock used to compute the current date/time written to field 005
   * @throws EventProcessingException if field 005 needed updating but the record could not be parsed or the
   *                                  final write-back failed
   */
  public static void executeStandardFieldsManipulation(Record targetRecord, MappingParameters mappingParameters,
                                                        Clock clock) {
    JaxrsRecordHolder holder = new JaxrsRecordHolder(targetRecord);
    org.marc4j.marc.Record marcRecord = MarcRecordEditor.computeMarcRecord(holder);
    boolean needsDate = isField005NeedToUpdate(marcRecord, mappingParameters);
    if (marcRecord == null) {
      if (needsDate) {
        throw new EventProcessingException(format("Failed to update field '005' to record with id '%s'",
          targetRecord != null ? targetRecord.getId() : "null"));
      }
      return;
    }
    if (needsDate) {
      String date = DATE_TIME_005_FORMATTER.format(ZonedDateTime.now(clock));
      MarcFieldEditor.addOrReplaceControlField(marcRecord, TAG_005, date, true);
    }
    move001To035OnRecord(marcRecord);
    normalize035OnRecord(marcRecord);
    if (!MarcRecordEditor.recalculateAndWriteBack(holder, marcRecord)) {
      throw new EventProcessingException(format("Failed to update field '005' to record with id '%s'",
        targetRecord.getId()));
    }
  }

  /**
   * Runs the update-005 / normalize-035 / remove-035-with-hrid sequence used by
   * {@code ReplaceInstanceEventHandler}'s MARC-source branch as a single parse -&gt; mutate -&gt; write-back.
   * Equivalent to calling {@link #updateLatestTransactionDate(Record, MappingParameters, Clock)},
   * {@link #normalize035(Record)}, and {@link #remove035FieldWhenRecordContainsHrId(Record)} in sequence.
   *
   * @param targetRecord      record to update
   * @param mappingParameters mapping parameters (for the 005 field-protection check)
   * @param clock             clock used to compute the current date/time written to field 005
   * @throws EventProcessingException if field 005 needed updating but the record could not be parsed or the
   *                                  final write-back failed
   */
  public static void executeReplaceFieldsManipulation(Record targetRecord, MappingParameters mappingParameters,
                                                       Clock clock) {
    JaxrsRecordHolder holder = new JaxrsRecordHolder(targetRecord);
    org.marc4j.marc.Record marcRecord = MarcRecordEditor.computeMarcRecord(holder);
    boolean needsDate = isField005NeedToUpdate(marcRecord, mappingParameters);
    if (marcRecord == null) {
      if (needsDate) {
        throw new EventProcessingException(format("Failed to update field '005' to record with id '%s'",
          targetRecord != null ? targetRecord.getId() : "null"));
      }
      return;
    }
    if (needsDate) {
      String date = DATE_TIME_005_FORMATTER.format(ZonedDateTime.now(clock));
      MarcFieldEditor.addOrReplaceControlField(marcRecord, TAG_005, date, true);
    }
    normalize035OnRecord(marcRecord);
    if (Record.RecordType.MARC_BIB.equals(targetRecord.getRecordType())) {
      String hrid = MarcFieldEditor.getControlFieldValue(marcRecord, TAG_001);
      // matches MarcRecordEditor.removeField's branching: remove035WithActualHrId ultimately calls
      // removeField(holder, TAG_035, TAG_035_SUB, actualHrId), which removes the whole first 035 field
      // when actualHrId is empty, rather than trying (and NPE-ing) to match an empty subfield value.
      if (StringUtils.isEmpty(hrid)) {
        MarcFieldEditor.removeFirstField(marcRecord, TAG_035);
      } else {
        MarcFieldEditor.removeFieldWithSubfieldValue(marcRecord, TAG_035, TAG_035_SUB, hrid);
      }
    }
    if (!MarcRecordEditor.recalculateAndWriteBack(holder, marcRecord)) {
      throw new EventProcessingException(format("Failed to update field '005' to record with id '%s'",
        targetRecord.getId()));
    }
  }

  private static void move001To035OnRecord(org.marc4j.marc.Record marcRecord) {
    String valueFrom001 = MarcFieldEditor.getControlFieldValue(marcRecord, TAG_001);
    if (StringUtils.isNotEmpty(valueFrom001)) {
      String valueFrom003 = MarcFieldEditor.getControlFieldValue(marcRecord, TAG_003);
      String new035Value = mergeFieldsFor035(valueFrom003, valueFrom001);
      if (!MarcFieldEditor.fieldExists(marcRecord, TAG_035, TAG_035_SUB, new035Value)) {
        MarcFactory factory = MarcFactory.newInstance();
        DataField dataField = factory.newDataField(TAG_035, TAG_035_IND, TAG_035_IND);
        dataField.addSubfield(factory.newSubfield(TAG_035_SUB, new035Value));
        MarcFieldEditor.addDataFieldInOrder(marcRecord, dataField);
      }
    }
    MarcFieldEditor.removeFirstField(marcRecord, TAG_003);
  }

  private static void normalize035OnRecord(org.marc4j.marc.Record marcRecord) {
    if (has035SubfieldWithOclcPrefix(marcRecord)) {
      MarcRecordNormalizer.normalize035Field(marcRecord);
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
    return MarcRecordEditor.getValueFromControlledField(new JaxrsRecordHolder(srcRecord), tag);
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
    return MarcRecordEditor.getValueFromDataField(new JaxrsRecordHolder(srcRecord), tag, ind1, ind2, subfield);
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
    return MarcRecordEditor.getValueFromDataField(new JaxrsRecordHolder(srcRecord), tag, subfield);
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
    return MarcRecordEditor.removeField(new JaxrsRecordHolder(recordForUpdate), fieldName, subfield, value);
  }

  /**
   * remove field from marc record
   *
   * @param recordForUpdate record that needs to be updated
   * @param field           tag of the field
   * @return true if succeeded, false otherwise
   */
  public static boolean removeField(Record recordForUpdate, String field) {
    return MarcRecordEditor.removeField(new JaxrsRecordHolder(recordForUpdate), field);
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
    return MarcRecordEditor.addDataFieldToMarcRecord(new JaxrsRecordHolder(recordForUpdate), tag, ind1, ind2,
      subfield, value);
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
    return MarcRecordEditor.isFieldExist(new JaxrsRecordHolder(recordForUpdate), tag, subfield, value);
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
      return MarcJsonFieldOrderer.reorderFields(sourceOrderContent, systemOrderContent);
    } catch (Exception e) {
      LOGGER.error("reorderMarcRecordFields:: Failed to reorder Marc record fields for record '{}', falling back "
        + "to the un-reordered system field order: {}", recordId, e.getMessage(), e);
      return systemOrderContent;
    }
  }

  private static boolean has035SubfieldWithOclcPrefix(org.marc4j.marc.Record marcRecord) {
    return marcRecord.getVariableFields(TAG_035).stream()
      .filter(DataField.class::isInstance)
      .map(DataField.class::cast)
      .flatMap(dataField -> dataField.getSubfields().stream())
      .anyMatch(sf -> sf.getData() != null && sf.getData().trim().startsWith(OCLC_PREFIX));
  }

  /**
   * Checks whether field 005 needs to be updated or this field is protected.
   *
   * @param srcRecord         record to check
   * @param mappingParameters mapping parameters
   * @return true for case when field 005 have to updated
   */
  private static boolean isField005NeedToUpdate(Record srcRecord, MappingParameters mappingParameters) {
    return isField005NeedToUpdate(MarcRecordEditor.computeMarcRecord(new JaxrsRecordHolder(srcRecord)),
      mappingParameters);
  }

  /**
   * Checks whether field 005 needs to be updated or this field is protected, given an already-parsed (possibly
   * null) marc4j record. Same defaulting logic as {@link #isField005NeedToUpdate(Record, MappingParameters)}: if
   * {@code marcRecord} is null (record could not be parsed) or {@code fieldProtectionSettings} is empty/null,
   * {@code needToUpdate} stays {@code true}.
   *
   * @param marcRecord        already-parsed marc4j record, or null if the record could not be parsed
   * @param mappingParameters mapping parameters
   * @return true for case when field 005 have to updated
   */
  private static boolean isField005NeedToUpdate(org.marc4j.marc.Record marcRecord,
                                                 MappingParameters mappingParameters) {
    boolean needToUpdate = true;
    List<MarcFieldProtectionSetting> fieldProtectionSettings = mappingParameters.getMarcFieldProtectionSettings();
    if (CollectionUtils.isNotEmpty(fieldProtectionSettings) && marcRecord != null) {
      List<VariableField> variableFields = marcRecord.getVariableFields(TAG_005);
      if (!variableFields.isEmpty()) {
        VariableField field = variableFields.getFirst();
        needToUpdate = isNotProtected(fieldProtectionSettings, (ControlField) field);
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

  private static boolean isValidIdAndHrid(String id, String hrid, String externalId, String externalHrid) {
    return isNotEmpty(externalId) && (Objects.equals(id, externalId) && !Objects.equals(hrid, externalHrid));
  }

}
