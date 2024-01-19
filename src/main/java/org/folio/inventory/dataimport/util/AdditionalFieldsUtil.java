package org.folio.inventory.dataimport.util;

import com.github.benmanes.caffeine.cache.CacheLoader;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import com.github.benmanes.caffeine.cache.stats.CacheStats;
import io.vertx.core.Context;
import io.vertx.core.Vertx;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.inventory.domain.instances.Instance;
import org.folio.processing.exceptions.EventProcessingException;
import org.folio.processing.mapping.defaultmapper.processor.parameters.MappingParameters;
import org.folio.rest.jaxrs.model.MarcFieldProtectionSetting;
import org.folio.rest.jaxrs.model.ParsedRecord;
import org.folio.rest.jaxrs.model.Record;
import org.marc4j.MarcJsonReader;
import org.marc4j.MarcJsonWriter;
import org.marc4j.MarcReader;
import org.marc4j.MarcStreamWriter;
import org.marc4j.MarcWriter;
import org.marc4j.marc.ControlField;
import org.marc4j.marc.DataField;
import org.marc4j.marc.MarcFactory;
import org.marc4j.marc.Subfield;
import org.marc4j.marc.VariableField;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ForkJoinPool;

import static java.lang.String.format;
import static org.apache.commons.lang3.StringUtils.isBlank;
import static org.apache.commons.lang3.StringUtils.isNotBlank;
import static org.apache.commons.lang3.StringUtils.isNotEmpty;

/**
 * Util to work with additional fields
 */
public final class AdditionalFieldsUtil {

  private static final Logger LOGGER = LogManager.getLogger();
  public static final DateTimeFormatter dateTime005Formatter = DateTimeFormatter.ofPattern("yyyyMMddHHmmss.S");
  public static final String TAG_005 = "005";
  public static final String TAG_999 = "999";
  public static final String TAG_001 = "001";
  private static final String TAG_003 = "003";
  private static final String TAG_035 = "035";
  private static final char TAG_035_SUB = 'a';
  private static final char TAG_035_IND = ' ';
  private static final String ANY_STRING = "*";
  private static final char INDICATOR = 'f';
  private static final CacheLoader<String, org.marc4j.marc.Record> parsedRecordContentCacheLoader;
  private static final LoadingCache<String, org.marc4j.marc.Record> parsedRecordContentCache;

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
      Caffeine.newBuilder().maximumSize(2000)
        // weak keys allows parsed content strings that are used as keys to be garbage
        // collected, even it is still
        // referenced by the cache.
        .weakKeys()
        .recordStats()
        .executor(
          serviceExecutor -> {
            // Due to the static nature and the API of this AdditionalFieldsUtil class, it is difficult to
            // pass a vertx instance or assume whether a call to any of its static methods here is by a Vertx
            // thread or a regular thread. The logic before is able to discern the type of thread and execute
            // cache operations using the appropriate threading model.
            Context context = Vertx.currentContext();
            if (context != null) {
              context.runOnContext(ar -> serviceExecutor.run());
            } else {
              // The common pool below is used because it is the  default executor for caffeine
              ForkJoinPool.commonPool().execute(serviceExecutor);
            }
          })
        .build(parsedRecordContentCacheLoader);
  }

  private AdditionalFieldsUtil() {
  }

  @FunctionalInterface
  public interface AddControlledFieldToMarcRecordFunction {
    void apply(String field, String value, org.marc4j.marc.Record marcRecord);
  }

  public static CacheStats getCacheStats() {
    return parsedRecordContentCache.stats();
  }

  /**
   * Adds field if it does not exist and a subfield with a value to that field
   *
   * @param record   record that needs to be updated
   * @param field    field that should contain new subfield
   * @param subfield new subfield to add
   * @param value    value of the subfield to add
   * @return true if succeeded, false otherwise
   */
  public static boolean addFieldToMarcRecord(Record record, String field, char subfield, String value) {
    boolean result = false;
    try (ByteArrayOutputStream os = new ByteArrayOutputStream()) {
      if (record != null && record.getParsedRecord() != null && record.getParsedRecord().getContent() != null) {
        MarcWriter streamWriter = new MarcStreamWriter(new ByteArrayOutputStream());
        MarcJsonWriter jsonWriter = new MarcJsonWriter(os);
        MarcFactory factory = MarcFactory.newInstance();
        org.marc4j.marc.Record marcRecord = computeMarcRecord(record);
        if (marcRecord != null) {
          VariableField variableField = getSingleFieldByIndicators(marcRecord.getVariableFields(field));
          DataField dataField;
          if (variableField != null
            && ((DataField) variableField).getIndicator1() == INDICATOR
            && ((DataField) variableField).getIndicator2() == INDICATOR
          ) {
            dataField = (DataField) variableField;
            marcRecord.removeVariableField(variableField);
            dataField.removeSubfield(dataField.getSubfield(subfield));
          } else {
            dataField = factory.newDataField(field, INDICATOR, INDICATOR);
          }
          dataField.addSubfield(factory.newSubfield(subfield, value));
          marcRecord.addVariableField(dataField);
          // use stream writer to recalculate leader
          streamWriter.write(marcRecord);
          jsonWriter.write(marcRecord);

          String parsedContentString = new JsonObject(os.toString()).encode();
          // save parsed content string to cache then set it on the record
          parsedRecordContentCache.put(parsedContentString, marcRecord);
          record.setParsedRecord(record.getParsedRecord().withContent(parsedContentString));
          result = true;
        }
      }
    } catch (Exception e) {
      LOGGER.warn("addFieldToMarcRecord:: Failed to add additional subfield {} for field {} to record {}",
        subfield, field, getRecordId(record), e);
    }
    return result;
  }

  /**
   * Updates field 005 for case when this field is not protected.
   *
   * @param record            record to update
   * @param mappingParameters mapping parameters
   */
  public static void updateLatestTransactionDate(Record record, MappingParameters mappingParameters) {
    if (isField005NeedToUpdate(record, mappingParameters)) {
      String date = dateTime005Formatter.format(ZonedDateTime.ofInstant(Instant.now(), ZoneId.systemDefault()));
      boolean isLatestTransactionDateUpdated = addControlledFieldToMarcRecord(
        record, TAG_005, date, AdditionalFieldsUtil::replaceControlledFieldInMarcRecord);
      if (!isLatestTransactionDateUpdated) {
        throw new EventProcessingException(format("Failed to update field '005' to record with id '%s'",
          record != null ? record.getId() : "null"));
      }
    }
  }

  /**
   * Adds new controlled field to marc record
   *
   * @param record record that needs to be updated
   * @param field  tag of controlled field
   * @param value  value of the field to add
   * @return true if succeeded, false otherwise
   */
  public static boolean addControlledFieldToMarcRecord(Record record, String field, String value,
                                                       AddControlledFieldToMarcRecordFunction addFieldFunc) {
    boolean result = false;
    try (ByteArrayOutputStream os = new ByteArrayOutputStream()) {
      if (record != null && record.getParsedRecord() != null && record.getParsedRecord().getContent() != null) {
        MarcWriter streamWriter = new MarcStreamWriter(new ByteArrayOutputStream());
        MarcJsonWriter jsonWriter = new MarcJsonWriter(os);

        org.marc4j.marc.Record marcRecord = computeMarcRecord(record);
        if (marcRecord != null) {
          addFieldFunc.apply(field, value, marcRecord);
          // use stream writer to recalculate leader
          streamWriter.write(marcRecord);
          jsonWriter.write(marcRecord);

          String parsedContentString = new JsonObject(os.toString()).encode();
          // save parsed content string to cache then set it on the record
          parsedRecordContentCache.put(parsedContentString, marcRecord);
          record.setParsedRecord(record.getParsedRecord().withContent(parsedContentString));
          result = true;
        }
      }
    } catch (Exception e) {
      LOGGER.warn("addControlledFieldToMarcRecord:: Failed to add additional controlled field {} to record {}",
        field, getRecordId(record), e);
    }
    return result;
  }

  public static void addControlledFieldToMarcRecord(String field, String value, org.marc4j.marc.Record marcRecord) {
    ControlField dataField = MarcFactory.newInstance().newControlField(field, value);
    marcRecord.addVariableField(dataField);
  }

  public static void replaceControlledFieldInMarcRecord(String field, String value, org.marc4j.marc.Record marcRecord) {
    ControlField currentField = (ControlField) marcRecord.getVariableField(field);
    if (currentField != null) {
      ControlField newControlField = MarcFactory.newInstance().newControlField(field, value);
      marcRecord.getControlFields().set(marcRecord.getControlFields().indexOf(currentField), newControlField);
    }
  }

  /**
   * Move 001 tag value to 035 tag
   *
   * @param record input record to modify
   */
  public static void move001To035(Record record) {
    String valueFrom001 = getValueFromControlledField(record, TAG_001);
    if (StringUtils.isNotEmpty(valueFrom001)) {
      String valueFrom003 = getValueFromControlledField(record, TAG_003);
      String new035Value = mergeFieldsFor035(valueFrom003, valueFrom001);
      if (!isFieldExist(record, TAG_035, TAG_035_SUB, new035Value)) {
        addDataFieldToMarcRecord(record, TAG_035, TAG_035_IND, TAG_035_IND, TAG_035_SUB, new035Value);
      }
    }
    removeField(record, TAG_003);
  }

  public static void fill001FieldInMarcRecord(Record record, String hrId) {
    String valueFrom001 = getValueFromControlledField(record, TAG_001);
    if (!StringUtils.equals(hrId, valueFrom001)) {
      removeField(record, TAG_001);
      if (StringUtils.isNotEmpty(hrId)) {
        addControlledFieldToMarcRecord(record, TAG_001, hrId, AdditionalFieldsUtil::addControlledFieldToMarcRecord);
      }
    }
  }

  /**
   * Read value from controlled field in marc record
   *
   * @param record marc record
   * @param tag    tag to read
   * @return value from field
   */
  public static String getValueFromControlledField(Record record, String tag) {
    try {
      org.marc4j.marc.Record marcRecord = computeMarcRecord(record);
      if (marcRecord != null) {
        Optional<ControlField> controlField = marcRecord.getControlFields()
          .stream()
          .filter(field -> field.getTag().equals(tag))
          .findFirst();
        if (controlField.isPresent()) {
          return controlField.get().getData();
        }
      }
    } catch (Exception e) {
      LOGGER.warn("getValueFromControlledField:: Failed to read controlled field {} from record {}", tag, record.getId(), e);
      return null;
    }
    return null;
  }

  private static MarcReader buildMarcReader(Record record) {
    String content = normalizeContent(record.getParsedRecord());
    return new MarcJsonReader(new ByteArrayInputStream(content.getBytes(StandardCharsets.UTF_8)));
  }

  private static VariableField getSingleFieldByIndicators(List<VariableField> list) {
    if (CollectionUtils.isEmpty(list)) {
      return null;
    }
    return list.stream()
      .filter(f -> ((DataField) f).getIndicator1() == INDICATOR && ((DataField) f).getIndicator2() == INDICATOR)
      .findFirst()
      .orElse(null);
  }

  /**
   * Checks whether field 005 needs to be updated or this field is protected.
   *
   * @param record            record to check
   * @param mappingParameters mapping parameters
   * @return true for case when field 005 have to updated
   */
  private static boolean isField005NeedToUpdate(Record record, MappingParameters mappingParameters) {
    boolean needToUpdate = true;
    List<MarcFieldProtectionSetting> fieldProtectionSettings = mappingParameters.getMarcFieldProtectionSettings();
    if (CollectionUtils.isNotEmpty(fieldProtectionSettings)) {
      MarcReader reader = new MarcJsonReader(new ByteArrayInputStream(record.getParsedRecord().getContent().toString().getBytes()));
      if (reader.hasNext()) {
        org.marc4j.marc.Record marcRecord = reader.next();
        VariableField field = marcRecord.getVariableFields(TAG_005).get(0);
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

  private static org.marc4j.marc.Record computeMarcRecord(Record record) {
    if (record != null && record.getParsedRecord() != null && isNotBlank(record.getParsedRecord().getContent().toString())) {
      try {
        var content = normalizeContent(record.getParsedRecord().getContent());
        return parsedRecordContentCache.get(content);
      } catch (Exception e) {
        LOGGER.warn("computeMarcRecord:: Error during the transformation to marc record", e);
        try {
          MarcReader reader = buildMarcReader(record);
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
   * Normalize parsed record content of {@link ParsedRecord} is type {@link String}
   *
   * @param parsedRecord parsed record
   * @return parsed record normalized content
   */
  private static String normalizeContent(ParsedRecord parsedRecord) {
    Object content = parsedRecord.getContent();
    return (content instanceof String contentStr ? new JsonObject(contentStr) : JsonObject.mapFrom(content)).encode();
  }

  private static String normalizeContent(Object content) {
    return content instanceof String contentStr ? contentStr : Json.encode(content);
  }

  /**
   * Remove field from marc record
   *
   * @param record    record that needs to be updated
   * @param fieldName tag of the field
   * @return true if succeeded, false otherwise
   */
  public static boolean removeField(Record record, String fieldName) {
    boolean isFieldRemoveSucceed = false;
    try (ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
      if (record != null && record.getParsedRecord() != null && record.getParsedRecord().getContent() != null) {
        MarcWriter marcStreamWriter = new MarcStreamWriter(new ByteArrayOutputStream());
        MarcJsonWriter marcJsonWriter = new MarcJsonWriter(baos);
        org.marc4j.marc.Record marcRecord = computeMarcRecord(record);
        if (marcRecord != null) {
          isFieldRemoveSucceed = removeFirstFoundFieldByName(marcRecord, fieldName);
          if (isFieldRemoveSucceed) {
            // use stream writer to recalculate leader
            marcStreamWriter.write(marcRecord);
            marcJsonWriter.write(marcRecord);

            String parsedContentString = new JsonObject(baos.toString()).encode();
            // save parsed content string to cache then set it on the record
            parsedRecordContentCache.put(parsedContentString, marcRecord);
            record.setParsedRecord(record.getParsedRecord().withContent(parsedContentString));
          }
        }
      }
    } catch (Exception e) {
      LOGGER.warn("removeField:: Failed to remove controlled field {} from record {}",
        fieldName, getRecordId(record), e);
    }
    return isFieldRemoveSucceed;
  }

  private static boolean removeFirstFoundFieldByName(org.marc4j.marc.Record marcRecord, String fieldName) {
    boolean isFieldFound = false;
    VariableField variableField = marcRecord.getVariableField(fieldName);
    if (variableField != null) {
      marcRecord.removeVariableField(variableField);
      isFieldFound = true;
    }
    return isFieldFound;
  }

  /**
   * Check if record should be filled with specific fields.
   *
   * @param record   - source record.
   * @param instance - instance.
   * @return - true if filling needed.
   */
  public static boolean isFieldsFillingNeeded(Record record, Instance instance) {
    var externalIdsHolder = record.getExternalIdsHolder();
    return isValidIdAndHrid(instance.getId(), instance.getHrid(),
      externalIdsHolder.getInstanceId(), externalIdsHolder.getInstanceHrid());
  }

  private static boolean isValidIdAndHrid(String id, String hrid, String externalId, String externalHrid) {
    return (isNotEmpty(externalId) && isNotEmpty(externalHrid)) && (id.equals(externalId) && !hrid.equals(externalHrid));
  }

  /**
   * Adds new data field to marc record
   *
   * @param record record that needs to be updated
   * @param tag    tag of data field
   * @param value  value of the field to add
   * @return true if succeeded, false otherwise
   */
  public static boolean addDataFieldToMarcRecord(Record record, String tag, char ind1, char ind2, char subfield, String value) {
    boolean result = false;
    try (ByteArrayOutputStream os = new ByteArrayOutputStream()) {
      if (record != null && record.getParsedRecord() != null && record.getParsedRecord().getContent() != null) {
        MarcWriter streamWriter = new MarcStreamWriter(new ByteArrayOutputStream());
        MarcJsonWriter jsonWriter = new MarcJsonWriter(os);
        MarcFactory factory = MarcFactory.newInstance();
        org.marc4j.marc.Record marcRecord = computeMarcRecord(record);
        if (marcRecord != null) {
          DataField dataField = factory.newDataField(tag, ind1, ind2);
          dataField.addSubfield(factory.newSubfield(subfield, value));
          addDataFieldInNumericalOrder(dataField, marcRecord);
          // use stream writer to recalculate leader
          streamWriter.write(marcRecord);
          jsonWriter.write(marcRecord);

          String parsedContentString = new JsonObject(os.toString()).encode();
          // save parsed content string to cache then set it on the record
          parsedRecordContentCache.put(parsedContentString, marcRecord);
          record.setParsedRecord(record.getParsedRecord().withContent(parsedContentString));
          result = true;
        }
      }
    } catch (Exception e) {
      LOGGER.warn("addDataFieldToMarcRecord:: Failed to add additional data field {} to record {}",
        tag, getRecordId(record), e);
    }
    return result;
  }

  private static void addDataFieldInNumericalOrder(DataField field, org.marc4j.marc.Record marcRecord) {
    String tag = field.getTag();
    List<DataField> dataFields = marcRecord.getDataFields();
    for (int i = 0; i < dataFields.size(); i++) {
      if (dataFields.get(i).getTag().compareTo(tag) > 0) {
        marcRecord.getDataFields().add(i, field);
        return;
      }
    }
    marcRecord.addVariableField(field);
  }

  private static String mergeFieldsFor035(String valueFrom003, String valueFrom001) {
    if (isBlank(valueFrom003)) {
      return valueFrom001;
    }
    return "(" + valueFrom003 + ")" + valueFrom001;
  }

  /**
   * Check if data field with the same value exist
   *
   * @param record record that needs to be updated
   * @param tag    tag of data field
   * @param value  value of the field to add
   * @return true if exist
   */
  public static boolean isFieldExist(Record record, String tag, char subfield, String value) {
    try {
      org.marc4j.marc.Record marcRecord = computeMarcRecord(record);
      if (marcRecord != null) {
        for (VariableField field : marcRecord.getVariableFields(tag)) {
          if (field instanceof DataField dataField) {
            for (Subfield sub : dataField.getSubfields(subfield)) {
              if (isNotEmpty(sub.getData()) && sub.getData().equals(value.trim())) {
                return true;
              }
            }
          } else if (field instanceof ControlField controlField
            && isNotEmpty(controlField.getData())
            && ((ControlField) field).getData().equals(value.trim())) {
            return true;
          }
        }
      }
    } catch (Exception e) {
      LOGGER.warn("isFieldExist:: Error during the search a field in the record", e);
      return false;
    }
    return false;
  }

  private static String getRecordId(Record record) {
    return record != null ? record.getId() : "";
  }
}
