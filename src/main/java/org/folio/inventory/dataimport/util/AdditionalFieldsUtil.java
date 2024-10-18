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
import com.github.benmanes.caffeine.cache.stats.CacheStats;
import io.vertx.core.Context;
import io.vertx.core.Vertx;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ForkJoinPool;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
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

/**
 * Util to work with additional fields
 */
public final class AdditionalFieldsUtil {

  private static final Logger LOGGER = LogManager.getLogger();
  public static final DateTimeFormatter dateTime005Formatter = DateTimeFormatter.ofPattern("yyyyMMddHHmmss.S");
  public static final String TAG_00X_PREFIX = "00";
  public static final String TAG_005 = "005";
  public static final String TAG_999 = "999";
  public static final String TAG_001 = "001";
  private static final String TAG_003 = "003";
  public static final String TAG_035 = "035";
  public static final char TAG_035_SUB = 'a';
  private static final char TAG_035_IND = ' ';
  private static final String ANY_STRING = "*";
  private static final char INDICATOR = 'f';
  public static final char SUBFIELD_I = 'i';
  public static final char SUBFIELD_L = 'l';
  private static final String HR_ID_FIELD = "hrid";
  private static final CacheLoader<String, org.marc4j.marc.Record> parsedRecordContentCacheLoader;
  private static final LoadingCache<String, org.marc4j.marc.Record> parsedRecordContentCache;
  private static final String OCLC = "OCoLC";
  private static final String OCLC_PREFIX = "(OCoLC)";
  private static final ObjectMapper objectMapper = new ObjectMapper();
  public static final String FIELDS = "fields";
  private static final String OCLC_PATTERN = "\\((" + OCLC + ")\\)((ocm|ocn|on)?0*|([a-zA-Z]+)0*)(\\d+\\w*)";

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
   * @param recordForUpdate   record that needs to be updated
   * @param field    field that should contain new subfield
   * @param subfield new subfield to add
   * @param value    value of the subfield to add
   * @return true if succeeded, false otherwise
   */
  public static boolean addFieldToMarcRecord(Record recordForUpdate, String field, char subfield, String value) {
    boolean result = false;
    try (ByteArrayOutputStream os = new ByteArrayOutputStream()) {
      if (recordForUpdate != null && recordForUpdate.getParsedRecord() != null && recordForUpdate.getParsedRecord().getContent() != null) {
        MarcWriter streamWriter = new MarcStreamWriter(new ByteArrayOutputStream());
        MarcJsonWriter jsonWriter = new MarcJsonWriter(os);
        MarcFactory factory = MarcFactory.newInstance();
        org.marc4j.marc.Record marcRecord = computeMarcRecord(recordForUpdate);
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
          recordForUpdate.setParsedRecord(recordForUpdate.getParsedRecord().withContent(parsedContentString));
          result = true;
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
   * @param recordForUpdate            record to update
   * @param mappingParameters mapping parameters
   */
  public static void updateLatestTransactionDate(Record recordForUpdate, MappingParameters mappingParameters) {
    if (isField005NeedToUpdate(recordForUpdate, mappingParameters)) {
      String date = dateTime005Formatter.format(ZonedDateTime.ofInstant(Instant.now(), ZoneId.systemDefault()));
      boolean isLatestTransactionDateUpdated = addControlledFieldToMarcRecord(
        recordForUpdate, TAG_005, date, AdditionalFieldsUtil::replaceOrAddControlledFieldInMarcRecord);
      if (!isLatestTransactionDateUpdated) {
        throw new EventProcessingException(format("Failed to update field '005' to record with id '%s'",
          recordForUpdate != null ? recordForUpdate.getId() : "null"));
      }
    }
  }

  /**
   * Adds new controlled field to marc record
   *
   * @param recordForUpdate record that needs to be updated
   * @param field  tag of controlled field
   * @param value  value of the field to add
   * @return true if succeeded, false otherwise
   */
  public static boolean addControlledFieldToMarcRecord(Record recordForUpdate, String field, String value,
                                                       AddControlledFieldToMarcRecordFunction addFieldFunc) {
    boolean result = false;
    try (ByteArrayOutputStream os = new ByteArrayOutputStream()) {
      if (recordForUpdate != null && recordForUpdate.getParsedRecord() != null && recordForUpdate.getParsedRecord().getContent() != null) {
        MarcWriter streamWriter = new MarcStreamWriter(new ByteArrayOutputStream());
        MarcJsonWriter jsonWriter = new MarcJsonWriter(os);

        org.marc4j.marc.Record marcRecord = computeMarcRecord(recordForUpdate);
        if (marcRecord != null) {
          addFieldFunc.apply(field, value, marcRecord);
          // use stream writer to recalculate leader
          streamWriter.write(marcRecord);
          jsonWriter.write(marcRecord);

          String parsedContentString = new JsonObject(os.toString()).encode();
          // save parsed content string to cache then set it on the record
          parsedRecordContentCache.put(parsedContentString, marcRecord);
          recordForUpdate.setParsedRecord(recordForUpdate.getParsedRecord().withContent(parsedContentString));
          result = true;
        }
      }
    } catch (Exception e) {
      LOGGER.warn("addControlledFieldToMarcRecord:: Failed to add additional controlled field {} to record {}",
        field, getRecordId(recordForUpdate), e);
    }
    return result;
  }

  public static void addControlledFieldToMarcRecord(String field, String value, org.marc4j.marc.Record marcRecord) {
    ControlField dataField = MarcFactory.newInstance().newControlField(field, value);
    marcRecord.addVariableField(dataField);
  }

  public static void replaceOrAddControlledFieldInMarcRecord(String field, String value, org.marc4j.marc.Record marcRecord) {
    var currentField =  (ControlField) marcRecord.getVariableField(field);
    var newControlField = MarcFactory.newInstance().newControlField(field, value);
    if (currentField != null) {
      marcRecord.getControlFields().set(marcRecord.getControlFields().indexOf(currentField), newControlField);
    } else {
      marcRecord.addVariableField(newControlField);
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
    List<Subfield> subfields = get035SubfieldOclcValues(srcRecord, TAG_035);
    if (!subfields.isEmpty()) {
      formatOclc(subfields);
      deduplicateOclc(srcRecord, subfields, TAG_035);
      recalculateLeaderAndParsedRecord(srcRecord);
    }
  }

  private static void formatOclc(List<Subfield> subfields) {
    Pattern pattern = Pattern.compile(OCLC_PATTERN);

    for (Subfield subfield : subfields) {
      String data = subfield.getData().replaceAll("[.\\s]", "");
      Matcher matcher = pattern.matcher(data);
      if (matcher.find()) {
        String oclcTag = matcher.group(1); // "OCoLC"
        String numericAndTrailing = matcher.group(5); // Numeric part and any characters that follow
        String prefix = matcher.group(2); // Entire prefix including letters and potentially leading zeros

        if (prefix != null && (prefix.startsWith("ocm") || prefix.startsWith("ocn") || prefix.startsWith("on"))) {
          // If "ocm" or "ocn", strip entirely from the prefix
          subfield.setData("(" + oclcTag + ")" + numericAndTrailing);
        } else {
          // For other cases, strip leading zeros only from the numeric part
          numericAndTrailing = numericAndTrailing.replaceFirst("^0+", "");
          if (prefix != null) {
            prefix = prefix.replaceAll("\\d+", ""); // Safely remove digits from the prefix if not null
          }
          // Add back any other prefix that might have been included like "tfe"
          subfield.setData("(" + oclcTag + ")" + (prefix != null ? prefix : "") + numericAndTrailing);
        }
      }
    }
  }

  private static void deduplicateOclc(Record srcRecord, List<Subfield> subfields, String tag) {
    List<Subfield> subfieldsToDelete = new ArrayList<>();

    for (Subfield subfield: new ArrayList<>(subfields)) {
      if (subfields.stream().anyMatch(s -> isOclcSubfieldDuplicated(subfield, s))) {
        subfieldsToDelete.add(subfield);
        subfields.remove(subfield);
      }
    }
    Optional.ofNullable(computeMarcRecord(srcRecord)).ifPresent(marcRecord -> {
      List<VariableField> variableFields = marcRecord.getVariableFields(tag);

      subfieldsToDelete.forEach(subfieldToDelete ->
        variableFields.forEach(field -> removeSubfieldIfExist(marcRecord, field, subfieldToDelete)));
    });
  }

  private static boolean isOclcSubfieldDuplicated(Subfield s1, Subfield s2) {
    return s1 != s2 && s1.getData().equals(s2.getData()) && s1.getCode() == s2.getCode();
  }

  private static void removeSubfieldIfExist(org.marc4j.marc.Record marcRecord, VariableField field, Subfield subfieldToDelete) {
    if (field instanceof DataField dataField && dataField.getSubfields().contains(subfieldToDelete)) {
      if (dataField.getSubfields().size() > 1) {
        dataField.removeSubfield(subfieldToDelete);
      } else {
        marcRecord.removeVariableField(dataField);
      }
    }
  }

  private static void recalculateLeaderAndParsedRecord(Record recordForUpdate) {
    try (ByteArrayOutputStream os = new ByteArrayOutputStream()) {
      MarcWriter streamWriter = new MarcStreamWriter(new ByteArrayOutputStream());
      MarcJsonWriter jsonWriter = new MarcJsonWriter(os);
      org.marc4j.marc.Record marcRecord = computeMarcRecord(recordForUpdate);

      if (marcRecord != null) {
        // use stream writer to recalculate leader
        streamWriter.write(marcRecord);
        jsonWriter.write(marcRecord);

        String parsedContentString = new JsonObject(os.toString()).encode();
        // save parsed content string to cache then set it on the record
        parsedRecordContentCache.put(parsedContentString, marcRecord);
        recordForUpdate.setParsedRecord(recordForUpdate.getParsedRecord().withContent(parsedContentString));
      }
    } catch (Exception e) {
      LOGGER.warn("recalculateLeaderAndParsedRecord:: Failed to recalculate leader and parsed record for record: {}", recordForUpdate.getId(), e);
    }
  }

  public static List<Subfield> get035SubfieldOclcValues(Record srcRecord, String tag) {
    return Optional.ofNullable(computeMarcRecord(srcRecord))
      .stream()
      .flatMap(marcRecord -> marcRecord.getVariableFields(tag).stream())
      .flatMap(field -> get035oclcSubfields(field).stream())
      .collect(Collectors.toList());
  }

  private static List<Subfield> get035oclcSubfields(VariableField field) {
    if (field instanceof DataField dataField) {
      return dataField.getSubfields().stream()
        .filter(sf -> sf.getData().startsWith(OCLC_PREFIX))
        .toList();
    }
    return Collections.emptyList();
  }

  public static void fill001FieldInMarcRecord(Record marcRecord, String hrId) {
    String valueFrom001 = getValueFromControlledField(marcRecord, TAG_001);
    if (!StringUtils.equals(hrId, valueFrom001)) {
      removeField(marcRecord, TAG_001);
      if (StringUtils.isNotEmpty(hrId)) {
        addControlledFieldToMarcRecord(marcRecord, TAG_001, hrId, AdditionalFieldsUtil::addControlledFieldToMarcRecord);
      }
    }
  }

  /**
   * Read value from controlled field in marc record
   *
   * @param srcRecord marc record
   * @param tag    tag to read
   * @return value from field
   */
  public static String getValueFromControlledField(Record srcRecord, String tag) {
    try {
      org.marc4j.marc.Record marcRecord = computeMarcRecord(srcRecord);
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
      LOGGER.warn("getValueFromControlledField:: Failed to read controlled field {} from record {}", tag, srcRecord.getId(), e);
      return null;
    }
    return null;
  }

  public static Optional<String> getValue(Record srcRecord, String tag, char subfield) {
      return Optional.ofNullable(computeMarcRecord(srcRecord))
        .stream()
        .flatMap(marcRecord -> marcRecord.getVariableFields(tag).stream())
        .flatMap(field -> getFieldValue(field, subfield).stream())
        .findFirst();
  }

  private static Optional<String> getFieldValue(VariableField field, char subfield) {
    if (field instanceof DataField dataField) {
      return dataField.getSubfields(subfield).stream().findFirst().map(Subfield::getData);
    } else if (field instanceof ControlField controlField) {
      return Optional.ofNullable(controlField.getData());
    } else {
      return Optional.empty();
    }
  }

  private static MarcReader buildMarcReader(Record srcRecord) {
    String content = normalizeContent(srcRecord.getParsedRecord());
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
   * @param srcRecord            record to check
   * @param mappingParameters mapping parameters
   * @return true for case when field 005 have to updated
   */
  private static boolean isField005NeedToUpdate(Record srcRecord, MappingParameters mappingParameters) {
    boolean needToUpdate = true;
    List<MarcFieldProtectionSetting> fieldProtectionSettings = mappingParameters.getMarcFieldProtectionSettings();
    if (CollectionUtils.isNotEmpty(fieldProtectionSettings)) {
      MarcReader reader = new MarcJsonReader(new ByteArrayInputStream(srcRecord.getParsedRecord().getContent().toString().getBytes()));
      if (reader.hasNext()) {
        org.marc4j.marc.Record marcRecord = reader.next();
        List<VariableField> variableFields = marcRecord.getVariableFields(TAG_005);
        if(!variableFields.isEmpty()) {
          VariableField field = variableFields.get(0);
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

  private static org.marc4j.marc.Record computeMarcRecord(Record srcRecord) {
    if (srcRecord != null && srcRecord.getParsedRecord() != null && isNotBlank(srcRecord.getParsedRecord().getContent().toString())) {
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
   * @param recordForUpdate    record that needs to be updated
   * @param fieldName tag of the field
   * @param subfield  subfield of the field
   * @param value     value of the field
   * @return true if succeeded, false otherwise
   */
  public static boolean removeField(Record recordForUpdate, String fieldName, char subfield, String value) {
    boolean isFieldRemoveSucceed = false;
    try (ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
      if (recordForUpdate != null && recordForUpdate.getParsedRecord() != null && recordForUpdate.getParsedRecord().getContent() != null) {
        MarcWriter marcStreamWriter = new MarcStreamWriter(new ByteArrayOutputStream());
        MarcJsonWriter marcJsonWriter = new MarcJsonWriter(baos);
        org.marc4j.marc.Record marcRecord = computeMarcRecord(recordForUpdate);
        if (marcRecord != null) {
          if (StringUtils.isEmpty(value)) {
            isFieldRemoveSucceed = removeFirstFoundFieldByName(marcRecord, fieldName);
          } else {
            isFieldRemoveSucceed = removeFieldByNameAndValue(marcRecord, fieldName, subfield, value);
          }

          if (isFieldRemoveSucceed) {
            // use stream writer to recalculate leader
            marcStreamWriter.write(marcRecord);
            marcJsonWriter.write(marcRecord);

            String parsedContentString = new JsonObject(baos.toString()).encode();
            // save parsed content string to cache then set it on the record
            parsedRecordContentCache.put(parsedContentString, marcRecord);
            recordForUpdate.setParsedRecord(recordForUpdate.getParsedRecord().withContent(parsedContentString));
          }
        }
      }
    } catch (Exception e) {
      LOGGER.warn("removeField:: Failed to remove controlled field {} from record {}",
        fieldName, getRecordId(recordForUpdate), e);
    }
    return isFieldRemoveSucceed;
  }

  private static boolean removeFieldByNameAndValue(org.marc4j.marc.Record marcRecord, String fieldName, char subfield, String value) {
    boolean isFieldFound = false;
    List<VariableField> variableFields = marcRecord.getVariableFields(fieldName);
    for (VariableField variableField : variableFields) {
      if (isFieldContainsValue(variableField, subfield, value)) {
        marcRecord.removeVariableField(variableField);
        isFieldFound = true;
        break;
      }
    }
    return isFieldFound;
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
   * Checks if the field contains a certain value in the selected subfield
   *
   * @param field    from MARC BIB record
   * @param subfield subfield of the field
   * @param value    value of the field
   * @return true if contains, false otherwise
   */
  private static boolean isFieldContainsValue(VariableField field, char subfield, String value) {
    boolean isContains = false;
    if (field instanceof DataField) {
      for (Subfield sub : ((DataField) field).getSubfields(subfield)) {
        if (isNotEmpty(sub.getData()) && sub.getData().contains(value.trim())) {
          isContains = true;
          break;
        }
      }
    }
    return isContains;
  }

  /**
   * remove field from marc record
   *
   * @param recordForUpdate record that needs to be updated
   * @param field  tag of the field
   * @return true if succeeded, false otherwise
   */
  public static boolean removeField(Record recordForUpdate, String field) {
    return removeField(recordForUpdate, field, '\0', null);
  }

  /**
   * Check if record should be filled with specific fields.
   *
   * @param srcRecord   - source record.
   * @param instance - instance.
   * @return - true if filling needed.
   */
  public static boolean isFieldsFillingNeeded(Record srcRecord, Instance instance) {
    var externalIdsHolder = srcRecord.getExternalIdsHolder();
    return isValidIdAndHrid(instance.getId(), instance.getHrid(),
      externalIdsHolder.getInstanceId(), externalIdsHolder.getInstanceHrid());
  }

  private static boolean isValidIdAndHrid(String id, String hrid, String externalId, String externalHrid) {
    return (isNotEmpty(externalId)) && (id.equals(externalId) && !hrid.equals(externalHrid));
  }

  /**
   * Adds new data field to marc record
   *
   * @param recordForUpdate record that needs to be updated
   * @param tag    tag of data field
   * @param value  value of the field to add
   * @return true if succeeded, false otherwise
   */
  public static boolean addDataFieldToMarcRecord(Record recordForUpdate, String tag, char ind1, char ind2, char subfield, String value) {
    boolean result = false;
    try (ByteArrayOutputStream os = new ByteArrayOutputStream()) {
      if (recordForUpdate != null && recordForUpdate.getParsedRecord() != null && recordForUpdate.getParsedRecord().getContent() != null) {
        MarcWriter streamWriter = new MarcStreamWriter(new ByteArrayOutputStream());
        MarcJsonWriter jsonWriter = new MarcJsonWriter(os);
        MarcFactory factory = MarcFactory.newInstance();
        org.marc4j.marc.Record marcRecord = computeMarcRecord(recordForUpdate);
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
          recordForUpdate.setParsedRecord(recordForUpdate.getParsedRecord().withContent(parsedContentString));
          result = true;
        }
      }
    } catch (Exception e) {
      LOGGER.warn("addDataFieldToMarcRecord:: Failed to add additional data field {} to record {}",
        tag, getRecordId(recordForUpdate), e);
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
   * @param tag    tag of data field
   * @param value  value of the field to add
   * @return true if exist
   */
  public static boolean isFieldExist(Record recordForUpdate, String tag, char subfield, String value) {
    try {
      org.marc4j.marc.Record marcRecord = computeMarcRecord(recordForUpdate);
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

  private static String getRecordId(Record srcRecord) {
    return srcRecord != null ? srcRecord.getId() : "";
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
   * @param recordInstancePair pair of related instance and record
   */
  public static void fillHrIdFieldInMarcRecord(Pair<Record, JsonObject> recordInstancePair) {
    String hrid = recordInstancePair.getValue().getString(HR_ID_FIELD);
    String valueFrom001 = AdditionalFieldsUtil.getValueFromControlledField(recordInstancePair.getKey(), TAG_001);
    if (!StringUtils.equals(hrid, valueFrom001)) {
      if (StringUtils.isNotEmpty(valueFrom001)) {
        String originalHrIdPrefix = AdditionalFieldsUtil.getValueFromControlledField(recordInstancePair.getKey(), TAG_003);
        String originalHrId = AdditionalFieldsUtil.mergeFieldsFor035(originalHrIdPrefix, valueFrom001);
        if (!AdditionalFieldsUtil.isFieldExist(recordInstancePair.getKey(), TAG_035, TAG_035_SUB, originalHrId)) {
          AdditionalFieldsUtil.addDataFieldToMarcRecord(recordInstancePair.getKey(), TAG_035, TAG_035_IND, TAG_035_IND, TAG_035_SUB, originalHrId);
        }
      }
      AdditionalFieldsUtil.removeField(recordInstancePair.getKey(), TAG_001);
      if (StringUtils.isNotEmpty(hrid)) {
        AdditionalFieldsUtil.addControlledFieldToMarcRecord(recordInstancePair.getKey(), TAG_001, hrid, AdditionalFieldsUtil::addControlledFieldToMarcRecord);
      }
    } else {
      AdditionalFieldsUtil.remove035WithActualHrId(recordInstancePair.getKey(), hrid);
    }
    AdditionalFieldsUtil.removeField(recordInstancePair.getKey(), TAG_003);
  }

  /**
   * Take field values from system modified record content while preserving incoming record content`s field order.
   * Put system fields (001, 005) first, regardless of incoming record fields order.
   *
   * @param sourceOrderContent content with incoming record fields order
   * @param systemOrderContent system modified record content with reordered fields
   * @return MARC record parsed content with desired fields order
   */
  public static String reorderMarcRecordFields(String sourceOrderContent, String systemOrderContent) {
    try {
      var parsedContent = objectMapper.readTree(systemOrderContent);
      var fieldsArrayNode = (ArrayNode) parsedContent.path(FIELDS);

      var nodes = toNodeList(fieldsArrayNode);
      var nodes00X = removeAndGetNodesByTagPrefix(nodes, TAG_00X_PREFIX);
      var sourceOrderTags = getSourceFields(sourceOrderContent);
      var reorderedFields = objectMapper.createArrayNode();

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
          var node = tag.startsWith(TAG_00X_PREFIX) ? removeAndGetNodeByTag(nodes00X, tag)
            : nodes.isEmpty() ? null : nodes.remove(0);
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
      LOGGER.error("An error occurred while reordering Marc record fields: {}", e.getMessage(), e);
      return systemOrderContent;
    }
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
    for (int i = 0; i < nodes.size(); i++) {
      var nodeTag = getTagFromNode(nodes.get(i));
      if (nodeTag.startsWith(prefix)) {
        startsWithNodes.add(nodes.get(i));
      }
    }

    nodes.removeAll(startsWithNodes);
    return startsWithNodes;
  }

  private static String getTagFromNode(JsonNode node) {
    return node.fieldNames().next();
  }

  private static List<String> getSourceFields(String source) {
    var sourceFields = new ArrayList<String>();
    var remainingFields = new ArrayList<String>();
    var has001 = false;
    try {
      var sourceJson = objectMapper.readTree(source);
      var fieldsNode = sourceJson.get(FIELDS);

      for (JsonNode fieldNode : fieldsNode) {
        var tag = getTagFromNode(fieldNode);
        if (tag.equals(TAG_001)) {
          sourceFields.add(0, tag);
          has001 = true;
        } else if (tag.equals(TAG_005)) {
          if (!has001) {
            sourceFields.add(0, tag);
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
