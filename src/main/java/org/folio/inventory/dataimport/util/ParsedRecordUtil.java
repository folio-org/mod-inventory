package org.folio.inventory.dataimport.util;

import static org.apache.commons.lang3.StringUtils.EMPTY;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.util.Objects;
import java.util.Optional;
import org.folio.rest.jaxrs.model.ParsedRecord;
import org.folio.rest.jaxrs.model.Record;
import org.marc4j.MarcJsonReader;
import org.marc4j.MarcReader;
import org.marc4j.marc.ControlField;

public final class ParsedRecordUtil {

  public static final String TAG_999 = "999";
  public static final String INDICATOR_F = "f";
  public static final char LEADER_STATUS_DELETED = 'd';
  private static final String LEADER = "leader";
  private static final int LEADER_STATUS_SUBFIELD_POSITION = 5;

  private ParsedRecordUtil() {
  }

  public static String getAdditionalSubfieldValue(ParsedRecord parsedRecord, AdditionalSubfields additionalSubfield) {
    JsonObject parsedContent = normalize(parsedRecord.getContent());
    JsonArray fields = parsedContent.getJsonArray("fields");
    if (fields == null) {
      return EMPTY;
    }

    return fields.stream()
      .map(o -> (JsonObject) o)
      .filter(field -> field.containsKey(TAG_999)
                       && INDICATOR_F.equals(field.getJsonObject(TAG_999).getString("ind1"))
                       && INDICATOR_F.equals(field.getJsonObject(TAG_999).getString("ind2")))
      .flatMap(targetField -> targetField.getJsonObject(TAG_999).getJsonArray("subfields").stream())
      .map(JsonObject.class::cast)
      .filter(subfield -> subfield.containsKey(additionalSubfield.subfieldCode))
      .findFirst()
      .map(targetSubfield -> targetSubfield.getString(additionalSubfield.subfieldCode))
      .orElse(EMPTY);
  }

  public static JsonObject normalize(Object content) {
    return (content instanceof String s)
           ? new JsonObject(s)
           : JsonObject.mapFrom(content);
  }

  /**
   * Extracts value from specified field
   *
   * @param dataRecord record
   * @param tag    tag of data field
   * @return value from the specified field, or null
   */
  public static String getControlFieldValue(Record dataRecord, String tag) {
    if (dataRecord != null && dataRecord.getParsedRecord() != null && dataRecord.getParsedRecord().getContent() != null) {
      MarcReader reader = buildMarcReader(dataRecord);
      try {
        if (reader.hasNext()) {
          org.marc4j.marc.Record marcRecord = reader.next();
          return marcRecord.getControlFields().stream()
            .filter(controlField -> controlField.getTag().equals(tag))
            .findFirst()
            .map(ControlField::getData)
            .orElse(null);
        }
      } catch (Exception e) {
        return null;
      }
    }
    return null;
  }

  /**
   * Retrieves the leader status from the given ParsedRecord.
   *
   * @param parsedRecord the ParsedRecord object containing MARC data
   * @return an Optional containing the leader status character at the specified position, or an empty Optional if not found
   */
  public static Optional<Character> getLeaderStatus(ParsedRecord parsedRecord) {
    if (Objects.nonNull(parsedRecord)) {
      JsonObject marcJson = normalize(parsedRecord.getContent());
      String leader = marcJson.getString(LEADER);
      if (Objects.nonNull(leader) && leader.length() > LEADER_STATUS_SUBFIELD_POSITION) {
        return Optional.of(leader.charAt(LEADER_STATUS_SUBFIELD_POSITION));
      }
    }
    return Optional.empty();
  }

  /**
   * Update MARC Leader status 05 for the given {@link ParsedRecord} content
   *
   * @param parsedRecord parsedRecord parsed record
   * @param status       new MARC Leader status
   */
  public static void updateLeaderStatus(ParsedRecord parsedRecord, Character status) {
    if (Objects.isNull(parsedRecord) || Objects.isNull(parsedRecord.getContent()) || Objects.isNull(status)) {
      return;
    }

    JsonObject marcJson = normalize(parsedRecord.getContent());
    String leader = marcJson.getString(LEADER);
    if (Objects.nonNull(leader) && leader.length() > LEADER_STATUS_SUBFIELD_POSITION) {
      StringBuilder builder = new StringBuilder(leader);
      builder.setCharAt(LEADER_STATUS_SUBFIELD_POSITION, status);
      marcJson.put(LEADER, builder.toString());
      parsedRecord.setContent(marcJson.encode());
    }
  }

  private static MarcReader buildMarcReader(Record dataRecord) {
    return new MarcJsonReader(
      new ByteArrayInputStream(dataRecord.getParsedRecord().getContent().toString().getBytes(StandardCharsets.UTF_8)));
  }

  public enum AdditionalSubfields {
    H("h"), I("i");

    private final String subfieldCode;

    AdditionalSubfields(String subfieldCode) {
      this.subfieldCode = subfieldCode;
    }
  }
}
