package org.folio.inventory.dataimport.util;

import static org.apache.commons.lang3.StringUtils.EMPTY;
import static org.folio.dataimport.util.marc.MarcConstants.FIELD_999;
import static org.folio.dataimport.util.marc.MarcConstants.INDICATOR_F;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import java.util.Objects;
import java.util.Optional;
import org.folio.dataimport.util.marc.MarcContentCodec;
import org.folio.rest.jaxrs.model.ParsedRecord;

public final class ParsedRecordUtil {

  public static final char LEADER_STATUS_DELETED = 'd';
  private static final String LEADER = "leader";
  private static final int LEADER_STATUS_SUBFIELD_POSITION = 5;

  private ParsedRecordUtil() {
  }

  public static String getAdditionalSubfieldValue(ParsedRecord parsedRecord, char subfieldValue) {
    JsonObject parsedContent = MarcContentCodec.canonicalizeJson(parsedRecord.getContent());
    JsonArray fields = parsedContent.getJsonArray("fields");
    if (fields == null) {
      return EMPTY;
    }

    return fields.stream()
      .map(o -> (JsonObject) o)
      .filter(field -> field.containsKey(FIELD_999)
                       && INDICATOR_F == field.getJsonObject(FIELD_999).getString("ind1").charAt(0)
                       && INDICATOR_F == field.getJsonObject(FIELD_999).getString("ind2").charAt(0))
      .flatMap(targetField -> targetField.getJsonObject(FIELD_999).getJsonArray("subfields").stream())
      .map(JsonObject.class::cast)
      .filter(subfield -> subfield.containsKey(String.valueOf(subfieldValue)))
      .findFirst()
      .map(targetSubfield -> targetSubfield.getString(String.valueOf(subfieldValue)))
      .orElse(EMPTY);
  }

  /**
   * Retrieves the leader status from the given ParsedRecord.
   *
   * @param parsedRecord the ParsedRecord object containing MARC data
   * @return an Optional containing the leader status character at the
   *   specified position, or an empty Optional if not found
   */
  public static Optional<Character> getLeaderStatus(ParsedRecord parsedRecord) {
    if (Objects.nonNull(parsedRecord)) {
      JsonObject marcJson = MarcContentCodec.canonicalizeJson(parsedRecord.getContent());
      String leader = marcJson.getString(LEADER);
      if (Objects.nonNull(leader) && leader.length() > LEADER_STATUS_SUBFIELD_POSITION) {
        return Optional.of(leader.charAt(LEADER_STATUS_SUBFIELD_POSITION));
      }
    }
    return Optional.empty();
  }

  /**
   * Update MARC Leader status 05 for the given {@link ParsedRecord} content.
   *
   * @param parsedRecord parsedRecord parsed record
   * @param status       new MARC Leader status
   */
  public static void updateLeaderStatus(ParsedRecord parsedRecord, Character status) {
    if (Objects.isNull(parsedRecord) || Objects.isNull(parsedRecord.getContent()) || Objects.isNull(status)) {
      return;
    }

    JsonObject marcJson = MarcContentCodec.canonicalizeJson(parsedRecord.getContent());
    String leader = marcJson.getString(LEADER);
    if (Objects.nonNull(leader) && leader.length() > LEADER_STATUS_SUBFIELD_POSITION) {
      StringBuilder builder = new StringBuilder(leader);
      builder.setCharAt(LEADER_STATUS_SUBFIELD_POSITION, status);
      marcJson.put(LEADER, builder.toString());
      parsedRecord.setContent(marcJson.encode());
    }
  }
}
