package org.folio.inventory.dataimport.util;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.folio.rest.jaxrs.model.ParsedRecord;

import static org.apache.commons.lang3.StringUtils.EMPTY;

public final class ParsedRecordUtil {

  public static final String TAG_999 = "999";
  public static final String INDICATOR_F = "f";

  private ParsedRecordUtil() {
  }

  public static String getAdditionalSubfieldValue(ParsedRecord parsedRecord, AdditionalSubfields additionalSubfield) {
    JsonObject parsedContent = new JsonObject(parsedRecord.getContent().toString());
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
      .map(subfieldAsObject -> (JsonObject) subfieldAsObject)
      .filter(subfield -> subfield.containsKey(additionalSubfield.subfieldCode))
      .findFirst()
      .map(targetSubfield -> targetSubfield.getString(additionalSubfield.subfieldCode))
      .orElse(EMPTY);
  }

  public static enum AdditionalSubfields {
    H("h");

    private String subfieldCode;

    AdditionalSubfields(String subfieldCode) {
      this.subfieldCode = subfieldCode;
    }
  }
}
