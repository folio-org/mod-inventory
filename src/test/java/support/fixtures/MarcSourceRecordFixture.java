package support.fixtures;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

public class MarcSourceRecordFixture {

  public static JsonObject buildMarcSourceRecord(UUID holdingsId) {
    return buildMarcSourceRecord(holdingsId, " ", " ");
  }

  public static JsonObject buildMarcSourceRecord(UUID holdingsId, String ind1, String ind2) {
    final var srsId = UUID.randomUUID();
    return new JsonObject()
      .put("id", srsId.toString())
      .put("snapshotId", UUID.randomUUID().toString())
      .put("matchedId", srsId.toString())
      .put("recordType", "MARC_HOLDING")
      .put("externalIdsHolder", new JsonObject().put("holdingsId", holdingsId.toString()))
      .put("parsedRecord", new JsonObject()
        .put("id", srsId.toString())
        .put("content", new JsonObject()
          .put("leader", "00000nu  a2200000   4500")
          .put("fields", new JsonArray()
            .add(new JsonObject().put("001", holdingsId.toString()))
            .add(new JsonObject().put("852", new JsonObject()
              .put("subfields", new JsonArray()
                .add(new JsonObject().put("b", "OLD_LOCATION_CODE"))
                .add(new JsonObject().put("h", "Some call number")))
              .put("ind1", ind1)
              .put("ind2", ind2))))
        )
      );
  }

  public static List<String> getField852bValues(JsonObject parsedContent) {
    List<String> values = new ArrayList<>();
    JsonArray fields = parsedContent.getJsonArray("fields", new JsonArray());
    for (int i = 0; i < fields.size(); i++) {
      JsonObject field = fields.getJsonObject(i);
      if (field == null || !field.containsKey("852")) {
        continue;
      }

      JsonObject dataField = field.getJsonObject("852");
      JsonArray subfields = dataField.getJsonArray("subfields", new JsonArray());
      for (int j = 0; j < subfields.size(); j++) {
        JsonObject subfield = subfields.getJsonObject(j);
        if (subfield != null && subfield.containsKey("b")) {
          values.add(subfield.getString("b"));
        }
      }
    }
    return values;
  }

  public static JsonObject getParsedContent(JsonObject parsedRecord) {
    Object content = parsedRecord.getValue("content");
    if (content instanceof JsonObject contentJson) {
      return contentJson;
    }
    if (content instanceof String contentString) {
      return new JsonObject(contentString);
    }
    throw new IllegalStateException(
      "Unexpected parsedRecord.content type: " + (content == null ? "null" : content.getClass().getName()));
  }
}
