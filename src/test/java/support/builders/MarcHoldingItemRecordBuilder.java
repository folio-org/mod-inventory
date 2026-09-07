package support.builders;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

/**
 * Builds MARC holding/item parsed-record content JSON strings for unit tests.
 *
 * <p>Covers the family of simple bib records used by CreateHoldingEventHandlerTest,
 * UpdateHoldingEventHandlerTest, CreateItemEventHandlerTest, and UpdateItemEventHandlerTest.
 * All share a common structure: a fixed leader and 001, with optional 945 data fields and
 * an optional 999 ff field carrying either an instance id (subfield "i") or a holdings id
 * (subfield "h").
 */
public final class MarcHoldingItemRecordBuilder {

  public static final String INSTANCE_ID = "957985c6-97e3-4038-b0e7-343ecd0b8120";

  private static final String DEFAULT_LEADER = "01314nam  22003851a 4500";

  private String leader = DEFAULT_LEADER;
  private final JsonArray fields = new JsonArray();
  private JsonObject field999 = null;

  private MarcHoldingItemRecordBuilder() {
    fields.add(new JsonObject().put("001", "ybp7406411"));
  }

  public static MarcHoldingItemRecordBuilder newRecord() {
    return new MarcHoldingItemRecordBuilder();
  }

  public MarcHoldingItemRecordBuilder withLeader(String value) {
    this.leader = value;
    return this;
  }

  /**
   * Adds a 945 data field with ind1=" " and ind2=" ".
   *
   * @param subfieldPairs alternating subfield-code and value, e.g. "a", "OM", "h", "KU/CC/DI/M"
   */
  public MarcHoldingItemRecordBuilder with945(String... subfieldPairs) {
    fields.add(build945(" ", " ", subfieldPairs));
    return this;
  }

  /**
   * Adds a 945 data field with ind1="" and ind2="" (empty string indicators).
   *
   * @param subfieldPairs alternating subfield-code and value
   */
  public MarcHoldingItemRecordBuilder with945EmptyIndicators(String... subfieldPairs) {
    fields.add(build945("", "", subfieldPairs));
    return this;
  }

  /**
   * Adds a 999 ff field carrying an instance id in subfield "i".
   */
  public MarcHoldingItemRecordBuilder withInstanceId999(String instanceId) {
    this.field999 = new JsonObject()
      .put("ind1", "f").put("ind2", "f")
      .put("subfields", new JsonArray().add(new JsonObject().put("i", instanceId)));
    return this;
  }

  /**
   * Adds a 999 ff field carrying a holdings id in subfield "h".
   */
  public MarcHoldingItemRecordBuilder withHoldingsId999(String holdingsId) {
    this.field999 = new JsonObject()
      .put("ind1", "f").put("ind2", "f")
      .put("subfields", new JsonArray().add(new JsonObject().put("h", holdingsId)));
    return this;
  }

  public String build() {
    var allFields = fields.copy();
    if (field999 != null) {
      allFields.add(new JsonObject().put("999", field999));
    }
    return new JsonObject()
      .put("leader", leader)
      .put("fields", allFields)
      .encode();
  }

  private static JsonObject build945(String ind1, String ind2, String... subfieldPairs) {
    if (subfieldPairs.length % 2 != 0) {
      throw new IllegalArgumentException("subfieldPairs must be even (code, value, code, value, ...)");
    }
    var subfields = new JsonArray();
    for (int i = 0; i < subfieldPairs.length; i += 2) {
      subfields.add(new JsonObject().put(subfieldPairs[i], subfieldPairs[i + 1]));
    }
    return new JsonObject().put("945", new JsonObject()
      .put("ind1", ind1).put("ind2", ind2)
      .put("subfields", subfields));
  }
}
