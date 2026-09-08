package support.builders;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import java.util.ArrayList;
import java.util.List;

/**
 * Builds MARC parsed-record content JSON strings for unit tests.
 *
 * <p>Use {@link #newBibRecord()} for bibliographic records (pre-loads standard bib fields)
 * or {@link #newRecord()} for holding/item records (pre-loads only the 001 control field).
 * Any MARC field can be added via the generic {@link #withControlField} and
 * {@link #withDataField} methods; named convenience methods delegate to these for the
 * field types most commonly needed across the test suite.
 */
public final class MarcRecordBuilder {

  public static final String INSTANCE_ID = "957985c6-97e3-4038-b0e7-343ecd0b8120";

  private static final String DEFAULT_LEADER = "01314nam  22003851a 4500";
  private static final String DELETED_LEADER = "01314dam  22003851a 4500";

  private String leader;
  private final List<JsonObject> fields;
  private JsonObject pendingField999;

  private MarcRecordBuilder(String leader) {
    this.leader = leader;
    this.fields = new ArrayList<>();
  }

  /**
   * Returns a builder pre-loaded with standard bib fields: 001, 245, 336, 780, 785, 500.
   */
  public static MarcRecordBuilder newBibRecord() {
    var builder = new MarcRecordBuilder(DEFAULT_LEADER);
    builder.fields.add(new JsonObject().put("001", "ybp7406411"));
    builder.fields.add(new JsonObject().put("245", new JsonObject()
      .put("ind1", "1").put("ind2", "0")
      .put("subfields", new JsonArray().add(new JsonObject().put("a", "titleValue")))));
    builder.fields.add(new JsonObject().put("336", new JsonObject()
      .put("ind1", "1").put("ind2", "0")
      .put("subfields", new JsonArray()
        .add(new JsonObject().put("b", "b6698d38-149f-11ec-82a8-0242ac130003")))));
    builder.fields.add(new JsonObject().put("780", new JsonObject()
      .put("ind1", "0").put("ind2", "0")
      .put("subfields", new JsonArray().add(new JsonObject().put("t", "Houston oil directory")))));
    builder.fields.add(new JsonObject().put("785", new JsonObject()
      .put("ind1", "0").put("ind2", "0")
      .put("subfields", new JsonArray()
        .add(new JsonObject().put("t", "SAIS review of international affairs"))
        .add(new JsonObject().put("x", "1945-4724")))));
    builder.fields.add(new JsonObject().put("500", new JsonObject()
      .put("ind1", " ").put("ind2", " ")
      .put("subfields", new JsonArray()
        .add(new JsonObject().put("a", "Adaptation of Xi xiang ji by Wang Shifu.")))));
    builder.fields.add(new JsonObject().put("520", new JsonObject()
      .put("ind1", " ").put("ind2", " ")
      .put("subfields", new JsonArray()
        .add(new JsonObject().put("a", "Ben shu miao shu le cui ying ying he.")))));
    return builder;
  }

  /**
   * Returns a builder pre-loaded with only 001. Use for holding/item records.
   */
  public static MarcRecordBuilder newRecord() {
    var builder = new MarcRecordBuilder(DEFAULT_LEADER);
    builder.fields.add(new JsonObject().put("001", "ybp7406411"));
    return builder;
  }

  public MarcRecordBuilder withLeader(String value) {
    this.leader = value;
    return this;
  }

  public MarcRecordBuilder deleted() {
    this.leader = DELETED_LEADER;
    return this;
  }

  public MarcRecordBuilder withControlField(String tag, String value) {
    fields.add(new JsonObject().put(tag, value));
    return this;
  }

  /**
   * Adds a MARC data field. {@code subfieldPairs} must be an even-length sequence of
   * alternating subfield codes and values, e.g. {@code "a", "OM", "h", "KU/CC/DI/M"}.
   */
  public MarcRecordBuilder withDataField(String tag, String ind1, String ind2,
                                         String... subfieldPairs) {
    if (subfieldPairs.length % 2 != 0) {
      throw new IllegalArgumentException("subfieldPairs must be even (code, value, ...)");
    }
    var subfields = new JsonArray();
    for (int ix = 0; ix < subfieldPairs.length; ix += 2) {
      subfields.add(new JsonObject().put(subfieldPairs[ix], subfieldPairs[ix + 1]));
    }
    fields.add(new JsonObject().put(tag, new JsonObject()
      .put("ind1", ind1).put("ind2", ind2).put("subfields", subfields)));
    return this;
  }

  public MarcRecordBuilder with003(String value) {
    return withControlField("003", value);
  }

  public MarcRecordBuilder with005(String value) {
    return withControlField("005", value);
  }

  /**
   * Adds a 945 data field with ind1=" " and ind2=" ".
   */
  public MarcRecordBuilder with945(String... subfieldPairs) {
    return withDataField("945", " ", " ", subfieldPairs);
  }

  /**
   * Adds a 945 data field with ind1="" and ind2="" (empty string indicators).
   */
  public MarcRecordBuilder with945EmptyIndicators(String... subfieldPairs) {
    return withDataField("945", "", "", subfieldPairs);
  }

  /**
   * Adds a 999 ff field with subfield "i" = instanceId.
   */
  public MarcRecordBuilder withInstanceId999(String instanceId) {
    this.pendingField999 = new JsonObject()
      .put("ind1", "f").put("ind2", "f")
      .put("subfields", new JsonArray().add(new JsonObject().put("i", instanceId)));
    return this;
  }

  /**
   * Adds a 999 ff field with subfield "h" = holdingsId.
   */
  public MarcRecordBuilder withHoldingsId999(String holdingsId) {
    this.pendingField999 = new JsonObject()
      .put("ind1", "f").put("ind2", "f")
      .put("subfields", new JsonArray().add(new JsonObject().put("h", holdingsId)));
    return this;
  }

  public MarcRecordBuilder with999ff(String instanceId) {
    return withInstanceId999(instanceId);
  }

  /**
   * Adds a 999 field with blank indicators and subfield "i" = instanceId.
   */
  public MarcRecordBuilder with999NonFf(String instanceId) {
    this.pendingField999 = new JsonObject()
      .put("ind1", " ").put("ind2", " ")
      .put("subfields", new JsonArray().add(new JsonObject().put("i", instanceId)));
    return this;
  }

  /**
   * Builds and returns the JSON-encoded MARC parsed-record content string.
   */
  public String build() {
    var allFields = new JsonArray();
    fields.forEach(allFields::add);
    if (pendingField999 != null) {
      allFields.add(new JsonObject().put("999", pendingField999));
    }
    return new JsonObject()
      .put("leader", leader)
      .put("fields", allFields)
      .encode();
  }
}
