package support.builders;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

/**
 * Builds MARC bib parsed-record content JSON strings for unit tests.
 *
 * <p>All eight near-duplicate bib records in CreateInstanceEventHandlerTest and
 * ReplaceInstanceEventHandlerTest share the same core fields (001, 245, 336, 780, 785, 500, 520).
 * The variants differ only in the presence/value of 003, 005, leader status byte, 520 summary
 * text, and the presence/indicators of a 999 field.
 */
public final class MarcBibRecordBuilder {

  public static final String DEFAULT_520_SUMMARY = "Ben shu miao shu le cui ying ying he.";
  public static final String SHORT_520_SUMMARY = "Ben shu miao shu.";
  public static final String INSTANCE_ID = "957985c6-97e3-4038-b0e7-343ecd0b8120";

  private static final String DEFAULT_LEADER = "01314nam  22003851a 4500";
  private static final String DELETED_LEADER = "01314dam  22003851a 4500";

  private String field003 = null;
  private String field005 = null;
  private boolean deleted = false;
  private String summary520 = DEFAULT_520_SUMMARY;
  private String field999InstanceId = null;
  private String field999Ind1 = "f";
  private String field999Ind2 = "f";

  private MarcBibRecordBuilder() { }

  public static MarcBibRecordBuilder newBibRecord() {
    return new MarcBibRecordBuilder();
  }

  public MarcBibRecordBuilder with003(String value) {
    this.field003 = value;
    return this;
  }

  public MarcBibRecordBuilder with005(String value) {
    this.field005 = value;
    return this;
  }

  public MarcBibRecordBuilder deleted() {
    this.deleted = true;
    return this;
  }

  public MarcBibRecordBuilder with520Summary(String value) {
    this.summary520 = value;
    return this;
  }

  public MarcBibRecordBuilder with999ff(String instanceId) {
    this.field999InstanceId = instanceId;
    this.field999Ind1 = "f";
    this.field999Ind2 = "f";
    return this;
  }

  public MarcBibRecordBuilder with999NonFf(String instanceId) {
    this.field999InstanceId = instanceId;
    this.field999Ind1 = " ";
    this.field999Ind2 = " ";
    return this;
  }

  public String build() {
    var fields = new JsonArray();
    fields.add(new JsonObject().put("001", "ybp7406411"));
    if (field003 != null) {
      fields.add(new JsonObject().put("003", field003));
    }
    if (field005 != null) {
      fields.add(new JsonObject().put("005", field005));
    }
    fields.add(new JsonObject().put("245", new JsonObject()
      .put("ind1", "1").put("ind2", "0")
      .put("subfields", new JsonArray().add(new JsonObject().put("a", "titleValue")))));
    fields.add(new JsonObject().put("336", new JsonObject()
      .put("ind1", "1").put("ind2", "0")
      .put("subfields", new JsonArray().add(new JsonObject().put("b", "b6698d38-149f-11ec-82a8-0242ac130003")))));
    fields.add(new JsonObject().put("780", new JsonObject()
      .put("ind1", "0").put("ind2", "0")
      .put("subfields", new JsonArray().add(new JsonObject().put("t", "Houston oil directory")))));
    fields.add(new JsonObject().put("785", new JsonObject()
      .put("ind1", "0").put("ind2", "0")
      .put("subfields", new JsonArray()
        .add(new JsonObject().put("t", "SAIS review of international affairs"))
        .add(new JsonObject().put("x", "1945-4724")))));
    fields.add(new JsonObject().put("500", new JsonObject()
      .put("ind1", " ").put("ind2", " ")
      .put("subfields", new JsonArray()
        .add(new JsonObject().put("a", "Adaptation of Xi xiang ji by Wang Shifu.")))));
    fields.add(new JsonObject().put("520", new JsonObject()
      .put("ind1", " ").put("ind2", " ")
      .put("subfields", new JsonArray().add(new JsonObject().put("a", summary520)))));
    if (field999InstanceId != null) {
      fields.add(new JsonObject().put("999", new JsonObject()
        .put("ind1", field999Ind1).put("ind2", field999Ind2)
        .put("subfields", new JsonArray().add(new JsonObject().put("i", field999InstanceId)))));
    }
    return new JsonObject()
      .put("leader", deleted ? DELETED_LEADER : DEFAULT_LEADER)
      .put("fields", fields)
      .encode();
  }
}
