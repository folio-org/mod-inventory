package api.support.builders;

import io.vertx.core.json.JsonObject;

public class SourceRecordRequestBuilder extends AbstractBuilder {
  private static final String MARC_TYPE = "MARC";

  private final String id;
  private final String recordType;
  private final JsonObject additionalInfo;

  public SourceRecordRequestBuilder(String id) {
    this.id = id;
    this.recordType = MARC_TYPE;
    this.additionalInfo = new JsonObject().put("suppressDiscovery", false);
  }

  @Override
  public JsonObject create() {
    return new JsonObject()
      .put("id", this.id)
      .put("recordType", this.recordType)
      .put("additionalInfo", this.additionalInfo);
  }

}
