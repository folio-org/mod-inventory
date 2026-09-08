package support.builders;

import io.vertx.core.json.JsonObject;

public class BoundWithPartRequestBuilder extends AbstractBuilder {
  private final String itemId;
  private final String holdingsRecordId;

  public BoundWithPartRequestBuilder(String itemId, String holdingsRecordId) {
    this.itemId = itemId;
    this.holdingsRecordId = holdingsRecordId;
  }

  @Override
  public JsonObject create() {
    JsonObject boundWithPart = new JsonObject();
    boundWithPart.put("itemId", itemId);
    boundWithPart.put("holdingsRecordId", holdingsRecordId);
    return boundWithPart;
  }
}
