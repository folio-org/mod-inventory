package support.builders;

import api.ApiTestSuite;
import io.vertx.core.json.JsonObject;
import java.util.UUID;

public class InvalidHoldingRequestBuilder extends AbstractBuilder {
  private final UUID instanceId;
  private final UUID permanentLocationId;

  public InvalidHoldingRequestBuilder() {
    this(null, UUID.fromString(ApiTestSuite.getThirdFloorLocation()));
  }

  InvalidHoldingRequestBuilder(
    UUID instanceId,
    UUID permanentLocationId) {
    this.instanceId = instanceId;
    this.permanentLocationId = permanentLocationId;
  }

  public InvalidHoldingRequestBuilder forInstance(UUID instanceId) {
    return new InvalidHoldingRequestBuilder(
      instanceId,
      this.permanentLocationId);
  }

  @Override
  public JsonObject create() {
    JsonObject holding = new JsonObject();

    holding.put("instanceId", instanceId.toString())
      .put("permanentLocationId", permanentLocationId.toString());

    holding.put("unspecified", "unspecified");

    return holding;
  }
}
