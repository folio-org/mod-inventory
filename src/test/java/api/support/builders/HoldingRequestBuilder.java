package api.support.builders;

import api.ApiTestSuite;
import io.vertx.core.json.JsonObject;

import java.util.UUID;

public class HoldingRequestBuilder implements Builder {

  private final UUID instanceId;
  private final UUID permanentLocationId;

  public HoldingRequestBuilder() {
    this(
      null,
      UUID.fromString(ApiTestSuite.getMainLibraryLocation()));
  }

  private HoldingRequestBuilder(
    UUID instanceId,
    UUID permanentLocationId) {

    this.instanceId = instanceId;
    this.permanentLocationId = permanentLocationId;
  }

  @Override
  public JsonObject create() {
    return new JsonObject()
      .put("instanceId", instanceId.toString())
      .put("permanentLocationId", permanentLocationId.toString());
  }

  public HoldingRequestBuilder withPermanentLocation(UUID permanentLocationId) {
    return new HoldingRequestBuilder(
      this.instanceId,
      permanentLocationId);
  }

  public HoldingRequestBuilder inMainLibrary() {
    return withPermanentLocation(UUID.fromString(ApiTestSuite.getMainLibraryLocation()));
  }

  public HoldingRequestBuilder inAnnex() {
    return withPermanentLocation(UUID.fromString(ApiTestSuite.getAnnexLocation()));
  }

  public HoldingRequestBuilder forInstance(UUID instanceId) {
    return new HoldingRequestBuilder(
      instanceId,
      this.permanentLocationId);
  }
}
