package api.support.builders;

import api.ApiTestSuite;
import io.vertx.core.json.JsonObject;

import java.util.UUID;

public class HoldingRequestBuilder implements Builder {

  private final UUID instanceId;
  private final UUID permanentLocationId;
  private final UUID temporaryLocationId;

  public HoldingRequestBuilder() {
    this(
      null,
      UUID.fromString(ApiTestSuite.getThirdFloorLocation()),
      UUID.fromString(ApiTestSuite.getReadingRoomLocation()));
  }

  private HoldingRequestBuilder(
    UUID instanceId,
    UUID permanentLocationId,
    UUID temporaryLocationId) {

    this.instanceId = instanceId;
    this.permanentLocationId = permanentLocationId;
    this.temporaryLocationId = temporaryLocationId;
  }

  @Override
  public JsonObject create() {
    JsonObject holding = new JsonObject();

    holding.put("instanceId", instanceId.toString())
           .put("permanentLocationId", permanentLocationId.toString());
    
    if (temporaryLocationId != null) 
      holding.put("temporaryLocationId", temporaryLocationId.toString());
    
    return holding;
  }

  public HoldingRequestBuilder withPermanentLocation(UUID permanentLocationId) {
    return new HoldingRequestBuilder(
      this.instanceId,
      permanentLocationId,
      this.temporaryLocationId);
  }
  
  public HoldingRequestBuilder withTemporaryLocation(UUID temporaryLocationId) {
    return new HoldingRequestBuilder(
            this.instanceId,
            this.permanentLocationId,
            temporaryLocationId);
  }
    
  public HoldingRequestBuilder permanentlyInMainLibrary() {
    return withPermanentLocation(UUID.fromString(ApiTestSuite.getMainLibraryLocation()));
  }
  
  public HoldingRequestBuilder temporarilyInMezzanine() {
    return withTemporaryLocation(UUID.fromString(ApiTestSuite.getMezzanineDisplayCaseLocation()));
  }
  
  public HoldingRequestBuilder withNoTemporaryLocation() {
    return withTemporaryLocation(null);
  }

  public HoldingRequestBuilder forInstance(UUID instanceId) {
    return new HoldingRequestBuilder(
      instanceId,
      this.permanentLocationId,
      this.temporaryLocationId);
  }
}
