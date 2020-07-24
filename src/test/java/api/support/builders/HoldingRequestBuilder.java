package api.support.builders;

import api.ApiTestSuite;
import io.vertx.core.json.JsonObject;

import java.util.UUID;

public class HoldingRequestBuilder extends AbstractBuilder {

  private final UUID instanceId;
  private final UUID permanentLocationId;
  private final UUID temporaryLocationId;
  private final String callNumber;
  private final String callNumberSuffix;
  private final String callNumberPrefix;
  private final String callNumberTypeId;

  public HoldingRequestBuilder() {
    this(
      null,
      UUID.fromString(ApiTestSuite.getThirdFloorLocation()),
      UUID.fromString(ApiTestSuite.getReadingRoomLocation()),
      null,
      null,
      null,
      null);
  }

  private HoldingRequestBuilder(
    UUID instanceId,
    UUID permanentLocationId,
    UUID temporaryLocationId,
    String callNumber,
    String callNumberSuffix,
    String callNumberPrefix,
    String callNumberTypeId) {

    this.instanceId = instanceId;
    this.permanentLocationId = permanentLocationId;
    this.temporaryLocationId = temporaryLocationId;
    this.callNumber = callNumber;
    this.callNumberSuffix = callNumberSuffix;
    this.callNumberPrefix = callNumberPrefix;
    this.callNumberTypeId = callNumberTypeId;
  }

  @Override
  public JsonObject create() {
    JsonObject holding = new JsonObject();

    holding.put("instanceId", instanceId.toString())
           .put("permanentLocationId", permanentLocationId.toString());

    if (temporaryLocationId != null) {
      holding.put("temporaryLocationId", temporaryLocationId.toString());
    }

    holding.put("callNumber", callNumber);
    holding.put("callNumberPrefix", callNumberPrefix);
    holding.put("callNumberSuffix", callNumberSuffix);
    holding.put("callNumberTypeId", callNumberTypeId);

    return holding;
  }

  private HoldingRequestBuilder withPermanentLocation(UUID permanentLocationId) {
    return new HoldingRequestBuilder(
      this.instanceId,
      permanentLocationId,
      this.temporaryLocationId,
      callNumber,
      this.callNumberSuffix,
      this.callNumberPrefix,
      this.callNumberTypeId);
  }

  private HoldingRequestBuilder withTemporaryLocation(UUID temporaryLocationId) {
    return new HoldingRequestBuilder(
      this.instanceId,
      this.permanentLocationId,
      temporaryLocationId,
      this.callNumber,
      this.callNumberSuffix,
      this.callNumberPrefix,
      this.callNumberTypeId);
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
      this.temporaryLocationId,
      this.callNumber,
      this.callNumberSuffix,
      this.callNumberPrefix,
      this.callNumberTypeId);
  }

  public HoldingRequestBuilder withCallNumber(String callNumber) {
    return new HoldingRequestBuilder(
      this.instanceId,
      this.permanentLocationId,
      this.temporaryLocationId,
      callNumber,
      this.callNumberSuffix,
      this.callNumberPrefix,
      this.callNumberTypeId);
  }

  public HoldingRequestBuilder withCallNumberSuffix(String suffix) {
    return new HoldingRequestBuilder(
      this.instanceId,
      this.permanentLocationId,
      this.temporaryLocationId,
      this.callNumber,
      suffix,
      this.callNumberPrefix,
      this.callNumberTypeId);
  }

  public HoldingRequestBuilder withCallNumberPrefix(String prefix) {
    return new HoldingRequestBuilder(
      this.instanceId,
      this.permanentLocationId,
      this.temporaryLocationId,
      this.callNumber,
      this.callNumberSuffix,
      prefix,
      this.callNumberTypeId);
  }

  public HoldingRequestBuilder withCallNumberTypeId(String callNumberTypeId) {
    return new HoldingRequestBuilder(
      this.instanceId,
      this.permanentLocationId,
      this.temporaryLocationId,
      this.callNumber,
      this.callNumberSuffix,
      this.callNumberPrefix,
      callNumberTypeId);
  }
}
