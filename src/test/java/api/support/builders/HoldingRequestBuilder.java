package api.support.builders;

import api.ApiTestSuite;
import io.vertx.core.json.JsonObject;

import java.util.List;
import java.util.UUID;

import org.folio.inventory.domain.items.AdditionalCallNumberComponents;

public class HoldingRequestBuilder extends AbstractBuilder {

  private static final UUID MARC_SOURCE_HOLDINGS_ID = UUID.fromString("036ee84a-6afd-4c3c-9ad3-4a12ab875f59");
  private static final UUID FOLIO_SOURCE_HOLDINGS_ID = UUID.fromString("f32d531e-df79-46b3-8932-cdd35f7a2264");

  private final UUID instanceId;
  private final UUID permanentLocationId;
  private final UUID temporaryLocationId;
  private final String callNumber;
  private final String callNumberSuffix;
  private final String callNumberPrefix;
  private final String callNumberTypeId;
  private final List<AdditionalCallNumberComponents> additionalCallNumbers;
  private final UUID sourceId;
  private final List<String> administrativeNotes;
  private final String hrId;

  public HoldingRequestBuilder() {
    this(
        null,
        UUID.fromString(ApiTestSuite.getThirdFloorLocation()),
        UUID.fromString(ApiTestSuite.getReadingRoomLocation()),
        null,
        null,
        null,
        null,
        null,
        FOLIO_SOURCE_HOLDINGS_ID,
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
      String callNumberTypeId,
      List<AdditionalCallNumberComponents> additionalCallNumbers,
      UUID sourceId,
      List<String> administrativeNotes,
      String hrId) {

    this.instanceId = instanceId;
    this.permanentLocationId = permanentLocationId;
    this.temporaryLocationId = temporaryLocationId;
    this.callNumber = callNumber;
    this.callNumberSuffix = callNumberSuffix;
    this.callNumberPrefix = callNumberPrefix;
    this.callNumberTypeId = callNumberTypeId;
    this.additionalCallNumbers = additionalCallNumbers;
    this.sourceId = sourceId;
    this.administrativeNotes = administrativeNotes;
    this.hrId = hrId;
  }

  @Override
  public JsonObject create() {
    JsonObject holding = new JsonObject();

    holding.put("instanceId", instanceId.toString())
           .put("permanentLocationId", permanentLocationId.toString());

    if (temporaryLocationId != null) {
      holding.put("temporaryLocationId", temporaryLocationId.toString());
    }
    holding.put("administrativeNotes", administrativeNotes);
    holding.put("callNumber", callNumber);
    holding.put("callNumberPrefix", callNumberPrefix);
    holding.put("callNumberSuffix", callNumberSuffix);
    holding.put("callNumberTypeId", callNumberTypeId);
    holding.put("additionalCallNumbers", additionalCallNumbers);
    holding.put("sourceId", sourceId);
    holding.put("hrid", hrId);
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
        this.callNumberTypeId,
        this.additionalCallNumbers,
        this.sourceId,
        this.administrativeNotes,
        this.hrId);
  }

  private HoldingRequestBuilder withTemporaryLocation(UUID temporaryLocationId) {
    return new HoldingRequestBuilder(
        this.instanceId,
        this.permanentLocationId,
        temporaryLocationId,
        this.callNumber,
        this.callNumberSuffix,
        this.callNumberPrefix,
        this.callNumberTypeId,
        this.additionalCallNumbers,
        this.sourceId,
        this.administrativeNotes,
        this.hrId);
  }

  public JsonObject createFolioHoldingsSource() {
    return new JsonObject()
      .put("id", FOLIO_SOURCE_HOLDINGS_ID)
      .put("name", "FOLIO");
  }

  public JsonObject createMarcHoldingsSource() {
    return new JsonObject()
      .put("id", MARC_SOURCE_HOLDINGS_ID)
      .put("name", "MARC");
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
        this.callNumberTypeId,
        this.additionalCallNumbers,
        this.sourceId,
        this.administrativeNotes,
        this.hrId);
  }

  public HoldingRequestBuilder withCallNumber(String callNumber) {
    return new HoldingRequestBuilder(
        this.instanceId,
        this.permanentLocationId,
        this.temporaryLocationId,
        callNumber,
        this.callNumberSuffix,
        this.callNumberPrefix,
        this.callNumberTypeId,
        this.additionalCallNumbers,
        this.sourceId,
        this.administrativeNotes,
        this.hrId);
  }

  public HoldingRequestBuilder withCallNumberSuffix(String suffix) {
    return new HoldingRequestBuilder(
        this.instanceId,
        this.permanentLocationId,
        this.temporaryLocationId,
        this.callNumber,
        suffix,
        this.callNumberPrefix,
        this.callNumberTypeId,
        this.additionalCallNumbers,
        this.sourceId,
        this.administrativeNotes,
        this.hrId);
  }

  public HoldingRequestBuilder withCallNumberPrefix(String prefix) {
    return new HoldingRequestBuilder(
        this.instanceId,
        this.permanentLocationId,
        this.temporaryLocationId,
        this.callNumber,
        this.callNumberSuffix,
        prefix,
        this.callNumberTypeId,
        this.additionalCallNumbers,
        this.sourceId,
        this.administrativeNotes,
        this.hrId);
  }

  public HoldingRequestBuilder withCallNumberTypeId(String callNumberTypeId) {
    return new HoldingRequestBuilder(
        this.instanceId,
        this.permanentLocationId,
        this.temporaryLocationId,
        this.callNumber,
        this.callNumberSuffix,
        this.callNumberPrefix,
        callNumberTypeId,
        this.additionalCallNumbers,
        this.sourceId,
        this.administrativeNotes,
        this.hrId);
  }

  public HoldingRequestBuilder withAdditionalCallNumbers(List<AdditionalCallNumberComponents> additionalCallNumbers) {
    return new HoldingRequestBuilder(
        this.instanceId,
        this.permanentLocationId,
        this.temporaryLocationId,
        this.callNumber,
        this.callNumberSuffix,
        this.callNumberPrefix,
        this.callNumberTypeId,
        additionalCallNumbers,
        this.sourceId,
        this.administrativeNotes,
        this.hrId);
  }

  public HoldingRequestBuilder withMarcSource() {
    return new HoldingRequestBuilder(
        this.instanceId,
        this.permanentLocationId,
        this.temporaryLocationId,
        this.callNumber,
        this.callNumberSuffix,
        this.callNumberPrefix,
        this.callNumberTypeId,
        this.additionalCallNumbers,
        MARC_SOURCE_HOLDINGS_ID,
        this.administrativeNotes,
        this.hrId);
  }

  public HoldingRequestBuilder withAdministrativeNotes(List<String> administrativeNotes) {
    return new HoldingRequestBuilder(
        this.instanceId,
        this.permanentLocationId,
        this.temporaryLocationId,
        this.callNumber,
        this.callNumberSuffix,
        this.callNumberPrefix,
        this.callNumberTypeId,
        this.additionalCallNumbers,
        this.sourceId,
        administrativeNotes,
        this.hrId);
  }

  public HoldingRequestBuilder withHrId(String hrId) {
    return new HoldingRequestBuilder(
        this.instanceId,
        this.permanentLocationId,
        this.temporaryLocationId,
        this.callNumber,
        this.callNumberSuffix,
        this.callNumberPrefix,
        this.callNumberTypeId,
        this.additionalCallNumbers,
        this.sourceId,
        this.administrativeNotes,
        hrId);
  }
}
