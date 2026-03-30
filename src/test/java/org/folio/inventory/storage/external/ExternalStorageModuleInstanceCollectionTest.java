package org.folio.inventory.storage.external;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.client.WireMock.put;
import static com.github.tomakehurst.wiremock.client.WireMock.urlPathMatching;
import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.wireMockConfig;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.github.tomakehurst.wiremock.junit5.WireMockExtension;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import java.util.List;
import java.util.UUID;
import lombok.SneakyThrows;
import org.folio.inventory.common.Context;
import org.folio.inventory.common.VertxAssistant;
import org.folio.inventory.domain.instances.Instance;
import org.folio.inventory.domain.instances.InstanceCollection;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

/**
 * Tests for the private {@code modifyInstance} method in
 * {@link ExternalStorageModuleInstanceCollection}, exercised indirectly through
 * {@link ExternalStorageModuleInstanceCollection#findByIdAndUpdate}.
 */
class ExternalStorageModuleInstanceCollectionTest {

  private static final String TENANT = "test_tenant";
  private static final String TOKEN = "test_token";
  private static final String USER_ID = "test_user";
  private static final String REQUEST_ID = "test_request";

  private static final String INSTANCE_PATH_PATTERN = "/instance-storage/instances/[a-z0-9-]+";

  private static final VertxAssistant vertxAssistant = new VertxAssistant();

  @RegisterExtension
  static WireMockExtension wireMock = WireMockExtension.newInstance()
    .options(wireMockConfig().dynamicPort())
    .build();

  @BeforeAll
  static void beforeAll() {
    vertxAssistant.start();
  }

  @AfterAll
  static void afterAll() {
    vertxAssistant.stop();
  }

  // ---------------------------------------------------------------------------
  // Tests for modifyInstance – tested indirectly via findByIdAndUpdate
  // ---------------------------------------------------------------------------

  /**
   * Both ID and source must always be taken from the existing instance,
   * regardless of what the incoming instance carries for those fields.
   */
  @Test
  @SneakyThrows
  void modifyInstance_preservesIdAndSourceFromExistingInstance() {
    var existingId = UUID.randomUUID().toString();
    var existingSource = "MARC";
    var existing = existingInstanceJson(existingId, existingSource, false, false, false);
    var incoming = incomingInstanceJson("New Title", false);
    incoming.put(Instance.ID, UUID.randomUUID().toString()); // caller tries to change id
    incoming.put(Instance.SOURCE_KEY, "FOLIO");              // caller tries to change source

    stubGet(existing);
    stubPut();

    var result = createCollection().findByIdAndUpdate(existingId, incoming, createContext());

    assertNotNull(result);
    assertEquals(existingId, result.getId(),
      "ID must be preserved from the existing instance");

    assertEquals(existingSource, result.getSource(),
      "Source must be preserved from the existing instance");
  }

  /**
   * When the existing instance is NOT deleted and the incoming IS deleted,
   * both discoverySuppress and staffSuppress must be set to {@code true}.
   */
  @Test
  @SneakyThrows
  void modifyInstance_whenBecomingDeleted_setsDiscoveryAndStaffSuppressToTrue() {
    var id = UUID.randomUUID().toString();
    // Existing: not deleted, both suppress flags false
    var existing = existingInstanceJson(id, "FOLIO", false, false, false);
    // Incoming: deleted = true
    var incoming = incomingInstanceJson("Title", true);

    stubGet(existing);
    stubPut();

    var result = createCollection().findByIdAndUpdate(id, incoming, createContext());

    assertTrue(result.getDiscoverySuppress(),
      "discoverySuppress must be true when instance transitions to deleted");
    assertTrue(result.getStaffSuppress(),
      "staffSuppress must be true when instance transitions to deleted");
  }

  /**
   * When neither the existing nor the incoming instance is deleted,
   * suppress flags must be copied from the existing instance.
   */
  @Test
  @SneakyThrows
  void modifyInstance_whenNotDeleted_copiesSuppressFlagsFromExisting() {
    var id = UUID.randomUUID().toString();
    // Existing: not deleted, both suppress flags true
    var existing = existingInstanceJson(id, "FOLIO", false, true, true);
    // Incoming: not deleted, caller sets suppress flags to false (should be ignored)
    var incoming = incomingInstanceJson("Title", false);

    stubGet(existing);
    stubPut();

    var result = createCollection().findByIdAndUpdate(id, incoming, createContext());

    assertTrue(result.getDiscoverySuppress(),
      "discoverySuppress must be preserved from existing when not transitioning to deleted");
    assertTrue(result.getStaffSuppress(),
      "staffSuppress must be preserved from existing when not transitioning to deleted");
  }

  /**
   * When the existing instance is already deleted (deleted = true) and the
   * incoming is also deleted, suppress flags must be taken from the existing
   * instance (not forced to true by the branch that handles new deletions).
   */
  @Test
  @SneakyThrows
  void modifyInstance_whenAlreadyDeleted_copiesSuppressFlagsFromExisting() {
    var id = UUID.randomUUID().toString();
    // Existing: already deleted, suppress flags false
    var existing = existingInstanceJson(id, "FOLIO", true, false, false);
    // Incoming: also deleted
    var incoming = incomingInstanceJson("Title", true);

    stubGet(existing);
    stubPut();

    var result = createCollection().findByIdAndUpdate(id, incoming, createContext());

    assertFalse(result.getDiscoverySuppress(),
      "discoverySuppress should remain false when already deleted");
    assertFalse(result.getStaffSuppress(),
      "staffSuppress should remain false when already deleted");
  }

  /**
   * mergeInstances merges the two JSONs via {@code JsonObject.mergeIn}, so scalar
   * and regular-array fields are taken from the incoming instance.  However, five
   * array fields are explicitly re-applied from the existing instance after mergeIn
   * because {@code mergeIn} would otherwise lose the existing values:
   * statisticalCodeIds, natureOfContentTermIds, administrativeNotes,
   * parentInstances, and childInstances.
   */
  @Test
  @SneakyThrows
  void modifyInstance_mergesIncomingFieldsIntoResult() {
    var id = UUID.randomUUID().toString();

    // --- existing instance carries distinct values for all protected arrays ---
    var existing = existingInstanceJson(id, "FOLIO", false, false, false);
    existing.put(Instance.TITLE_KEY, "Old Title");
    addJsonArrayFields(existing, "existing");

    // --- incoming carries a new title and different values for every protected array ---
    var incoming = incomingInstanceJson("Incoming Title", false);
    addJsonArrayFields(incoming, "incoming");

    stubGet(existing);
    stubPut();

    var result = createCollection().findByIdAndUpdate(id, incoming, createContext());

    // Regular mergeIn field – incoming wins
    assertEquals("Incoming Title", result.getTitle(),
      "Title from incoming instance must overwrite existing via mergeIn");

    // Protected array fields – existing always wins, incoming values are discarded
    assertEquals(List.of("existing-stat-code"), result.getStatisticalCodeIds(),
      "statisticalCodeIds must be preserved from existing instance");
    assertEquals(List.of("existing-nature-term"), result.getNatureOfContentTermIds(),
      "natureOfContentTermIds must be preserved from existing instance");
    assertEquals(List.of("existing-admin-note"), result.getAdministrativeNotes(),
      "administrativeNotes must be preserved from existing instance");
    assertEquals("existing-parent-id",
      result.getParentInstances().getFirst().getSuperInstanceId(),
      "parentInstances must be preserved from existing instance");
    assertEquals("existing-child-id",
      result.getChildInstances().getFirst().getSubInstanceId(),
      "childInstances must be preserved from existing instance");
  }

  // ---------------------------------------------------------------------------
  // Helper builders
  // ---------------------------------------------------------------------------

  private InstanceCollection createCollection() {
    return vertxAssistant.createUsingVertx(vertx ->
      new ExternalStorageModuleInstanceCollection(
        wireMock.baseUrl(), TENANT, TOKEN, USER_ID, REQUEST_ID,
        vertx.createHttpClient()));
  }

  private Context createContext() {
    return new Context() {
      @Override public String getTenantId()      { return TENANT; }
      @Override public String getToken()         { return TOKEN; }
      @Override public String getOkapiLocation() { return wireMock.baseUrl(); }
      @Override public String getUserId()        { return USER_ID; }
      @Override public String getRequestId()     { return REQUEST_ID; }
    };
  }

  /**
   * Builds a minimal existing-instance JSON suitable for a GET response.
   */
  private JsonObject existingInstanceJson(String id, String source,
                                          Boolean deleted,
                                          Boolean discoverySuppress,
                                          Boolean staffSuppress) {
    return new JsonObject()
      .put(Instance.ID, id)
      .put(Instance.SOURCE_KEY, source)
      .put(Instance.TITLE_KEY, "Existing Title")
      .put(Instance.INSTANCE_TYPE_ID_KEY, UUID.randomUUID().toString())
      .put(Instance.DELETED_KEY, deleted)
      .put(Instance.DISCOVERY_SUPPRESS_KEY, discoverySuppress)
      .put(Instance.STAFF_SUPPRESS_KEY, staffSuppress);
  }

  /**
   * Builds a minimal incoming-instance JSON (what the caller wants to apply).
   */
  private JsonObject incomingInstanceJson(String title, Boolean deleted) {
    return new JsonObject()
      .put(Instance.TITLE_KEY, title)
      .put(Instance.INSTANCE_TYPE_ID_KEY, UUID.randomUUID().toString())
      .put(Instance.DELETED_KEY, deleted)
      .put(Instance.DISCOVERY_SUPPRESS_KEY, false)
      .put(Instance.STAFF_SUPPRESS_KEY, false);
  }

  private void stubGet(JsonObject responseBody) {
    wireMock.stubFor(get(urlPathMatching(INSTANCE_PATH_PATTERN))
      .willReturn(aResponse()
        .withStatus(200)
        .withHeader("Content-Type", "application/json")
        .withBody(responseBody.encode())));
  }

  /**
   * Attaches all five array fields that {@code InstanceUtil.mergeInstances} protects
   * from being overwritten by {@code mergeIn}.  Each value is prefixed with
   * {@code prefix} so existing and incoming values are unambiguously distinguishable
   * in assertions (e.g. {@code "existing-stat-code"} vs {@code "incoming-stat-code"}).
   */
  private void addJsonArrayFields(JsonObject instance, String prefix) {
    instance.put(Instance.STATISTICAL_CODE_IDS_KEY,
      new JsonArray().add(prefix + "-stat-code"));
    instance.put(Instance.NATURE_OF_CONTENT_TERM_IDS_KEY,
      new JsonArray().add(prefix + "-nature-term"));
    instance.put(Instance.ADMININSTRATIVE_NOTES_KEY,
      new JsonArray().add(prefix + "-admin-note"));
    instance.put(Instance.PARENT_INSTANCES_KEY,
      new JsonArray().add(new JsonObject().put("superInstanceId", prefix + "-parent-id")));
    instance.put(Instance.CHILD_INSTANCES_KEY,
      new JsonArray().add(new JsonObject().put("subInstanceId", prefix + "-child-id")));
  }

  private void stubPut() {
    wireMock.stubFor(put(urlPathMatching(INSTANCE_PATH_PATTERN))
      .willReturn(aResponse().withStatus(204)));
  }

}

