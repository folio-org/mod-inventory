package org.folio.inventory.storage.external;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.equalTo;
import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.client.WireMock.getRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlPathEqualTo;
import static org.assertj.core.api.Assertions.assertThat;

import io.netty.handler.codec.http.HttpHeaderValues;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import lombok.SneakyThrows;
import org.folio.HttpHeaders;
import org.folio.dataimport.testsupport.rest.BaseWireMockTest;
import org.folio.inventory.common.VertxAssistant;
import org.folio.inventory.common.domain.MultipleRecords;
import org.folio.inventory.common.domain.PagingParameters;
import org.folio.inventory.domain.instances.Instance;
import org.folio.inventory.domain.instances.InstanceCollection;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Tests for {@link ExternalStorageModuleInstanceCollection#findByCql(String, boolean,
 * PagingParameters, java.util.function.Consumer, java.util.function.Consumer)}, which overrides
 * the {@code InstanceCollection} default to forward the {@code includeShadowCopies} flag to
 * storage.
 */
class ExternalStorageModuleInstanceCollectionFindByCqlTest extends BaseWireMockTest {

  private static final String TENANT = "test_tenant";
  private static final String TOKEN = "test_token";
  private static final String USER_ID = "test_user";
  private static final String REQUEST_ID = "test_request";
  private static final String INSTANCES_PATH = "/instance-storage/instances";

  private static final VertxAssistant VERTX_ASSISTANT = new VertxAssistant();
  private static final String MATCHING_TITLE_CQL = "title=\"*Angry*\"";

  @BeforeAll
  static void beforeAll() {
    VERTX_ASSISTANT.start();
  }

  @AfterAll
  static void afterAll() {
    VERTX_ASSISTANT.stop();
  }

  @BeforeEach
  void stubEmptySearchResponse() {
    var emptyResults = new JsonObject()
      .put("instances", new JsonArray())
      .put("totalRecords", 0);

    WIRE_MOCK.stubFor(get(urlPathEqualTo(INSTANCES_PATH))
      .willReturn(aResponse()
        .withStatus(200)
        .withHeader(HttpHeaders.CONTENT_TYPE, HttpHeaderValues.APPLICATION_JSON.toString())
        .withBody(emptyResults.encode())));
  }

  @ParameterizedTest(name = "includeShadowCopies={0}")
  @ValueSource(booleans = {true, false})
  @DisplayName("should forward the includeShadowCopies flag to the storage query string")
  @SneakyThrows
  void shouldForwardIncludeShadowCopiesFlag_toStorageQuery(boolean includeShadowCopies) {
    // arrange
    var collection = createCollection();

    // act
    findByCql(collection, includeShadowCopies);

    // assert
    WIRE_MOCK.verify(getRequestedFor(urlPathEqualTo(INSTANCES_PATH))
      .withQueryParam("includeShadowCopies", equalTo(String.valueOf(includeShadowCopies))));
  }

  @Test
  @DisplayName("should still return matching results when includeShadowCopies is forwarded")
  @SneakyThrows
  void shouldReturnMatchingInstances_whenCqlSearchSucceeds() {
    // arrange
    var collection = createCollection();

    // act
    var result = findByCql(collection, true);

    // assert
    assertThat(result.totalRecords()).isZero();
  }

  private InstanceCollection createCollection() {
    return VERTX_ASSISTANT.createUsingVertx(vertx ->
      new ExternalStorageModuleInstanceCollection(
        WIRE_MOCK.baseUrl(), TENANT, TOKEN, USER_ID, REQUEST_ID,
        vertx.createHttpClient()));
  }

  @SneakyThrows
  private MultipleRecords<Instance> findByCql(InstanceCollection collection, boolean includeShadowCopies) {
    CompletableFuture<MultipleRecords<Instance>> future = new CompletableFuture<>();

    collection.findByCql(MATCHING_TITLE_CQL, includeShadowCopies, PagingParameters.defaults(),
      success -> future.complete(success.result()),
      failure -> future.completeExceptionally(
        new AssertionError("Expected success but got failure: " + failure.reason())));

    return future.get(5, TimeUnit.SECONDS);
  }
}
