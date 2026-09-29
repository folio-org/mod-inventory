package org.folio.inventory.storage.external;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.post;
import static com.github.tomakehurst.wiremock.client.WireMock.urlPathEqualTo;
import static org.assertj.core.api.Assertions.assertThat;

import com.github.tomakehurst.wiremock.http.Fault;
import io.netty.handler.codec.http.HttpHeaderValues;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import lombok.SneakyThrows;
import org.folio.HttpHeaders;
import org.folio.dataimport.testsupport.rest.BaseWireMockTest;
import org.folio.inventory.common.VertxAssistant;
import org.folio.inventory.common.domain.Failure;
import org.folio.inventory.common.domain.Success;
import org.folio.inventory.domain.BatchResult;
import org.folio.inventory.domain.instances.Instance;
import org.folio.inventory.domain.instances.InstanceCollection;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link ExternalStorageModuleInstanceCollection#addBatch}, including the
 * {@code isBatchResponse} classification that decides whether a response is parsed as a batch
 * result or reported through the failure callback.
 */
class ExternalStorageModuleInstanceCollectionAddBatchTest extends BaseWireMockTest {

  private static final String TENANT = "test_tenant";
  private static final String TOKEN = "test_token";
  private static final String USER_ID = "test_user";
  private static final String REQUEST_ID = "test_request";
  private static final String INSTANCE_TITLE = "A title";
  private static final String BATCH_PATH = "/instance-storage/batch/synchronous";

  private static final VertxAssistant VERTX_ASSISTANT = new VertxAssistant();

  @BeforeAll
  static void beforeAll() {
    VERTX_ASSISTANT.start();
  }

  @AfterAll
  static void afterAll() {
    VERTX_ASSISTANT.stop();
  }

  @Test
  @DisplayName("should return created instances and no error messages when batch request succeeds")
  @SneakyThrows
  void shouldReturnCreatedInstances_whenBatchRequestSucceeds() {
    // arrange
    var createdId = UUID.randomUUID().toString();
    var responseBody = new JsonObject()
      .put("instances", jsonArrayOf(createdInstanceJson(createdId)))
      .put("errorMessages", jsonArrayOfStrings());

    stubBatchPost(201, HttpHeaderValues.APPLICATION_JSON.toString(), responseBody.encode());

    // act
    var result = addBatch(List.of(createInstance()));

    // assert
    assertThat(result.getBatchItems()).extracting(Instance::getId).containsExactly(createdId);
    assertThat(result.getErrorMessages()).isEmpty();
  }

  @Test
  @DisplayName("should return error messages when batch response reports partial failures")
  @SneakyThrows
  void shouldReturnErrorMessages_whenBatchPartiallyFails() {
    // arrange
    var responseBody = new JsonObject()
      .put("instances", jsonArrayOf())
      .put("errorMessages", jsonArrayOfStrings("instance 1 is invalid"));

    stubBatchPost(201, HttpHeaderValues.APPLICATION_JSON.toString(), responseBody.encode());

    // act
    var result = addBatch(List.of(createInstance()));

    // assert
    assertThat(result.getBatchItems()).isEmpty();
    assertThat(result.getErrorMessages()).containsExactly("instance 1 is invalid");
  }

  @Test
  @DisplayName("should treat a JSON 500 response as a batch response and report its error messages")
  @SneakyThrows
  void shouldTreatServerErrorWithJsonBody_asBatchResponse() {
    // arrange
    var responseBody = new JsonObject()
      .put("instances", jsonArrayOf())
      .put("errorMessages", jsonArrayOfStrings("internal failure while batching"));

    stubBatchPost(500, HttpHeaderValues.APPLICATION_JSON.toString(), responseBody.encode());

    // act
    var result = addBatch(List.of(createInstance()));

    // assert
    assertThat(result.getBatchItems()).isEmpty();
    assertThat(result.getErrorMessages()).containsExactly("internal failure while batching");
  }

  @Test
  @DisplayName("should invoke failure callback when response is not a batch response")
  @SneakyThrows
  void shouldInvokeFailureCallback_whenResponseIsNotBatchResponse() {
    // arrange
    stubBatchPost(400, HttpHeaderValues.TEXT_PLAIN.toString(), "Bad Request");

    // act
    var failure = addBatchExpectingFailure(List.of(createInstance()));

    // assert
    assertThat(failure.reason()).isEqualTo("Bad Request");
    assertThat(failure.statusCode()).isEqualTo(400);
  }

  @Test
  @DisplayName("should invoke failure callback when the batch response body cannot be parsed")
  @SneakyThrows
  void shouldInvokeFailureCallback_whenBatchResponseBodyIsMalformed() {
    // arrange
    stubBatchPost(201, HttpHeaderValues.APPLICATION_JSON.toString(), "{\"notInstances\": []}");

    // act
    var failure = addBatchExpectingFailure(List.of(createInstance()));

    // assert
    assertThat(failure.statusCode()).isEqualTo(201);
  }

  @Test
  @DisplayName("should invoke failure callback when the batch request fails to send")
  @SneakyThrows
  void shouldInvokeFailureCallback_whenRequestFailsToSend() {
    // arrange
    WIRE_MOCK.stubFor(post(urlPathEqualTo(BATCH_PATH))
      .willReturn(aResponse().withFault(Fault.CONNECTION_RESET_BY_PEER)));

    // act
    var failure = addBatchExpectingFailure(List.of(createInstance()));

    // assert
    assertThat(failure.statusCode()).isEqualTo(-1);
  }

  private InstanceCollection createCollection() {
    return VERTX_ASSISTANT.createUsingVertx(vertx ->
      new ExternalStorageModuleInstanceCollection(
        WIRE_MOCK.baseUrl(), TENANT, TOKEN, USER_ID, REQUEST_ID,
        vertx.createHttpClient()));
  }

  @SneakyThrows
  private BatchResult<Instance> addBatch(List<Instance> instances) {
    CompletableFuture<BatchResult<Instance>> future = new CompletableFuture<>();

    createCollection().addBatch(instances,
      (Success<BatchResult<Instance>> success) -> future.complete(success.result()),
      failure -> future.completeExceptionally(
        new AssertionError("Expected success but got failure: " + failure.reason())));

    return future.get(5, TimeUnit.SECONDS);
  }

  @SneakyThrows
  private Failure addBatchExpectingFailure(List<Instance> instances) {
    CompletableFuture<Failure> future = new CompletableFuture<>();

    createCollection().addBatch(instances,
      success -> future.completeExceptionally(new AssertionError("Expected failure but got success")),
      future::complete);

    return future.get(5, TimeUnit.SECONDS);
  }

  private Instance createInstance() {
    return new Instance(UUID.randomUUID().toString(), null, null, "FOLIO", INSTANCE_TITLE,
      UUID.randomUUID().toString());
  }

  private JsonObject createdInstanceJson(String id) {
    return new JsonObject()
      .put(Instance.ID, id)
      .put(Instance.SOURCE_KEY, "FOLIO")
      .put(Instance.TITLE_KEY, INSTANCE_TITLE)
      .put(Instance.INSTANCE_TYPE_ID_KEY, UUID.randomUUID().toString());
  }

  private JsonArray jsonArrayOf(JsonObject... instances) {
    return new JsonArray(Arrays.asList(instances));
  }

  private JsonArray jsonArrayOfStrings(String... messages) {
    return new JsonArray(Arrays.asList(messages));
  }

  private void stubBatchPost(int status, String contentType, String body) {
    WIRE_MOCK.stubFor(post(urlPathEqualTo(BATCH_PATH))
      .willReturn(aResponse()
        .withStatus(status)
        .withHeader(HttpHeaders.CONTENT_TYPE, contentType)
        .withBody(body)));
  }
}
