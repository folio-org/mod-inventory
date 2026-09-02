package org.folio.inventory.support.http.client;

import static com.github.tomakehurst.wiremock.client.WireMock.created;
import static com.github.tomakehurst.wiremock.client.WireMock.delete;
import static com.github.tomakehurst.wiremock.client.WireMock.equalTo;
import static com.github.tomakehurst.wiremock.client.WireMock.equalToJson;
import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.client.WireMock.noContent;
import static com.github.tomakehurst.wiremock.client.WireMock.okJson;
import static com.github.tomakehurst.wiremock.client.WireMock.post;
import static com.github.tomakehurst.wiremock.client.WireMock.put;
import static com.github.tomakehurst.wiremock.client.WireMock.urlPathEqualTo;
import static io.vertx.core.http.HttpHeaders.CONTENT_TYPE;
import static io.vertx.core.http.HttpHeaders.LOCATION;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.hc.core5.http.ContentType.APPLICATION_JSON;
import static org.folio.HttpStatus.HTTP_CREATED;
import static org.folio.HttpStatus.HTTP_NO_CONTENT;
import static org.folio.HttpStatus.HTTP_OK;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.emptyOrNullString;

import com.github.tomakehurst.wiremock.client.MappingBuilder;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.junit5.VertxExtension;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.Map;
import lombok.SneakyThrows;
import org.folio.dataimport.testsupport.rest.BaseWireMockTest;
import org.folio.okapi.common.XOkapiHeaders;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(VertxExtension.class)
class OkapiHttpClientTest extends BaseWireMockTest {

  private static Vertx vertx;

  private final URL okapiUrl = new URI("https://okapi.com").toURL();
  private final String tenantId = "test-tenant";
  private final String token = "token";
  private final String userId = "aa26cb88-76b1-5459-a235-fca4fd270c23";
  private final String requestId = "test-request-id";

  OkapiHttpClientTest() throws MalformedURLException, URISyntaxException { }

  @BeforeAll
  static void beforeAll(Vertx v) {
    vertx = v;
  }

  @SneakyThrows
  @Test
  void canPostWithJson() {
    final String locationResponseHeader = "/a-different-location";

    WIRE_MOCK.stubFor(matchingFolioHeaders(post(urlPathEqualTo("/record")))
      .withHeader(CONTENT_TYPE.toString(), equalTo(APPLICATION_JSON.getMimeType()))
      .withRequestBody(equalToJson(dummyJsonRequestBody().encodePrettily()))
      .willReturn(created().withBody(dummyJsonResponseBody())
        .withHeader(CONTENT_TYPE.toString(), APPLICATION_JSON.getMimeType())
        .withHeader(LOCATION.toString(), locationResponseHeader)));

    OkapiHttpClient client = createClient();

    final var postCompleted = client.post(WIRE_MOCK.url("/record"), dummyJsonRequestBody());

    final var response = postCompleted.toCompletableFuture().get(2, SECONDS);

    assertThat(response.getStatusCode(), is(HTTP_CREATED.toInt()));
    assertThat(response.getJson().getString("message"), is("hello"));
    assertThat(response.getContentType(), is(APPLICATION_JSON.getMimeType()));
    assertThat(response.getLocation(), is(locationResponseHeader));
  }

  @SneakyThrows
  @Test
  void canPostWithHeaders() {
    final String locationResponseHeader = "/a-different-location";
    Map<String, String> headers = Map.of(CONTENT_TYPE.toString(), "application/test");

    WIRE_MOCK.stubFor(matchingFolioHeaders(post(urlPathEqualTo("/record")))
      .withHeader(CONTENT_TYPE.toString(), equalTo("application/test"))
      .withRequestBody(equalToJson(dummyJsonRequestBody().encodePrettily()))
      .willReturn(created().withBody(dummyJsonResponseBody())
        .withHeader(CONTENT_TYPE.toString(), APPLICATION_JSON.getMimeType())
        .withHeader(LOCATION.toString(), locationResponseHeader)));

    OkapiHttpClient client = createClient();

    final var postCompleted = client.post(WIRE_MOCK.url("/record"), dummyJsonRequestBody().encode(), headers);

    final var response = postCompleted.toCompletableFuture().get(2, SECONDS);

    assertThat(response.getStatusCode(), is(HTTP_CREATED.toInt()));
    assertThat(response.getJson().getString("message"), is("hello"));
    assertThat(response.getContentType(), is(APPLICATION_JSON.getMimeType()));
    assertThat(response.getLocation(), is(locationResponseHeader));
  }

  @Test
  @SneakyThrows
  void canGetJson() {
    final String locationResponseHeader = "/a-different-location";
    WIRE_MOCK.stubFor(matchingFolioHeaders(get(urlPathEqualTo("/record")))
      .willReturn(okJson(dummyJsonResponseBody())
        .withHeader(LOCATION.toString(), locationResponseHeader)));

    OkapiHttpClient client = createClient();

    final var getCompleted = client.get(WIRE_MOCK.url("/record"));

    final Response response = getCompleted.toCompletableFuture().get(2, SECONDS);

    assertThat(response.getStatusCode(), is(HTTP_OK.toInt()));
    assertThat(response.getJson().getString("message"), is("hello"));
    assertThat(response.getContentType(), is(APPLICATION_JSON.getMimeType()));
    assertThat(response.getLocation(), is(locationResponseHeader));
  }

  @Test
  @SneakyThrows
  void canPutWithJson() {
    WIRE_MOCK.stubFor(matchingFolioHeaders(put(urlPathEqualTo("/record/12345")))
      .withHeader(CONTENT_TYPE.toString(), equalTo(APPLICATION_JSON.getMimeType()))
      .withRequestBody(equalToJson(dummyJsonRequestBody().encodePrettily()))
      .willReturn(noContent()));

    OkapiHttpClient client = createClient();

    final var postCompleted = client.put(WIRE_MOCK.url("/record/12345"), dummyJsonRequestBody());

    final Response response = postCompleted.toCompletableFuture().get(2, SECONDS);

    assertThat(response.getStatusCode(), is(HTTP_NO_CONTENT.toInt()));
    assertThat(response.getBody(), is(emptyOrNullString()));
  }

  @Test
  @SneakyThrows
  void canDelete() {
    WIRE_MOCK.stubFor(matchingFolioHeaders(delete(urlPathEqualTo("/record")))
      .willReturn(noContent()));

    OkapiHttpClient client = createClient();

    final var deleteCompleted = client.delete(WIRE_MOCK.url("/record"));

    final Response response = deleteCompleted.toCompletableFuture().get(2, SECONDS);

    assertThat(response.getStatusCode(), is(HTTP_NO_CONTENT.toInt()));
    assertThat(response.getBody(), is(emptyOrNullString()));
  }

  private MappingBuilder matchingFolioHeaders(MappingBuilder mappingBuilder) {
    return mappingBuilder
      .withHeader(XOkapiHeaders.URL, equalTo(okapiUrl.toString()))
      .withHeader(XOkapiHeaders.TENANT, equalTo(tenantId))
      .withHeader(XOkapiHeaders.TOKEN, equalTo(token))
      .withHeader(XOkapiHeaders.USER_ID, equalTo(userId))
      .withHeader(XOkapiHeaders.REQUEST_ID, equalTo(requestId));
  }

  private OkapiHttpClient createClient() {
    return new OkapiHttpClient(vertx, okapiUrl, tenantId, token, userId, requestId, error -> { });
  }

  private JsonObject dummyJsonRequestBody() {
    return new JsonObject().put("from", "James");
  }

  private String dummyJsonResponseBody() {
    return new JsonObject().put("message", "hello").encodePrettily();
  }
}
