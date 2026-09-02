package org.folio.inventory.support.http.client;

import static com.github.tomakehurst.wiremock.client.WireMock.equalTo;
import static com.github.tomakehurst.wiremock.client.WireMock.equalToJson;
import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.client.WireMock.noContent;
import static com.github.tomakehurst.wiremock.client.WireMock.okJson;
import static com.github.tomakehurst.wiremock.client.WireMock.put;
import static com.github.tomakehurst.wiremock.client.WireMock.urlPathEqualTo;
import static io.vertx.core.http.HttpHeaders.LOCATION;
import static org.apache.hc.core5.http.ContentType.APPLICATION_JSON;
import static org.folio.HttpStatus.HTTP_NO_CONTENT;
import static org.folio.HttpStatus.HTTP_OK;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.emptyOrNullString;

import com.github.tomakehurst.wiremock.client.MappingBuilder;
import io.vertx.core.json.JsonObject;
import java.net.URI;
import lombok.SneakyThrows;
import org.folio.dataimport.testsupport.rest.BaseWireMockTest;
import org.folio.okapi.common.XOkapiHeaders;
import org.junit.jupiter.api.Test;

class SynchronousHttpClientTest extends BaseWireMockTest {

  private final String okapiUrl = "https://okapi.com";
  private final String tenantId = "test-tenant";
  private final String token = "token";
  private final String userId = "aa26cb88-76b1-5459-a235-fca4fd270c23";
  private final String requestId = "test-request-id";

  @Test
  @SneakyThrows
  void canGetJson() {
    final String locationResponseHeader = "/a-different-location";

    WIRE_MOCK.stubFor(matchingFolioHeaders(get(urlPathEqualTo("/record")))
      .willReturn(okJson(dummyJsonResponseBody())
        .withHeader(LOCATION.toString(), locationResponseHeader)));

    SynchronousHttpClient client = createClient();

    var response = client.get(new URI(WIRE_MOCK.url("/record")).toURL());

    assertThat(response.getStatusCode(), is(HTTP_OK.toInt()));
    assertThat(response.getJson().getString("message"), is("hello"));
    assertThat(response.getContentType(), is(APPLICATION_JSON.getMimeType()));
    assertThat(response.getLocation(), is(locationResponseHeader));
  }

  @Test
  @SneakyThrows
  void canPutWithJson() {
    WIRE_MOCK.stubFor(matchingFolioHeaders(put(urlPathEqualTo("/record/12345")))
      .withRequestBody(equalToJson(dummyJsonRequestBody().encodePrettily()))
      .willReturn(noContent()));

    SynchronousHttpClient client = createClient();

    var response = client.put(new URI(WIRE_MOCK.url("/record/12345")).toURL(), dummyJsonRequestBody());

    assertThat(response.getStatusCode(), is(HTTP_NO_CONTENT.toInt()));
    assertThat(response.getBody(), is(emptyOrNullString()));
  }

  private JsonObject dummyJsonRequestBody() {
    return new JsonObject().put("from", "James");
  }

  private String dummyJsonResponseBody() {
    return new JsonObject().put("message", "hello").encodePrettily();
  }

  @SneakyThrows
  private SynchronousHttpClient createClient() {
    return new SynchronousHttpClient(new URI(okapiUrl).toURL(), tenantId, token, userId, requestId, error -> { });
  }

  private MappingBuilder matchingFolioHeaders(MappingBuilder mappingBuilder) {
    return mappingBuilder
      .withHeader(XOkapiHeaders.URL, equalTo(okapiUrl))
      .withHeader(XOkapiHeaders.TENANT, equalTo(tenantId))
      .withHeader(XOkapiHeaders.TOKEN, equalTo(token))
      .withHeader(XOkapiHeaders.USER_ID, equalTo(userId))
      .withHeader(XOkapiHeaders.REQUEST_ID, equalTo(requestId));
  }
}
