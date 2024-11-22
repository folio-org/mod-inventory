package org.folio.inventory.support.http.client;

import com.github.tomakehurst.wiremock.client.MappingBuilder;
import com.github.tomakehurst.wiremock.junit.WireMockRule;
import io.vertx.core.json.JsonObject;
import lombok.SneakyThrows;
import org.junit.Rule;
import org.junit.Test;

import java.net.URL;
import static com.github.tomakehurst.wiremock.client.WireMock.equalTo;
import static com.github.tomakehurst.wiremock.client.WireMock.equalToJson;
import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.client.WireMock.noContent;
import static com.github.tomakehurst.wiremock.client.WireMock.okJson;
import static com.github.tomakehurst.wiremock.client.WireMock.put;
import static com.github.tomakehurst.wiremock.client.WireMock.urlPathEqualTo;
import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.wireMockConfig;
import static org.folio.HttpStatus.HTTP_NO_CONTENT;
import static org.folio.HttpStatus.HTTP_OK;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.emptyOrNullString;


public class SynchronousHttpClientTest {

  @Rule
  public WireMockRule fakeWebServer = new WireMockRule(wireMockConfig().dynamicPort());

  private final String okapiUrl = "http://okapi.com";
  private final String tenantId = "test-tenant";
  private final String token = "eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJkaWt1X2FkbWluIiwidXNlcl9pZCI6ImFhMjZjYjg4LTc2YjEtNTQ1OS1hMjM1LWZjYTRmZDI3MGMyMyIsImlhdCI6MTU3NjAxMzY3MiwidGVuYW50IjoiZGlrdSJ9.oGCb0gDIdkXGlCiECvJHgQMXD3QKKW2vTh7PPCrpds8";
  private final String userId = "aa26cb88-76b1-5459-a235-fca4fd270c23";
  private final String requestId = "test-request-id";

  @Test
  @SneakyThrows
  public void canGetJson() {
    final String locationResponseHeader = "/a-different-location";

    fakeWebServer.stubFor(matchingFolioHeaders(get(urlPathEqualTo("/record")))
      .willReturn(okJson(dummyJsonResponseBody())
        .withHeader("Location", locationResponseHeader)));

    SynchronousHttpClient client = createClient();

    var response = client.get(new URL(fakeWebServer.url("/record")));

    assertThat(response.getStatusCode(), is(HTTP_OK.toInt()));
    assertThat(response.getJson().getString("message"), is("hello"));
    assertThat(response.getContentType(), is("application/json"));
    assertThat(response.getLocation(), is(locationResponseHeader));
  }

  @Test
  @SneakyThrows
  public void canPutWithJson() {
    fakeWebServer.stubFor(
      matchingFolioHeaders(put(urlPathEqualTo("/record/12345")))
        .withRequestBody(equalToJson(dummyJsonRequestBody().encodePrettily()))
        .willReturn(noContent()));

    SynchronousHttpClient client = createClient();

    var response = client.put(new URL(fakeWebServer.url("/record/12345")), dummyJsonRequestBody());

    assertThat(response.getStatusCode(), is(HTTP_NO_CONTENT.toInt()));
    assertThat(response.getBody(), is(emptyOrNullString()));
  }

  private JsonObject dummyJsonRequestBody() {
    return new JsonObject().put("from", "James");
  }

  private String dummyJsonResponseBody() {
    return new JsonObject().put("message", "hello")
      .encodePrettily();
  }

  @SneakyThrows
  private SynchronousHttpClient createClient() {
    return new SynchronousHttpClient(new URL(okapiUrl), tenantId, token, userId, requestId, error -> {});
  }

  private MappingBuilder matchingFolioHeaders(MappingBuilder mappingBuilder) {
    return mappingBuilder
      .withHeader("X-Okapi-Url", equalTo(okapiUrl))
      .withHeader("X-Okapi-Tenant", equalTo(tenantId))
      .withHeader("X-Okapi-Token", equalTo(token))
      .withHeader("X-Okapi-User-Id", equalTo(userId))
      .withHeader("X-Okapi-Request-Id", equalTo(requestId));
  }
}
