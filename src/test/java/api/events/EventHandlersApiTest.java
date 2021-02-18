package api.events;

import api.support.ApiRoot;
import api.support.ApiTests;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;
import org.folio.ActionProfile;
import org.folio.DataImportEventPayload;
import org.folio.JobProfile;
import org.folio.MappingProfile;
import org.folio.UserInfo;
import org.folio.inventory.TestUtil;
import org.folio.inventory.support.http.ContentType;
import org.folio.inventory.support.http.client.Response;
import org.folio.inventory.support.http.client.ResponseHandler;
import org.folio.processing.events.utils.ZIPArchiver;
import org.folio.rest.jaxrs.model.EntityType;
import org.folio.rest.jaxrs.model.MappingDetail;
import org.folio.rest.jaxrs.model.MappingRule;
import org.folio.rest.jaxrs.model.ParsedRecord;
import org.folio.rest.jaxrs.model.ProfileSnapshotWrapper;
import org.folio.rest.jaxrs.model.Record;
import org.junit.Test;
import support.fakes.FakeOkapi;

import java.io.IOException;
import java.net.MalformedURLException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

public class EventHandlersApiTest extends ApiTests {

  private static final String PARSED_RECORD = "{ \"leader\":\"01314nam  22003851a 4500\", \"fields\":[ {\"001\":\"009221\"},   { \"042\": { \"ind1\": \" \", \"ind2\": \" \", \"subfields\": [ { \"a\": \"pcc\" } ] } }, { \"042\": { \"ind1\": \" \", \"ind2\": \" \", \"subfields\": [ { \"a\": \"pcc\" } ] } }, { \"245\":\"American Bar Association journal\" } ] }";
  private static final String MAPPING_RULES_PATH = "src/test/resources/handlers/rules.json";

  @Test
  public void handleInstancesShouldReturnBadRequestOnEmptyBody() throws MalformedURLException, InterruptedException, ExecutionException, TimeoutException {
    final var conversionCompleted = okapiClient.post(ApiRoot.instancesEventHandler().toString(), (String) null);
    Response response = conversionCompleted.toCompletableFuture().get(1, TimeUnit.SECONDS);
    assertThat(response.getStatusCode(), is(500));
    assertThat(response.getContentType(), is(ContentType.TEXT_PLAIN));
  }

  @Test
  public void handleInstancesShouldReturnNoContentOnValidBody() throws IOException, InterruptedException, ExecutionException, TimeoutException {
    final var conversionCompleted = okapiClient.post(ApiRoot.instancesEventHandler().toString(),
      ZIPArchiver.zip(new JsonObject().toString()));

    Response response = conversionCompleted.toCompletableFuture().get(5, TimeUnit.SECONDS);
    assertThat(response.getStatusCode(), is(204));
  }

  @Test
  public void handleInstancesShouldReturnNoContentOnValidData() throws IOException, InterruptedException, ExecutionException, TimeoutException {
    final var conversionCompleted = okapiClient.post(ApiRoot.instancesEventHandler().toString(),
      ZIPArchiver.zip(JsonObject.mapFrom(prepareEventPayload()).toString()));

    Response response = conversionCompleted.toCompletableFuture().get(5, TimeUnit.SECONDS);
    assertThat(response.getStatusCode(), is(204));
  }

  private HashMap<String, String> prepareEventPayload() throws IOException {
    HashMap<String, String> eventPayload = new HashMap<>();
    eventPayload.put("MARC", PARSED_RECORD);
    eventPayload.put("MAPPING_RULES", TestUtil.readFileFromPath(MAPPING_RULES_PATH));
    eventPayload.put("MAPPING_PARAMS", "{}");
    return eventPayload;
  }

}
