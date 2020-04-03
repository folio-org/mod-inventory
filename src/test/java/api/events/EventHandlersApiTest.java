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
import static org.junit.Assert.assertThat;

public class EventHandlersApiTest extends ApiTests {

  @Test
  public void shouldReturnBadRequestOnEmptyBody() throws MalformedURLException, InterruptedException, ExecutionException, TimeoutException {
    CompletableFuture<Response> conversionCompleted = new CompletableFuture<>();
    okapiClient.post(ApiRoot.dataImportEventHandler(), null, ResponseHandler.text(conversionCompleted));
    Response response = conversionCompleted.get(1, TimeUnit.SECONDS);
    assertThat(response.getStatusCode(), is(500));
  }

  @Test
  public void shouldReturnNoContentOnValidBody() throws IOException, InterruptedException, ExecutionException, TimeoutException {
    CompletableFuture<Response> conversionCompleted = new CompletableFuture<>();
    okapiClient.post(ApiRoot.dataImportEventHandler(), ZIPArchiver.zip(new JsonObject().toString()), ResponseHandler.any(conversionCompleted));
    Response response = conversionCompleted.get(5, TimeUnit.SECONDS);
    assertThat(response.getStatusCode(), is(204));
  }

  @Test
  public void shouldReturnNoContentOnValidData() throws IOException, InterruptedException, ExecutionException, TimeoutException {
    CompletableFuture<Response> conversionCompleted = new CompletableFuture<>();
    okapiClient.post(ApiRoot.dataImportEventHandler(), ZIPArchiver.zip(JsonObject.mapFrom(prepareSnapshot()).toString()), ResponseHandler.any(conversionCompleted));
    Response response = conversionCompleted.get(5, TimeUnit.SECONDS);
    assertThat(response.getStatusCode(), is(204));
  }

  private DataImportEventPayload prepareSnapshot() {

    DataImportEventPayload payload = new DataImportEventPayload();
    payload.setJobExecutionId(UUID.randomUUID().toString());
    payload.setEventType("DI_SRS_MARC_BIB_RECORD_CREATED");
    payload.setOkapiUrl(FakeOkapi.getAddress());
    payload.setTenant("diku");
    payload.setToken("token");
    payload.setContext(new HashMap<>());
    payload.getContext().put(EntityType.MARC_BIBLIOGRAPHIC.value(), Json.encode(new Record()
      .withId(UUID.randomUUID().toString())
      .withParsedRecord(new ParsedRecord()
        .withId(UUID.randomUUID().toString())
        .withContent("{ \"leader\":\"01314nam  22003851a 4500\", \"fields\":[ {\"001\":\"009221\"},   { \"042\": { \"ind1\": \" \", \"ind2\": \" \", \"subfields\": [ { \"a\": \"pcc\" } ] } }, { \"042\": { \"ind1\": \" \", \"ind2\": \" \", \"subfields\": [ { \"a\": \"pcc\" } ] } }, { \"245\":\"American Bar Association journal\" } ] }"))));
    payload.getContext().put("MAPPING_RULES", "{}");
    payload.getContext().put("MAPPING_PARAMS", "{}");

    String jobProfileId = UUID.randomUUID().toString();
    ProfileSnapshotWrapper root = new ProfileSnapshotWrapper();
    root.setContentType(ProfileSnapshotWrapper.ContentType.JOB_PROFILE);
    root.setId(UUID.randomUUID().toString());
    root.setProfileId(jobProfileId);
    root.setOrder(0);
    root.setContent(new JobProfile()
      .withId(jobProfileId)
      .withDataType(JobProfile.DataType.MARC)
      .withDescription("description")
      .withUserInfo(new UserInfo()
        .withUserName("diku")
        .withFirstName("diku")
        .withLastName("admin"))
      .withName("test"));
    root.setChildSnapshotWrappers(Collections.singletonList(new ProfileSnapshotWrapper()
      .withId(UUID.randomUUID().toString())
      .withProfileId(UUID.randomUUID().toString())
      .withReactTo(ProfileSnapshotWrapper.ReactTo.NON_MATCH)
      .withOrder(0)
      .withContentType(ProfileSnapshotWrapper.ContentType.ACTION_PROFILE)
      .withContent(new ActionProfile()
        .withDescription("test")
        .withName("test")
        .withFolioRecord(ActionProfile.FolioRecord.INSTANCE)
        .withAction(ActionProfile.Action.CREATE)
        .withReactTo(ActionProfile.ReactTo.NON_MATCH)
        .withId(UUID.randomUUID().toString()))
      .withChildSnapshotWrappers(Collections.singletonList(new ProfileSnapshotWrapper()
        .withId(UUID.randomUUID().toString())
        .withProfileId(UUID.randomUUID().toString())
        .withContentType(ProfileSnapshotWrapper.ContentType.MAPPING_PROFILE)
        .withOrder(0)
        .withReactTo(ProfileSnapshotWrapper.ReactTo.NON_MATCH)
        .withContent(new MappingProfile()
          .withId(UUID.randomUUID().toString())
          .withName("test")
          .withExistingRecordType(EntityType.INSTANCE)
          .withIncomingRecordType(EntityType.MARC_BIBLIOGRAPHIC)
          .withDescription("test")
          .withMappingDetails(new MappingDetail()
            .withName("instance")
            .withRecordType(EntityType.INSTANCE)
            .withMappingFields(Arrays.asList(new MappingRule()
                .withName("instance.title")
                .withEnabled("true")
                .withPath("instance.title")
                .withValue("042$a"),
              new MappingRule()
                .withName("instance.instanceTypeId")
                .withEnabled("true")
                .withPath("instance.instanceTypeId")
                .withValue("\"" + UUID.randomUUID().toString() + "\"")))

          ))))));
    payload.setProfileSnapshot(root);
    return payload;
  }

}
