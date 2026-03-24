package api.support.http;

import static java.lang.String.format;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.folio.util.StringUtil.urlEncode;
import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsNull.notNullValue;
import static org.hamcrest.junit.MatcherAssert.assertThat;

import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import org.folio.inventory.support.JsonArrayHelper;
import org.folio.inventory.support.http.client.IndividualResource;
import org.folio.inventory.support.http.client.OkapiHttpClient;
import org.folio.inventory.support.http.client.Response;
import org.folio.util.PercentCodec;
import org.joda.time.DateTime;

import api.support.builders.Builder;
import io.vertx.core.json.JsonObject;
import lombok.SneakyThrows;
import support.fakes.EndpointFailureDescriptor;

public class ResourceClient {

  private final OkapiHttpClient client;
  private final UrlMaker urlMaker;
  private final String resourceName;
  private final String collectionArrayPropertyName;

  public static ResourceClient forHoldingsStorage(OkapiHttpClient client) {
    return new ResourceClient(client, StorageInterfaceUrls::holdingStorageUrl,
      "holdingsRecords");
  }

  public static ResourceClient forItemsStorage(OkapiHttpClient client) {
    return new ResourceClient(client, StorageInterfaceUrls::itemsStorageUrl,
      "items");
  }

  public static ResourceClient forItems(OkapiHttpClient client) {
    return new ResourceClient(client, BusinessLogicInterfaceUrls::items,
      "items");
  }

  public static ResourceClient forIsbns(OkapiHttpClient client) {
    return new ResourceClient(client, BusinessLogicInterfaceUrls::isbns,
      "isbns");
  }

  public static ResourceClient forInstances(OkapiHttpClient client) {
    return new ResourceClient(client, BusinessLogicInterfaceUrls::instances,
      "instances");
  }

  public static ResourceClient forInstancesStorage(OkapiHttpClient okapiClient) {
    return new ResourceClient(okapiClient, StorageInterfaceUrls::instancesStorageUrl,
      "instances");
  }

  public static ResourceClient forInstancesBatch(OkapiHttpClient client) {
    return new ResourceClient(client, BusinessLogicInterfaceUrls::instancesBatch,
      "instances");
  }

  public static ResourceClient forBoundWithItems (OkapiHttpClient okapiClient) {
    return new ResourceClient( okapiClient, BusinessLogicInterfaceUrls::boundWithItemsUrl,
      "bound-with items", "items");
  }

  public static ResourceClient forInstitutions(OkapiHttpClient client) {
    return new ResourceClient(client, StorageInterfaceUrls::institutionsStorageUrl,
      "institutions", "locinsts");
  }

  public static ResourceClient forCampuses(OkapiHttpClient client) {
    return new ResourceClient(client, StorageInterfaceUrls::campusesStorageUrl,
      "campuses", "loccamps");
  }

  public static ResourceClient forLibraries(OkapiHttpClient client) {
    return new ResourceClient(client, StorageInterfaceUrls::librariesStorageUrl,
      "libraries", "loclibs");
  }

  public static ResourceClient forLocations(OkapiHttpClient client) {
    return new ResourceClient(client, StorageInterfaceUrls::locationsStorageUrl,
      "locations");
  }

  public static ResourceClient forUsers(OkapiHttpClient client) {
    return new ResourceClient(client, StorageInterfaceUrls::usersStorageUrl,
      "users");
  }

  public static ResourceClient forUserTenants(OkapiHttpClient client) {
    return new ResourceClient(client, StorageInterfaceUrls::userTenantsStorageUrl,
      "user-tenants");
  }

  public static ResourceClient forNatureOfContentTerms(OkapiHttpClient client) {
    return new ResourceClient(
      client,
      StorageInterfaceUrls::natureOfContentTermsStorageUrl,
      "Nature of content terms",
      "natureOfContentTerms"
    );
  }

  public static ResourceClient forPrecedingSucceedingTitles(OkapiHttpClient okapiClient) {
    return new ResourceClient(okapiClient, StorageInterfaceUrls::precedingSucceedingTitlesUrl,
      "Preceding-succeeding titles", "precedingSucceedingTitles");
  }

  public static ResourceClient forInstanceRelationship(OkapiHttpClient okapiClient) {
    return new ResourceClient(okapiClient, StorageInterfaceUrls::instanceRelationshipUrl,
      "Instance relationships", "instanceRelationships");
  }

  public static ResourceClient forInstanceRelationshipType(OkapiHttpClient okapiClient) {
    return new ResourceClient(okapiClient, StorageInterfaceUrls::instanceRelationshipTypeUrl,
      "Instance relationship types", "instanceRelationshipTypes");
  }

  public static ResourceClient forRequestStorage(OkapiHttpClient okapiClient) {
    return new ResourceClient(okapiClient, StorageInterfaceUrls::requestStorageUrl,
      "request storage", "requests");
  }

  public static ResourceClient forSourceRecordStorage(OkapiHttpClient okapiClient) {
    return new ResourceClient(okapiClient, StorageInterfaceUrls::sourceRecordStorageUrl,
      "source record storage", "records");
  }

  public static ResourceClient forHoldingSourceRecord(OkapiHttpClient okapiClient) {
    return new ResourceClient(okapiClient, StorageInterfaceUrls::holdingRecordSourcesUrl,
      "holdings source record", "holdingsRecordsSources");
  }

  public static ResourceClient forBoundWithPartsStorage (OkapiHttpClient okapiClient) {
    return new ResourceClient( okapiClient, StorageInterfaceUrls::boundWithPartsUrl,
      "bound-with parts storage", "boundWithParts");
  }

  private ResourceClient(
    OkapiHttpClient client,
    UrlMaker urlMaker, String resourceName,
    String collectionArrayPropertyName) {

    this.client = client;
    this.urlMaker = urlMaker;
    this.resourceName = resourceName;
    this.collectionArrayPropertyName = collectionArrayPropertyName;
  }

  private ResourceClient(
    OkapiHttpClient client,
    UrlMaker urlMaker, String resourceName) {

    this.client = client;
    this.urlMaker = urlMaker;
    this.resourceName = resourceName;
    this.collectionArrayPropertyName = resourceName;
  }

  public IndividualResource create(Builder builder) {
    return create(builder.create());
  }

  @SneakyThrows
  public IndividualResource create(JsonObject request) {
    Response response = attemptToCreate(request);

    assertThat(
      String.format("Failed to create %s: %s", resourceName, response.getBody()),
      response.getStatusCode(), is(HttpURLConnection.HTTP_CREATED));

    if(response.hasBody()) {
      return new IndividualResource(response);
    }
    else {
      assertThat(response.getLocation(), is(notNullValue()));

      final var getCompleted = client.get(response.getLocation());

      return new IndividualResource(getCompleted.toCompletableFuture().get(5, SECONDS));
    }
  }

  public Response attemptToCreate(JsonObject request)
    throws MalformedURLException, InterruptedException, ExecutionException,
    TimeoutException {

    //TODO: Reinstate json checking
    final var createCompleted = client.post(urlMaker.combine(""), request);

    return createCompleted.toCompletableFuture().get(5, SECONDS);
  }

  public void replace(UUID id, Builder builder)
    throws MalformedURLException,
    InterruptedException,
    ExecutionException,
    TimeoutException {

    replace(id, builder.create());
  }

  public void replace(UUID id, JsonObject request)
    throws MalformedURLException,
    InterruptedException,
    ExecutionException,
    TimeoutException {

    Response putResponse = attemptToReplace(id, request);

    assertThat(
      String.format("Failed to update %s %s: %s", resourceName, id, putResponse.getBody()),
      putResponse.getStatusCode(), is(HttpURLConnection.HTTP_NO_CONTENT));
  }

  public Response attemptToReplace(UUID id, JsonObject request)
    throws MalformedURLException, InterruptedException, ExecutionException,
    TimeoutException {

    final var putCompleted = client.put(
      urlMaker.combine(String.format("/%s", id)), request);

    return putCompleted.toCompletableFuture().get(5, SECONDS);
  }

  public void patch(UUID id, JsonObject request)
    throws MalformedURLException,
    InterruptedException,
    ExecutionException,
    TimeoutException {

    Response patchResponse = attemptToPatch(id, request);

    assertThat(
      String.format("Failed to update %s %s: %s", resourceName, id, patchResponse.getBody()),
      patchResponse.getStatusCode(), is(HttpURLConnection.HTTP_NO_CONTENT));
  }

  public Response attemptToPatch(UUID id, JsonObject request)
    throws MalformedURLException, InterruptedException, ExecutionException,
    TimeoutException {

    final var patchCompleted = client.patch(
      urlMaker.combine(String.format("/%s", id)).toString(), request);

    return patchCompleted.toCompletableFuture().get(5, SECONDS);
  }

  @SneakyThrows
  public Response getById(UUID id) {
    final var getCompleted = client.get(urlMaker.combine(String.format("/%s", id)));

    return getCompleted.toCompletableFuture().get(5, SECONDS);
  }

  public void delete(UUID id)
    throws MalformedURLException,
    InterruptedException,
    ExecutionException,
    TimeoutException {

    final var deleteCompleted
      = client.delete(urlMaker.combine(String.format("/%s", id)));

    Response response = deleteCompleted.toCompletableFuture().get(5, SECONDS);

    assertThat(String.format(
      "Failed to delete %s %s: %s", resourceName, id, response.getBody()),
      response.getStatusCode(), is(204));
  }

  public void deleteAll()
    throws MalformedURLException,
    InterruptedException,
    ExecutionException,
    TimeoutException {

    final var deleteCompleted = client.delete(urlMaker.combine(
        "?query=" + PercentCodec.encode("cql.allRecords=1")));

    Response response = deleteCompleted.toCompletableFuture().get(5, SECONDS);

    assertThat(String.format(
      "Failed to delete %s: %s", resourceName, response.getBody()),
      response.getStatusCode(), is(204));
  }

  public List<JsonObject> getAll()
    throws MalformedURLException,
    InterruptedException,
    ExecutionException,
    TimeoutException {

    final var getFinished = client.get(urlMaker.combine(""));

    Response response = getFinished.toCompletableFuture().get(5, SECONDS);

    assertThat(format("Get all records failed: %s", response.getBody()),
      response.getStatusCode(), is(200));

    return JsonArrayHelper.toList(response.getJson()
      .getJsonArray(collectionArrayPropertyName));
  }

  public List<JsonObject> getMany(String query, Integer limit) throws MalformedURLException,
    InterruptedException, ExecutionException, TimeoutException {

    Response response = attemptGetMany(query, limit);

    assertThat(format("Get all records failed: %s", response.getBody()),
      response.getStatusCode(), is(200));

    return JsonArrayHelper.toList(response.getJson()
      .getJsonArray(collectionArrayPropertyName));
  }

  public Response attemptGetMany(String query, Integer limit) throws MalformedURLException,
    InterruptedException, ExecutionException, TimeoutException {

    final var getFinished = client.get(
      urlMaker.combine(format("?query=%s&limit=%s", urlEncode(query), limit)));

    return getFinished.toCompletableFuture().get(5, SECONDS);
  }

  public void emulateFailure(EndpointFailureDescriptor failureDescriptor)
    throws MalformedURLException, InterruptedException, ExecutionException, TimeoutException {

    final var future = client.post(urlMaker.combine("/emulate-failure"),
      JsonObject.mapFrom(failureDescriptor));

    assertThat(future.toCompletableFuture().get(5, SECONDS).getStatusCode(), is(201));
  }

  public void disableFailureEmulation()
    throws MalformedURLException, InterruptedException, ExecutionException, TimeoutException {

    emulateFailure(new EndpointFailureDescriptor()
      .setFailureExpireDate(DateTime.now().minusMinutes(1).toDate()));
  }

  @FunctionalInterface
  public interface UrlMaker {
    URL combine(String subPath) throws MalformedURLException;
  }
}
