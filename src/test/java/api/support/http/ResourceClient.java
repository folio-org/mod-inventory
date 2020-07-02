package api.support.http;

import static java.lang.String.format;
import static org.folio.inventory.support.http.client.ResponseHandler.any;
import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsNull.notNullValue;
import static org.hamcrest.junit.MatcherAssert.assertThat;

import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.folio.inventory.support.JsonArrayHelper;
import org.folio.inventory.support.http.client.IndividualResource;
import org.folio.inventory.support.http.client.OkapiHttpClient;
import org.folio.inventory.support.http.client.Response;
import org.folio.inventory.support.http.client.ResponseHandler;
import org.folio.util.StringUtil;
import org.joda.time.DateTime;

import api.support.builders.Builder;
import io.vertx.core.json.JsonObject;
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

  public IndividualResource create(Builder builder)
    throws InterruptedException,
    MalformedURLException,
    TimeoutException,
    ExecutionException {

    return create(builder.create());
  }

  public IndividualResource create(Object request)
    throws MalformedURLException,
    InterruptedException,
    ExecutionException,
    TimeoutException {

    Response response = attemptToCreate(request);

    assertThat(
      String.format("Failed to create %s: %s", resourceName, response.getBody()),
      response.getStatusCode(), is(HttpURLConnection.HTTP_CREATED));

    if(response.hasBody()) {
      return new IndividualResource(response);
    }
    else {
      assertThat(response.getLocation(), is(notNullValue()));

      CompletableFuture<Response> getCompleted = new CompletableFuture<>();

      client.get(response.getLocation(),
        ResponseHandler.any(getCompleted));

      return new IndividualResource(getCompleted.get(5, TimeUnit.SECONDS));
    }
  }

  public Response attemptToCreate(Object request)
    throws MalformedURLException, InterruptedException, ExecutionException,
    TimeoutException {

    CompletableFuture<Response> createCompleted = new CompletableFuture<>();

    //TODO: Reinstate json checking
    client.post(urlMaker.combine(""), request,
      ResponseHandler.any(createCompleted));

    return createCompleted.get(5, TimeUnit.SECONDS);
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

    CompletableFuture<Response> putCompleted = new CompletableFuture<>();

    client.put(urlMaker.combine(String.format("/%s", id)), request,
      ResponseHandler.any(putCompleted));

    return putCompleted.get(5, TimeUnit.SECONDS);
  }

  public Response getById(UUID id)
    throws MalformedURLException,
    InterruptedException,
    ExecutionException,
    TimeoutException {

    CompletableFuture<Response> getCompleted = new CompletableFuture<>();

    client.get(urlMaker.combine(String.format("/%s", id)),
      ResponseHandler.any(getCompleted));

    return getCompleted.get(5, TimeUnit.SECONDS);
  }

  public void delete(UUID id)
    throws MalformedURLException,
    InterruptedException,
    ExecutionException,
    TimeoutException {

    CompletableFuture<Response> deleteFinished = new CompletableFuture<>();

    client.delete(urlMaker.combine(String.format("/%s", id)),
      ResponseHandler.any(deleteFinished));

    Response response = deleteFinished.get(5, TimeUnit.SECONDS);

    assertThat(String.format(
      "Failed to delete %s %s: %s", resourceName, id, response.getBody()),
      response.getStatusCode(), is(204));
  }

  public void deleteAll()
    throws MalformedURLException,
    InterruptedException,
    ExecutionException,
    TimeoutException {

    CompletableFuture<Response> deleteAllFinished = new CompletableFuture<>();

    client.delete(urlMaker.combine(""),
      ResponseHandler.any(deleteAllFinished));

    Response response = deleteAllFinished.get(5, TimeUnit.SECONDS);

    assertThat(String.format(
      "Failed to delete %s: %s", resourceName, response.getBody()),
      response.getStatusCode(), is(204));
  }

  public void deleteAllIndividually()
    throws MalformedURLException,
    InterruptedException,
    ExecutionException,
    TimeoutException {

    List<JsonObject> records = getAll();

    records.stream().forEach(record -> {
      try {
        CompletableFuture<Response> deleteFinished = new CompletableFuture<>();

        client.delete(urlMaker.combine(String.format("/%s",
          record.getString("id"))),
          ResponseHandler.any(deleteFinished));

        Response deleteResponse = deleteFinished.get(5, TimeUnit.SECONDS);

        assertThat(String.format(
          "Failed to delete %s: %s", resourceName, deleteResponse.getBody()),
          deleteResponse.getStatusCode(), is(204));

      } catch (Throwable e) {
        assertThat(String.format("Exception whilst deleting %s individually: %s",
          resourceName, e.toString()),
          true, is(false));
      }
    });
  }

  public List<JsonObject> getAll()
    throws MalformedURLException,
    InterruptedException,
    ExecutionException,
    TimeoutException {

    CompletableFuture<Response> getFinished = new CompletableFuture<>();

    client.get(urlMaker.combine(""),
      ResponseHandler.any(getFinished));

    Response response = getFinished.get(5, TimeUnit.SECONDS);

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

    CompletableFuture<Response> getFinished = new CompletableFuture<>();

    client.get(urlMaker.combine(format("?query=%s&limit=%s", StringUtil.urlEncode(query), limit)),
      ResponseHandler.any(getFinished));

    return getFinished.get(5, TimeUnit.SECONDS);
  }

  public void emulateFailure(EndpointFailureDescriptor failureDescriptor)
    throws MalformedURLException, InterruptedException, ExecutionException, TimeoutException {

    final CompletableFuture<Response> future = new CompletableFuture<>();

    client.post(urlMaker.combine("/emulate-failure"),
      JsonObject.mapFrom(failureDescriptor), any(future));

    assertThat(future.get(5, TimeUnit.SECONDS).getStatusCode(), is(201));
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
