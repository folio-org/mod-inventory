package api;

import api.support.ApiRoot;
import api.support.ApiTests;
import api.support.InstanceApiClient;
import api.support.Preparation;
import com.github.jsonldjava.core.DocumentLoader;
import com.github.jsonldjava.core.JsonLdError;
import com.github.jsonldjava.core.JsonLdOptions;
import com.github.jsonldjava.core.JsonLdProcessor;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.apache.http.Header;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.cache.CachingHttpClientBuilder;
import org.apache.http.message.BasicHeader;
import org.folio.inventory.support.JsonArrayHelper;
import org.folio.inventory.support.http.client.OkapiHttpClient;
import org.folio.inventory.support.http.client.Response;
import org.folio.inventory.support.http.client.ResponseHandler;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static api.support.InstanceSamples.*;
import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.assertThat;

public class InstancesApiExamples extends ApiTests {
  private final OkapiHttpClient okapiClient;

  public InstancesApiExamples() throws MalformedURLException {
    okapiClient = ApiTestSuite.createOkapiHttpClient();
  }

  @Before
  public void setup()
    throws InterruptedException,
    MalformedURLException,
    TimeoutException,
    ExecutionException {

    new Preparation(okapiClient).deleteInstances();
  }

  @Test
  public void canCreateAnInstance()
    throws InterruptedException,
    MalformedURLException,
    TimeoutException,
    ExecutionException,
    JsonLdError {

    JsonObject newInstanceRequest = new JsonObject()
      .put("title", "Long Way to a Small Angry Planet")
      .put("identifiers", new JsonArray().add(new JsonObject()
        .put("identifierTypeId", ApiTestSuite.getIsbnIdentifierType())
        .put("value", "9781473619777")))
      .put("creators", new JsonArray().add(new JsonObject()
        .put("creatorTypeId", ApiTestSuite.getPersonalCreatorType())
        .put("name", "Chambers, Becky")))
      .put("source", "Local")
      .put("instanceTypeId", ApiTestSuite.getBooksInstanceType());

    CompletableFuture<Response> postCompleted = new CompletableFuture<>();

    okapiClient.post(ApiRoot.instances(),
      newInstanceRequest, ResponseHandler.any(postCompleted));

    Response postResponse = postCompleted.get(5, TimeUnit.SECONDS);

    String location = postResponse.getLocation();

    assertThat(postResponse.getStatusCode(), is(201));
    assertThat(location, is(notNullValue()));

    CompletableFuture<Response> getCompleted = new CompletableFuture<>();

    okapiClient.get(location, ResponseHandler.json(getCompleted));

    Response getResponse = getCompleted.get(5, TimeUnit.SECONDS);

    assertThat(getResponse.getStatusCode(), is(200));

    JsonObject createdInstance = getResponse.getJson();

    assertThat(createdInstance.containsKey("id"), is(true));
    assertThat(createdInstance.getString("title"), is("Long Way to a Small Angry Planet"));
    assertThat(createdInstance.getString("source"), is("Local"));
    assertThat(createdInstance.getString("instanceTypeId"), is(ApiTestSuite.getBooksInstanceType()));

    JsonObject firstIdentifier = createdInstance.getJsonArray("identifiers")
      .getJsonObject(0);

    assertThat(firstIdentifier.getString("identifierTypeId"),
      is(ApiTestSuite.getIsbnIdentifierType()));

    assertThat(firstIdentifier.getString("value"), is("9781473619777"));

    JsonObject firstCreator = createdInstance.getJsonArray("creators")
      .getJsonObject(0);

    assertThat(firstCreator.getString("creatorTypeId"),
      is(ApiTestSuite.getPersonalCreatorType()));

    assertThat(firstCreator.getString("name"), is("Chambers, Becky"));

    expressesDublinCoreMetadata(createdInstance);
    dublinCoreContextLinkRespectsWayResourceWasReached(createdInstance);
    selfLinkRespectsWayResourceWasReached(createdInstance);
    selfLinkShouldBeReachable(createdInstance);
  }

  @Test
  public void canCreateAnInstanceWithAnID()
    throws InterruptedException,
    MalformedURLException,
    TimeoutException,
    ExecutionException,
    JsonLdError {

    String instanceId = UUID.randomUUID().toString();

    JsonObject newInstanceRequest = new JsonObject()
      .put("id", instanceId)
      .put("title", "Long Way to a Small Angry Planet")
      .put("identifiers", new JsonArray().add(new JsonObject()
      .put("identifierTypeId", ApiTestSuite.getIsbnIdentifierType())
      .put("value", "9781473619777")))
      .put("creators", new JsonArray().add(new JsonObject()
      .put("creatorTypeId", ApiTestSuite.getPersonalCreatorType())
      .put("name", "Chambers, Becky")))
      .put("source", "Local")
      .put("instanceTypeId", ApiTestSuite.getBooksInstanceType());

    CompletableFuture<Response> postCompleted = new CompletableFuture<>();

    okapiClient.post(ApiRoot.instances(),
      newInstanceRequest, ResponseHandler.any(postCompleted));

    Response postResponse = postCompleted.get(5, TimeUnit.SECONDS);

    String location = postResponse.getLocation();

    assertThat(postResponse.getStatusCode(), is(201));
    assertThat(location, is(notNullValue()));

    CompletableFuture<Response> getCompleted = new CompletableFuture<>();

    okapiClient.get(location, ResponseHandler.json(getCompleted));

    Response getResponse = getCompleted.get(5, TimeUnit.SECONDS);

    assertThat(getResponse.getStatusCode(), is(200));

    JsonObject createdInstance = getResponse.getJson();

    assertThat(createdInstance.containsKey("id"), is(true));
    assertThat(createdInstance.getString("title"), is("Long Way to a Small Angry Planet"));
    assertThat(createdInstance.getString("source"), is("Local"));
    assertThat(createdInstance.getString("instanceTypeId"), is(ApiTestSuite.getBooksInstanceType()));

    JsonObject firstIdentifier = createdInstance.getJsonArray("identifiers")
      .getJsonObject(0);

    assertThat(firstIdentifier.getString("identifierTypeId"),
      is(ApiTestSuite.getIsbnIdentifierType()));

    assertThat(firstIdentifier.getString("value"), is("9781473619777"));

    JsonObject firstCreator = createdInstance.getJsonArray("creators")
      .getJsonObject(0);

    assertThat(firstCreator.getString("creatorTypeId"),
      is(ApiTestSuite.getPersonalCreatorType()));

    assertThat(firstCreator.getString("name"), is("Chambers, Becky"));

    expressesDublinCoreMetadata(createdInstance);
    dublinCoreContextLinkRespectsWayResourceWasReached(createdInstance);
    selfLinkRespectsWayResourceWasReached(createdInstance);
    selfLinkShouldBeReachable(createdInstance);
  }

  @Test
  public void instanceTitleIsMandatory()
    throws InterruptedException,
    MalformedURLException,
    TimeoutException,
    ExecutionException {

    JsonObject newInstanceRequest = new JsonObject();

    CompletableFuture<Response> postCompleted = new CompletableFuture<>();

    okapiClient.post(ApiRoot.instances(),
      newInstanceRequest, ResponseHandler.text(postCompleted));

    Response postResponse = postCompleted.get(5, TimeUnit.SECONDS);

    assertThat(postResponse.getStatusCode(), is(400));
    assertThat(postResponse.getLocation(), is(nullValue()));
    assertThat(postResponse.getBody(), is("Title must be provided for an instance"));
  }

  @Test
  public void canUpdateAnExistingInstance()
    throws InterruptedException,
    MalformedURLException,
    TimeoutException,
    ExecutionException {

    UUID id = UUID.randomUUID();

    JsonObject newInstance = createInstance(smallAngryPlanet(id));

    JsonObject updateInstanceRequest = smallAngryPlanet(id)
      .put("title", "The Long Way to a Small, Angry Planet");

    URL instanceLocation = new URL(String.format("%s/%s", ApiRoot.instances(),
      newInstance.getString("id")));

    CompletableFuture<Response> putCompleted = new CompletableFuture<>();

    okapiClient.put(instanceLocation, updateInstanceRequest,
      ResponseHandler.any(putCompleted));

    Response putResponse = putCompleted.get(5, TimeUnit.SECONDS);

    assertThat(putResponse.getStatusCode(), is(204));

    CompletableFuture<Response> getCompleted = new CompletableFuture<>();

    okapiClient.get(instanceLocation, ResponseHandler.json(getCompleted));

    Response getResponse = getCompleted.get(5, TimeUnit.SECONDS);

    assertThat(getResponse.getStatusCode(), is(200));

    JsonObject updatedInstance = getResponse.getJson();

    assertThat(updatedInstance.getString("id"), is(newInstance.getString("id")));
    assertThat(updatedInstance.getString("title"), is("The Long Way to a Small, Angry Planet"));
    assertThat(updatedInstance.getJsonArray("identifiers").size(), is(1));

    selfLinkRespectsWayResourceWasReached(updatedInstance);
    selfLinkShouldBeReachable(updatedInstance);
  }

  @Test
  public void cannotUpdateAnInstanceThatDoesNotExist()
    throws InterruptedException,
    MalformedURLException,
    TimeoutException,
    ExecutionException {

    JsonObject updateInstanceRequest = smallAngryPlanet(UUID.randomUUID());

    CompletableFuture<Response> putCompleted = new CompletableFuture<>();

    URL instanceLocation = new URL(String.format("%s/%s", ApiRoot.instances(),
      updateInstanceRequest.getString("id")));

    okapiClient.put(instanceLocation, updateInstanceRequest,
        ResponseHandler.any(putCompleted));

    Response putResponse = putCompleted.get(5, TimeUnit.SECONDS);

    assertThat(putResponse.getStatusCode(), is(404));
  }

  @Test
  public void canDeleteAllInstances()
    throws InterruptedException,
    MalformedURLException,
    TimeoutException,
    ExecutionException {

    createInstance(smallAngryPlanet(UUID.randomUUID()));
    createInstance(nod(UUID.randomUUID()));
    createInstance(leviathanWakes(UUID.randomUUID()));

    CompletableFuture<Response> deleteCompleted = new CompletableFuture<Response>();

    okapiClient.delete(ApiRoot.instances(), ResponseHandler.any(deleteCompleted));

    Response deleteResponse = deleteCompleted.get(5, TimeUnit.SECONDS);

    assertThat(deleteResponse.getStatusCode(), is(204));
    assertThat(deleteResponse.hasBody(), is(false));

    CompletableFuture<Response> getAllCompleted = new CompletableFuture<>();

    okapiClient.get(ApiRoot.instances(), ResponseHandler.json(getAllCompleted));

    Response getAllResponse = getAllCompleted.get(5, TimeUnit.SECONDS);

    assertThat(getAllResponse.getJson().getJsonArray("instances").size(), is(0));
    assertThat(getAllResponse.getJson().getInteger("totalRecords"), is(0));
  }

  @Test
  public void canDeleteAnInstance()
    throws InterruptedException,
    MalformedURLException,
    TimeoutException,
    ExecutionException {

    createInstance(smallAngryPlanet(UUID.randomUUID()));
    createInstance(nod(UUID.randomUUID()));

    JsonObject instanceToDelete = createInstance(leviathanWakes(UUID.randomUUID()));

    URL instanceToDeleteLocation = new URL(String.format("%s/%s",
      ApiRoot.instances(), instanceToDelete.getString("id")));

    CompletableFuture<Response> deleteCompleted = new CompletableFuture<>();

    okapiClient.delete(instanceToDeleteLocation,
      ResponseHandler.any(deleteCompleted));

    Response deleteResponse = deleteCompleted.get(5, TimeUnit.SECONDS);

    assertThat(deleteResponse.getStatusCode(), is(204));
    assertThat(deleteResponse.hasBody(), is(false));

    CompletableFuture<Response> getCompleted = new CompletableFuture<>();

    okapiClient.get(instanceToDeleteLocation, ResponseHandler.any(getCompleted));

    Response getResponse = getCompleted.get(5, TimeUnit.SECONDS);

    assertThat(getResponse.getStatusCode(), is(404));

    CompletableFuture<Response> getAllCompleted = new CompletableFuture<>();

    okapiClient.get(ApiRoot.instances(), ResponseHandler.json(getAllCompleted));

    Response getAllResponse = getAllCompleted.get(5, TimeUnit.SECONDS);

    assertThat(getAllResponse.getJson().getJsonArray("instances").size(), is(2));
    assertThat(getAllResponse.getJson().getInteger("totalRecords"), is(2));
  }

  @Test
  public void canGetAllInstances()
    throws InterruptedException,
    MalformedURLException,
    TimeoutException,
    ExecutionException {

    createInstance(smallAngryPlanet(UUID.randomUUID()));
    createInstance(nod(UUID.randomUUID()));
    createInstance(temeraire(UUID.randomUUID()));

    CompletableFuture<Response> getAllCompleted = new CompletableFuture<>();

    okapiClient.get(ApiRoot.instances(), ResponseHandler.json(getAllCompleted));

    Response getAllResponse = getAllCompleted.get(5, TimeUnit.SECONDS);

    assertThat(getAllResponse.getStatusCode(), is(200));

    List<JsonObject> instances = JsonArrayHelper.toList(
      getAllResponse.getJson().getJsonArray("instances"));

    assertThat(instances.size(), is(3));
    assertThat(getAllResponse.getJson().getInteger("totalRecords"), is(3));

    hasCollectionProperties(instances);
  }

  @Test
  public void canPageAllInstances()
    throws InterruptedException,
    MalformedURLException,
    TimeoutException,
    ExecutionException {

    createInstance(smallAngryPlanet(UUID.randomUUID()));
    createInstance(nod(UUID.randomUUID()));
    createInstance(temeraire(UUID.randomUUID()));
    createInstance(leviathanWakes(UUID.randomUUID()));
    createInstance(taoOfPooh(UUID.randomUUID()));

    CompletableFuture<Response> firstPageGetCompleted = new CompletableFuture<>();
    CompletableFuture<Response> secondPageGetCompleted = new CompletableFuture<>();

    okapiClient.get(ApiRoot.instances("limit=3"),
      ResponseHandler.json(firstPageGetCompleted));

    okapiClient.get(ApiRoot.instances("limit=3&offset=3"),
      ResponseHandler.json(secondPageGetCompleted));

    Response firstPageResponse = firstPageGetCompleted.get(5, TimeUnit.SECONDS);
    Response secondPageResponse = secondPageGetCompleted.get(5, TimeUnit.SECONDS);

    assertThat(firstPageResponse.getStatusCode(), is(200));
    assertThat(secondPageResponse.getStatusCode(), is(200));

    List<JsonObject> firstPageInstances = JsonArrayHelper.toList(
      firstPageResponse.getJson().getJsonArray("instances"));

    assertThat(firstPageInstances.size(), is(3));
    assertThat(firstPageResponse.getJson().getInteger("totalRecords"), is(5));

    hasCollectionProperties(firstPageInstances);

    List<JsonObject> secondPageInstances = JsonArrayHelper.toList(
      secondPageResponse.getJson().getJsonArray("instances"));

    assertThat(secondPageInstances.size(), is(2));
    assertThat(secondPageResponse.getJson().getInteger("totalRecords"), is(5));

    hasCollectionProperties(secondPageInstances);
  }

  @Test
  public void pageParametersMustBeNumeric()
    throws InterruptedException,
    MalformedURLException,
    TimeoutException,
    ExecutionException {

    CompletableFuture<Response> getPagedCompleted = new CompletableFuture<>();

    okapiClient.get(ApiRoot.instances("limit=&offset="),
      ResponseHandler.text(getPagedCompleted));

    Response getPagedResponse = getPagedCompleted.get(5, TimeUnit.SECONDS);

    assertThat(getPagedResponse.getStatusCode(), is(400));
    assertThat(getPagedResponse.getBody(),
      is("limit and offset must be numeric when supplied"));
  }

  @Test
  public void canSearchForInstancesByTitle()
    throws InterruptedException,
    MalformedURLException,
    TimeoutException,
    ExecutionException {

    createInstance(smallAngryPlanet(UUID.randomUUID()));
    createInstance(nod(UUID.randomUUID()));
    createInstance(uprooted(UUID.randomUUID()));

    CompletableFuture<Response> searchGetCompleted = new CompletableFuture<>();

    okapiClient.get(ApiRoot.instances("query=title=*Small%20Angry*"),
      ResponseHandler.json(searchGetCompleted));

    Response searchGetResponse = searchGetCompleted.get(5, TimeUnit.SECONDS);

    assertThat(searchGetResponse.getStatusCode(), is(200));

    List<JsonObject> instances = JsonArrayHelper.toList(
      searchGetResponse.getJson().getJsonArray("instances"));

    assertThat(instances.size(), is(1));
    assertThat(searchGetResponse.getJson().getInteger("totalRecords"), is(1));
    assertThat(instances.get(0).getString("title"), is("Long Way to a Small Angry Planet"));

    hasCollectionProperties(instances);
  }

  @Test
  public void cannotFindAnUnknownInstance()
    throws InterruptedException,
    MalformedURLException,
    TimeoutException,
    ExecutionException {

    CompletableFuture<Response> getCompleted = new CompletableFuture<>();

    okapiClient.get(String.format("%s/%s", ApiRoot.instances(), UUID.randomUUID()),
      ResponseHandler.any(getCompleted));

    Response getResponse = getCompleted.get(5, TimeUnit.SECONDS);

    assertThat(getResponse.getStatusCode(), is(404));
  }

  private void hasCollectionProperties(List<JsonObject> instances) {
    instances.stream().forEach(instance -> {
      try {
        expressesDublinCoreMetadata(instance);
      } catch (JsonLdError jsonLdError) {
        Assert.fail(jsonLdError.toString());
      }
    });

    instances.stream().forEach(instance ->
      dublinCoreContextLinkRespectsWayResourceWasReached(instance));

    instances.stream().forEach(instance ->
      selfLinkRespectsWayResourceWasReached(instance));

    instances.stream().forEach(instance -> {
      try {
        selfLinkShouldBeReachable(instance);
      } catch (Exception e) {
        Assert.fail(e.toString());
      }
    });
  }

  private static void expressesDublinCoreMetadata(JsonObject instance)
    throws JsonLdError {

    JsonLdOptions options = new JsonLdOptions();
    DocumentLoader documentLoader = new DocumentLoader();

    ArrayList<Header> headers = new ArrayList<>();

    headers.add(new BasicHeader("X-Okapi-Tenant", ApiTestSuite.TENANT_ID));

    CloseableHttpClient httpClient = CachingHttpClientBuilder
      .create()
      .setDefaultHeaders(headers)
      .build();

    documentLoader.setHttpClient(httpClient);

    options.setDocumentLoader(documentLoader);

    List<Object> expandedLinkedData = JsonLdProcessor.expand(instance.getMap(), options);

    assertThat("No Linked Data present", expandedLinkedData.isEmpty(), is(false));
    assertThat(LinkedDataValue(expandedLinkedData,
      "http://purl.org/dc/terms/title"), is(instance.getString("title")));
  }

  private static String LinkedDataValue(List<Object> expanded, String field) {
    //TODO: improve on how to traverse JSON-LD results
    return ((Map<String, Object>)((ArrayList<Map>)
      ((Map<String, Object>)expanded.get(0))
        .get(field)).get(0))
        .get("@value").toString();
  }

  private JsonObject createInstance(JsonObject newInstanceRequest)
    throws InterruptedException,
    MalformedURLException,
    TimeoutException,
    ExecutionException {

    return InstanceApiClient.createInstance(okapiClient, newInstanceRequest);
  }

  private void selfLinkShouldBeReachable(JsonObject instance)
    throws InterruptedException,
    ExecutionException,
    TimeoutException {

    CompletableFuture<Response> getCompleted = new CompletableFuture<>();

    okapiClient.get(instance.getJsonObject("links").getString("self"),
      ResponseHandler.json(getCompleted));

    Response getResponse = getCompleted.get(5, TimeUnit.SECONDS);

    assertThat(getResponse.getStatusCode(), is(200));
  }

  private static void dublinCoreContextLinkRespectsWayResourceWasReached(
    JsonObject instance) {

    containsApiRoot(instance.getString("@context"));
  }

  private static void selfLinkRespectsWayResourceWasReached(JsonObject instance) {
    containsApiRoot(instance.getJsonObject("links").getString("self"));
  }

  private static void containsApiRoot(String link) {
    assertThat(link.contains(ApiTestSuite.apiRoot()), is(true));
  }
}
