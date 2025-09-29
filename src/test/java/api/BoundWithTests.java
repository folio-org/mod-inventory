package api;

import api.support.ApiTests;
import api.support.InstanceSamples;
import api.support.builders.HoldingRequestBuilder;
import api.support.builders.ItemRequestBuilder;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.folio.inventory.support.http.client.IndividualResource;
import org.folio.inventory.support.http.client.OkapiHttpClient;
import org.folio.inventory.support.http.client.Response;
import org.junit.Test;

import java.net.MalformedURLException;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.fail;

public class BoundWithTests extends ApiTests {
  @Test
  public void boundWithFlagsArePresentOnInstancesAndItemsAsExpected() throws InterruptedException, MalformedURLException, TimeoutException, ExecutionException
  {
    IndividualResource instance1 = instancesStorageClient.create( InstanceSamples.smallAngryPlanet( UUID.randomUUID() ) );
    IndividualResource holdings1a = holdingsStorageClient.create(new HoldingRequestBuilder()
      .forInstance(instance1.getId()).permanentlyInMainLibrary().withCallNumber( "HOLDINGS 1A" ));
    IndividualResource item1a = itemsClient.create(new ItemRequestBuilder()
      .forHolding( holdings1a.getId() ).withBarcode( "ITEM 1A" ));

    IndividualResource instance2 = instancesStorageClient.create( InstanceSamples.girlOnTheTrain( UUID.randomUUID() ) );
    IndividualResource holdings2a = holdingsStorageClient.create(new HoldingRequestBuilder()
      .forInstance( instance2.getId()).permanentlyInMainLibrary().withCallNumber( "HOLDINGS 2A" ) );
    IndividualResource item2a = itemsClient.create(new ItemRequestBuilder()
      .forHolding(holdings2a.getId()  ).withBarcode( "ITEM 2A" ) );

    IndividualResource instance3 = instancesStorageClient.create( InstanceSamples.leviathanWakes( UUID.randomUUID() ) );
    IndividualResource holdings3a = holdingsStorageClient.create(new HoldingRequestBuilder()
      .forInstance(instance3.getId()).permanentlyInMainLibrary().withCallNumber( "HOLDINGS 3A" ));
    IndividualResource item3a = itemsClient.create(new ItemRequestBuilder()
      .forHolding( holdings3a.getId() ).withBarcode( "ITEM 3A" ));

    // Adding 'holdings2a' to bound-with item 'item1a' means
    //    'item1a' becomes a bound-with and
    //    'holdings2a' becomes a bound-with part
    //    'holdings1a' becomes a bound-with part, inferred by it's direct relation to bound-with item 'item1a'
    //    'instance1' becomes a bound-with part (through holdings1a)
    //    'instance2' becomes a bound-with part (through holdings2a)
    //    'item2a' and 'item3a' are not bound-with items
    //    'holdings3a', 'instance3a' are not parts of any bound-with
    JsonObject boundWithPart = makeObjectBoundWithPart( item1a.getJson().getString("id"), holdings2a.getJson().getString( "id" ) );
    boundWithPartsStorageClient.create(boundWithPart);

    for (JsonObject item : itemsClient.getAll()) {
      if (item.getString("id").equals( item1a.getJson().getString( "id" ) )) {
        assertThat( "Item1a is bound-with " + item.encodePrettily(), item.getBoolean( "isBoundWith" ), is(true) );
      } else if (item.getString("id").equals( item2a.getJson().getString( "id" ))) {
        assertThat( "Item2a is NOT bound-with " + item.encodePrettily(), item.getBoolean ( "isBoundWith"), is (false));
      } else if (item.getString("id").equals (item3a.getJson().getString( "id" ))) {
        assertThat ("Item3a is NOT bound-with " + item.encodePrettily(), item.getBoolean( "isBoundWith" ), is (false));
      } else {
        fail("Unexpected item received from storage: " + item.encodePrettily());
      }
    }

    for (JsonObject instance : instancesClient.getAll()) {
      if (instance.getString("id").equals(instance1.getJson().getString( "id" ))) {
        assertThat ("Instance 1 is bound-with: " + instance.encodePrettily(),instance.getBoolean("isBoundWith"), is(true));
      } else if (instance.getString("id").equals( instance2.getJson().getString( "id" ) )) {
        assertThat("Instance 2 is bound-with: " + instance.encodePrettily(), instance.getBoolean( "isBoundWith" ), is(true));
      } else if (instance.getString( "id" ).equals( instance3.getJson().getString( "id" ))) {
        assertThat("Instance 3 is NOT bound-with: " + instance.encodePrettily(), instance.getBoolean( "isBoundWith" ), is(false));
      } else {
        fail("Unexpected Instance received from storage: " + instance.encodePrettily());
      }
    }

    Response instance1byId = instancesClient.getById( instance1.getId() );
    assertThat("Instance 1 fetched by ID is bound-with", instance1byId.getJson().getBoolean( "isBoundWith" ), is(true));
    Response instance3byId = instancesClient.getById( instance3.getId() );
    assertThat("instance 3 fetched by ID is NOT bound-with", instance3byId.getJson().getBoolean( "isBoundWith" ), is(false));
    Response item1byId = itemsClient.getById( item1a.getId() );
    assertThat("Item 1 fetched by ID is a bound-with", item1byId.getJson().getBoolean( "isBoundWith" ), is(true));
    Response item2byId = itemsClient.getById( item2a.getId() );
    assertThat("Item 2 fetched by ID is NOT a bound-with", item2byId.getJson().getBoolean( "isBoundWith" ), is(false));

  }

  @Test
  public void willSetBoundWithFlagsOnManyItemsContainingTheSameTitle () throws InterruptedException, MalformedURLException, TimeoutException, ExecutionException
  {
    IndividualResource instance1 = instancesStorageClient.create( InstanceSamples.smallAngryPlanet( UUID.randomUUID() ) );
    IndividualResource holdings1a = holdingsStorageClient.create(new HoldingRequestBuilder()
      .forInstance(instance1.getId()).permanentlyInMainLibrary().withCallNumber( "HOLDINGS 1A" ));
    IndividualResource instance2 = instancesStorageClient.create( InstanceSamples.girlOnTheTrain( UUID.randomUUID() ) );
    IndividualResource holdings2a = holdingsStorageClient.create(new HoldingRequestBuilder()
      .forInstance( instance2.getId()).permanentlyInMainLibrary().withCallNumber( "HOLDINGS 2A" ) );

    for (int i=0; i<200; i++) {
      IndividualResource item = itemsClient.create(new ItemRequestBuilder()
        .forHolding( holdings1a.getId() ).withBarcode( "ITEM1 " + i ));
      boundWithPartsStorageClient.create(
        makeObjectBoundWithPart( item.getJson().getString("id"), holdings1a.getJson().getString( "id" ) ));
    }
    for (int i=0; i<100; i++) {
      IndividualResource item = itemsClient.create(new ItemRequestBuilder()
        .forHolding( holdings2a.getId() ).withBarcode( "ITEM2 " + i ));
      boundWithPartsStorageClient.create(
        makeObjectBoundWithPart( item.getJson().getString("id"), holdings1a.getJson().getString( "id" ) ));
    }

    for (JsonObject item : itemsClient.getMany("",1000)) {
      assertThat("Every item haa bound-with flag ", item.getBoolean( "isBoundWith" ), is(true) );
    }
  }

  @Test
  public void boundWithTitlesArePresentInBoundWithItem () throws InterruptedException, TimeoutException, ExecutionException
  {
    IndividualResource instance1 = instancesStorageClient.create( InstanceSamples.smallAngryPlanet( UUID.randomUUID() ) );
    IndividualResource holdings1a = holdingsStorageClient.create(new HoldingRequestBuilder()
      .forInstance(instance1.getId()).permanentlyInMainLibrary().withCallNumber( "HOLDINGS 1A" ));
    IndividualResource item1a = itemsClient.create(new ItemRequestBuilder()
      .forHolding( holdings1a.getId() ).withBarcode( "ITEM 1A" ));

    IndividualResource instance2 = instancesStorageClient.create( InstanceSamples.girlOnTheTrain( UUID.randomUUID() ) );
    IndividualResource holdings2a = holdingsStorageClient.create(new HoldingRequestBuilder()
      .forInstance( instance2.getId()).permanentlyInMainLibrary().withCallNumber( "HOLDINGS 2A" ) );

    IndividualResource instance3 = instancesStorageClient.create( InstanceSamples.leviathanWakes( UUID.randomUUID() ) );
    IndividualResource holdings3a = holdingsStorageClient.create(new HoldingRequestBuilder()
      .forInstance(instance3.getId()).permanentlyInMainLibrary().withCallNumber( "HOLDINGS 3A" ));

    boundWithPartsStorageClient.create(
      makeObjectBoundWithPart( item1a.getJson().getString("id"), holdings1a.getJson().getString( "id" ) ));
    boundWithPartsStorageClient.create(
      makeObjectBoundWithPart( item1a.getJson().getString("id"), holdings2a.getJson().getString( "id" ) ));
    boundWithPartsStorageClient.create(
      makeObjectBoundWithPart( item1a.getJson().getString("id"), holdings3a.getJson().getString( "id" ) ));

    Response itemResponse = okapiClient.get(ApiTestSuite.apiRoot()+
      "/inventory/items/"+item1a.getId())
      .toCompletableFuture().get(5, SECONDS);

    assertThat("Item has boundWithTitles array with three titles",
      itemResponse.getJson().getJsonArray( "boundWithTitles" ).size(), is(3));
  }

  // NOTE: To be investigated: Several of these tests could possibly falsely pass
  //       due to the complexities of the queries, not all of which are
  //       necessarily supported by the fake storage modules.
  @Test
  public void canRetrieveBoundWithItemByHoldingsRecordId() throws InterruptedException, MalformedURLException, TimeoutException, ExecutionException
  {
    IndividualResource instance1 = instancesStorageClient.create( InstanceSamples.smallAngryPlanet( UUID.randomUUID() ) );
    IndividualResource holdings1a = holdingsStorageClient.create(new HoldingRequestBuilder()
      .forInstance(instance1.getId()).permanentlyInMainLibrary().withCallNumber( "HOLDINGS 1A" ));
    IndividualResource item1a = itemsClient.create(new ItemRequestBuilder()
      .forHolding( holdings1a.getId() ).withBarcode( "ITEM 1A" ));

    IndividualResource instance2 = instancesStorageClient.create( InstanceSamples.girlOnTheTrain( UUID.randomUUID() ) );
    IndividualResource holdings2a = holdingsStorageClient.create(new HoldingRequestBuilder()
      .forInstance( instance2.getId()).permanentlyInMainLibrary().withCallNumber( "HOLDINGS 2A" ) );
    itemsClient.create(new ItemRequestBuilder()
      .forHolding(holdings2a.getId()  ).withBarcode( "ITEM 2A" ) );

    IndividualResource instance3 = instancesStorageClient.create( InstanceSamples.leviathanWakes( UUID.randomUUID() ) );
    IndividualResource holdings3a = holdingsStorageClient.create(new HoldingRequestBuilder()
      .forInstance(instance3.getId()).permanentlyInMainLibrary().withCallNumber( "HOLDINGS 3A" ));
    itemsClient.create(new ItemRequestBuilder()
      .forHolding( holdings3a.getId() ).withBarcode( "ITEM 3A" ));

    boundWithPartsStorageClient.create(
      makeObjectBoundWithPart( item1a.getJson().getString("id"), holdings1a.getJson().getString( "id" ) ));
    boundWithPartsStorageClient.create(
      makeObjectBoundWithPart( item1a.getJson().getString("id"), holdings2a.getJson().getString( "id" ) ));

    // Need straight Okapi client for the option to set extra parameters
    OkapiHttpClient okapiClient = ApiTestSuite.createOkapiHttpClient();
    Response itemsResponse1 = okapiClient.get(ApiTestSuite.apiRoot()+
      "/inventory/items-by-holdings-id?query=holdingsRecordId=="
      +holdings2a.getJson().getString( "id" ))
      .toCompletableFuture().get(5, SECONDS);

    assertThat("Two items are found for holdings record: " + itemsResponse1.getJson().encodePrettily(), itemsResponse1.getJson().getInteger( "totalRecords" ), is(2));
    JsonArray itemsForHoldingsId1 = itemsResponse1.getJson().getJsonArray( "items" );
    boolean foundBoundWithItem = false;
    boolean foundNonBoundWithItem = false;
    for (Object o : itemsForHoldingsId1) {
      JsonObject item = ((JsonObject) o);
      String barcode = item.getString( "barcode" );
      assertThat("The items returned are 'ITEM 1A' and 'ITEM 2A'",
        Arrays.asList("ITEM 1A", "ITEM 2A").contains( barcode ), is(true));
      if (barcode.equals( "ITEM 1A" )) {
        assertThat("ITEM 1A is bound-with item ", item.getBoolean( "isBoundWith" ), is(true));
        foundBoundWithItem = item.getBoolean( "isBoundWith" );
      } else if (barcode.equals( "ITEM 2A" )) {
        assertThat("ITEM 2A is not bound-with ", item.getBoolean( "isBoundWith" ), is(false));
        foundNonBoundWithItem = !(item.getBoolean( "isBoundWith" ));
      }
    }
    assertThat("Found one bound-with and one non-bound-with item " + itemsResponse1.getJson().encodePrettily(),
      foundBoundWithItem && foundNonBoundWithItem, is(true));

    Response itemsResponse2 = okapiClient.get(ApiTestSuite.apiRoot()+
      "/inventory/items-by-holdings-id?query=holdingsRecordId=="
      +holdings1a.getJson().getString( "id" )+"&offset=0&limit=20000")
      .toCompletableFuture().get(5, SECONDS);

    assertThat("One and only one bound-with item is found: ", itemsResponse2.getJson().getInteger( "totalRecords" ), is(1));
    JsonArray boundWithItems2 = itemsResponse2.getJson().getJsonArray( "items" );
    JsonObject item2 = boundWithItems2.getJsonObject( 0 );
    assertThat("The bound-with item returned is indeed flagged as a bound-with", item2.getBoolean("isBoundWith"), is(true));
    assertThat( "The bound-with item returned is the item with barcode 'ITEM 1A'", item2.getString("barcode"), is("ITEM 1A"));

    Response itemsResponse3 = okapiClient.get(ApiTestSuite.apiRoot()+
      "/inventory/items-by-holdings-id?query=holdingsRecordId=="
      +holdings1a.getJson().getString( "id" ) + "&relations=onlyBoundWithsSkipDirectlyLinkedItem")
      .toCompletableFuture().get(5, SECONDS);
    assertThat("No item is found for 'holdings1a' when relations is set to onlyBoundWithsSkipDirectlyLinkedItem: ", itemsResponse3.getJson().getInteger( "totalRecords" ), is(0));

    Response itemsResponse4 = okapiClient.get(ApiTestSuite.apiRoot()+
      "/inventory/items-by-holdings-id?query=holdingsRecordId=="
      +holdings1a.getJson().getString( "id" ) + "&relations=onlyBoundWiths")
      .toCompletableFuture().get(5, SECONDS);
    assertThat("One item is found for 'holdings1a' when relations is set to onlyBoundWiths: ", itemsResponse4.getJson().getInteger( "totalRecords" ), is(1));

    Response itemsResponse5 = okapiClient.get(ApiTestSuite.apiRoot()+
      "/inventory/items-by-holdings-id?query=holdingsRecordId=="
      +holdings3a.getJson().getString( "id" )+"&offset=string&limit=string")
      .toCompletableFuture().get(5, SECONDS);
    assertThat("One item is found for 'holdings3a' (non-bound-with) with relations criterion: ", itemsResponse5.getJson().getInteger( "totalRecords" ), is(1));

  }

  @Test
  public void canRetrieveManySingleTitleItemsThroughItemsByHoldingsId() throws InterruptedException, TimeoutException, ExecutionException
  {
    IndividualResource instance1 = instancesStorageClient.create( InstanceSamples.smallAngryPlanet( UUID.randomUUID() ).put("title", "Instance 1") );
    IndividualResource holdings1 = holdingsStorageClient.create(new HoldingRequestBuilder()
      .forInstance(instance1.getId()).permanentlyInMainLibrary().withCallNumber( "HOLDINGS 1" ));
    for (int i=1; i<=1100; i++) {
      itemsClient.create(new ItemRequestBuilder()
        .forHolding( holdings1.getId() ).withBarcode("bc-" + i));

    }
    Response itemsResponse = okapiClient.get(ApiTestSuite.apiRoot()+
        "/inventory/items-by-holdings-id?query=holdingsRecordId=="
        +holdings1.getJson().getString( "id" )+"&offset=0&limit=700")
      .toCompletableFuture().get(5, SECONDS);
    assertThat("page of 700 items returned for 'holdings1': ", itemsResponse.getJson().getJsonArray("items").size(), is(700));
    assertThat("a total of 1100 items reported for 'holdings1': ", itemsResponse.getJson().getInteger( "totalRecords" ), is(1100));
  }

  @Test
  public void canRetrieveManyBoundWithsAndRegularItemsThroughItemsByHoldingsId() throws InterruptedException, TimeoutException, ExecutionException, MalformedURLException
  {
    IndividualResource instance1 = instancesStorageClient.create( InstanceSamples.smallAngryPlanet( UUID.randomUUID() ) );
    IndividualResource holdings1a = holdingsStorageClient.create(new HoldingRequestBuilder()
      .forInstance(instance1.getId()).permanentlyInMainLibrary().withCallNumber( "HOLDINGS 1A" ));

    for (int j=400; j<800; j++) {
      itemsClient.create(new ItemRequestBuilder()
        .forHolding( holdings1a.getId() )
        .withBarcode( "ITEM1 " + j ));
    }

    List<JsonObject> items =itemsStorageClient.getMany("", 1000);
    assertThat("There are 400 items in storage ", items.size(), is(400));

    for (int i=0; i<400; i++) {
      IndividualResource item = itemsClient.create(new ItemRequestBuilder()
        .forHolding( holdings1a.getId() ).withBarcode( "ITEM1 " + i ));
      boundWithPartsStorageClient.create(
        makeObjectBoundWithPart( item.getJson().getString("id"), holdings1a.getJson().getString( "id" ) ));
    }

    List<JsonObject> items2 =itemsStorageClient.getMany("", 1000);
    assertThat("There are 800 items in storage ", items2.size(), is(800));

    Response itemsByHoldingsIdResponse = okapiClient.get(ApiTestSuite.apiRoot()+
        "/inventory/items-by-holdings-id?query=holdingsRecordId=="
        +holdings1a.getJson().getString( "id" ))
      .toCompletableFuture().get(5, SECONDS);

    assertThat("Can retrieve many items by holdings record id", itemsByHoldingsIdResponse.getStatusCode(), is(200));
    assertThat("default limit of 200 is applied", itemsByHoldingsIdResponse.getJson().getJsonArray("items").size(), is(200));
    assertThat("total records is 800", itemsByHoldingsIdResponse.getJson().getInteger("totalRecords"), is(800));

    Response itemsByHoldingsIdResponseWithLimit = okapiClient.get(ApiTestSuite.apiRoot()+
        "/inventory/items-by-holdings-id?query=holdingsRecordId=="
        +holdings1a.getJson().getString( "id" ) + "&limit=600")
      .toCompletableFuture().get(5, SECONDS);

    assertThat("Can retrieve many items by holdings record id with limit", itemsByHoldingsIdResponseWithLimit.getStatusCode(), is(200));
    assertThat("requested limit of 600 is applied", itemsByHoldingsIdResponseWithLimit.getJson().getJsonArray("items").size(), is(600));
    assertThat("total records is 800 with limit applied", itemsByHoldingsIdResponseWithLimit.getJson().getInteger("totalRecords"), is(800));

    Response itemsByHoldingsIdResponseOnlyBoundWiths = okapiClient.get(ApiTestSuite.apiRoot()+
        "/inventory/items-by-holdings-id?query=holdingsRecordId=="
        +holdings1a.getJson().getString( "id" ) + "&limit=600&relations=onlyBoundWiths")
      .toCompletableFuture().get(5, SECONDS);

    assertThat("Can retrieve many items by holdings record id with relations=onlyBoundWiths", itemsByHoldingsIdResponseOnlyBoundWiths.getStatusCode(), is(200));
    assertThat("response has 400 items with 'onlyBoundWiths'", itemsByHoldingsIdResponseOnlyBoundWiths.getJson().getJsonArray("items").size(), is(400));
    assertThat("total records is 400 with 'onlyBoundWiths'", itemsByHoldingsIdResponseOnlyBoundWiths.getJson().getInteger("totalRecords"), is(400));

  }


  @Test
  public void canFetchBoundWithItemWithManyParts() throws InterruptedException, TimeoutException, ExecutionException
  {
    IndividualResource instance1 = instancesStorageClient.create( InstanceSamples.smallAngryPlanet( UUID.randomUUID() ).put("title", "Instance 1") );
    IndividualResource holdings1 = holdingsStorageClient.create(new HoldingRequestBuilder()
      .forInstance(instance1.getId()).permanentlyInMainLibrary().withCallNumber( "HOLDINGS 1" ));
    IndividualResource item = itemsClient.create(new ItemRequestBuilder()
      .forHolding( holdings1.getId() ).withBarcode( "ITEM" ));
    boundWithPartsStorageClient.create(
      makeObjectBoundWithPart( item.getJson().getString("id"), holdings1.getJson().getString( "id" ) ));

    for (int i=2; i<=200; i++) {
      IndividualResource instance = instancesStorageClient.create( InstanceSamples.smallAngryPlanet( UUID.randomUUID() ).put("title", "Instance " + i) );
      IndividualResource holdings = holdingsStorageClient.create(new HoldingRequestBuilder()
        .forInstance(instance.getId()).permanentlyInMainLibrary().withCallNumber( "HOLDINGS " + i ));
      boundWithPartsStorageClient.create(
        makeObjectBoundWithPart( item.getJson().getString("id"), holdings.getJson().getString( "id" ) ));
    }
    Response itemResponse = okapiClient.get(ApiTestSuite.apiRoot() +
        "/inventory/items/" + item.getId())
      .toCompletableFuture().get(5, SECONDS);

    var boundWithTitles = itemResponse.getJson().getJsonArray("boundWithTitles");
    assertThat("Item has boundWithTitles array with 200 titles", boundWithTitles.size(), is(200));
    for (int i = 1; i<=200; i++) {
      var title = boundWithTitles.getJsonObject(i - 1).getJsonObject("briefInstance").getString("title");
      assertThat("sorted by createdDate", title, is("Instance " + i));
    }
  }

  @Test
  public void mustQueryBoundWithItemsByHoldingsRecordIdId()
    throws InterruptedException, TimeoutException, ExecutionException {

    Response itemsResponse1 = okapiClient.get(ApiTestSuite.apiRoot()+
      "/inventory/items-by-holdings-id?query=holdingsRecordId=="
      + UUID.randomUUID() )
      .toCompletableFuture().get(5, SECONDS);

    assertThat("Response code 200 (OK) expected when querying by holdingsRecordId",
      itemsResponse1.getStatusCode(), is(200));

    Response itemsResponse3 = okapiClient.get(ApiTestSuite.apiRoot()+
      "/inventory/items-by-holdings-id/")
      .toCompletableFuture().get(5, SECONDS);

    assertThat("Response code 400 (bad request) expected when not querying by holdingsRecordId",
      itemsResponse3.getStatusCode(), is(400));

    Response itemsResponse4 = okapiClient.get(ApiTestSuite.apiRoot()+
        "/inventory/items-by-holdings-id/?query=holdingsRecordId")
      .toCompletableFuture().get(5, SECONDS);

    assertThat("Response code 400 (bad request) expected when not querying by holdingsRecordId",
      itemsResponse4.getStatusCode(), is(400));

  }

  @Test
  public void mustPassValidRelationsParameterValue ()
    throws InterruptedException, TimeoutException, ExecutionException {

    Response itemsResponse1 = okapiClient.get(ApiTestSuite.apiRoot()+
      "/inventory/items-by-holdings-id?query=holdingsRecordId=="
      + UUID.randomUUID()  + "&relations=onlyBoundWiths" )
      .toCompletableFuture().get(5, SECONDS);

    assertThat("Response code 200 (OK) expected when querying by holdingsRecordId and relations=onlyBoundWiths",
      itemsResponse1.getStatusCode(), is(200));

    Response itemsResponse2 = okapiClient.get(ApiTestSuite.apiRoot()+
      "/inventory/items-by-holdings-id?query=holdingsRecordId=="
      + UUID.randomUUID()  + "&relations=RANDOM" )
      .toCompletableFuture().get(5, SECONDS);

    assertThat("Response code 400 (bad request) expected when providing invalid 'relations' parameter value",
      itemsResponse2.getStatusCode(), is(400));

  }

  public static JsonObject makeObjectBoundWithPart (String itemId, String holdingsRecordId) {
    JsonObject boundWithPart = new JsonObject();
    boundWithPart.put("itemId", itemId);
    boundWithPart.put("holdingsRecordId", holdingsRecordId);
    return boundWithPart;
  }

}
