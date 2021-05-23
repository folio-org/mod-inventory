package api;

import api.support.ApiTests;
import api.support.InstanceSamples;
import api.support.builders.HoldingRequestBuilder;
import api.support.builders.ItemRequestBuilder;

import api.support.http.ResourceClient;
import io.vertx.core.json.JsonObject;
import org.folio.inventory.support.http.client.IndividualResource;
import org.folio.inventory.support.http.client.Response;
import org.junit.Test;

import java.net.MalformedURLException;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.fail;

public class BoundWithTests extends ApiTests
{
  protected final ResourceClient boundWithPartsStorageClient;

  public BoundWithTests () {
    super();
    boundWithPartsStorageClient = ResourceClient.forBoundWithPartsStorage(okapiClient);
  }

  @Test
  public void canCreateAnInstance() throws InterruptedException, MalformedURLException, TimeoutException, ExecutionException
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

  private JsonObject makeObjectBoundWithPart (String itemId, String holdingsRecordId) {
    JsonObject boundWithPart = new JsonObject();
    boundWithPart.put("itemId", itemId);
    boundWithPart.put("holdingsRecordId", holdingsRecordId);
    return boundWithPart;
  }

}
