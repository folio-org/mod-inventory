package support.fakes;

import java.time.Instant;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Promise;
import io.vertx.core.http.HttpServer;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.Router;
import support.fakes.processors.StorageConstraintsProcessors;
import support.fakes.processors.StorageRecordPreProcessors;

public class FakeOkapi extends AbstractVerticle {
  private static final int PORT_TO_USE = 9493;
  private static final String address =
    String.format("http://localhost:%s", PORT_TO_USE);

  private HttpServer server;

  public static String getAddress() {
    return address;
  }

  @Override
  public void start(Promise<Void> startFuture) {
    System.out.println("Starting fake modules");

    Router router = Router.router(vertx);

    this.server = vertx.createHttpServer();

    registerFakeInstanceStorageModule(router);
    registerFakeHoldingStorageModule(router);
    registerFakeAuthorityStorageModule(router);
    registerFakeItemsStorageModule(router);
    registerFakeTenantItemsStorageModule(router);
    registerFakeMaterialTypesModule(router);
    registerFakeLoanTypesModule(router);
    registerFakeLocationsModule(router);
    registerFakeInstanceTypesModule(router);
    registerFakeIdentifierTypesModule(router);
    registerFakeContributorNameTypesModule(router);
    registerFakeUsersModule(router);
    registerFakeUserTenantsModule(router);
    registerFakeNatureOfContentTermsModule(router);
    registerFakePubSubModule(router);
    registerFakeRequestsModule(router);
    registerFakeSourceRecordStorage(router);
    registerFakeHoldingSourcesModule(router);

    server.requestHandler(router)
      .listen(PORT_TO_USE, result -> {
        if (result.succeeded()) {
          System.out.printf("Fake Okapi listening on %s%n", server.actualPort());
          startFuture.complete();
        } else {
          startFuture.fail(result.cause());
        }
      });
  }

  @Override
  public void stop(Promise<Void> stopFuture) {
    System.out.println("Stopping fake modules");

    if (server != null) {
      server.close(result -> {
        if (result.succeeded()) {
          System.out.printf("Stopped listening on %s%n", server.actualPort());
          stopFuture.complete();
        } else {
          stopFuture.fail(result.cause());
        }
      });
    }
  }

  private void registerFakeInstanceStorageModule(Router router) {
    FakeStorageModule fakeInstanceStorageModule = new FakeStorageModuleBuilder()
      .withRecordName("instance")
      .withRootPath("/instance-storage/instances")
      .withCollectionPropertyName("instances")
      .withRequiredProperties("source", "title", "contributors", "instanceTypeId")
      .withRecordPreProcessors(
        StorageRecordPreProcessors.setHridProcessor("in")
      ).create();
    fakeInstanceStorageModule.register(router);
    fakeInstanceStorageModule.registerBatch(router, "/instance-storage/batch/synchronous");

    new FakeStorageModuleBuilder()
      .withRecordName("instance relationship")
      .withRootPath("/instance-storage/instance-relationships")
      .withCollectionPropertyName("instanceRelationships")
      .withRequiredProperties("superInstanceId", "subInstanceId", "instanceRelationshipTypeId")
      .withRecordPreProcessors(
        StorageConstraintsProcessors::instanceRelationshipsConstraints)
      .create().register(router);

    new FakeStorageModuleBuilder()
      .withRecordName("bound-with part")
      .withRootPath("/inventory-storage/bound-with-parts")
      .withCollectionPropertyName("boundWithParts")
      .withRequiredProperties("holdingsRecordId", "itemId")
      .withDefault("metadata", () -> JsonObject.of("createdDate", Instant.now().toString()))
      .create().register(router);

    new FakeStorageModuleBuilder()
      .withRecordName("preceding succeeding titles")
      .withRootPath("/preceding-succeeding-titles")
      .withCollectionPropertyName("precedingSucceedingTitles")
      .withRecordPreProcessors(
        StorageConstraintsProcessors::instancePrecedingSucceedingTitleConstraints)
      .create().register(router);

    new FakeStorageModuleBuilder()
      .withRecordName("Instance relationship types")
      .withRootPath("/instance-relationship-types")
      .withCollectionPropertyName("instanceRelationshipTypes")
      .withRequiredProperties("name")
      .create().register(router);
  }

  private void registerFakeRequestsModule(Router router) {
    new FakeStorageModuleBuilder()
      .withRecordName("Request storage")
      .withRootPath("/request-storage/requests")
      .withCollectionPropertyName("requests")
      .withRequiredProperties("itemId", "requesterId", "requestType", "requestDate",
        "fulfillmentPreference")
      .create().register(router);
  }

  private void registerFakeSourceRecordStorage(Router router) {
    new FakeStorageModuleBuilder()
      .withRecordName("Source record storage")
      .withRootPath("/source-storage/records")
      .withCollectionPropertyName("records")
      .create().register(router);
  }

  private void registerFakeHoldingStorageModule(Router router) {
    new FakeStorageModuleBuilder()
      .withRecordName("holding")
      .withRootPath("/holdings-storage/holdings")
      .withCollectionPropertyName("holdingsRecords")
      .withRequiredProperties("instanceId", "permanentLocationId")
      .create().register(router);
  }

  private void registerFakeHoldingSourcesModule(Router router) {
      new FakeStorageModuleBuilder()
        .withRecordName("Holding record sources")
        .withRootPath("/holdings-sources")
        .withCollectionPropertyName("holdingsRecordsSources")
        .create().register(router);
  }

  private void registerFakeAuthorityStorageModule(Router router) {
    new FakeStorageModuleBuilder()
        .withRecordName("authority")
        .withRootPath("/authority-storage/authorities")
        .withCollectionPropertyName("authorities")
        .create().register(router);
  }

  private void registerFakeItemsStorageModule(Router router) {
    new FakeStorageModuleBuilder()
      .withRecordName("item")
      .withRootPath("/item-storage/items")
      .withRequiredProperties("materialTypeId", "permanentLoanTypeId", "status.name")
      .withRecordPreProcessors(
        StorageRecordPreProcessors.setHridProcessor("it"),
        StorageRecordPreProcessors::setEffectiveLocationForItem,
        StorageRecordPreProcessors::setEffectiveCallNumberComponents,
        StorageRecordPreProcessors::setEffectiveShelvingOrder,
        StorageRecordPreProcessors::setStatusDateProcessor
      )
      .create().register(router);
  }

  private void registerFakeTenantItemsStorageModule(Router router) {
    new FakeStorageModuleBuilder()
      .withRecordName("tenantItem")
      .withRootPath("/inventory/tenant-items")
      .withCollectionPropertyName("items")
      .create().register(router);
  }

  private void registerFakeMaterialTypesModule(Router router) {

    new FakeStorageModuleBuilder()
      .withRecordName("material type")
      .withRootPath("/material-types")
      .withCollectionPropertyName("mtypes")
      .create().register(router);
  }

  private void registerFakeLoanTypesModule(Router router) {

    new FakeStorageModuleBuilder()
      .withRecordName("loan type")
      .withRootPath("/loan-types")
      .withCollectionPropertyName("loantypes")
      .create().register(router);
  }

  private void registerFakeLocationsModule(Router router) {

    new FakeStorageModuleBuilder()
      .withRecordName("institution")
      .withRootPath("/location-units/institutions")
      .withCollectionPropertyName("locinsts")
      .withRequiredProperties("name", "code")
      .create().register(router);

    new FakeStorageModuleBuilder()
      .withRecordName("campus")
      .withRootPath("/location-units/campuses")
      .withCollectionPropertyName("loccamps")
      .withRequiredProperties("name", "institutionId", "code")
      .create().register(router);

    new FakeStorageModuleBuilder()
      .withRecordName("library")
      .withRootPath("/location-units/libraries")
      .withCollectionPropertyName("loclibs")
      .withRequiredProperties("name", "campusId", "code")
      .create().register(router);

    new FakeStorageModuleBuilder()
      .withRecordName("locations")
      .withRootPath("/locations")
      .withCollectionPropertyName("locations")
      .withRequiredProperties(
        "name",
        "code",
        "institutionId",
        "campusId",
        "libraryId",
        "primaryServicePoint")
      .create()
      .register(router);
  }

  private void registerFakeIdentifierTypesModule(Router router) {

    new FakeStorageModuleBuilder()
      .withRecordName("identifier type")
      .withRootPath("/identifier-types")
      .withCollectionPropertyName("identifierTypes")
      .create().register(router);
  }

  private void registerFakeInstanceTypesModule(Router router) {
    new FakeStorageModuleBuilder()
      .withRecordName("instance type")
      .withRootPath("/instance-types")
      .withCollectionPropertyName("instanceTypes")
      .withRequiredProperties("name", "code", "source")
      .create().register(router);
  }

  private void registerFakeContributorNameTypesModule(Router router) {
    new FakeStorageModuleBuilder()
      .withRecordName("contributor type names")
      .withRootPath("/contributor-name-types")
      .withCollectionPropertyName("contributorNameTypes")
      .create().register(router);
  }

  private void registerFakeUsersModule(Router router) {
    new FakeStorageModuleBuilder()
      .withRecordName("users")
      .withRootPath("/users")
      .withCollectionPropertyName("users")
      .create().register(router);
  }

  private void registerFakeUserTenantsModule(Router router) {
    new FakeStorageModuleBuilder()
      .withRecordName("user-tenants")
      .withRootPath("/user-tenants")
      .withCollectionPropertyName("userTenants")
      .create().register(router);
  }

  private void registerFakeNatureOfContentTermsModule(Router router) {
    new FakeStorageModuleBuilder()
      .withRecordName("natureOfContentTerms")
      .withRootPath("/nature-of-content-terms")
      .withCollectionPropertyName("natureOfContentTerms")
      .create().register(router);
  }

  private void registerFakePubSubModule(Router router) {
    new FakeStorageModuleBuilder()
      .withRootPath("/pubsub/event-types")
      .withRequiredProperties("eventType", "eventTTL")
      .create().register(router);

    new FakeStorageModuleBuilder()
      .withRootPath("/pubsub/publish")
      .create().register(router);

    new FakeStorageModuleBuilder()
      .withRootPath("/pubsub/event-types/declare/publisher")
      .withRequiredProperties("moduleId", "eventDescriptors")
      .create().register(router);

    new FakeStorageModuleBuilder()
      .withRootPath("/pubsub/event-types/declare/subscriber")
      .withRequiredProperties("moduleId", "subscriptionDefinitions")
      .create().register(router);
  }
}
