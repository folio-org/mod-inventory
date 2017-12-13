package support.fakes;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.core.http.HttpServer;
import io.vertx.ext.web.Router;

import java.util.ArrayList;

public class FakeOkapi extends AbstractVerticle {

  private static final String TENANT_ID = "test_tenant";
  private static final int PORT_TO_USE = 9493;
  private static final String address =
    String.format("http://localhost:%s", PORT_TO_USE);

  private HttpServer server;

  public static String getAddress() {
    return address;
  }

  public void start(Future<Void> startFuture) {
    System.out.println("Starting fake modules");

    Router router = Router.router(vertx);

    this.server = vertx.createHttpServer();

    registerFakeInstanceStorageModule(router);
    registerFakeHoldingStorageModule(router);
    registerFakeItemsStorageModule(router);
    registerFakeMaterialTypesModule(router);
    registerFakeLoanTypesModule(router);
    registerFakeShelfLocationsModule(router);
    registerFakeInstanceTypesModule(router);
    registerFakeIdentifierTypesModule(router);
    registerFakeContributorNameTypesModule(router);

    server.requestHandler(router::accept)
      .listen(PORT_TO_USE, result -> {
        if (result.succeeded()) {
          System.out.println(
            String.format("Fake Okapi listening on %s", server.actualPort()));
          startFuture.complete();
        } else {
          startFuture.fail(result.cause());
        }
      });
  }

  public void stop(Future<Void> stopFuture) {
    System.out.println("Stopping fake modules");

    if(server != null) {
      server.close(result -> {
        if (result.succeeded()) {
          System.out.println(
            String.format("Stopped listening on %s", server.actualPort()));
          stopFuture.complete();
        } else {
          stopFuture.fail(result.cause());
        }
      });
    }
  }

  private void registerFakeInstanceStorageModule(Router router) {
    new FakeStorageModuleBuilder()
      .withRecordName("instance")
      .withRootPath("/instance-storage/instances")
      .withCollectionPropertyName("instances")
      .withRequiredProperties("source", "title", "contributors", "instanceTypeId")
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

  private void registerFakeItemsStorageModule(Router router) {
    new FakeStorageModuleBuilder()
      .withRecordName("item")
      .withRootPath("/item-storage/items")
      .withRequiredProperties("materialTypeId", "permanentLoanTypeId")
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

  private void registerFakeShelfLocationsModule(Router router) {

    new FakeStorageModuleBuilder()
      .withRecordName("location")
      .withRootPath("/shelf-locations")
      .withCollectionPropertyName("shelflocations")
      .create().register(router);
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
      .create().register(router);
  }

  private void registerFakeContributorNameTypesModule(Router router) {
    new FakeStorageModuleBuilder()
      .withRecordName("contributor type names")
      .withRootPath("/contributor-name-types")
      .withCollectionPropertyName("contributorNameTypes")
      .create().register(router);
  }
}
