package api;

import api.items.ItemApiExamples;
import api.items.ItemApiLocationExamples;
import api.support.ControlledVocabularyPreparation;
import io.vertx.core.Vertx;
import org.folio.inventory.InventoryVerticle;
import org.folio.inventory.common.VertxAssistant;
import org.folio.inventory.support.http.client.OkapiHttpClient;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import support.fakes.FakeOkapi;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

@RunWith(Suite.class)
@Suite.SuiteClasses({
  InstancesApiExamples.class,
  ItemApiExamples.class,
  ItemApiLocationExamples.class,
  ModsIngestExamples.class
})
public class ApiTestSuite {
  public static final int INVENTORY_VERTICLE_TEST_PORT = 9603;
  public static final String TENANT_ID = "test_tenant";

  public static final String TOKEN = "eyJhbGciOiJIUzUxMiJ9.eyJzdWIiOiJhZG1pbiIsInRlbmFudCI6ImRlbW9fdGVuYW50In0.29VPjLI6fLJzxQW0UhQ0jsvAn8xHz501zyXAxRflXfJ9wuDzT8TDf-V75PjzD7fe2kHjSV2dzRXbstt3BTtXIQ";

  private static String bookMaterialTypeId;
  private static String dvdMaterialTypeId;

  private static String canCirculateLoanTypeId;
  private static String courseReserveLoanTypeId;

  private static String mainLibraryLocationId;
  private static String annexLocationId;

  private static String isbnIdentifierTypeId;
  private static String asinIdentifierTypeId;
  private static String booksInstanceTypeId;
  private static String personalCreatorTypeId;

  private static VertxAssistant vertxAssistant = new VertxAssistant();
  private static String inventoryModuleDeploymentId;
  private static String fakeModulesDeploymentId;

  private static Boolean useOkapiForApiRequests =
    Boolean.parseBoolean(System.getProperty("use.okapi.initial.requests", ""));
  private static Boolean useOkapiForStorageRequests =
    Boolean.parseBoolean(System.getProperty("use.okapi.storage.requests", ""));
  private static String okapiAddress = System.getProperty("okapi.address", "");

  private static boolean initialised;

  @BeforeClass
  public static void before()
    throws InterruptedException,
    ExecutionException,
    TimeoutException,
    MalformedURLException {

    System.out.println(String.format("Use Okapi For Initial Requests:%s",
      System.getProperty("use.okapi.initial.requests")));

    System.out.println(String.format("Use Okapi For Storage Requests:%s",
      System.getProperty("use.okapi.storage.requests")));

    startVertx();
    startFakeModules();
    createMaterialTypes();
    createLoanTypes();
    createLocations();
    createIdentifierTypes();
    createInstanceTypes();
    createCreatorTypes();
    startInventoryVerticle();

    initialised = true;
  }

  @AfterClass
  public static void after()
    throws InterruptedException, ExecutionException, TimeoutException {

    stopInventoryVerticle();
    stopFakeModules();
    stopVertx();

    initialised = false;
  };

  public static boolean isNotInitialised() {
    return !initialised;
  }

  public static String getBookMaterialType() {
    return bookMaterialTypeId;
  }

  public static String getDvdMaterialType() {
    return dvdMaterialTypeId;
  }

  public static String getCanCirculateLoanType() {
    return canCirculateLoanTypeId;
  }

  public static String getCourseReserveLoanType() {
    return courseReserveLoanTypeId;
  }

  public static String getMainLibraryLocation() {
		return mainLibraryLocationId;
  }

  public static String getAnnexLocation() {
		return annexLocationId;
	}

  public static String getIsbnIdentifierType() {
    return isbnIdentifierTypeId;
  }

  public static String getAsinIdentifierType() {
    return asinIdentifierTypeId;
  }

  public static String getBooksInstanceType() {
    return booksInstanceTypeId;
  }

  public static String getPersonalCreatorType() {
    return personalCreatorTypeId;
  }

  public static OkapiHttpClient createOkapiHttpClient()
    throws MalformedURLException {

    return new OkapiHttpClient(
      vertxAssistant.createUsingVertx(Vertx::createHttpClient),
      new URL(storageOkapiUrl()), TENANT_ID, TOKEN, it ->
      System.out.println(
        String.format("Request failed: %s",
          it.toString())));
  }

  public static String storageOkapiUrl() {
    if(useOkapiForStorageRequests) {
      return okapiAddress;
    }
    else {
      return FakeOkapi.getAddress();
    }
  }

  public static String apiRoot() {
    String directRoot = String.format("http://localhost:%s",
      ApiTestSuite.INVENTORY_VERTICLE_TEST_PORT);

    return useOkapiForApiRequests ? okapiAddress : directRoot;
  }

  private static void stopVertx() {
    vertxAssistant.stop();
  }

  private static void startVertx() {
    vertxAssistant.start();
  }

  private static void startInventoryVerticle()
    throws InterruptedException, ExecutionException, TimeoutException {

    CompletableFuture<String> deployed = new CompletableFuture<>();

    String storageType = "okapi";
    String storageLocation = "";

    System.out.println(String.format("Storage Type: %s", storageType));
    System.out.println(String.format("Storage Location: %s", storageLocation));

    Map<String, Object> config = new HashMap<>();

    config.put("port", INVENTORY_VERTICLE_TEST_PORT);
    config.put("storage.type", storageType);
    config.put("storage.location", storageLocation);

    vertxAssistant.deployVerticle(
      InventoryVerticle.class.getName(), config, deployed);

    inventoryModuleDeploymentId = deployed.get(20000, TimeUnit.MILLISECONDS);
  }

  private static void stopInventoryVerticle()
    throws InterruptedException, ExecutionException, TimeoutException {

    CompletableFuture<Void> undeployed = new CompletableFuture<>();

    if(inventoryModuleDeploymentId != null) {
      vertxAssistant.undeployVerticle(inventoryModuleDeploymentId, undeployed);

      undeployed.get(20000, TimeUnit.MILLISECONDS);
    }
  }

  private static void startFakeModules()
    throws InterruptedException, ExecutionException, TimeoutException {

    if(!useOkapiForStorageRequests) {
      CompletableFuture<String> fakeModulesDeployed = new CompletableFuture<>();

        vertxAssistant.deployVerticle(FakeOkapi.class.getName(),
          new HashMap<>(), fakeModulesDeployed);

      fakeModulesDeploymentId = fakeModulesDeployed.get(10, TimeUnit.SECONDS);
    }
  }

  private static void stopFakeModules()
    throws InterruptedException, ExecutionException, TimeoutException {

    if(!useOkapiForStorageRequests && fakeModulesDeploymentId != null) {
      CompletableFuture<Void> undeployed = new CompletableFuture<>();

      vertxAssistant.undeployVerticle(fakeModulesDeploymentId, undeployed);

      undeployed.get(20000, TimeUnit.MILLISECONDS);
    }
  }

  private static void createMaterialTypes()
    throws MalformedURLException,
    InterruptedException,
    ExecutionException,
    TimeoutException {

    OkapiHttpClient client = createOkapiHttpClient();

    URL materialTypesUrl = new URL(String.format("%s/material-types", storageOkapiUrl()));

    ControlledVocabularyPreparation materialTypePreparation =
      new ControlledVocabularyPreparation(client, materialTypesUrl, "mtypes");

    bookMaterialTypeId = materialTypePreparation.createOrReferenceTerm("Book");
    dvdMaterialTypeId = materialTypePreparation.createOrReferenceTerm("DVD");
  }

  private static void createLoanTypes()
    throws MalformedURLException,
    InterruptedException,
    ExecutionException,
    TimeoutException {

    OkapiHttpClient client = createOkapiHttpClient();

    URL loanTypes = new URL(String.format("%s/loan-types", storageOkapiUrl()));

    ControlledVocabularyPreparation loanTypePreparation =
      new ControlledVocabularyPreparation(client, loanTypes, "loantypes");

    canCirculateLoanTypeId = loanTypePreparation.createOrReferenceTerm("Can Circulate");
    courseReserveLoanTypeId = loanTypePreparation.createOrReferenceTerm("Course Reserves");
  }

  private static void createLocations()
    throws MalformedURLException,
    InterruptedException,
    ExecutionException,
    TimeoutException {

    OkapiHttpClient client = createOkapiHttpClient();

    URL locationsUrl = new URL(String.format("%s/shelf-locations", storageOkapiUrl()));

		ControlledVocabularyPreparation locationPreparation =
      new ControlledVocabularyPreparation(client, locationsUrl, "shelflocations");

		mainLibraryLocationId = locationPreparation.createOrReferenceTerm("Main Library");
		annexLocationId = locationPreparation.createOrReferenceTerm("Annex Library");
  }

  private static void createIdentifierTypes()
    throws MalformedURLException,
    InterruptedException,
    ExecutionException,
    TimeoutException {

    OkapiHttpClient client = createOkapiHttpClient();

    URL identifierTypesUrl = new URL(String.format("%s/identifier-types", storageOkapiUrl()));

    ControlledVocabularyPreparation identifierTypesPreparation =
      new ControlledVocabularyPreparation(client, identifierTypesUrl, "identifierTypes");

    isbnIdentifierTypeId = identifierTypesPreparation.createOrReferenceTerm("ISBN");
    asinIdentifierTypeId = identifierTypesPreparation.createOrReferenceTerm("ASIN");
  }

  private static void createInstanceTypes()
    throws MalformedURLException,
      InterruptedException,
      ExecutionException,
      TimeoutException {

    OkapiHttpClient client = createOkapiHttpClient();

    URL instanceTypes = new URL(String.format("%s/instance-types", storageOkapiUrl()));

    ControlledVocabularyPreparation instanceTypesPreparation =
      new ControlledVocabularyPreparation(client, instanceTypes, "instanceTypes");

    booksInstanceTypeId = instanceTypesPreparation.createOrReferenceTerm("Books");
  }

  private static void createCreatorTypes()
    throws MalformedURLException,
    InterruptedException,
    ExecutionException,
    TimeoutException {

    OkapiHttpClient client = createOkapiHttpClient();

    URL creatorTypes = new URL(String.format("%s/creator-types", storageOkapiUrl()));

    ControlledVocabularyPreparation creatorTypesPreparation =
      new ControlledVocabularyPreparation(client, creatorTypes, "creatorTypes");

    personalCreatorTypeId = creatorTypesPreparation.createOrReferenceTerm("Personal name");
  }
}
