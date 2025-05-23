package api;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import api.holdings.HoldingsUpdateOwnershipApiTest;
import api.items.ItemUpdateOwnershipApiTest;
import org.folio.inventory.InventoryVerticle;
import org.folio.inventory.common.VertxAssistant;
import org.folio.inventory.consortium.util.ConsortiumUtil;
import org.folio.inventory.rest.impl.PgPoolContainer;
import org.folio.inventory.support.http.client.OkapiHttpClient;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

import api.holdings.HoldingApiExample;
import api.holdings.HoldingsApiMoveExamples;
import api.isbns.IsbnUtilsApiExamples;
import api.items.ItemAllowedStatusesSchemaTest;
import api.items.ItemApiExamples;
import api.items.ItemApiMoveExamples;
import api.items.ItemApiTitleExamples;
import api.items.MarkItemInProcessApiTests;
import api.items.MarkItemInProcessNonRequestableApiTests;
import api.items.MarkItemIntellectualItemApiTests;
import api.items.MarkItemLongMissingApiTests;
import api.items.MarkItemMissingApiTests;
import api.items.MarkItemRestrictedApiTests;
import api.items.MarkItemUnavailableApiTests;
import api.items.MarkItemUnknownApiTests;
import api.items.MarkItemWithdrawnApiTests;
import api.items.TenantItemApiTests;
import api.support.ControlledVocabularyPreparation;
import api.support.http.ResourceClient;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import support.fakes.FakeOkapi;

@RunWith(Suite.class)
@Suite.SuiteClasses({
  InstancesApiExamples.class,
  HoldingsApiExamples.class,
  ItemApiExamples.class,
  ItemApiTitleExamples.class,
  IsbnUtilsApiExamples.class,
  ItemAllowedStatusesSchemaTest.class,
  PrecedingSucceedingTitlesApiExamples.class,
  InstanceRelationshipsTest.class,
  HoldingApiExample.class,
  MarkItemWithdrawnApiTests.class,
  ItemApiMoveExamples.class,
  MarkItemInProcessApiTests.class,
  MarkItemInProcessNonRequestableApiTests.class,
  MarkItemIntellectualItemApiTests.class,
  MarkItemLongMissingApiTests.class,
  MarkItemMissingApiTests.class,
  MarkItemRestrictedApiTests.class,
  MarkItemUnavailableApiTests.class,
  MarkItemUnknownApiTests.class,
  HoldingsApiMoveExamples.class,
  BoundWithTests.class,
  TenantApiTest.class,
  AdminApiTest.class,
  InventoryConfigApiTest.class,
  HoldingsUpdateOwnershipApiTest.class,
  ItemUpdateOwnershipApiTest.class,
  TenantItemApiTests.class
})
public class ApiTestSuite {
  public static final int INVENTORY_VERTICLE_TEST_PORT = 9603;
  public static final String TENANT_ID = "test_tenant";
  public static final String CONSORTIA_TENANT_ID = "consortium";
  public static final String COLLEGE_TENANT_ID = "college";
  public static final UUID ID_FOR_FAILURE = UUID.fromString("fa45a95b-38a3-430b-8f34-548ca005a176");
  public static final UUID ID_FOR_OPTIMISTIC_LOCKING_FAILURE = UUID.fromString("40900409-0409-4444-8888-409000000409");

  public static final String TOKEN = "eyJhbGciOiJIUzUxMiJ9.eyJzdWIiOiJhZG1pbiIsInRlbmFudCI6ImRlbW9fdGVuYW50In0.29VPjLI6fLJzxQW0UhQ0jsvAn8xHz501zyXAxRflXfJ9wuDzT8TDf-V75PjzD7fe2kHjSV2dzRXbstt3BTtXIQ";
  public static final String USER_ID = "7e115dfb-d1d6-46ac-b2dc-2b3e74cda694";
  public static final String CENTRAL_TENANT_ID_FIELD = "centralTenantId";
  public static final String CONSORTIUM_ID_FIELD = "consortiumId";
  public static final String REQUEST_ID = "test_request_1234";

  private static String bookMaterialTypeId;
  private static String dvdMaterialTypeId;

  private static String canCirculateLoanTypeId;
  private static String courseReserveLoanTypeId;

  private static UUID nottinghamUniversityInstitution;
  private static UUID jubileeCampus;
  private static UUID djanoglyLibrary;
  private static UUID businessLibrary;
  private static UUID thirdFloorLocationId;
  private static UUID mezzanineDisplayCaseLocationId;
  private static UUID readingRoomLocationId;
  private static UUID mainLibraryLocationId;
  private static UUID audiobookNatureOfContentTermId;
  private static UUID bibliographyNatureOfContentTermId;

  private static String isbnIdentifierTypeId;
  private static String asinIdentifierTypeId;
  private static String textInstanceTypeId;
  private static String personalContributorNameTypeId;

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
    stopPostgresqlContainer();
    startPostgresqlContainer();
    startFakeModules();
    createMaterialTypes();
    createLoanTypes();
    createLocations();
    createIdentifierTypes();
    createInstanceTypes();
    createContributorNameTypes();
    createNatureOfContentTerms();
    startInventoryVerticle();

    initialised = true;
  }

  @AfterClass
  public static void after()
    throws InterruptedException, ExecutionException, TimeoutException {

    stopInventoryVerticle();
    stopFakeModules();
    stopPostgresqlContainer();
    stopVertx();

    initialised = false;
  }

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

  public static String getThirdFloorLocation() {
    return thirdFloorLocationId.toString();
  }

  public static String getMezzanineDisplayCaseLocation() {
    return mezzanineDisplayCaseLocationId.toString();
  }

  public static String getReadingRoomLocation() {
    return readingRoomLocationId.toString();
  }

  public static String getMainLibraryLocation() {
    return mainLibraryLocationId.toString();
  }

  public static String getIsbnIdentifierType() {
    return isbnIdentifierTypeId;
  }

  public static String getAsinIdentifierType() {
    return asinIdentifierTypeId;
  }

  public static String getTextInstanceType() {
    return textInstanceTypeId;
  }

  public static String getPersonalContributorNameType() {
    return personalContributorNameTypeId;
  }

  public static String getAudiobookNatureOfContentTermId() {
    return audiobookNatureOfContentTermId.toString();
  }

  public static String getBibliographyNatureOfContentTermId() {
    return bibliographyNatureOfContentTermId.toString();
  }

  public static OkapiHttpClient createOkapiHttpClient()
    throws MalformedURLException {

    return createOkapiHttpClient(TENANT_ID);
  }

  public static OkapiHttpClient createOkapiHttpClient(String tenantId)
    throws MalformedURLException {

    return new OkapiHttpClient(
      vertxAssistant.getVertx(),
      new URL(storageOkapiUrl()), tenantId, TOKEN, USER_ID, null,
      it -> System.out.printf("Request failed: %s%n", it.toString()));
  }

  public static OkapiHttpClient createOkapiHttpClient(String tenantId, String token, String userId)
    throws MalformedURLException {

    return new OkapiHttpClient(
      vertxAssistant.getVertx(),
      new URL(storageOkapiUrl()), tenantId, token, userId, null,
      it -> System.out.printf("Request failed: %s%n", it.toString()));
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

    System.setProperty(ConsortiumUtil.EXPIRATION_TIME_PARAM, "0");

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

    final OkapiHttpClient client = createOkapiHttpClient();

    ResourceClient institutionsClient = ResourceClient.forInstitutions(client);

    nottinghamUniversityInstitution = createReferenceRecord(institutionsClient,
      "Nottingham University", "NOTT");

    ResourceClient campusesClient = ResourceClient.forCampuses(client);

    jubileeCampus = createReferenceRecord(campusesClient,
      new JsonObject()
        .put("name", "Jubilee Campus")
        .put("institutionId", nottinghamUniversityInstitution.toString())
        .put("code", "JUBILEE"));

    ResourceClient librariesClient = ResourceClient.forLibraries(client);

    djanoglyLibrary = createReferenceRecord(librariesClient,
      new JsonObject()
        .put("name", "Djanogly Learning Resource Centre")
        .put("campusId", jubileeCampus.toString())
        .put("code", "DJANOGLY"));

    businessLibrary = createReferenceRecord(librariesClient,
      new JsonObject()
        .put("name", "Business Library")
        .put("campusId", jubileeCampus.toString())
        .put("code", "BUSINESS"));

    ResourceClient locationsClient = ResourceClient.forLocations(client);

    final UUID fakeServicePointId = UUID.randomUUID();

    thirdFloorLocationId = createReferenceRecord(locationsClient,
      new JsonObject()
        .put("name", "3rd Floor")
        .put("code", "NU/JC/DL/3F")
        .put("institutionId", nottinghamUniversityInstitution.toString())
        .put("campusId", jubileeCampus.toString())
        .put("libraryId", djanoglyLibrary.toString())
        //TODO: Replace with created service point
        .put("primaryServicePoint", fakeServicePointId.toString())
        .put("servicePointIds", new JsonArray().add(fakeServicePointId.toString())));

    mezzanineDisplayCaseLocationId = createReferenceRecord(locationsClient,
      new JsonObject()
        .put("name", "Display Case, Mezzanine")
        .put("code", "NU/JC/BL/DM")
        .put("institutionId", nottinghamUniversityInstitution.toString())
        .put("campusId", jubileeCampus.toString())
        .put("libraryId", businessLibrary.toString())
        //TODO: Replace with created service point
        .put("primaryServicePoint", fakeServicePointId.toString())
        .put("servicePointIds", new JsonArray().add(fakeServicePointId.toString())));

    readingRoomLocationId = createReferenceRecord(locationsClient,
      new JsonObject()
        .put("name", "Reading Room")
        .put("code","NU/JC/BL/PR")
        .put("institutionId", nottinghamUniversityInstitution.toString())
        .put("campusId", jubileeCampus.toString())
        .put("libraryId", businessLibrary.toString())
        //TODO: Replace with created service point
        .put("primaryServicePoint", fakeServicePointId.toString())
        .put("servicePointIds", new JsonArray().add(fakeServicePointId.toString())));

    mainLibraryLocationId = createReferenceRecord(locationsClient,
      new JsonObject()
        .put("name", "Main Library")
        .put("code", "NU/JC/DL/ML")
        .put("institutionId", nottinghamUniversityInstitution.toString())
        .put("campusId", jubileeCampus.toString())
        .put("libraryId", djanoglyLibrary.toString())
        //TODO: Replace with created service point
        .put("primaryServicePoint", fakeServicePointId.toString())
        .put("servicePointIds", new JsonArray().add(fakeServicePointId.toString())));
  }

  private static void createNatureOfContentTerms()
    throws MalformedURLException,
    InterruptedException,
    ExecutionException,
    TimeoutException {

    ResourceClient client = ResourceClient.forNatureOfContentTerms(createOkapiHttpClient());

    audiobookNatureOfContentTermId = createReferenceRecord(client,
      new JsonObject()
        .put("name", "audiobook")
        .put("source", "folio")
    );

    bibliographyNatureOfContentTermId = createReferenceRecord(client,
      new JsonObject()
        .put("name", "bibliography")
        .put("source", "folio")
    );
  }

  public static void createConsortiumTenant() throws MalformedURLException {
    String expectedConsortiumId = UUID.randomUUID().toString();

    JsonObject userTenantsCollection = new JsonObject()
      .put(ApiTestSuite.CENTRAL_TENANT_ID_FIELD, ApiTestSuite.CONSORTIA_TENANT_ID)
      .put(ApiTestSuite.CONSORTIUM_ID_FIELD, expectedConsortiumId);

    ResourceClient client = ResourceClient.forUserTenants(createOkapiHttpClient());

    client.create(userTenantsCollection);
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

    textInstanceTypeId = instanceTypesPreparation.createOrReferenceTerm("text",
      "txt", "rdacontent");
  }

  private static void createContributorNameTypes()
    throws MalformedURLException,
    InterruptedException,
    ExecutionException,
    TimeoutException {

    OkapiHttpClient client = createOkapiHttpClient();

    URL contributorNameTypes = new URL(String.format("%s/contributor-name-types", storageOkapiUrl()));

    ControlledVocabularyPreparation contributorNameTypesPreparation =
      new ControlledVocabularyPreparation(client, contributorNameTypes, "contributorNameTypes");

    personalContributorNameTypeId = contributorNameTypesPreparation.createOrReferenceTerm("Personal name");
  }

  private static UUID createReferenceRecord(
    ResourceClient client,
    JsonObject record)
    throws MalformedURLException,
    InterruptedException,
    ExecutionException,
    TimeoutException {

    List<JsonObject> existingRecords = client.getAll();

    String name = record.getString("name");

    if(name == null) {
      throw new IllegalArgumentException("Reference records must have a name");
    }

    if(existsInList(existingRecords, name)) {
      return client.create(record).getId();
    }
    else {
      return findFirstByName(existingRecords, name);
    }
  }

  private static UUID createReferenceRecord(
    ResourceClient client,
    String name,
    String code)
    throws MalformedURLException,
    InterruptedException,
    ExecutionException,
    TimeoutException {

    return createReferenceRecord(client, new JsonObject()
      .put("name", name)
      .put("code", code));
  }

  private static UUID findFirstByName(List<JsonObject> existingRecords, String name) {
    return UUID.fromString(existingRecords.stream()
      .filter(record -> record.getString("name").equals(name))
      .findFirst()
      .orElseThrow(() -> new IllegalArgumentException("No record with name: " + name))
      .getString("id"));
  }

  private static boolean existsInList(List<JsonObject> existingRecords, String name) {
    return existingRecords.stream()
      .noneMatch(materialType -> materialType.getString("name").equals(name));
  }

  private static void startPostgresqlContainer() {
    PgPoolContainer.create();
  }

  private static void stopPostgresqlContainer() {
    if (PgPoolContainer.isRunning()) {
      PgPoolContainer.stop();
    }
  }
}
