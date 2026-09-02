package api;

import api.holdings.HoldingsApiMoveTest;
import api.holdings.HoldingsApiTest;
import api.holdings.HoldingsUpdateOwnershipApiTest;
import api.instance.InstanceRelationshipsTest;
import api.instance.InstancesApiTest;
import api.instance.PrecedingSucceedingTitlesApiTest;
import api.isbns.IsbnUtilsApiTest;
import api.items.ItemsApiMoveTest;
import api.items.ItemsApiTest;
import api.items.ItemsUpdateOwnershipApiTest;
import api.items.MarkItemInProcessApiTest;
import api.items.MarkItemInProcessNonRequestableApiTest;
import api.items.MarkItemIntellectualItemApiTest;
import api.items.MarkItemLongMissingApiTest;
import api.items.MarkItemMissingApiTest;
import api.items.MarkItemRestrictedApiTest;
import api.items.MarkItemUnavailableApiTest;
import api.items.MarkItemUnknownApiTest;
import api.items.MarkItemWithdrawnApiTest;
import api.items.TenantItemApiTest;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import java.net.URI;
import java.net.URL;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import lombok.SneakyThrows;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.inventory.InventoryVerticle;
import org.folio.inventory.common.VertxAssistant;
import org.folio.inventory.consortium.util.ConsortiumUtil;
import org.folio.inventory.support.http.client.OkapiHttpClient;
import org.junit.platform.suite.api.AfterSuite;
import org.junit.platform.suite.api.BeforeSuite;
import org.junit.platform.suite.api.SelectClasses;
import org.junit.platform.suite.api.Suite;
import support.ControlledVocabularyPreparation;
import support.PgPoolContainer;
import support.fakes.FakeOkapi;
import support.http.ResourceClient;

@Suite
@SelectClasses({
  InstancesApiTest.class,
  ItemsApiTest.class,
  IsbnUtilsApiTest.class,
  PrecedingSucceedingTitlesApiTest.class,
  InstanceRelationshipsTest.class,
  HoldingsApiTest.class,
  MarkItemWithdrawnApiTest.class,
  ItemsApiMoveTest.class,
  MarkItemInProcessApiTest.class,
  MarkItemInProcessNonRequestableApiTest.class,
  MarkItemIntellectualItemApiTest.class,
  MarkItemLongMissingApiTest.class,
  MarkItemMissingApiTest.class,
  MarkItemRestrictedApiTest.class,
  MarkItemUnavailableApiTest.class,
  MarkItemUnknownApiTest.class,
  HoldingsApiMoveTest.class,
  BoundWithTest.class,
  TenantApiTest.class,
  AdminApiTest.class,
  InventoryConfigApiTest.class,
  HoldingsUpdateOwnershipApiTest.class,
  ItemsUpdateOwnershipApiTest.class,
  TenantItemApiTest.class
})
public class ApiTestSuite {

  public static final String TENANT_ID = "test_tenant";
  public static final String CONSORTIA_TENANT_ID = "consortium";
  public static final String COLLEGE_TENANT_ID = "college";
  public static final UUID ID_FOR_FAILURE = UUID.fromString("fa45a95b-38a3-430b-8f34-548ca005a176");
  public static final UUID ID_FOR_OPTIMISTIC_LOCKING_FAILURE = UUID.fromString("40900409-0409-4444-8888-409000000409");
  public static final String USER_ID = "7e115dfb-d1d6-46ac-b2dc-2b3e74cda694";
  public static final String REQUEST_ID = "test_request_1234";

  private static final Logger log = LogManager.getLogger(ApiTestSuite.class);

  private static final VertxAssistant VERTX_ASSISTANT = new VertxAssistant();
  private static final Boolean USE_OKAPI_FOR_API_REQUESTS =
    Boolean.parseBoolean(System.getProperty("use.okapi.initial.requests", ""));
  private static final Boolean USE_OKAPI_FOR_STORAGE_REQUESTS =
    Boolean.parseBoolean(System.getProperty("use.okapi.storage.requests", ""));
  private static final String OKAPI_ADDRESS = System.getProperty("okapi.address", "");

  private static final int INVENTORY_VERTICLE_TEST_PORT = 9603;
  private static final String CENTRAL_TENANT_ID_FIELD = "centralTenantId";
  private static final String CONSORTIUM_ID_FIELD = "consortiumId";

  private static String bookMaterialTypeId;
  private static String dvdMaterialTypeId;
  private static String canCirculateLoanTypeId;
  private static String courseReserveLoanTypeId;
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
  private static String inventoryModuleDeploymentId;
  private static String fakeModulesDeploymentId;
  private static boolean initialised;

  @BeforeSuite
  public static void before() {
    log.info("Use Okapi For Initial Requests:{}", System.getProperty("use.okapi.initial.requests"));
    log.info("Use Okapi For Storage Requests:{}", System.getProperty("use.okapi.storage.requests"));

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

  @AfterSuite
  @SneakyThrows
  public static void after(){
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

  public static OkapiHttpClient createOkapiHttpClient() {
    return createOkapiHttpClient(TENANT_ID);
  }

  @SneakyThrows
  public static OkapiHttpClient createOkapiHttpClient(String tenantId) {
    return new OkapiHttpClient(
      VERTX_ASSISTANT.getVertx(),
      URI.create(storageOkapiUrl()).toURL(), tenantId, "token", USER_ID, null,
      it -> log.error("Request failed.", it));
  }

  @SneakyThrows
  public static OkapiHttpClient createOkapiHttpClient(String tenantId, String token, String userId) {
    return new OkapiHttpClient(
      VERTX_ASSISTANT.getVertx(),
      URI.create(storageOkapiUrl()).toURL(), tenantId, token, userId, null,
      it -> log.error("Request failed.", it));
  }

  public static String storageOkapiUrl() {
    if (USE_OKAPI_FOR_STORAGE_REQUESTS) {
      return OKAPI_ADDRESS;
    } else {
      return FakeOkapi.getADDRESS();
    }
  }

  public static String apiRoot() {
    String directRoot = String.format("http://localhost:%s",
      ApiTestSuite.INVENTORY_VERTICLE_TEST_PORT);

    return USE_OKAPI_FOR_API_REQUESTS ? OKAPI_ADDRESS : directRoot;
  }

  public static void createConsortiumTenant() {
    String expectedConsortiumId = UUID.randomUUID().toString();

    JsonObject userTenantsCollection = new JsonObject()
      .put(ApiTestSuite.CENTRAL_TENANT_ID_FIELD, ApiTestSuite.CONSORTIA_TENANT_ID)
      .put(ApiTestSuite.CONSORTIUM_ID_FIELD, expectedConsortiumId);

    ResourceClient client = ResourceClient.forUserTenants(createOkapiHttpClient());

    client.create(userTenantsCollection);
  }

  private static void stopVertx() {
    VERTX_ASSISTANT.stop();
  }

  private static void startVertx() {
    VERTX_ASSISTANT.start();
  }

  @SneakyThrows
  private static void startInventoryVerticle() {
    CompletableFuture<String> deployed = new CompletableFuture<>();

    String storageType = "okapi";
    String storageLocation = "";

    log.info("Storage Type: {}", storageType);
    log.info("Storage Location: {}", storageLocation);

    Map<String, Object> config = new HashMap<>();

    config.put("port", INVENTORY_VERTICLE_TEST_PORT);
    config.put("storage.type", storageType);
    config.put("storage.location", storageLocation);

    System.setProperty(ConsortiumUtil.EXPIRATION_TIME_PARAM, "0");

    VERTX_ASSISTANT.deployVerticle(
      InventoryVerticle.class.getName(), config, deployed);

    inventoryModuleDeploymentId = deployed.get(20000, TimeUnit.MILLISECONDS);
  }

  private static void stopInventoryVerticle()
    throws InterruptedException, ExecutionException, TimeoutException {

    CompletableFuture<Void> undeployed = new CompletableFuture<>();

    if (inventoryModuleDeploymentId != null) {
      VERTX_ASSISTANT.undeployVerticle(inventoryModuleDeploymentId, undeployed);

      undeployed.get(20000, TimeUnit.MILLISECONDS);
    }
  }

  @SneakyThrows
  private static void startFakeModules() {
    if (!USE_OKAPI_FOR_STORAGE_REQUESTS) {
      CompletableFuture<String> fakeModulesDeployed = new CompletableFuture<>();

      VERTX_ASSISTANT.deployVerticle(FakeOkapi.class.getName(),
        new HashMap<>(), fakeModulesDeployed);

      fakeModulesDeploymentId = fakeModulesDeployed.get(10, TimeUnit.SECONDS);
    }
  }

  @SneakyThrows
  private static void stopFakeModules() {
    if (!USE_OKAPI_FOR_STORAGE_REQUESTS && fakeModulesDeploymentId != null) {
      CompletableFuture<Void> undeployed = new CompletableFuture<>();

      VERTX_ASSISTANT.undeployVerticle(fakeModulesDeploymentId, undeployed);

      undeployed.get(20000, TimeUnit.MILLISECONDS);
    }
  }

  @SneakyThrows
  private static void createMaterialTypes() {
    OkapiHttpClient client = createOkapiHttpClient();
    URL materialTypesUrl = new URI(String.format("%s/material-types", storageOkapiUrl())).toURL();

    ControlledVocabularyPreparation materialTypePreparation =
      new ControlledVocabularyPreparation(client, materialTypesUrl, "mtypes");

    bookMaterialTypeId = materialTypePreparation.createOrReferenceTerm("Book");
    dvdMaterialTypeId = materialTypePreparation.createOrReferenceTerm("DVD");
  }

  @SneakyThrows
  private static void createLoanTypes() {
    OkapiHttpClient client = createOkapiHttpClient();
    URL loanTypes = new URI(String.format("%s/loan-types", storageOkapiUrl())).toURL();

    ControlledVocabularyPreparation loanTypePreparation =
      new ControlledVocabularyPreparation(client, loanTypes, "loantypes");

    canCirculateLoanTypeId = loanTypePreparation.createOrReferenceTerm("Can Circulate");
    courseReserveLoanTypeId = loanTypePreparation.createOrReferenceTerm("Course Reserves");
  }

  @SneakyThrows
  private static void createLocations() {
    final OkapiHttpClient client = createOkapiHttpClient();
    ResourceClient institutionsClient = ResourceClient.forInstitutions(client);

    UUID nottinghamUniversityInstitution = createReferenceRecord(institutionsClient,
      new JsonObject()
        .put("name", "Nottingham University")
        .put("code", "NOTT"));

    ResourceClient campusesClient = ResourceClient.forCampuses(client);

    UUID jubileeCampus = createReferenceRecord(campusesClient,
      new JsonObject()
        .put("name", "Jubilee Campus")
        .put("institutionId", nottinghamUniversityInstitution.toString())
        .put("code", "JUBILEE"));

    ResourceClient librariesClient = ResourceClient.forLibraries(client);

    UUID djanoglyLibrary = createReferenceRecord(librariesClient,
      new JsonObject()
        .put("name", "Djanogly Learning Resource Centre")
        .put("campusId", jubileeCampus.toString())
        .put("code", "DJANOGLY"));

    UUID businessLibrary = createReferenceRecord(librariesClient,
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
        .put("primaryServicePoint", fakeServicePointId.toString())
        .put("servicePointIds", new JsonArray().add(fakeServicePointId.toString())));

    mezzanineDisplayCaseLocationId = createReferenceRecord(locationsClient,
      new JsonObject()
        .put("name", "Display Case, Mezzanine")
        .put("code", "NU/JC/BL/DM")
        .put("institutionId", nottinghamUniversityInstitution.toString())
        .put("campusId", jubileeCampus.toString())
        .put("libraryId", businessLibrary.toString())
        .put("primaryServicePoint", fakeServicePointId.toString())
        .put("servicePointIds", new JsonArray().add(fakeServicePointId.toString())));

    readingRoomLocationId = createReferenceRecord(locationsClient,
      new JsonObject()
        .put("name", "Reading Room")
        .put("code", "NU/JC/BL/PR")
        .put("institutionId", nottinghamUniversityInstitution.toString())
        .put("campusId", jubileeCampus.toString())
        .put("libraryId", businessLibrary.toString())
        .put("primaryServicePoint", fakeServicePointId.toString())
        .put("servicePointIds", new JsonArray().add(fakeServicePointId.toString())));

    mainLibraryLocationId = createReferenceRecord(locationsClient,
      new JsonObject()
        .put("name", "Main Library")
        .put("code", "NU/JC/DL/ML")
        .put("institutionId", nottinghamUniversityInstitution.toString())
        .put("campusId", jubileeCampus.toString())
        .put("libraryId", djanoglyLibrary.toString())
        .put("primaryServicePoint", fakeServicePointId.toString())
        .put("servicePointIds", new JsonArray().add(fakeServicePointId.toString())));
  }

  @SneakyThrows
  private static void createNatureOfContentTerms() {
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

  @SneakyThrows
  private static void createIdentifierTypes() {
    OkapiHttpClient client = createOkapiHttpClient();
    URL identifierTypesUrl = new URI(String.format("%s/identifier-types", storageOkapiUrl())).toURL();

    ControlledVocabularyPreparation identifierTypesPreparation =
      new ControlledVocabularyPreparation(client, identifierTypesUrl, "identifierTypes");

    isbnIdentifierTypeId = identifierTypesPreparation.createOrReferenceTerm("ISBN");
    asinIdentifierTypeId = identifierTypesPreparation.createOrReferenceTerm("ASIN");
  }

  @SneakyThrows
  private static void createInstanceTypes() {
    OkapiHttpClient client = createOkapiHttpClient();
    URL instanceTypes = new URI(String.format("%s/instance-types", storageOkapiUrl())).toURL();

    ControlledVocabularyPreparation instanceTypesPreparation =
      new ControlledVocabularyPreparation(client, instanceTypes, "instanceTypes");

    textInstanceTypeId = instanceTypesPreparation.createOrReferenceTerm("text", "txt", "rdacontent");
  }

  @SneakyThrows
  private static void createContributorNameTypes() {
    OkapiHttpClient client = createOkapiHttpClient();
    URL contributorNameTypes = new URI(String.format("%s/contributor-name-types", storageOkapiUrl())).toURL();

    ControlledVocabularyPreparation contributorNameTypesPreparation =
      new ControlledVocabularyPreparation(client, contributorNameTypes, "contributorNameTypes");

    personalContributorNameTypeId = contributorNameTypesPreparation.createOrReferenceTerm("Personal name");
  }

  @SneakyThrows
  private static UUID createReferenceRecord(ResourceClient client, JsonObject jsonRecord) {
    List<JsonObject> existingRecords = client.getAll();
    String name = jsonRecord.getString("name");

    if (name == null) {
      throw new IllegalArgumentException("Reference records must have a name");
    }

    if (existsInList(existingRecords, name)) {
      return client.create(jsonRecord).getId();
    } else {
      return findFirstByName(existingRecords, name);
    }
  }

  private static UUID findFirstByName(List<JsonObject> existingRecords, String name) {
    return UUID.fromString(existingRecords.stream()
      .filter(jsonRecord -> jsonRecord.getString("name").equals(name))
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
