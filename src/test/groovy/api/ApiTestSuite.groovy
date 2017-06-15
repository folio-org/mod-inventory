package api

import api.support.ControlledVocabularyPreparation
import org.folio.inventory.InventoryVerticle
import org.folio.inventory.common.VertxAssistant
import org.folio.inventory.support.http.client.OkapiHttpClient
import org.junit.AfterClass
import org.junit.BeforeClass
import org.junit.runner.RunWith
import org.junit.runners.Suite
import support.fakes.FakeOkapi

import java.util.concurrent.CompletableFuture
import java.util.concurrent.TimeUnit

@RunWith(Suite.class)

@Suite.SuiteClasses([
  InstancesApiExamples.class,
  ItemApiExamples.class,
  ModsIngestExamples.class
])

class ApiTestSuite {
  public static final INVENTORY_VERTICLE_TEST_PORT = 9603
  public static final String TENANT_ID = "test_tenant"

  public static final String TOKEN = "eyJhbGciOiJIUzUxMiJ9.eyJzdWIiOiJhZG1pbiIsInRlbmFudCI6ImRlbW9fdGVuYW50In0.29VPjLI6fLJzxQW0UhQ0jsvAn8xHz501zyXAxRflXfJ9wuDzT8TDf-V75PjzD7fe2kHjSV2dzRXbstt3BTtXIQ"

  private static String bookMaterialTypeId
  private static String dvdMaterialTypeId

  private static String canCirculateLoanTypeId
  private static String courseReserveLoanTypeId

  private static VertxAssistant vertxAssistant = new VertxAssistant();
  private static String inventoryModuleDeploymentId
  private static String fakeModulesDeploymentId

  private static Boolean useOkapiForApiRequests =
    (System.getProperty("use.okapi.initial.requests") ?: "").toBoolean()
  private static Boolean useOkapiForStorageRequests =
    (System.getProperty("use.okapi.storage.requests") ?: "").toBoolean()
  private static String okapiAddress = System.getProperty("okapi.address", "")

  @BeforeClass
  static void before() {

    println("Use Okapi For Initial Requests:" + System.getProperty("use.okapi.initial.requests"))
    println("Use Okapi For Storage Requests:" + System.getProperty("use.okapi.storage.requests"))

    startVertx()
    startFakeModules()
    createMaterialTypes()
    createLoanTypes()
    startInventoryVerticle()
  }

  @AfterClass
  static void after() {
    stopInventoryVerticle()
    stopFakeModules()
    stopVertx()
  }

  static String getBookMaterialType() {
    bookMaterialTypeId
  }

  static String getDvdMaterialType() {
    dvdMaterialTypeId
  }

  static String getCanCirculateLoanType() {
    canCirculateLoanTypeId
  }

  static String getCourseReserveLoanType() {
    courseReserveLoanTypeId
  }

  static OkapiHttpClient createOkapiHttpClient() {
    return new OkapiHttpClient(
      vertxAssistant.createUsingVertx({ it.createHttpClient() }),
      new URL(storageOkapiUrl()), TENANT_ID, TOKEN, {
      System.out.println(
        String.format("Request failed: %s",
          it.toString())) })
  }

  static String storageOkapiUrl() {
    if(useOkapiForStorageRequests) {
      okapiAddress
    }
    else {
      FakeOkapi.address
    }
  }

  static String apiRoot() {
    def directRoot = "http://localhost:${ApiTestSuite.INVENTORY_VERTICLE_TEST_PORT}"

    useOkapiForApiRequests ? okapiAddress : directRoot
  }

  private static stopVertx() {
    vertxAssistant.stop()
  }

  private static startVertx() {
    vertxAssistant.start()
  }

  private static startInventoryVerticle() {
    def deployed = new CompletableFuture()

    def storageType = "okapi"
    def storageLocation = ""

    println("Storage Type: ${storageType}")
    println("Storage Location: ${storageLocation}")

    def config = ["port": INVENTORY_VERTICLE_TEST_PORT,
                  "storage.type" : storageType,
                  "storage.location" : storageLocation]

    vertxAssistant.deployGroovyVerticle(
      InventoryVerticle.class.name, config,  deployed)

    inventoryModuleDeploymentId = deployed.get(20000, TimeUnit.MILLISECONDS)
  }

  private static stopInventoryVerticle() {
    def undeployed = new CompletableFuture()

    if(inventoryModuleDeploymentId != null) {
      vertxAssistant.undeployVerticle(inventoryModuleDeploymentId, undeployed)

      undeployed.get(20000, TimeUnit.MILLISECONDS)
    }
  }

  private static startFakeModules() {
    if(!useOkapiForStorageRequests) {
      def fakeModulesDeployed = new CompletableFuture<>();

        vertxAssistant.deployVerticle(FakeOkapi.class.getName(),
          [:], fakeModulesDeployed);

      fakeModulesDeploymentId = fakeModulesDeployed.get(10, TimeUnit.SECONDS);
    }
  }

  private static stopFakeModules() {
    if(!useOkapiForStorageRequests && fakeModulesDeploymentId != null) {
      def undeployed = new CompletableFuture()

      vertxAssistant.undeployVerticle(fakeModulesDeploymentId, undeployed)

      undeployed.get(20000, TimeUnit.MILLISECONDS)
    }
  }

  private static def createMaterialTypes() {
    def client = createOkapiHttpClient()

    def materialTypesUrl = new URL("${storageOkapiUrl()}/material-types")

    def materialTypePreparation = new ControlledVocabularyPreparation(client,
      materialTypesUrl, "mtypes")

    bookMaterialTypeId = materialTypePreparation.createOrReferenceTerm("Book")
    dvdMaterialTypeId = materialTypePreparation.createOrReferenceTerm("DVD")
  }

  private static def createLoanTypes() {
    def client = createOkapiHttpClient()

    def materialTypesUrl = new URL("${storageOkapiUrl()}/loan-types")

    def loanTypePreparation = new ControlledVocabularyPreparation(client,
      materialTypesUrl, "loantypes")

    canCirculateLoanTypeId = loanTypePreparation.createOrReferenceTerm("Can Circulate")
    courseReserveLoanTypeId = loanTypePreparation.createOrReferenceTerm("Course Reserves")
  }
}
