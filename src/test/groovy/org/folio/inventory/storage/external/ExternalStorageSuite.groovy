package org.folio.inventory.storage.external

import io.vertx.core.Vertx
import org.folio.inventory.common.VertxAssistant
import org.folio.inventory.storage.external.support.FakeInventoryStorageModule
import org.junit.AfterClass
import org.junit.BeforeClass
import org.junit.runner.RunWith
import org.junit.runners.Suite

import java.util.concurrent.CompletableFuture
import java.util.concurrent.TimeUnit
import java.util.function.Function

@RunWith(Suite.class)

@Suite.SuiteClasses([
  ExternalItemCollectionExamples.class,
  ExternalInstanceCollectionExamples.class
])

public class ExternalStorageSuite {
  private static final VertxAssistant vertxAssistant = new VertxAssistant();
  private static String storageModuleDeploymentId

  public static <T> T useVertx(Function<Vertx, T> action) {
    vertxAssistant.createUsingVertx(action)
  }

  static String getItemStorageAddress() {
    if(!useFakeStorageModule()) {
      System.getProperty("inventory.storage.address")
    }
    else {
      FakeInventoryStorageModule.address
    }
  }

  static String getInstanceStorageAddress() {
    if(!useFakeStorageModule()) {
      System.getProperty("inventory.storage.address")
    }
    else {
      FakeInventoryStorageModule.address
    }
  }

  static Collection<String> getExpectedTenants() {
    ["test_tenant_1", "test_tenant_2"]
  }

  @BeforeClass
  static void beforeAll() {
    vertxAssistant.start()

    if(useFakeStorageModule()) {
      println("Starting Fake Storage Module")

      def deployed = new CompletableFuture()

      vertxAssistant.deployGroovyVerticle(
        FakeInventoryStorageModule.class.name,
        ["expectedTenants": expectedTenants],
        deployed)

      storageModuleDeploymentId = deployed.get(20000, TimeUnit.MILLISECONDS)
    }
  }

  private static boolean useFakeStorageModule() {
      System.getProperty('inventory.storage.use', "fake") == "fake"
  }

  @AfterClass()
  static void afterAll() {

    if(useFakeStorageModule()) {
      def undeployed = new CompletableFuture()

      vertxAssistant.undeployVerticle(storageModuleDeploymentId, undeployed)

      undeployed.get(20000, TimeUnit.MILLISECONDS)
    }

    vertxAssistant.stop()
  }
}
