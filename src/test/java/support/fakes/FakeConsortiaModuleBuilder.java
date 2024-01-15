package support.fakes;

import api.ApiTestSuite;
import support.fakes.processors.RecordPreProcessor;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

public class FakeConsortiaModuleBuilder extends FakeStorageModuleBuilder {
  private FakeStorageModule fakeInstanceStorageModule;

  FakeConsortiaModuleBuilder() {
    this(null, null, ApiTestSuite.CONSORTIA_TENANT_ID, new ArrayList<>(), true, "",
      new ArrayList<>(), new HashMap<>(), Collections.emptyList(), null);
  }

  private FakeConsortiaModuleBuilder(
    String rootPath,
    String collectionPropertyName,
    String tenantId,
    Collection<String> requiredProperties,
    boolean hasCollectionDelete,
    String recordName,
    Collection<String> uniqueProperties,
    Map<String, Supplier<Object>> defaultProperties,
    List<RecordPreProcessor> recordPreProcessors,
    FakeStorageModule fakeInstanceStorageModule) {
    super(rootPath, collectionPropertyName, tenantId, requiredProperties, hasCollectionDelete, recordName, uniqueProperties, defaultProperties, recordPreProcessors);
    this.fakeInstanceStorageModule = fakeInstanceStorageModule;
  }

  @Override
  public FakeConsortiaModule create() {
    return new FakeConsortiaModule(rootPath, collectionPropertyName, tenantId,
      requiredProperties, hasCollectionDelete, recordName, uniqueProperties,
      defaultProperties, recordPreProcessors, fakeInstanceStorageModule);
  }

  FakeConsortiaModuleBuilder withInstanceStorageModule(FakeStorageModule fakeInstanceStorageModule) {
    this.fakeInstanceStorageModule = fakeInstanceStorageModule;
    return this;
  }
}
