package support.fakes;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

import api.ApiTestSuite;
import support.fakes.processors.RecordPreProcessor;

public class FakeStorageModuleBuilder {
  protected String rootPath;
  protected String collectionPropertyName;
  protected String tenantId;
  protected Collection<String> requiredProperties;
  protected Collection<String> uniqueProperties;
  protected Map<String, Supplier<Object>> defaultProperties;
  protected Boolean hasCollectionDelete;
  protected String recordName;
  protected List<RecordPreProcessor> recordPreProcessors;

  FakeStorageModuleBuilder() {
    this(null, null, ApiTestSuite.TENANT_ID, new ArrayList<>(), true, "",
      new ArrayList<>(), new HashMap<>(), Collections.emptyList());
  }

  protected FakeStorageModuleBuilder(
    String rootPath,
    String collectionPropertyName,
    String tenantId,
    Collection<String> requiredProperties,
    boolean hasCollectionDelete,
    String recordName,
    Collection<String> uniqueProperties,
    Map<String, Supplier<Object>> defaultProperties,
    List<RecordPreProcessor> recordPreProcessors) {

    this.rootPath = rootPath;
    this.collectionPropertyName = collectionPropertyName;
    this.tenantId = tenantId;
    this.requiredProperties = requiredProperties;
    this.hasCollectionDelete = hasCollectionDelete;
    this.recordName = recordName;
    this.uniqueProperties = uniqueProperties;
    this.defaultProperties = defaultProperties;
    this.recordPreProcessors = recordPreProcessors;
  }

  public FakeStorageModule create() {
    return new FakeStorageModule(rootPath, collectionPropertyName, tenantId,
      requiredProperties, hasCollectionDelete, recordName, uniqueProperties,
      defaultProperties, recordPreProcessors);
  }

  FakeStorageModuleBuilder withRootPath(String rootPath) {
    String newCollectionPropertyName = collectionPropertyName == null
      ? rootPath.substring(rootPath.lastIndexOf("/") + 1)
      : collectionPropertyName;

    this.rootPath = rootPath;
    this.collectionPropertyName = newCollectionPropertyName;
    return this;
  }

  FakeStorageModuleBuilder withCollectionPropertyName(String collectionPropertyName) {
    this.collectionPropertyName = collectionPropertyName;
    return this;
  }

  FakeStorageModuleBuilder withRecordName(String recordName) {
    this.recordName = recordName;
    return this;
  }

  private FakeStorageModuleBuilder withRequiredProperties(
    Collection<String> requiredProperties) {
    this.requiredProperties = requiredProperties;
    return this;
  }

  FakeStorageModuleBuilder withRequiredProperties(String... requiredProperties) {
    return withRequiredProperties(Arrays.asList(requiredProperties));
  }

  FakeStorageModuleBuilder withDefault(String property, Object value) {
    final Map<String, Supplier<Object>> newDefaults = new HashMap<>(this.defaultProperties);

    newDefaults.put(property, () -> value);

    this.defaultProperties = newDefaults;
    return this;
  }

  final FakeStorageModuleBuilder withRecordPreProcessors(RecordPreProcessor... preProcessors) {
    this.recordPreProcessors = Arrays.asList(preProcessors);
    return this;
  }
}

