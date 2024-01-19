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
  private final String rootPath;
  private final String collectionPropertyName;
  private final String tenantId;
  private final Collection<String> requiredProperties;
  private final Collection<String> uniqueProperties;
  private final Map<String, Supplier<Object>> defaultProperties;
  private final Boolean hasCollectionDelete;
  private final String recordName;
  private final List<RecordPreProcessor> recordPreProcessors;

  FakeStorageModuleBuilder() {
    this(null, null, ApiTestSuite.TENANT_ID, new ArrayList<>(), true, "",
      new ArrayList<>(), new HashMap<>(), Collections.emptyList());
  }

  private FakeStorageModuleBuilder(
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

    return new FakeStorageModuleBuilder(
      rootPath,
      newCollectionPropertyName,
      this.tenantId,
      this.requiredProperties,
      this.hasCollectionDelete,
      this.recordName,
      this.uniqueProperties,
      this.defaultProperties,
      this.recordPreProcessors);
  }

  FakeStorageModuleBuilder withCollectionPropertyName(String collectionPropertyName) {
    return new FakeStorageModuleBuilder(
      this.rootPath,
      collectionPropertyName,
      this.tenantId,
      this.requiredProperties,
      this.hasCollectionDelete,
      this.recordName,
      this.uniqueProperties,
      this.defaultProperties,
      this.recordPreProcessors);
  }

  FakeStorageModuleBuilder withRecordName(String recordName) {
    return new FakeStorageModuleBuilder(
      this.rootPath,
      this.collectionPropertyName,
      this.tenantId,
      this.requiredProperties,
      this.hasCollectionDelete,
      recordName,
      this.uniqueProperties,
      this.defaultProperties,
      this.recordPreProcessors);
  }

  private FakeStorageModuleBuilder withRequiredProperties(
    Collection<String> requiredProperties) {

    return new FakeStorageModuleBuilder(
      this.rootPath,
      this.collectionPropertyName,
      this.tenantId,
      requiredProperties,
      this.hasCollectionDelete,
      this.recordName,
      this.uniqueProperties,
      this.defaultProperties,
      this.recordPreProcessors);
  }

  FakeStorageModuleBuilder withRequiredProperties(String... requiredProperties) {
    return withRequiredProperties(Arrays.asList(requiredProperties));
  }

  FakeStorageModuleBuilder withDefault(String property, Object value) {
    final Map<String, Supplier<Object>> newDefaults = new HashMap<>(this.defaultProperties);

    newDefaults.put(property, () -> value);

    return new FakeStorageModuleBuilder(
      this.rootPath,
      this.collectionPropertyName,
      this.tenantId,
      this.requiredProperties,
      this.hasCollectionDelete,
      this.recordName,
      this.uniqueProperties,
      newDefaults,
      this.recordPreProcessors);
  }

  final FakeStorageModuleBuilder withRecordPreProcessors(RecordPreProcessor... preProcessors) {
    return new FakeStorageModuleBuilder(
      this.rootPath,
      this.collectionPropertyName,
      this.tenantId,
      this.requiredProperties,
      this.hasCollectionDelete,
      this.recordName,
      this.uniqueProperties,
      this.defaultProperties,
      Arrays.asList(preProcessors));
  }
}

