package support.fakes;

import api.ApiTestSuite;

import java.util.*;
import java.util.function.Supplier;

public class FakeStorageModuleBuilder {
  private final String rootPath;
  private final String collectionPropertyName;
  private final String tenantId;
  private final Collection<String> requiredProperties;
  private final Collection<String> uniqueProperties;
  private final Map<String, Supplier<Object>> defaultProperties;
  private final Boolean hasCollectionDelete;
  private final String recordName;

  FakeStorageModuleBuilder() {
    this(null, null, ApiTestSuite.TENANT_ID, new ArrayList<>(), true, "",
      new ArrayList<>(), new HashMap<>());
  }

  private FakeStorageModuleBuilder(
    String rootPath,
    String collectionPropertyName,
    String tenantId,
    Collection<String> requiredProperties,
    boolean hasCollectionDelete,
    String recordName,
    Collection<String> uniqueProperties,
    Map<String, Supplier<Object>> defaultProperties) {

    this.rootPath = rootPath;
    this.collectionPropertyName = collectionPropertyName;
    this.tenantId = tenantId;
    this.requiredProperties = requiredProperties;
    this.hasCollectionDelete = hasCollectionDelete;
    this.recordName = recordName;
    this.uniqueProperties = uniqueProperties;
    this.defaultProperties = defaultProperties;
  }

  public FakeStorageModule create() {
    return new FakeStorageModule(rootPath, collectionPropertyName, tenantId,
      requiredProperties, hasCollectionDelete, recordName, uniqueProperties,
      defaultProperties);
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
      this.defaultProperties);
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
      this.defaultProperties);
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
      this.defaultProperties);
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
      this.defaultProperties);
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
      newDefaults);
  }
}

