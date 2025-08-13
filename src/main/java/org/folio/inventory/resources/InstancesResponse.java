package org.folio.inventory.resources;

import org.folio.inventory.common.domain.MultipleRecords;
import org.folio.inventory.common.domain.Success;
import org.folio.inventory.domain.instances.Instance;
import org.folio.inventory.domain.instances.InstanceRelationshipToChild;
import org.folio.inventory.domain.instances.InstanceRelationshipToParent;
import org.folio.inventory.domain.instances.titles.PrecedingSucceedingTitle;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class InstancesResponse {
  private Success<MultipleRecords<Instance>> success;
  private Map<String, List<InstanceRelationshipToParent>> parentInstanceMap = new HashMap();
  private Map<String, List<InstanceRelationshipToChild>> childInstanceMap = new HashMap();
  private Map<String, List<PrecedingSucceedingTitle>> precedingTitlesMap = new HashMap();
  private Map<String, List<PrecedingSucceedingTitle>> succeedingTitlesMap = new HashMap();
  private Set<String> instancesThatAreBoundWith = new HashSet<>();

  public Success<MultipleRecords<Instance>> getSuccess() {
    return success;
  }

  public InstancesResponse setSuccess(Success<MultipleRecords<Instance>> success) {
    this.success = success;
    return this;
  }

  public boolean hasRecords () {
    return !success.getResult().records.isEmpty();
  }

  public Map<String, List<InstanceRelationshipToParent>> getParentInstanceMap() {
    return Collections.unmodifiableMap(parentInstanceMap);
  }

  public InstancesResponse setParentInstanceMap(
    Map<String, List<InstanceRelationshipToParent>> parentInstanceMap) {

    this.parentInstanceMap = parentInstanceMap;
    return this;
  }

  public Map<String, List<InstanceRelationshipToChild>> getChildInstanceMap() {
    return Collections.unmodifiableMap(childInstanceMap);
  }

  public InstancesResponse setChildInstanceMap(
    Map<String, List<InstanceRelationshipToChild>> childInstanceMap) {

    this.childInstanceMap = childInstanceMap;
    return this;
  }

  public Map<String, List<PrecedingSucceedingTitle>> getPrecedingTitlesMap() {
    return Collections.unmodifiableMap(precedingTitlesMap);
  }

  public InstancesResponse setPrecedingTitlesMap(
    Map<String, List<PrecedingSucceedingTitle>> precedingTitlesMap) {

    this.precedingTitlesMap = precedingTitlesMap;
    return this;
  }

  public Map<String, List<PrecedingSucceedingTitle>> getSucceedingTitlesMap() {
    return Collections.unmodifiableMap(succeedingTitlesMap);
  }

  public InstancesResponse setSucceedingTitlesMap(
    Map<String, List<PrecedingSucceedingTitle>> succeedingTitlesMap) {

    this.succeedingTitlesMap = succeedingTitlesMap;
    return this;
  }

  public InstancesResponse setBoundWithInstanceIds(List<String> instanceIds) {
    this.instancesThatAreBoundWith = Set.copyOf(instanceIds);
    for (Instance instance : success.getResult().records) {
      instance.setIsBoundWith(isBoundWith(instance.getId()));
    }
    return this;
  }

  public boolean isBoundWith (String instanceId) {
    return instancesThatAreBoundWith.contains(instanceId);
  }
}
