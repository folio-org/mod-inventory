package org.folio.inventory.resources;

import org.folio.inventory.common.domain.MultipleRecords;
import org.folio.inventory.common.domain.Success;
import org.folio.inventory.domain.instances.Instance;
import org.folio.inventory.domain.instances.InstanceRelationshipToChild;
import org.folio.inventory.domain.instances.InstanceRelationshipToParent;
import org.folio.inventory.domain.instances.titles.PrecedingSucceedingTitle;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class InstancesResponseWrapper {
  private Success<MultipleRecords<Instance>> success;
  private Map<String, List<InstanceRelationshipToParent>> parentInstanceMap = new HashMap();
  private Map<String, List<InstanceRelationshipToChild>> childInstanceMap = new HashMap();
  private Map<String, List<PrecedingSucceedingTitle>> precedingTitlesMap = new HashMap();
  private Map<String, List<PrecedingSucceedingTitle>> succeedingTitlesMap = new HashMap();

  public Success<MultipleRecords<Instance>> getSuccess() {
    return success;
  }

  public void setSuccess(Success<MultipleRecords<Instance>> success) {
    this.success = success;
  }

  public Map<String, List<InstanceRelationshipToParent>> getParentInstanceMap() {
    return parentInstanceMap;
  }

  public void setParentInstanceMap(Map<String, List<InstanceRelationshipToParent>> parentInstanceMap) {
    this.parentInstanceMap = parentInstanceMap;
  }

  public Map<String, List<InstanceRelationshipToChild>> getChildInstanceMap() {
    return childInstanceMap;
  }

  public void setChildInstanceMap(Map<String, List<InstanceRelationshipToChild>> childInstanceMap) {
    this.childInstanceMap = childInstanceMap;
  }

  public Map<String, List<PrecedingSucceedingTitle>> getPrecedingTitlesMap() {
    return precedingTitlesMap;
  }

  public void setPrecedingTitlesMap(Map<String, List<PrecedingSucceedingTitle>> precedingTitlesMap) {
    this.precedingTitlesMap = precedingTitlesMap;
  }

  public Map<String, List<PrecedingSucceedingTitle>> getSucceedingTitlesMap() {
    return succeedingTitlesMap;
  }

  public void setSucceedingTitlesMap(Map<String, List<PrecedingSucceedingTitle>> succeedingTitlesMap) {
    this.succeedingTitlesMap = succeedingTitlesMap;
  }
}
