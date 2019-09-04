package api.support;

import api.ApiTestSuite;

public class TestInstanceRelationships {
  private String precedingId;
  private String succeedingId;

  private String childId;
  private String childRelationshipType = ApiTestSuite.getMultipartMonographRelationship();

  private String parentId;
  private String parentRelationshipType = ApiTestSuite.getMultipartMonographRelationship();

  public String getChildRelationshipType() {
    return childRelationshipType;
  }

  public TestInstanceRelationships withPrecedingId(String precedingId) {
    this.precedingId = precedingId;

    return this;
  }

  public String getSucceedingId() {
    return succeedingId;
  }

  public TestInstanceRelationships withSucceedingId(String succeedingId) {
    this.succeedingId = succeedingId;

    return this;
  }

  public String getChildId() {
    return childId;
  }

  public TestInstanceRelationships withChildId(String childId) {
    this.childId = childId;

    return this;
  }

  public String getParentId() {
    return parentId;
  }

  public TestInstanceRelationships withParentId(String parentId) {
    this.parentId = parentId;

    return this;
  }

  public TestInstanceRelationships withChildRelationshipType(String childRelationshipType) {
    this.childRelationshipType = childRelationshipType;

    return this;
  }

  public String getParentRelationshipType() {
    return parentRelationshipType;
  }

  public TestInstanceRelationships withParentRelationshipType(String parentRelationshipType) {
    this.parentRelationshipType = parentRelationshipType;

    return this;
  }

  public String getPrecedingId() {
    return precedingId;
  }
}
