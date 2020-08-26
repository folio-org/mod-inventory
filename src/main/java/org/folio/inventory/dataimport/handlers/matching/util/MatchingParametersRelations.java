package org.folio.inventory.dataimport.handlers.matching.util;

import java.util.HashMap;
import java.util.Map;

/**
 * Class for storing matching parameters(field for retrieving data) relation between matching profile and inventory-storage.
 */
public class MatchingParametersRelations {

  private Map<String, String> matchingRelations = new HashMap<>();

  public MatchingParametersRelations() {
    matchingRelations.put("instance.alternativeTitles[].alternativeTitleTypeId", "alternativeTitleTypes");
    matchingRelations.put("instance.identifiers[].identifierTypeId", "identifierTypes");
    matchingRelations.put("instance.classifications[].classificationTypeId", "classificationTypes");
    matchingRelations.put("instance.contributors[].contributorTypeId", "contributorTypes");
    matchingRelations.put("instance.instanceFormatIds", "instanceFormats");
    matchingRelations.put("instance.notes[].instanceNoteTypeId", "instanceNoteTypes");
    matchingRelations.put("instance.statusId", "instanceStatuses");
    matchingRelations.put("instance.modeOfIssuanceId", "issuanceModes");
    matchingRelations.put("instance.natureOfContentTermIds", "natureOfContentTerms");
    matchingRelations.put("instance.instanceTypeId", "instanceTypes");
    matchingRelations.put("holdingsrecord.holdingsTypeId", "holdingsTypes");
    matchingRelations.put("holdingsrecord.notes[].holdingsNoteTypeId", "holdingsNoteTypes");
    matchingRelations.put("instance.electronicAccess[].relationshipId", "electronicAccessRelationships");
    matchingRelations.put("item.electronicAccess[].relationshipId", "electronicAccessRelationships");
    matchingRelations.put("holdingsrecord.electronicAccess[].relationshipId", "electronicAccessRelationships");
    matchingRelations.put("instance.contributors[].contributorNameTypeId", "contributorNameTypes");
    matchingRelations.put("instancerelationship.instanceRelationshipTypeId", "instanceRelationshipTypes");
    matchingRelations.put("holdingsrecord.illPolicyId", "illPolicies");
    matchingRelations.put("holdingsrecord.callNumberTypeId", "callNumberTypes");
    matchingRelations.put("item.itemLevelCallNumberTypeId", "callNumberTypes");
    matchingRelations.put("instance.statisticalCodeIds[]", "statisticalCodes");
    matchingRelations.put("holdingsrecord.statisticalCodeIds[]", "statisticalCodes");
    matchingRelations.put("item.statisticalCodeIds[]", "statisticalCode");
    matchingRelations.put("item.permanentLocationId", "locations");
    matchingRelations.put("item.temporaryLocationId", "locations");
    matchingRelations.put("holdingsrecord.temporaryLocationId", "locations");
    matchingRelations.put("holdingsrecord.permanentLocationId", "locations");
    matchingRelations.put("item.materialTypeId", "materialTypes");
    matchingRelations.put("item.itemDamagedStatusId", "itemDamageStatuses");
    matchingRelations.put("item.permanentLoanTypeId", "loantypes");
    matchingRelations.put("item.temporaryLoanTypeId", "loantypes");
    matchingRelations.put("item.notes[].itemNoteTypeId", "itemNoteTypes");
  }

  public Map<String, String> getMatchingRelations() {
    return matchingRelations;
  }

  public void setMatchingRelations(Map<String, String> matchingRelations) {
    this.matchingRelations = matchingRelations;
  }
}
