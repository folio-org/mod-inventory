package org.folio.inventory.support;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import org.folio.inventory.domain.instances.AlternativeTitle;
import org.folio.inventory.domain.instances.Classification;
import org.folio.inventory.domain.instances.Contributor;
import org.folio.inventory.domain.instances.Identifier;
import org.folio.inventory.domain.instances.Instance;
import org.folio.inventory.domain.instances.InstanceRelationshipToChild;
import org.folio.inventory.domain.instances.InstanceRelationshipToParent;
import org.folio.inventory.domain.instances.Note;
import org.folio.inventory.domain.instances.Publication;
import org.folio.inventory.domain.instances.titles.PrecedingSucceedingTitle;
import org.folio.inventory.domain.sharedproperties.ElectronicAccess;

import io.vertx.core.json.JsonObject;

public class InstanceUtil {

  private InstanceUtil() {

  }

  public static Instance jsonToInstance(JsonObject instanceRequest) {
    List<InstanceRelationshipToParent> parentInstances = instanceRequest.containsKey(Instance.PARENT_INSTANCES_KEY)
      ? JsonArrayHelper.toList(instanceRequest.getJsonArray(Instance.PARENT_INSTANCES_KEY)).stream()
      .map(InstanceRelationshipToParent::new)
      .collect(Collectors.toList())
      : new ArrayList<>();

    List<InstanceRelationshipToChild> childInstances = instanceRequest.containsKey(Instance.CHILD_INSTANCES_KEY)
      ? JsonArrayHelper.toList(instanceRequest.getJsonArray(Instance.CHILD_INSTANCES_KEY)).stream()
      .map(InstanceRelationshipToChild::new)
      .collect(Collectors.toList())
      : new ArrayList<>();

    List<PrecedingSucceedingTitle> precedingTitles = instanceRequest.containsKey(Instance.PRECEDING_TITLES_KEY)
      ? JsonArrayHelper.toList(instanceRequest.getJsonArray(Instance.PRECEDING_TITLES_KEY)).stream()
      .map(PrecedingSucceedingTitle::from)
      .collect(Collectors.toList())
      : new ArrayList<>();

    List<PrecedingSucceedingTitle> succeedingTitles = instanceRequest.containsKey(Instance.SUCCEEDING_TITLES_KEY)
      ? JsonArrayHelper.toList(instanceRequest.getJsonArray(Instance.SUCCEEDING_TITLES_KEY)).stream()
      .map(PrecedingSucceedingTitle::from)
      .collect(Collectors.toList())
      : new ArrayList<>();

    List<Identifier> identifiers = instanceRequest.containsKey(Instance.IDENTIFIERS_KEY)
      ? JsonArrayHelper.toList(instanceRequest.getJsonArray(Instance.IDENTIFIERS_KEY)).stream()
      .map(Identifier::new)
      .collect(Collectors.toList())
      : new ArrayList<>();

    List<AlternativeTitle> alternativeTitles = instanceRequest.containsKey(Instance.ALTERNATIVE_TITLES_KEY)
      ? JsonArrayHelper.toList(instanceRequest.getJsonArray(Instance.ALTERNATIVE_TITLES_KEY)).stream()
      .map(AlternativeTitle::new)
      .collect(Collectors.toList())
      : new ArrayList<>();

    List<Contributor> contributors = instanceRequest.containsKey(Instance.CONTRIBUTORS_KEY)
      ? JsonArrayHelper.toList(instanceRequest.getJsonArray(Instance.CONTRIBUTORS_KEY)).stream()
      .map(Contributor::new)
      .collect(Collectors.toList())
      : new ArrayList<>();

    List<Classification> classifications = instanceRequest.containsKey(Instance.CLASSIFICATIONS_KEY)
      ? JsonArrayHelper.toList(instanceRequest.getJsonArray(Instance.CLASSIFICATIONS_KEY)).stream()
      .map(Classification::new)
      .collect(Collectors.toList())
      : new ArrayList<>();

    List<Publication> publications = instanceRequest.containsKey(Instance.PUBLICATION_KEY)
      ? JsonArrayHelper.toList(instanceRequest.getJsonArray(Instance.PUBLICATION_KEY)).stream()
      .map(Publication::new)
      .collect(Collectors.toList())
      : new ArrayList<>();

    List<ElectronicAccess> electronicAccess = instanceRequest.containsKey(Instance.ELECTRONIC_ACCESS_KEY)
      ? JsonArrayHelper.toList(instanceRequest.getJsonArray(Instance.ELECTRONIC_ACCESS_KEY)).stream()
      .map(ElectronicAccess::new)
      .collect(Collectors.toList())
      : new ArrayList<>();

    List<Note> notes = instanceRequest.containsKey(Instance.NOTES_KEY)
      ? JsonArrayHelper.toList(instanceRequest.getJsonArray(Instance.NOTES_KEY)).stream()
      .map(Note::new)
      .collect(Collectors.toList())
      : new ArrayList<>();

    List<String> tags = instanceRequest.containsKey(Instance.TAGS_KEY)
      ? getTags(instanceRequest) : new ArrayList<>();

    return new Instance(
      instanceRequest.getString("id"),
      instanceRequest.getString("hrid"),
      instanceRequest.getString(Instance.SOURCE_KEY),
      instanceRequest.getString(Instance.TITLE_KEY),
      instanceRequest.getString(Instance.INSTANCE_TYPE_ID_KEY))
      .setIndexTitle(instanceRequest.getString(Instance.INDEX_TITLE_KEY))
      .setParentInstances(parentInstances)
      .setChildInstances(childInstances)
      .setPrecedingTitles(precedingTitles)
      .setSucceedingTitles(succeedingTitles)
      .setAlternativeTitles(alternativeTitles)
      .setEditions(toListOfStrings(instanceRequest, Instance.EDITIONS_KEY))
      .setSeries(toListOfStrings(instanceRequest, Instance.SERIES_KEY))
      .setIdentifiers(identifiers)
      .setContributors(contributors)
      .setSubjects(toListOfStrings(instanceRequest, Instance.SUBJECTS_KEY))
      .setClassifications(classifications)
      .setPublication(publications)
      .setPublicationFrequency(toListOfStrings(instanceRequest, Instance.PUBLICATION_FREQUENCY_KEY))
      .setPublicationRange(toListOfStrings(instanceRequest, Instance.PUBLICATION_RANGE_KEY))
      .setElectronicAccess(electronicAccess)
      .setInstanceFormatIds(toListOfStrings(instanceRequest, Instance.INSTANCE_FORMAT_IDS_KEY))
      .setPhysicalDescriptions(toListOfStrings(instanceRequest, Instance.PHYSICAL_DESCRIPTIONS_KEY))
      .setLanguages(toListOfStrings(instanceRequest, Instance.LANGUAGES_KEY))
      .setNotes(notes)
      .setModeOfIssuanceId(instanceRequest.getString(Instance.MODE_OF_ISSUANCE_ID_KEY))
      .setCatalogedDate(instanceRequest.getString(Instance.CATALOGED_DATE_KEY))
      .setPreviouslyHeld(instanceRequest.getBoolean(Instance.PREVIOUSLY_HELD_KEY))
      .setStaffSuppress(instanceRequest.getBoolean(Instance.STAFF_SUPPRESS_KEY))
      .setDiscoverySuppress(instanceRequest.getBoolean(Instance.DISCOVERY_SUPPRESS_KEY))
      .setStatisticalCodeIds(toListOfStrings(instanceRequest, Instance.STATISTICAL_CODE_IDS_KEY))
      .setSourceRecordFormat(instanceRequest.getString(Instance.SOURCE_RECORD_FORMAT_KEY))
      .setStatusId(instanceRequest.getString(Instance.STATUS_ID_KEY))
      .setStatusUpdatedDate(instanceRequest.getString(Instance.STATUS_UPDATED_DATE_KEY))
      .setTags(tags)
      .setNatureOfContentIds(toListOfStrings(instanceRequest, Instance.NATURE_OF_CONTENT_TERM_IDS_KEY));
  }

  private static List<String> getTags(JsonObject instanceRequest) {

    final JsonObject tags = instanceRequest.getJsonObject(Instance.TAGS_KEY);
    return tags.containsKey(Instance.TAG_LIST_KEY) ?
      JsonArrayHelper.toListOfStrings(tags.getJsonArray(Instance.TAG_LIST_KEY)) : new ArrayList<>();
  }

  private static List<String> toListOfStrings(JsonObject source, String propertyName) {
    return source.containsKey(propertyName)
      ? JsonArrayHelper.toListOfStrings(source.getJsonArray(propertyName))
      : new ArrayList<>();
  }
}
