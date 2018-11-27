package org.folio.inventory.domain.instances;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;
import org.folio.inventory.domain.Metadata;

public class Instance {
  // JSON property names
  public static final String HRID_KEY = "hrid";
  public static final String SOURCE_KEY = "source";
  public static final String PARENT_INSTANCES_KEY = "parentInstances";
  public static final String CHILD_INSTANCES_KEY = "childInstances";
  public static final String TITLE_KEY = "title";
  public static final String INDEX_TITLE_KEY = "indexTitle";
  public static final String ALTERNATIVE_TITLES_KEY = "alternativeTitles";
  public static final String EDITIONS_KEY = "editions";
  public static final String SERIES_KEY = "series";
  public static final String IDENTIFIERS_KEY = "identifiers";
  public static final String CONTRIBUTORS_KEY = "contributors";
  public static final String SUBJECTS_KEY = "subjects";
  public static final String CLASSIFICATIONS_KEY = "classifications";
  public static final String PUBLICATION_KEY = "publication";
  public static final String PUBLICATION_FREQUENCY_KEY = "publicationFrequency";
  public static final String PUBLICATION_RANGE_KEY = "publicationRange";
  public static final String ELECTRONIC_ACCESS_KEY = "electronicAccess";
  public static final String INSTANCE_TYPE_ID_KEY = "instanceTypeId";
  public static final String INSTANCE_FORMAT_IDS_KEY = "instanceFormatIds";
  public static final String PHYSICAL_DESCRIPTIONS_KEY = "physicalDescriptions";
  public static final String LANGUAGES_KEY = "languages";
  public static final String NOTES_KEY = "notes";
  public static final String MODE_OF_ISSUANCE_ID_KEY = "modeOfIssuanceId";
  public static final String CATALOGED_DATE_KEY = "catalogedDate";
  public static final String PREVIOUSLY_HELD_KEY = "previouslyHeld";
  public static final String STAFF_SUPPRESS_KEY = "staffSuppress";
  public static final String DISCOVERY_SUPPRESS_KEY = "discoverySuppress";
  public static final String STATISTICAL_CODES_KEY = "statisticalCodes";
  public static final String SOURCE_RECORD_FORMAT_KEY = "sourceRecordFormat";
  public static final String STATUS_ID_KEY = "statusId";
  public static final String STATUS_UPDATED_DATE_KEY = "statusUpdatedDate";
  public static final String METADATA_KEY = "metadata";

  private final String id;
  private final String hrid;
  private final String source;
  private List<InstanceRelationshipToParent> parentInstances = new ArrayList();
  private List<InstanceRelationshipToChild> childInstances = new ArrayList();
  private final String title;
  private String indexTitle;
  private List<AlternativeTitle> alternativeTitles = new ArrayList();
  private List<String> editions = new ArrayList();
  private List<String> series = new ArrayList();
  private List<Identifier> identifiers = new ArrayList();
  private List<Contributor> contributors = new ArrayList();
  private List<String> subjects = new ArrayList();
  private List<Classification> classifications = new ArrayList();
  private List<Publication> publication = new ArrayList();
  private List<String> publicationFrequency = new ArrayList();
  private List<String> publicationRange = new ArrayList();
  private List<ElectronicAccess> electronicAccess = new ArrayList();
  private final String instanceTypeId;
  private List<String> instanceFormatIds;
  private List<String> physicalDescriptions = new ArrayList();
  private List<String> languages = new ArrayList();
  private List<String> notes = new ArrayList();
  private String modeOfIssuanceId;
  private String catalogedDate;
  private Boolean previouslyHeld;
  private Boolean staffSuppress;
  private Boolean discoverySuppress;
  private List<StatisticalCode> statisticalCodes = new ArrayList();
  private String sourceRecordFormat;
  private String statusId;
  private String statusUpdatedDate;
  private Metadata metadata = null;

  public Instance(
    String id,
    String hrid,
    String source,
    String title,
    String instanceTypeId) {

    this.id = id;
    this.hrid = hrid;
    this.source = source;
    this.title = title;
    this.instanceTypeId = instanceTypeId;
  }

  public Instance setIndexTitle(String indexTitle) {
    this.indexTitle = indexTitle;
    return this;
  }

  public Instance setParentInstances(List<InstanceRelationshipToParent> parentInstances) {
    this.parentInstances = parentInstances;
    return this;
  }

  public Instance setChildInstances(List<InstanceRelationshipToChild> childInstances) {
    this.childInstances = childInstances;
    return this;
  }

  public Instance setAlternativeTitles(List<AlternativeTitle> alternativeTitles) {
    this.alternativeTitles = alternativeTitles;
    return this;
  }

  public Instance setEditions(List<String> editions) {
    this.editions = editions;
    return this;
  }

  public Instance setSeries(List<String> series) {
    this.series = series;
    return this;
  }

  public Instance setIdentifiers(List<Identifier> identifiers) {
    this.identifiers = identifiers;
    return this;
  }

  public Instance setContributors(List<Contributor> contributors) {
    this.contributors = contributors;
    return this;
  }

  public Instance setSubjects(List<String> subjects) {
    this.subjects = subjects;
    return this;
  }

  public Instance setClassifications(List<Classification> classifications) {
    this.classifications = classifications;
    return this;
  }

  public Instance setPublication(List<Publication> publication) {
    this.publication = publication;
    return this;
  }

  public Instance setPublicationFrequency (List<String> publicationFrequency) {
    this.publicationFrequency = publicationFrequency;
    return this;
  }

  public Instance setPublicationRange (List<String> publicationRange) {
    this.publicationRange = publicationRange;
    return this;
  }

  public Instance setElectronicAccess(List<ElectronicAccess> electronicAccess) {
    this.electronicAccess = electronicAccess;
    return this;
  }

  public Instance setInstanceFormatIds(List<String> instanceFormatIds) {
    this.instanceFormatIds = instanceFormatIds;
    return this;
  }

  public Instance setPhysicalDescriptions(List<String> physicalDescriptions) {
    this.physicalDescriptions = physicalDescriptions;
    return this;
  }

  public Instance setLanguages(List<String> languages) {
    this.languages = languages;
    return this;
  }

  public Instance setNotes(List<String> notes) {
    this.notes = notes;
    return this;
  }

  public Instance setModeOfIssuanceId(String modeOfIssuanceId) {
    this.modeOfIssuanceId = modeOfIssuanceId;
    return this;
  }

  public Instance setCatalogedDate(String catalogedDate) {
    this.catalogedDate = catalogedDate;
    return this;
  }

  public Instance setPreviouslyHeld(Boolean previouslyHeld) {
    this.previouslyHeld = previouslyHeld;
    return this;
  }

  public Instance setStaffSuppress(Boolean staffSuppress) {
    this.staffSuppress = staffSuppress;
    return this;
  }

  public Instance setDiscoverySuppress(Boolean discoverySuppress) {
    this.discoverySuppress = discoverySuppress;
    return this;
  }

  public Instance setStatisticalCodes(List<StatisticalCode> statisticalCodes) {
    this.statisticalCodes = statisticalCodes;
    return this;
  }

  public Instance setSourceRecordFormat(String sourceRecordFormat) {
    this.sourceRecordFormat = sourceRecordFormat;
    return this;
  }

  public Instance setStatusId(String statusId) {
    this.statusId = statusId;
    return this;
  }

  public Instance setStatusUpdatedDate(String statusUpdatedDate) {
    this.statusUpdatedDate = statusUpdatedDate;
    return this;
  }

  public Instance setMetadata (Metadata metadata) {
    this.metadata = metadata;
    return this;
  }

  public boolean hasMetadata () {
    return this.metadata != null;
  }

    public String getId() {
    return id;
  }

  public String getHrid() {
    return hrid;
  }

  public String getSource() {
    return source;
  }

  public List<InstanceRelationshipToParent> getParentInstances() {
    return parentInstances;
  }

  public List<InstanceRelationshipToChild> getChildInstances() {
    return childInstances;
  }

  public String getTitle() {
    return title;
  }

  public String getIndexTitle() {
    return indexTitle;
  }

  public List<AlternativeTitle> getAlternativeTitles() {
    return alternativeTitles;
  }

  public List<String> getEditions() {
    return editions;
  }

  public List<String> getSeries() {
    return series;
  }

  public List<Identifier> getIdentifiers() {
    return identifiers;
  }

  public List<Contributor> getContributors() {
    return contributors;
  }

  public List<String> getSubjects() {
    return subjects;
  }

  public List<Classification> getClassifications() {
    return classifications;
  }

  public List<Publication> getPublication() {
    return publication;
  }

  public List<String> getPublicationFrequency() {
    return publicationFrequency;
  }

  public List<String> getPublicationRange() {
    return publicationRange;
  }

  public List<ElectronicAccess> getElectronicAccess() {
    return electronicAccess;
  }

  public String getInstanceTypeId() {
    return instanceTypeId;
  }

  public List<String> getInstanceFormatIds() {
    return instanceFormatIds;
  }

  public List<String> getPhysicalDescriptions() {
    return physicalDescriptions;
  }

  public List<String> getLanguages() {
    return languages;
  }

  public List<String> getNotes() {
    return notes;
  }


  public String getModeOfIssuanceId() {
    return modeOfIssuanceId;
  }

  public String getCatalogedDate() {
    return catalogedDate;
  }

  public Boolean getPreviouslyHeld() {
    return previouslyHeld;
  }

  public Boolean getStaffSuppress() {
    return staffSuppress;
  }

  public Boolean getDiscoverySuppress() {
    return discoverySuppress;
  }

  public List<StatisticalCode> getStatisticalCodes() {
    return statisticalCodes;
  }

  public String getSourceRecordFormat() {
    return sourceRecordFormat;
  }

  public String getStatusId() {
    return statusId;
  }

  public String getStatusUpdatedDate() {
    return statusUpdatedDate;
  }

  public Metadata getMetadata() {
    return metadata;
  }

  public Instance copyWithNewId(String newId) {
    return new Instance(newId, this.hrid, this.source, this.title, this.instanceTypeId)
            .setIndexTitle(indexTitle)
            .setAlternativeTitles(alternativeTitles)
            .setEditions(editions)
            .setSeries(series)
            .setIdentifiers(identifiers)
            .setContributors(contributors)
            .setSubjects(subjects)
            .setClassifications(classifications)
            .setPublication(publication)
            .setPublicationFrequency(publicationFrequency)
            .setPublicationRange(publicationRange)
            .setElectronicAccess(electronicAccess)
            .setInstanceFormatIds(instanceFormatIds)
            .setPhysicalDescriptions(physicalDescriptions)
            .setLanguages(languages)
            .setNotes(notes)
            .setModeOfIssuanceId(modeOfIssuanceId)
            .setCatalogedDate(catalogedDate)
            .setPreviouslyHeld(previouslyHeld)
            .setStaffSuppress(staffSuppress)
            .setDiscoverySuppress(discoverySuppress)
            .setStatisticalCodes(statisticalCodes)
            .setSourceRecordFormat(sourceRecordFormat)
            .setStatusId(statusId)
            .setStatusUpdatedDate(statusUpdatedDate);
  }

  public Instance copyInstance() {
    return new Instance(this.id, this.hrid, this.source, this.title, this.instanceTypeId)
            .setIndexTitle(indexTitle)
            .setAlternativeTitles(alternativeTitles)
            .setEditions(editions)
            .setSeries(series)
            .setIdentifiers(identifiers)
            .setContributors(contributors)
            .setSubjects(subjects)
            .setClassifications(classifications)
            .setPublication(publication)
            .setPublicationFrequency(publicationFrequency)
            .setPublicationRange(publicationRange)
            .setElectronicAccess(electronicAccess)
            .setInstanceFormatIds(instanceFormatIds)
            .setPhysicalDescriptions(physicalDescriptions)
            .setLanguages(languages)
            .setNotes(notes)
            .setModeOfIssuanceId(modeOfIssuanceId)
            .setCatalogedDate(catalogedDate)
            .setPreviouslyHeld(previouslyHeld)
            .setStaffSuppress(staffSuppress)
            .setDiscoverySuppress(discoverySuppress)
            .setStatisticalCodes(statisticalCodes)
            .setSourceRecordFormat(sourceRecordFormat)
            .setStatusId(statusId)
            .setStatusUpdatedDate(statusUpdatedDate)
            .setMetadata(metadata);
  }

  public Instance addIdentifier(Identifier identifier) {
    List<Identifier> newIdentifiers = new ArrayList<>(this.identifiers);

    newIdentifiers.add(identifier);

    return copyInstance().setIdentifiers(newIdentifiers);
  }

  public Instance addIdentifier(String identifierTypeId, String value) {
    Identifier identifier = new Identifier(identifierTypeId, value);

    return addIdentifier(identifier);
  }

  public Instance addContributor(String contributorNameTypeId, String name, String contributorTypeId, String contributorTypeText) {
    List<Contributor> newContributors = new ArrayList<>(this.contributors);

    newContributors.add(new Contributor(contributorNameTypeId, name, contributorTypeId, contributorTypeText));

    return copyInstance().setContributors(newContributors);
  }

  public Instance removeIdentifier(final String identifierTypeId, final String value) {
    List<Identifier> newIdentifiers = this.identifiers.stream()
      .filter(it -> !(StringUtils.equals(it.identifierTypeId, identifierTypeId)
        && StringUtils.equals(it.value, value)))
      .collect(Collectors.toList());

    return copyInstance().setIdentifiers(newIdentifiers);
  }

  @Override
  public String toString() {
    return String.format("Instance ID: %s, HRID: %s, Title: %s", id, hrid, title);
  }
}
