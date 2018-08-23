package org.folio.inventory.domain.instances;

import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import org.folio.inventory.domain.Metadata;

public class Instance {
  // JSON property names
  public static final String SOURCE_KEY = "source";
  public static final String TITLE_KEY = "title";
  public static final String ALTERNATIVE_TITLES_KEY = "alternativeTitles";
  public static final String EDITION_KEY = "edition";
  public static final String INDEX_TITLE_KEY = "indexTitle";
  public static final String SERIES_KEY = "series";
  public static final String IDENTIFIERS_KEY = "identifiers";
  public static final String CONTRIBUTORS_KEY = "contributors";
  public static final String SUBJECTS_KEY = "subjects";
  public static final String CLASSIFICATIONS_KEY = "classifications";
  public static final String PUBLICATION_KEY = "publication";
  public static final String PRECEDING_TITLE_KEY = "precedingTitle";
  public static final String URLS_KEY = "urls";
  public static final String INSTANCE_TYPE_ID_KEY = "instanceTypeId";
  public static final String INSTANCE_FORMAT_ID_KEY = "instanceFormatId";
  public static final String PHYSICAL_DESCRIPTIONS_KEY = "physicalDescriptions";
  public static final String LANGUAGES_KEY = "languages";
  public static final String NOTES_KEY = "notes";
  public static final String SOURCE_RECORD_FORMAT_KEY = "sourceRecordFormat";
  public static final String METADATA_KEY = "metadata";

  private final String id;
  private final String source;
  private final String title;
  private List<String> alternativeTitles = new ArrayList();
  private String edition;
  private String indexTitle;
  private List<String> series = new ArrayList();
  private List<Identifier> identifiers = new ArrayList();
  private List<Contributor> contributors = new ArrayList();
  private List<String> subjects = new ArrayList();
  private List<Classification> classifications = new ArrayList();
  private List<Publication> publication = new ArrayList();
  private List<PrecedingTitle> precedingTitle = new ArrayList();
  private List<String> urls = new ArrayList();
  private final String instanceTypeId;
  private String instanceFormatId;
  private List<String> physicalDescriptions = new ArrayList();
  private List<String> languages = new ArrayList();
  private List<String> notes = new ArrayList();
  private String sourceRecordFormat;
  private Metadata metadata = null;



  public Instance(
    String id,
    String source,
    String title,
    String instanceTypeId) {

    this.id = id;
    this.source = source;
    this.title = title;
    this.instanceTypeId = instanceTypeId;
  }

  public Instance setAlternativeTitles(List<String> alternativeTitles) {
    this.alternativeTitles = alternativeTitles;
    return this;
  }

  public Instance setEdition(String edition) {
    this.edition = edition;
    return this;
  }

  public Instance setIndexTitle(String indexTitle) {
    this.indexTitle = indexTitle;
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
  
  public Instance setPrecedingTitle(List<PrecedingTitle> precedingTitle) {
    this.precedingTitle = precedingTitle;
    return this;
  }

  public Instance setUrls(List<String> urls) {
    this.urls = urls;
    return this;
  }

  public Instance setInstanceFormatId(String instanceFormatId) {
    this.instanceFormatId = instanceFormatId;
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

  public Instance setSourceRecordFormat(String sourceRecordFormat) {
    this.sourceRecordFormat = sourceRecordFormat;
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

  public String getSource() {
    return source;
  }

  public String getTitle() {
    return title;
  }

  public List<String> getAlternativeTitles() {
    return alternativeTitles;
  }

  public String getEdition() {
    return edition;
  }
  
  public String getIndexTitle() {
    return indexTitle;
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
  
  public List<PrecedingTitle> getPrecedingTitle() {
    return precedingTitle;
  }

  public List<String> getUrls() {
    return urls;
  }

  public String getInstanceTypeId() {
    return instanceTypeId;
  }

  public String getInstanceFormatId() {
    return instanceFormatId;
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

  public String getSourceRecordFormat() {
    return sourceRecordFormat;
  }

  public Metadata getMetadata() {
    return metadata;
  }
  
  public Instance copyWithNewId(String newId) {
    return new Instance(newId, this.source, this.title, this.instanceTypeId)
            .setAlternativeTitles(alternativeTitles)
            .setEdition(edition)
            .setIndexTitle(indexTitle)
            .setSeries(series)
            .setIdentifiers(identifiers)
            .setContributors(contributors)
            .setSubjects(subjects)
            .setClassifications(classifications)
            .setPublication(publication)
            .setPrecedingTitle(precedingTitle)
            .setUrls(urls)
            .setInstanceFormatId(instanceFormatId)
            .setPhysicalDescriptions(physicalDescriptions)
            .setLanguages(languages)
            .setNotes(notes)
            .setSourceRecordFormat(sourceRecordFormat);
  }

  public Instance copyInstance() {
    return new Instance(this.id, this.source, this.title, this.instanceTypeId)
            .setAlternativeTitles(alternativeTitles)
            .setEdition(edition)
            .setIndexTitle(indexTitle)
            .setSeries(series)
            .setIdentifiers(identifiers)
            .setContributors(contributors)
            .setSubjects(subjects)
            .setClassifications(classifications)
            .setPublication(publication)
            .setPrecedingTitle(precedingTitle)
            .setUrls(urls)
            .setInstanceFormatId(instanceFormatId)
            .setPhysicalDescriptions(physicalDescriptions)
            .setLanguages(languages)
            .setNotes(notes)
            .setSourceRecordFormat(sourceRecordFormat)
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
    return String.format("Instance ID: %s, Title: %s", id, title);
  }
}
