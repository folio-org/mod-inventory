package org.folio.inventory.domain;

import io.vertx.core.json.JsonArray;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import org.folio.inventory.domain.Metadata;

public class Instance {
  // JSON property names
  public static final String SOURCE = "source";
  public static final String TITLE = "title";
  public static final String ALTERNATIVE_TITLES = "alternativeTitles";
  public static final String EDITION = "edition";
  public static final String SERIES = "series";
  public static final String IDENTIFIERS = "identifiers";
  public static final String CONTRIBUTORS = "contributors";
  public static final String SUBJECTS = "subjects";
  public static final String CLASSIFICATIONS = "classifications";
  public static final String PUBLICATION = "publication";
  public static final String URLS = "urls";
  public static final String INSTANCE_TYPE_ID = "instanceTypeId";
  public static final String INSTANCE_FORMAT_ID = "instanceFormatId";
  public static final String PHYSICAL_DESCRIPTIONS = "physicalDescriptions";
  public static final String LANGUAGES = "languages";
  public static final String NOTES = "notes";
  public static final String SOURCE_RECORD_FORMAT = "sourceRecordFormat";
  public static final String METADATA = "metadata";

  public final String id;
  public final String source;
  public final String title;
  public List<String> alternativeTitles = new ArrayList();
  public String edition;
  public List<String> series = new ArrayList();
  public List<Identifier> identifiers = new ArrayList();
  public List<Contributor> contributors = new ArrayList();
  public List<String> subjects = new ArrayList();
  public List<Classification> classifications = new ArrayList();
  public List<Publication> publication = new ArrayList();
  public List<String> urls = new ArrayList();
  public final String instanceTypeId;
  public String instanceFormatId;
  public List<String> physicalDescriptions = new ArrayList();
  public List<String> languages = new ArrayList();
  public List<String> notes = new ArrayList();
  public String sourceRecordFormat;
  public Metadata metadata = null;


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

  public Instance copyWithNewId(String newId) {
    return new Instance(newId, this.source, this.title, this.instanceTypeId)
            .setAlternativeTitles(alternativeTitles)
            .setEdition(edition)
            .setSeries(series)
            .setIdentifiers(identifiers)
            .setContributors(contributors)
            .setSubjects(subjects)
            .setClassifications(classifications)
            .setPublication(publication)
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
            .setSeries(series)
            .setIdentifiers(identifiers)
            .setContributors(contributors)
            .setSubjects(subjects)
            .setClassifications(classifications)
            .setPublication(publication)
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
