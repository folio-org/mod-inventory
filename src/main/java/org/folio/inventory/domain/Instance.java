package org.folio.inventory.domain;

import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class Instance {
  public final String id;
  public final String source;
  public final String title;
  public List<String> alternativeTitles;
  public String edition;
  public List<String> series;
  public List<Identifier> identifiers;
  public List<Contributor> contributors;
  public List<String> subjects;
  public List<Classification> classifications;
  public List<Publication> publications;
  public List<String> urls;
  public final String instanceTypeId;
  public String instanceFormatId;
  public List<String> physicalDescriptions;
  public List<String> languages;
  public List<String> notes;
  public String sourceRecordFormat;


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

  public Instance setPublication(List<Publication> publications) {
    this.publications = publications;
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

  public Instance copyWithNewId(String newId) {
    return new Instance(newId, this.source, this.title, this.instanceTypeId)
            .setAlternativeTitles(alternativeTitles)
            .setEdition(edition)
            .setSeries(series)
            .setIdentifiers(identifiers)
            .setContributors(contributors)
            .setSubjects(subjects)
            .setClassifications(classifications)
            .setPublication(publications)
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
            .setPublication(publications)
            .setUrls(urls)
            .setInstanceFormatId(instanceFormatId)
            .setPhysicalDescriptions(physicalDescriptions)
            .setLanguages(languages)
            .setNotes(notes)
            .setSourceRecordFormat(sourceRecordFormat);
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
