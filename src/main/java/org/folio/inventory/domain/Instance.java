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
  public final List<Identifier> identifiers;
  public final List<Contributor> contributors;
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
    List<String> alternativeTitles,
    List<Identifier> identifiers,
    String instanceTypeId,
    List<Contributor> contributors) {

    this.id = id;
    this.source = source;
    this.title = title;
    this.alternativeTitles = new ArrayList<>(alternativeTitles);
    this.identifiers = new ArrayList<>(identifiers);
    this.instanceTypeId = instanceTypeId;
    this.contributors = new ArrayList<>(contributors);
  }

  public Instance copyWithNewId(String newId) {
    return new Instance(newId, this.source, this.title, this.alternativeTitles, this.identifiers,
      this.instanceTypeId, this.contributors);
  }

  public Instance addIdentifier(Identifier identifier) {
    List<Identifier> newIdentifiers = new ArrayList<>(this.identifiers);

    newIdentifiers.add(identifier);

    return new Instance(this.id, this.source, this.title, this.alternativeTitles, newIdentifiers,
      this.instanceTypeId, this.contributors);
  }

  public Instance addIdentifier(String identifierTypeId, String value) {
    Identifier identifier = new Identifier(identifierTypeId, value);

    return addIdentifier(identifier);
  }

  public Instance addContributor(String contributorNameTypeId, String name, String contributorTypeId, String contributorTypeText) {
    List<Contributor> newContributors = new ArrayList<>(this.contributors);

    newContributors.add(new Contributor(contributorNameTypeId, name, contributorTypeId, contributorTypeText));

    return new Instance(this.id, this.source, this.title, this.alternativeTitles, new ArrayList<>(this.identifiers),
       this.instanceTypeId, newContributors);
  }

  public Instance removeIdentifier(final String identifierTypeId, final String value) {
    List<Identifier> newIdentifiers = this.identifiers.stream()
      .filter(it -> !(StringUtils.equals(it.identifierTypeId, identifierTypeId)
        && StringUtils.equals(it.value, value)))
      .collect(Collectors.toList());

    return new Instance(this.id, this.source, this.title, this.alternativeTitles, newIdentifiers,
      this.instanceTypeId, this.contributors);
  }

  @Override
  public String toString() {
    return String.format("Instance ID: %s, Title: %s", id, title);
  }
}
