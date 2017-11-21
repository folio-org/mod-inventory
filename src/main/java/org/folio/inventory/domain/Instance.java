package org.folio.inventory.domain;

import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class Instance {
  public final String id;
  public final String title;
  public final String source;
  public final String instanceTypeId;
  public final List<Identifier> identifiers;
  public final List<Creator> creators;

  public Instance(
    String id,
    String title,
    List<Identifier> identifiers,
    String source,
    String instanceTypeId,
    List<Creator> creators) {

    this.id = id;
    this.title = title;
    this.identifiers = new ArrayList<>(identifiers);
    this.source = source;
    this.instanceTypeId = instanceTypeId;
    this.creators = new ArrayList<>(creators);
  }

  public Instance copyWithNewId(String newId) {
    return new Instance(newId, this.title, this.identifiers, this.source,
      this.instanceTypeId, this.creators);
  }

  public Instance addIdentifier(Identifier identifier) {
    List<Identifier> newIdentifiers = new ArrayList<>(this.identifiers);

    newIdentifiers.add(identifier);

    return new Instance(this.id, this.title, newIdentifiers, this.source,
      this.instanceTypeId, this.creators);
  }

  public Instance addIdentifier(String identifierTypeId, String value) {
    Identifier identifier = new Identifier(identifierTypeId, value);

    return addIdentifier(identifier);
  }

  public Instance addCreator(String creatorTypeId, String name) {
    List<Creator> newCreators = new ArrayList<>(this.creators);

    newCreators.add(new Creator(creatorTypeId, name));

    return new Instance(this.id, this.title, new ArrayList<>(this.identifiers),
      this.source, this.instanceTypeId, newCreators);
  }

  public Instance removeIdentifier(final String identifierTypeId, final String value) {
    List<Identifier> newIdentifiers = this.identifiers.stream()
      .filter(it -> !(StringUtils.equals(it.identifierTypeId, identifierTypeId)
        && StringUtils.equals(it.value, value)))
      .collect(Collectors.toList());

    return new Instance(this.id, this.title, newIdentifiers, this.source,
      this.instanceTypeId, this.creators);
  }

  @Override
  public String toString() {
    return String.format("Instance ID: %s, Title: %s", id, title);
  }
}
