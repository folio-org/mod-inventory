package org.folio.inventory.domain

import java.util.stream.Collectors

class Instance {
  final String id
  final String title
  final String source
  final String instanceTypeId
  final List<Map> identifiers
  final List<Creator> creators

  def Instance(
    String id,
    String title,
    List<Map> identifiers,
    String source,
    String instanceTypeId,
    List<Creator> creators) {

    this.id = id
    this.title = title
    this.identifiers = identifiers.collect()
    this.source = source
    this.instanceTypeId = instanceTypeId
    this.creators = creators.collect()
  }

  def Instance copyWithNewId(String newId) {
    new Instance(newId, this.title, this.identifiers, this.source,
      this.instanceTypeId, new ArrayList<Creator>())
  }

  Instance addIdentifier(String identifierTypeId, String value) {
    def identifier = ['identifierTypeId' : identifierTypeId, 'value' : value]

    new Instance(id, title, this.identifiers.collect() << identifier,
      this.source, this.instanceTypeId, this.creators)
  }

  Instance addCreator(String creatorTypeId, String name) {
    new Instance(id, title, this.identifiers.collect(),
      this.source, this.instanceTypeId,
      this.creators.collect() << new Creator(creatorTypeId, name))
  }

  Instance removeIdentifier(String identifierTypeId, String value) {
    def newIdentifiers = this.identifiers.stream()
      .filter({ !(it.identifierTypeId == identifierTypeId && it.value == value) })
      .collect(Collectors.toList())

    new Instance(id, title, newIdentifiers, this.source, this.instanceTypeId,
      this.creators)
  }

  @Override
  public String toString() {
    return "Instance ID: ${id}, Title: ${title}";
  }
}
