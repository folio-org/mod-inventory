package org.folio.inventory.parsing

import org.apache.commons.lang3.StringUtils

import java.util.stream.Collectors

class ModsParser {

  private final CharacterEncoding characterEncoding

  ModsParser(CharacterEncoding characterEncoding) {
    this.characterEncoding = characterEncoding
  }

  public List<Map<String, Object>> parseRecords(String xml) {

    def modsRecord = new XmlSlurper()
      .parseText(xml)

    def records = []

    modsRecord.mods.each {
      def title = characterEncoding.decode(it.titleInfo.title.text())
      def barcode = it.location.holdingExternal.localHolds.objId.toString()

      def identifiers = it.identifier.list().collect( {
        [ "type" : it.@type, "value" : it.text() ]
      })

      def recordIdentifiers = it.recordInfo.recordIdentifier.list().collect( {
        [ "type" : it.@source, "value" : it.text() ]
      })

      def creators = it.name.namePart.list().stream()
        .filter({ namePart -> StringUtils.isBlank(namePart.@type.toString()) })
        .map({
          ["name" : characterEncoding.decode(it.text())]
        })
        .collect(Collectors.toList())

      def record = [:]
      record.put("title", title)
      record.put("barcode", barcode)
      record.put("identifiers", recordIdentifiers + identifiers)
      record.put("creators", creators)

      records.add(record)
    }

    records
  }
}
