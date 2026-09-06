package org.folio.inventory.dataimport.util;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Reorders MARC-in-JSON field order at the Jackson {@link JsonNode} level - the field order captured in incoming
 * record content is information that marc4j's object model discards (control fields and data fields go into two
 * separate lists internally), so this operation cannot be expressed as a marc4j-{@code Record}-in/out method the
 * way {@link MarcFieldEditor} and {@link MarcContentCodec} are. No dependency on any FOLIO {@code Record} type
 * or cache.
 *
 * <p>{@link #reorderFields(String, String)} throws a {@link MarcContentException} on any failure rather than
 * catching it internally - the previous behaviour of silently falling back to the un-reordered system field
 * order now lives at the caller ({@code AdditionalFieldsUtil.reorderMarcRecordFields}).
 *
 * <p>This is the extraction candidate for a future shared library (see the mod-inventory refactor plan): keeping
 * it free of {@code org.folio.*} types and Caffeine/cache types is what makes it portable.
 */
public final class MarcJsonFieldOrderer {

  /**
   * Prefix shared by all MARC control field tags ("001"-"009"). Duplicated (not referenced) from
   * {@code AdditionalFieldsUtil.TAG_00X_PREFIX} so this class carries no dependency on any FOLIO-facing class.
   */
  private static final String TAG_00X_PREFIX = "00";
  private static final String FIELDS = "fields";
  private static final String TAG_001 = "001";
  private static final String TAG_005 = "005";
  private static final Logger LOGGER = LogManager.getLogger();
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  private MarcJsonFieldOrderer() {
  }

  /**
   * Take field values from system modified record content while preserving incoming record content`s field
   * order. Put system fields (001, 005) first, regardless of incoming record fields order.
   *
   * @param sourceOrderContent  content with incoming record fields order
   * @param systemOrderContent  system modified record content with reordered fields
   * @return MARC record parsed content with desired fields order
   * @throws MarcContentException if reordering fails for any reason - the cause is attached
   */
  public static String reorderFields(String sourceOrderContent, String systemOrderContent) {
    try {
      var parsedContent = OBJECT_MAPPER.readTree(systemOrderContent);
      var fieldsArrayNode = (ArrayNode) parsedContent.path(FIELDS);

      var nodes = toNodeList(fieldsArrayNode);
      var nodes00X = removeAndGetNodesByTagPrefix(nodes, TAG_00X_PREFIX);
      var sourceOrderTags = getSourceFields(sourceOrderContent);
      var reorderedFields = OBJECT_MAPPER.createArrayNode();

      var node001 = removeAndGetNodeByTag(nodes00X, TAG_001);
      if (node001 != null && !node001.isEmpty()) {
        reorderedFields.add(node001);
      }

      var node005 = removeAndGetNodeByTag(nodes00X, TAG_005);
      if (node005 != null && !node005.isEmpty()) {
        reorderedFields.add(node005);
      }

      for (var tag : sourceOrderTags) {
        var nodeTag = tag;
        //loop will add system generated fields that are absent in initial record, preserving their order, f.e. 035
        do {
          var node = tag.startsWith(TAG_00X_PREFIX)
                     ? removeAndGetNodeByTag(nodes00X, tag)
                     : removeFirstNode(nodes);
          if (node != null && !node.isEmpty()) {
            nodeTag = getTagFromNode(node);
            reorderedFields.add(node);
          }
        } while (!tag.equals(nodeTag) && !nodes.isEmpty());
      }

      reorderedFields.addAll(nodes);

      ((ObjectNode) parsedContent).set(FIELDS, reorderedFields);
      return parsedContent.toString();
    } catch (Exception e) {
      throw new MarcContentException(
        "Failed to reorder Marc record fields: " + e.getMessage(), e);
    }
  }

  private static JsonNode removeFirstNode(List<JsonNode> nodes) {
    return nodes.isEmpty() ? null : nodes.removeFirst();
  }

  private static List<JsonNode> toNodeList(ArrayNode fieldsArrayNode) {
    var nodes = new LinkedList<JsonNode>();
    for (var node : fieldsArrayNode) {
      nodes.add(node);
    }
    return nodes;
  }

  private static JsonNode removeAndGetNodeByTag(List<JsonNode> nodes, String tag) {
    var toRemove = nodes.stream()
      .filter(node -> getTagFromNode(node).equals(tag))
      .findFirst();
    toRemove.ifPresent(nodes::remove);
    return toRemove.orElse(null);
  }

  private static List<JsonNode> removeAndGetNodesByTagPrefix(List<JsonNode> nodes, String prefix) {
    var startsWithNodes = new LinkedList<JsonNode>();
    for (JsonNode node : nodes) {
      var nodeTag = getTagFromNode(node);
      if (nodeTag.startsWith(prefix)) {
        startsWithNodes.add(node);
      }
    }

    nodes.removeAll(startsWithNodes);
    return startsWithNodes;
  }

  private static String getTagFromNode(JsonNode node) {
    // an empty field node ({}) has no tag and must never structurally match a real tag lookup, so callers
    // (removeAndGetNodeByTag's equality check, removeAndGetNodesByTagPrefix's startsWith check, and the
    // getSourceFields loop) all correctly treat "" as "never matches" for well-formed input.
    var fieldNames = node.fieldNames();
    return fieldNames.hasNext() ? fieldNames.next() : "";
  }

  private static List<String> getSourceFields(String source) {
    var sourceFields = new ArrayList<String>();
    var remainingFields = new ArrayList<String>();
    var has001 = false;
    try {
      var sourceJson = OBJECT_MAPPER.readTree(source);
      var fieldsNode = sourceJson.get(FIELDS);

      for (JsonNode fieldNode : fieldsNode) {
        var tag = getTagFromNode(fieldNode);
        if (tag.equals(TAG_001)) {
          sourceFields.addFirst(tag);
          has001 = true;
        } else if (tag.equals(TAG_005)) {
          if (!has001) {
            sourceFields.addFirst(tag);
          } else {
            sourceFields.add(1, tag);
          }
        } else {
          remainingFields.add(tag);
        }
      }
      sourceFields.addAll(remainingFields);
    } catch (Exception e) {
      LOGGER.error("An error occurred while parsing source JSON: {}", e.getMessage(), e);
    }
    return sourceFields;
  }
}
