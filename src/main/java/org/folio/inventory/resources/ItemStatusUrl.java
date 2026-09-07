package org.folio.inventory.resources;

import java.util.EnumMap;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import lombok.experimental.UtilityClass;
import org.folio.inventory.domain.items.ItemStatusName;

@UtilityClass
public class ItemStatusUrl {

  private static final Map<ItemStatusName, String> ITEM_STATUS_NAME_URL_MAP;
  private static final Map<String, ItemStatusName> URL_ITEM_STATUS_NAME_MAP;

  static {
    ITEM_STATUS_NAME_URL_MAP = new EnumMap<>(ItemStatusName.class);

    ITEM_STATUS_NAME_URL_MAP.put(ItemStatusName.IN_PROCESS, "/mark-in-process");
    ITEM_STATUS_NAME_URL_MAP.put(ItemStatusName.IN_PROCESS_NON_REQUESTABLE, "/mark-in-process-non-requestable");
    ITEM_STATUS_NAME_URL_MAP.put(ItemStatusName.INTELLECTUAL_ITEM, "/mark-intellectual-item");
    ITEM_STATUS_NAME_URL_MAP.put(ItemStatusName.LONG_MISSING, "/mark-long-missing");
    ITEM_STATUS_NAME_URL_MAP.put(ItemStatusName.MISSING, "/mark-missing");
    ITEM_STATUS_NAME_URL_MAP.put(ItemStatusName.RESTRICTED, "/mark-restricted");
    ITEM_STATUS_NAME_URL_MAP.put(ItemStatusName.UNAVAILABLE, "/mark-unavailable");
    ITEM_STATUS_NAME_URL_MAP.put(ItemStatusName.UNKNOWN, "/mark-unknown");
    ITEM_STATUS_NAME_URL_MAP.put(ItemStatusName.WITHDRAWN, "/mark-withdrawn");

    URL_ITEM_STATUS_NAME_MAP =
      ITEM_STATUS_NAME_URL_MAP.entrySet().stream().collect(Collectors.toMap(Map.Entry::getValue, Map.Entry::getKey));
  }

  public static Optional<String> getUrlForItemStatusName(ItemStatusName itemStatusName) {
    return Optional.ofNullable(ITEM_STATUS_NAME_URL_MAP.get(itemStatusName));
  }

  public static Optional<ItemStatusName> getItemStatusNameForUrl(String url) {
    return Optional.ofNullable(URL_ITEM_STATUS_NAME_MAP.get(url.substring(url.lastIndexOf("/"))));
  }
}
