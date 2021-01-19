package org.folio.inventory.resources;

import org.folio.inventory.domain.items.ItemStatusName;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

public class ItemStatusURL {
  private static final Map<ItemStatusName, String> itemStatusNameUrlMap;
  private static final Map<String, ItemStatusName> urlItemStatusNameMap;

  static {
    itemStatusNameUrlMap = new HashMap<>();

    itemStatusNameUrlMap.put(ItemStatusName.IN_PROCESS, "/mark-in-process");
    itemStatusNameUrlMap.put(ItemStatusName.IN_PROCESS_NON_REQUESTABLE, "/mark-in-process-non-requestable");
    itemStatusNameUrlMap.put(ItemStatusName.INTELLECTUAL_ITEM, "/mark-intellectual-item");
    itemStatusNameUrlMap.put(ItemStatusName.LONG_MISSING, "/mark-long-missing");
//    itemStatusNameUrlMap.put(ItemStatusName.MISSING, "/mark-missing");
//    itemStatusNameUrlMap.put(ItemStatusName.RESTRICTED, "/mark-restricted");
//    itemStatusNameUrlMap.put(ItemStatusName.UNAVAILABLE, "/mark-unavailable");
//    itemStatusNameUrlMap.put(ItemStatusName.UNKNOWN, "/mark-unknown");
//    itemStatusNameUrlMap.put(ItemStatusName.WITHDRAWN, "/mark-withdrawn");

    urlItemStatusNameMap = itemStatusNameUrlMap.entrySet().stream().collect(Collectors.toMap(Map.Entry::getValue, Map.Entry::getKey));
  }

  public static Optional<String> getUrlForItemStatusName(ItemStatusName itemStatusName) {
    return Optional.ofNullable(itemStatusNameUrlMap.get(itemStatusName));
  }

  public static Optional<ItemStatusName> getItemStatusNameForUrl(String url) {
    return Optional.ofNullable(urlItemStatusNameMap.get(url.substring(url.lastIndexOf("/"))));
  }
}
