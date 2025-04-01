package org.folio.inventory.common.api.request;

import org.apache.commons.lang3.StringUtils;
import org.folio.inventory.common.WebContext;
import org.folio.inventory.domain.items.CQLQueryRequestDto;

public class PagingParameters {
  public final Integer limit;
  public final Integer offset;

  public PagingParameters(Integer limit, Integer offset) {
    this.offset = offset;
    this.limit = limit;
  }

  public static PagingParameters defaults() {
    return new PagingParameters(10, 0);
  }

  public static PagingParameters from(WebContext context) {
    String limit = context.getStringParameter("limit", "10");
    String offset = context.getStringParameter("offset", "0");

    if (valid(limit, offset)) {
      return new PagingParameters(Integer.parseInt(limit), Integer.parseInt(offset));
    } else {
      return null;
    }

  }

  public static PagingParameters from(CQLQueryRequestDto cqlQueryRequestDto) {
    return new PagingParameters(cqlQueryRequestDto.getLimit(), cqlQueryRequestDto.getOffset());
  }

  public static boolean valid(String limit, String offset) {
    if (StringUtils.isEmpty(limit) || StringUtils.isEmpty(offset)) {
      return false;
    } else {
      return StringUtils.isNumeric(limit) && StringUtils.isNumeric(offset);
    }
  }
}
