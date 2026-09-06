package org.folio.inventory.common.domain;

import org.apache.commons.lang3.StringUtils;
import org.folio.inventory.common.WebContext;
import org.folio.inventory.domain.items.CQLQueryRequestDto;

public record PagingParameters(Integer limit, Integer offset) {

  public static final String LIMIT_PARAM = "limit";
  public static final String OFFSET_PARAM = "offset";

  public static PagingParameters defaults() {
    return new PagingParameters(10, 0);
  }

  public static PagingParameters from(WebContext context) {
    String limit = context.getStringParameter(LIMIT_PARAM, "10");
    String offset = context.getStringParameter(OFFSET_PARAM, "0");

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
