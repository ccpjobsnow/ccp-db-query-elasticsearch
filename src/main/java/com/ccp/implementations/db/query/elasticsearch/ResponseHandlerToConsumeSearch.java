package com.ccp.implementations.db.query.elasticsearch;

import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

import com.ccp.constantes.CcpConstants;
import com.ccp.decorators.CcpJsonRepresentation;

class ResponseHandlerToConsumeSearch implements Function<CcpJsonRepresentation, CcpJsonRepresentation>{
	private CcpSourceHandler handler = new CcpSourceHandler();
	
	public CcpJsonRepresentation apply(CcpJsonRepresentation values) {
		List<CcpJsonRepresentation> hits = values.getInnerJson("hits").getAsJsonList("hits");
		List<CcpJsonRepresentation> collect = hits.stream().map(x -> this.handler.apply(x)).collect(Collectors.toList());
		String _scroll_id = values.getAsString("_scroll_id");
		return CcpConstants.EMPTY_JSON.put("hits", collect).put("_scroll_id", _scroll_id);
	}

}
