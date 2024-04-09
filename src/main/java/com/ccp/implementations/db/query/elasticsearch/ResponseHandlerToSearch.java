package com.ccp.implementations.db.query.elasticsearch;

import java.util.List;
import java.util.stream.Collectors;

import com.ccp.decorators.CcpJsonRepresentation;
import com.ccp.process.CcpMapTransform;

class ResponseHandlerToSearch implements CcpMapTransform<List<CcpJsonRepresentation>>{
	private CcpSourceHandler handler = new CcpSourceHandler();
	
	public List<CcpJsonRepresentation> transform(CcpJsonRepresentation values) {
		List<CcpJsonRepresentation> hits = values.getInnerJson("hits").getAsJsonList("hits");
		List<CcpJsonRepresentation> collect = hits.stream().map(x -> this.handler.apply(x)).collect(Collectors.toList());
		return collect;
	}

}



