package com.ccp.implementations.db.query.elasticsearch;

import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

import com.ccp.decorators.CcpJsonRepresentation;
import com.ccp.process.CcpMapTransform;

class ResponseHandlerToSearch implements CcpMapTransform<List<CcpJsonRepresentation>>{
	private CcpSourceHandler handler = new CcpSourceHandler();
	@Override
	public List<CcpJsonRepresentation> transform(CcpJsonRepresentation values) {
		List<CcpJsonRepresentation> hits = values.getInnerJson("hits").getJsonList("hits");
		List<CcpJsonRepresentation> collect = hits.stream().map(x -> this.handler.apply(x)).collect(Collectors.toList());
		return collect;
	}

}
class CcpSourceHandler  implements Function<CcpJsonRepresentation, CcpJsonRepresentation>{

	@Override
	public CcpJsonRepresentation apply(CcpJsonRepresentation x) {
		CcpJsonRepresentation internalMap = x.getInnerJson("_source");
		String id = x.getAsString("_id");
		CcpJsonRepresentation put = internalMap.put("id", id);
		return put;
	}
	
}