package com.ccp.implementations.db.query.elasticsearch;

import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

import com.ccp.decorators.CcpMapDecorator;
import com.ccp.process.CcpMapTransform;

class ResponseHandlerToSearch implements CcpMapTransform<List<CcpMapDecorator>>{
	private CcpSourceHandler handler = new CcpSourceHandler();
	@Override
	public List<CcpMapDecorator> transform(CcpMapDecorator values) {
		List<CcpMapDecorator> hits = values.getInternalMap("hits").getAsMapList("hits");
		List<CcpMapDecorator> collect = hits.stream().map(x -> this.handler.apply(x)).collect(Collectors.toList());
		return collect;
	}

}
class CcpSourceHandler  implements Function<CcpMapDecorator, CcpMapDecorator>{

	@Override
	public CcpMapDecorator apply(CcpMapDecorator x) {
		CcpMapDecorator internalMap = x.getInternalMap("_source");
		String id = x.getAsString("_id");
		CcpMapDecorator put = internalMap.put("id", id);
		return put;
	}
	
}