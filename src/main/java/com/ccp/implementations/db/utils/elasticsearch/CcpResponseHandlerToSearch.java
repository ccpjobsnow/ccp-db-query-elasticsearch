package com.ccp.implementations.db.utils.elasticsearch;

import java.util.List;
import java.util.stream.Collectors;

import com.ccp.decorators.CcpMapDecorator;
import com.ccp.process.CcpProcess;

class CcpResponseHandlerToSearch implements CcpProcess{
	private CcpSourceHandler handler = new CcpSourceHandler();
	@Override
	public CcpMapDecorator execute(CcpMapDecorator values) {
		CcpMapDecorator response = values.getInternalMap("response");
		List<CcpMapDecorator> hits = response.getInternalMap("hits").getAsMapList("hits");
		List<CcpMapDecorator> collect = hits.stream().map(x -> this.handler.execute(x)).collect(Collectors.toList());
		return new CcpMapDecorator().put("response", collect);
	}

}
class CcpSourceHandler implements CcpProcess{

	@Override
	public CcpMapDecorator execute(CcpMapDecorator x) {
		CcpMapDecorator internalMap = x.getInternalMap("_source");
		String id = x.getAsString("_id");
		CcpMapDecorator put = internalMap.put("id", id);
		return put;
	}
	
}