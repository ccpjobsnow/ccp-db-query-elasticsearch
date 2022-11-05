package com.ccp.implementations.db.utils.elasticsearch;

import com.ccp.dependency.injection.CcpImplementationProvider;

public class ImplementationProvider implements CcpImplementationProvider  {

	@Override
	public Object getImplementation() {
		return new DbQueryExecutorToElasticSearch();
	}

}
