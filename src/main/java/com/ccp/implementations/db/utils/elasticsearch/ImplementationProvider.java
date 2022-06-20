package com.ccp.implementations.db.utils.elasticsearch;

import com.ccp.dependency.injection.CcpEspecification.DefaultImplementationProvider;

public class ImplementationProvider extends DefaultImplementationProvider  {

	@Override
	public Object getImplementation() {
		return new DbQueryExecutorToElasticSearch();
	}

}
