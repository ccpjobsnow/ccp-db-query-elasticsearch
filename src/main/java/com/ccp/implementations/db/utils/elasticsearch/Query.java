package com.ccp.implementations.db.utils.elasticsearch;

import com.ccp.dependency.injection.CcpInstanceProvider;

public class Query implements CcpInstanceProvider  {

	@Override
	public Object getInstance() {
		return new DbQueryExecutorToElasticSearch();
	}

}
