package com.ccp.implementations.db.query.elasticsearch;

import com.ccp.dependency.injection.CcpInstanceProvider;

public class CcpElasticSearchQueryExecutor implements CcpInstanceProvider  {

	@Override
	public Object getInstance() {
		return new ElasticSearchQueryExecutor();
	}

}
