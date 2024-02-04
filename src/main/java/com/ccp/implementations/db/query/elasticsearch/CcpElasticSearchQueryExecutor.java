package com.ccp.implementations.db.query.elasticsearch;

import com.ccp.dependency.injection.CcpInstanceProvider;

public class CcpElasticSearchQueryExecutor implements CcpInstanceProvider  {

	
	public Object getInstance() {
		return new ElasticSearchQueryExecutor();
	}

}
