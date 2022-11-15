package com.ccp.implementations.db.utils.elasticsearch;

import com.ccp.dependency.injection.CcpModuleExporter;

public class Query implements CcpModuleExporter  {

	@Override
	public Object export() {
		return new DbQueryExecutorToElasticSearch();
	}

}
