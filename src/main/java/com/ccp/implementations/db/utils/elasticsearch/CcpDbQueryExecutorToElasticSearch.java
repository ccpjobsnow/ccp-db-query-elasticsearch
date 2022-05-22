package com.ccp.implementations.db.utils.elasticsearch;

import java.util.List;
import java.util.function.Consumer;

import com.ccp.constantes.CcpConstants;
import com.ccp.decorators.CcpMapDecorator;
import com.ccp.dependency.injection.CcpEspecification;
import com.ccp.dependency.injection.CcpImplementation;
import com.ccp.especifications.db.query.CcpDbQueryExecutor;
import com.ccp.especifications.db.query.ElasticQuery;
import com.ccp.especifications.db.utils.CcpDbUtils;

@CcpImplementation
public class CcpDbQueryExecutorToElasticSearch implements CcpDbQueryExecutor {
	
	@CcpEspecification
	private CcpDbUtils dbUtils;
	
	private final CcpResponseHandlerToSearch handler = new CcpResponseHandlerToSearch();

	@Override
	public CcpMapDecorator getTermsStatis(ElasticQuery elasticQuery, String[] resourcesNames, String fieldName) {
		CcpMapDecorator md = new CcpMapDecorator();
		CcpMapDecorator aggregations = this.getAggregations(elasticQuery, resourcesNames);
		
		List<CcpMapDecorator> asMapList = aggregations.getAsMapList(fieldName);
		
		for (CcpMapDecorator mapDecorator : asMapList) {
			String key = mapDecorator.getAsString("key");
			Long asLongNumber = mapDecorator.getAsLongNumber("key");
			if(asLongNumber != null) {
				key = "" + asLongNumber;
			}
			md = md.put(key, mapDecorator.getAsLongNumber("value"));
		}
		return md;
	}
	@Override
	public CcpMapDecorator delete(ElasticQuery elasticQuery, String[] resourcesNames) {
		CcpMapDecorator executeHttpRequest = this.dbUtils.executeHttpRequest(200, resourcesNames, "/_delete_by_query", "POST", elasticQuery.values);

		return executeHttpRequest;
	}

	@Override
	public CcpMapDecorator update(ElasticQuery elasticQuery, String[] resourcesNames, CcpMapDecorator newValues) {
		CcpMapDecorator executeHttpRequest = this.dbUtils.executeHttpRequest(200, resourcesNames, "/_update_by_query", "POST", elasticQuery.values);
		
		return executeHttpRequest;
	}

	@Override
	public void consumeQueryResult(ElasticQuery elasticQuery, String[] resourcesNames,
			String scrollTime, int size, Consumer<List<CcpMapDecorator>> consumer, String... fields) {

		long total = this.total(elasticQuery, resourcesNames);
		
		String scrollId = "";                  
		
		for(int k = 0; k <= total; k += size) {
			
			boolean primeiraPagina = k == 0;
			
			if(primeiraPagina) {
				CcpMapDecorator resultAsPackage = this.getResultAsPackage(elasticQuery, resourcesNames, fields);
				List<CcpMapDecorator> hits = resultAsPackage.getAsMapList("hits");
				scrollId = resultAsPackage.getAsString("scrollId");
				consumer.accept(hits);
				continue;
			}
			
			CcpMapDecorator flows = new CcpMapDecorator().put("200", CcpConstants.doNothing).put("404", CcpConstants.returnEmpty);
			CcpMapDecorator scrollRequest = new CcpMapDecorator().put("scroll", scrollTime).put("scroll_id", scrollId);
			CcpMapDecorator executeHttpRequest = this.dbUtils.executeHttpRequest(flows, "/_search/scroll", "POST", scrollRequest);
			CcpMapDecorator execute = this.handler.execute(executeHttpRequest);
			List<CcpMapDecorator> hits = execute.getAsMapList("response");
			consumer.accept(hits);
		}
	}

	@Override
	public long total(ElasticQuery elasticQuery, String[] resourcesNames) {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public List<CcpMapDecorator> getResultAsList(ElasticQuery elasticQuery, String[] resourcesNames, String... fieldsToSearch) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public CcpMapDecorator getResultAsMap(ElasticQuery elasticQuery, String[] resourcesNames, String field) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public CcpMapDecorator getResultAsPackage(ElasticQuery elasticQuery, String[] resourcesNames, String... array) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public CcpMapDecorator getMap(ElasticQuery elasticQuery, String[] resourcesNames, String field) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public CcpMapDecorator getAggregations(ElasticQuery elasticQuery, String[] resourcesNames) {
		// TODO Auto-generated method stub
		return null;
	}


}
