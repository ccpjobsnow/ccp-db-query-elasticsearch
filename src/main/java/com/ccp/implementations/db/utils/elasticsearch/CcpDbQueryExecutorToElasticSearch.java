package com.ccp.implementations.db.utils.elasticsearch;

import java.util.Arrays;
import java.util.List;
import java.util.function.Consumer;

import com.ccp.decorators.CcpMapDecorator;
import com.ccp.dependency.injection.CcpEspecification;
import com.ccp.dependency.injection.CcpImplementation;
import com.ccp.especifications.db.query.CcpDbQueryExecutor;
import com.ccp.especifications.db.query.ElasticQuery;
import com.ccp.especifications.db.utils.CcpDbCredentialsGenerator;
import com.ccp.especifications.http.CcpHttp;
import com.ccp.especifications.http.CcpHttpHandler;
import com.ccp.process.CcpProcess;

@CcpImplementation
public class CcpDbQueryExecutorToElasticSearch implements CcpDbQueryExecutor {
	@CcpEspecification
	private CcpHttp ccpHttp;
	
	@CcpEspecification
	private CcpDbCredentialsGenerator dbCredentials;
	
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
	private String getUrl(String[] resourcesNames, String complemento) {
		return this.dbCredentials.getDatabaseCredentials().getAsString("DB_URL") +  Arrays.asList(resourcesNames).stream().toString()
				.replace("[", "").replace("]", "").replace(" ", "") + complemento;
	}

	@Override
	public CcpMapDecorator delete(ElasticQuery elasticQuery, String[] resourcesNames) {
		CcpMapDecorator executeHttpRequest = this.executeRequest(elasticQuery, resourcesNames, "/_delete");
		
		return executeHttpRequest;
	}
	private CcpMapDecorator executeRequest(ElasticQuery elasticQuery, String[] resourcesNames, String complemento) {
		String url = this.getUrl(resourcesNames, complemento);
		CcpMapDecorator headers = this.dbCredentials.getDatabaseCredentials();
		CcpHttpHandler http = new CcpHttpHandler(200, this.ccpHttp);
		CcpMapDecorator executeHttpRequest = http.executeHttpRequest(url, "POST", headers, elasticQuery.values);
		return executeHttpRequest;
	}

	@Override
	public CcpMapDecorator update(ElasticQuery elasticQuery, String[] resourcesNames, CcpMapDecorator newValues) {
		CcpMapDecorator executeHttpRequest = this.executeRequest(elasticQuery, resourcesNames, "/_update_by_query");
		
		return executeHttpRequest;
	}

	@Override
	public void consumeQueryResult(CcpDbQueryExecutor requestExecutor, ElasticQuery elasticQuery, String[] resourcesNames,
			String scrollTime, int size, Consumer<List<CcpMapDecorator>> consumer, String... fields) {

		long total = this.total(elasticQuery, resourcesNames);
		
		String scrollId = "";                  
		
		for(int k = 0; k <= total; k += size) {
			
			if(k == 0) {
				CcpMapDecorator resultAsPackage = elasticQuery.setScrollTime(scrollTime).selectFrom(requestExecutor, resourcesNames).getResultAsPackage(fields);
				List<CcpMapDecorator> hits = resultAsPackage.getAsMapList("hits");
				scrollId = resultAsPackage.getAsString("scrollId");
				consumer.accept(hits);
				continue;
			}
			
			CcpMapDecorator headers = this.dbCredentials.getDatabaseCredentials();
			CcpProcess scrollEnded = x -> new CcpMapDecorator();
			CcpProcess doNothing = x -> x;
			CcpMapDecorator flows = new CcpMapDecorator().put("200", doNothing).put("404", scrollEnded);
			CcpHttpHandler http = new CcpHttpHandler(flows, this.ccpHttp);
			String url = this.dbCredentials.getDatabaseCredentials().getAsString("DB_URL") + "/_search/scroll";
			CcpMapDecorator scrollRequest = new CcpMapDecorator().put("scroll", scrollTime).put("scroll_id", scrollId);
			CcpMapDecorator executeHttpRequest = http.executeHttpRequest(url, "POST", headers, scrollRequest);
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
