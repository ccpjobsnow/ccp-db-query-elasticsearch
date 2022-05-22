package com.ccp.implementations.db.utils.elasticsearch;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.function.Consumer;
import java.util.stream.Collectors;

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
	
	private final CcpResponseHandlerToSearch searchDataTransform = new CcpResponseHandlerToSearch();

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
		CcpMapDecorator executeHttpRequest = this.dbUtils.executeHttpRequest("/_delete_by_query", "POST", 200, elasticQuery.values,  resourcesNames);

		return executeHttpRequest;
	}

	@Override
	public CcpMapDecorator update(ElasticQuery elasticQuery, String[] resourcesNames, CcpMapDecorator newValues) {
		CcpMapDecorator executeHttpRequest = this.dbUtils.executeHttpRequest("/_update_by_query", "POST", 200, elasticQuery.values,  resourcesNames);
		
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
				CcpMapDecorator resultAsPackage = this.getResultAsPackage("/_search", "POST", 200, elasticQuery, resourcesNames, fields);
				List<CcpMapDecorator> hits = resultAsPackage.getAsMapList("hits");
				scrollId = resultAsPackage.getAsString("scrollId");
				consumer.accept(hits);
				continue;
			}
			
			CcpMapDecorator flows = new CcpMapDecorator().put("200", CcpConstants.doNothing).put("404", CcpConstants.returnEmpty);
			CcpMapDecorator scrollRequest = new CcpMapDecorator().put("scroll", scrollTime).put("scroll_id", scrollId);
			CcpMapDecorator executeHttpRequest = this.dbUtils.executeHttpRequest("/_search/scroll", "POST", flows,  scrollRequest);
			CcpMapDecorator execute = this.searchDataTransform.execute(executeHttpRequest);
			List<CcpMapDecorator> hits = execute.getAsMapList("response");
			consumer.accept(hits);
		}
	}

	@Override
	public long total(ElasticQuery elasticQuery, String[] resourcesNames) {
		CcpMapDecorator executeHttpRequest = this.dbUtils.executeHttpRequest("/_count", "GET", 200, elasticQuery.values);
		CcpMapDecorator response = executeHttpRequest.getInternalMap("response");
		Long count = response.getAsLongNumber("count");
		return count;
	}

	@Override
	public List<CcpMapDecorator> getResultAsList(ElasticQuery elasticQuery, String[] resourcesNames, String... fieldsToSearch) {
		CcpMapDecorator executeHttpRequest = this.getResultAsPackage("/_search", "POST", 200, elasticQuery, resourcesNames, fieldsToSearch);
		CcpMapDecorator execute = this.searchDataTransform.execute(executeHttpRequest);
		List<CcpMapDecorator> hits = execute.getAsMapList("response");
		return hits;
	}

	@Override
	public CcpMapDecorator getResultAsMap(ElasticQuery elasticQuery, String[] resourcesNames, String field) {
		List<CcpMapDecorator> resultAsList = this.getResultAsList(elasticQuery, resourcesNames, field);
		CcpMapDecorator result = new CcpMapDecorator();
		for (CcpMapDecorator md : resultAsList) {
			String id = md.getAsString("id");
			Object value = md.get(field);
			result = result.put(id, value);
		}
		return result;
	}

	@Override
	public CcpMapDecorator getResultAsPackage(String url, String method, int expectedStatus, ElasticQuery elasticQuery, String[] resourcesNames, String... fieldsToSearch) {
		CcpMapDecorator _source = elasticQuery.values.put("_source", Arrays.asList(fieldsToSearch));
		CcpMapDecorator executeHttpRequest = this.dbUtils.executeHttpRequest(url, method, expectedStatus,  _source, resourcesNames);
		return executeHttpRequest;
	}

	@Override
	public CcpMapDecorator getMap(ElasticQuery elasticQuery, String[] resourcesNames, String field) {
		CcpMapDecorator aggregations = this.getAggregations(elasticQuery, resourcesNames);
		List<CcpMapDecorator> asMapList = aggregations.getAsMapList(field);
		CcpMapDecorator retorno = new CcpMapDecorator();
		for (CcpMapDecorator md : asMapList) {
			Object value = md.get("value");
			String key = md.getAsString("key");
			retorno = retorno.put(key, value);
		}
		return retorno;
	}

	@Override
	public CcpMapDecorator getAggregations(ElasticQuery elasticQuery, String[] resourcesNames) {
		
		CcpMapDecorator resultAsPackage = this.getResultAsPackage("/_search", "POST", 200, elasticQuery, resourcesNames);
		Double total = resultAsPackage.getInternalMap("total").getAsDoubleNumber("value");
		CcpMapDecorator result = new CcpMapDecorator();
		result = result.put("total", total);
		CcpMapDecorator aggregations = resultAsPackage.getInternalMap("aggregations");
		Set<String> keySet = aggregations.keySet();
		for (String key : keySet) {
			CcpMapDecorator value = aggregations.getInternalMap(key);
			if(value.containsKey("buckets")) {
				List<CcpMapDecorator> bucket = new ArrayList<>();
				List<CcpMapDecorator> results = value.getAsMapList("buckets");
				for (CcpMapDecorator object : results) {
					String keyName = object.getAsString("key");
					Double keyCount = object.getAsDoubleNumber("doc_count");
					CcpMapDecorator res = new CcpMapDecorator();
					res = res.put("key", keyName);
					res = res.put("value", keyCount);
					bucket.add(res);
				}
				result = result.put(key, bucket.stream().map(x -> x.content).collect(Collectors.toList()));
				continue;
			}
			result = result.put(key, value.getAsDoubleNumber("value"));
		}
		
		return result;
	}


}
