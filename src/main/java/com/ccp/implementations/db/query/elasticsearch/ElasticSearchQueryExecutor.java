package com.ccp.implementations.db.query.elasticsearch;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import com.ccp.constantes.CcpConstants;
import com.ccp.decorators.CcpJsonRepresentation;
import com.ccp.dependency.injection.CcpDependencyInjection;
import com.ccp.especifications.db.query.CcpQueryExecutor;
import com.ccp.especifications.db.query.CcpDbQueryOptions;
import com.ccp.especifications.db.utils.CcpDbRequester;
import com.ccp.especifications.http.CcpHttpResponseType;

class ElasticSearchQueryExecutor implements CcpQueryExecutor {
	

	@Override
	public CcpJsonRepresentation getTermsStatis(CcpDbQueryOptions elasticQuery, String[] resourcesNames, String fieldName) {
		CcpJsonRepresentation md = CcpConstants.EMPTY_JSON;
		CcpJsonRepresentation aggregations = this.getAggregations(elasticQuery, resourcesNames);
		
		List<CcpJsonRepresentation> asMapList = aggregations.getJsonList(fieldName);
		
		for (CcpJsonRepresentation mapDecorator : asMapList) {
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
	public CcpJsonRepresentation delete(CcpDbQueryOptions elasticQuery, String[] resourcesNames) {
		CcpDbRequester dbUtils = CcpDependencyInjection.getDependency(CcpDbRequester.class);
		
		CcpJsonRepresentation executeHttpRequest = dbUtils.executeHttpRequest("/_delete_by_query", "POST", 200, elasticQuery.values,  resourcesNames, CcpHttpResponseType.singleRecord);

		return executeHttpRequest;
	}

	@Override
	public CcpJsonRepresentation update(CcpDbQueryOptions elasticQuery, String[] resourcesNames, CcpJsonRepresentation newValues) {
		CcpDbRequester dbUtils = CcpDependencyInjection.getDependency(CcpDbRequester.class);
		
		CcpJsonRepresentation executeHttpRequest = dbUtils.executeHttpRequest("/_update_by_query", "POST", 200, elasticQuery.values,  resourcesNames, CcpHttpResponseType.singleRecord);
		
		return executeHttpRequest;
	}

	@Override
	public void consumeQueryResult(CcpDbQueryOptions elasticQuery, String[] resourcesNames,
			String scrollTime, int size, Consumer<List<CcpJsonRepresentation>> consumer, String... fields) {

		long total = this.total(elasticQuery, resourcesNames);
		
		String scrollId = "";                  
		
		for(int k = 0; k <= total; k += size) {
			
			boolean primeiraPagina = k == 0;
			
			if(primeiraPagina) {
				CcpJsonRepresentation resultAsPackage = this.getResultAsPackage("/_search", "POST", 200, elasticQuery, resourcesNames, fields);
				List<CcpJsonRepresentation> hits = resultAsPackage.getJsonList("hits");
				scrollId = resultAsPackage.getAsString("scrollId");
				consumer.accept(hits);
				continue;
			}
			
			CcpJsonRepresentation flows = CcpConstants.EMPTY_JSON.put("200", CcpConstants.DO_NOTHING).put("404", CcpConstants.RETURNS_EMPTY_JSON);
			CcpJsonRepresentation scrollRequest = CcpConstants.EMPTY_JSON.put("scroll", scrollTime).put("scroll_id", scrollId);
			CcpDbRequester dbUtils = CcpDependencyInjection.getDependency(CcpDbRequester.class);
			
			ResponseHandlerToSearch searchDataTransform = new ResponseHandlerToSearch();
			CcpJsonRepresentation executeHttpRequest = dbUtils.executeHttpRequest("/_search/scroll", "POST", flows,  scrollRequest, CcpHttpResponseType.singleRecord);
			List<CcpJsonRepresentation> hits = searchDataTransform.transform(executeHttpRequest);
			consumer.accept(hits);
		}
	}

	@Override
	public long total(CcpDbQueryOptions elasticQuery, String[] resourcesNames) {
		CcpDbRequester dbUtils = CcpDependencyInjection.getDependency(CcpDbRequester.class);
		
		CcpJsonRepresentation executeHttpRequest = dbUtils.executeHttpRequest("/_count", "GET", 200, elasticQuery.values, CcpHttpResponseType.singleRecord);
		Long count = executeHttpRequest.getAsLongNumber("count");
		return count;
	}

	@Override
	public List<CcpJsonRepresentation> getResultAsList(CcpDbQueryOptions elasticQuery, String[] resourcesNames, String... fieldsToSearch) {
		CcpJsonRepresentation executeHttpRequest = this.getResultAsPackage("/_search", "POST", 200, elasticQuery, resourcesNames, fieldsToSearch);
		
		ResponseHandlerToSearch searchDataTransform = new ResponseHandlerToSearch();
		List<CcpJsonRepresentation> hits = searchDataTransform.transform(executeHttpRequest);
		return hits;
	}

	@Override
	public CcpJsonRepresentation getResultAsMap(CcpDbQueryOptions elasticQuery, String[] resourcesNames, String field) {
		List<CcpJsonRepresentation> resultAsList = this.getResultAsList(elasticQuery, resourcesNames, field);
		CcpJsonRepresentation result = CcpConstants.EMPTY_JSON;
		for (CcpJsonRepresentation md : resultAsList) {
			String id = md.getAsString("id");
			Object value = md.get(field);
			result = result.put(id, value);
		}
		return result;
	}

	@Override
	public CcpJsonRepresentation getResultAsPackage(String url, String method, int expectedStatus, CcpDbQueryOptions elasticQuery, String[] resourcesNames, String... fieldsToSearch) {
		CcpJsonRepresentation _source = elasticQuery.values.put("_source", Arrays.asList(fieldsToSearch));
		CcpDbRequester dbUtils = CcpDependencyInjection.getDependency(CcpDbRequester.class);
		
		CcpJsonRepresentation executeHttpRequest = dbUtils.executeHttpRequest(url, method, expectedStatus,  _source, resourcesNames, CcpHttpResponseType.singleRecord);
		return executeHttpRequest;
	}

	@Override
	public CcpJsonRepresentation getMap(CcpDbQueryOptions elasticQuery, String[] resourcesNames, String field) {
		CcpJsonRepresentation aggregations = this.getAggregations(elasticQuery, resourcesNames);
		List<CcpJsonRepresentation> asMapList = aggregations.getJsonList(field);
		CcpJsonRepresentation retorno = CcpConstants.EMPTY_JSON;
		for (CcpJsonRepresentation md : asMapList) {
			Object value = md.get("value");
			String key = md.getAsString("key");
			retorno = retorno.put(key, value);
		}
		return retorno;
	}

	@Override
	public CcpJsonRepresentation getAggregations(CcpDbQueryOptions elasticQuery, String[] resourcesNames) {
		
		CcpJsonRepresentation resultAsPackage = this.getResultAsPackage("/_search", "POST", 200, elasticQuery, resourcesNames);
		Double total = resultAsPackage.getInnerJson("total").getAsDoubleNumber("value");
		CcpJsonRepresentation result = CcpConstants.EMPTY_JSON;
		result = result.put("total", total);
		CcpJsonRepresentation aggregations = resultAsPackage.getInnerJson("aggregations");
		Set<String> keySet = aggregations.keySet();
		for (String key : keySet) {
			CcpJsonRepresentation value = aggregations.getInnerJson(key);
			if(value.containsKey("buckets")) {
				List<CcpJsonRepresentation> bucket = new ArrayList<>();
				List<CcpJsonRepresentation> results = value.getJsonList("buckets");
				for (CcpJsonRepresentation object : results) {
					String keyName = object.getAsString("key");
					Double keyCount = object.getAsDoubleNumber("doc_count");
					CcpJsonRepresentation res = CcpConstants.EMPTY_JSON;
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
