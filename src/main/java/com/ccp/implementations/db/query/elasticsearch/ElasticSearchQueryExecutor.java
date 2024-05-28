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
	

	
	public CcpJsonRepresentation getTermsStatis(CcpDbQueryOptions elasticQuery, String[] resourcesNames, String fieldName) {
		CcpJsonRepresentation md = CcpConstants.EMPTY_JSON;
		CcpJsonRepresentation aggregations = this.getAggregations(elasticQuery, resourcesNames);
		
		List<CcpJsonRepresentation> asMapList = aggregations.getAsJsonList(fieldName);
		
		for (CcpJsonRepresentation mapDecorator : asMapList) {
			String key = ""+ mapDecorator.getAsString("key");
			md = md.put(key, mapDecorator.getAsLongNumber("value"));
		}
		return md;
	}
	
	public CcpJsonRepresentation delete(CcpDbQueryOptions elasticQuery, String[] resourcesNames) {
		CcpDbRequester dbUtils = CcpDependencyInjection.getDependency(CcpDbRequester.class);
		
		CcpJsonRepresentation executeHttpRequest = dbUtils.executeHttpRequest("delete", "/_delete_by_query", "POST", 200, elasticQuery.json,  resourcesNames, CcpHttpResponseType.singleRecord);

		return executeHttpRequest;
	}

	
	public CcpJsonRepresentation update(CcpDbQueryOptions elasticQuery, String[] resourcesNames, CcpJsonRepresentation newValues) {
		CcpDbRequester dbUtils = CcpDependencyInjection.getDependency(CcpDbRequester.class);
		
		CcpJsonRepresentation executeHttpRequest = dbUtils.executeHttpRequest("update", "/_update_by_query", "POST", 200, elasticQuery.json,  resourcesNames, CcpHttpResponseType.singleRecord);
		
		return executeHttpRequest;
	}

	
	public void consumeQueryResult(CcpDbQueryOptions elasticQuery, String[] resourcesNames,
			String scrollTime, int size, Consumer<CcpJsonRepresentation> consumer, String... fields) {

		long total = this.total(elasticQuery, resourcesNames);
		String indexes = this.getIndexes(resourcesNames);
	
		String scrollId = "";                  
		CcpDbRequester dbUtils = CcpDependencyInjection.getDependency(CcpDbRequester.class);
		
		for(int k = 0; k <= total; k += size) {
			boolean firstPage = k == 0;
			
			if(firstPage) {
				String url = indexes + "/_search?size=" + size + "&scroll="+ scrollTime;
				ResponseHandlerToConsumeSearch searchDataTransform = new ResponseHandlerToConsumeSearch();
				CcpJsonRepresentation flows = CcpConstants.EMPTY_JSON.put("200", CcpConstants.DO_BY_PASS).put("404", CcpConstants.RETURNS_EMPTY_JSON);
				CcpJsonRepresentation executeHttpRequest = dbUtils.executeHttpRequest("consumeQueryResult", url, "POST", flows,  CcpConstants.EMPTY_JSON, CcpHttpResponseType.singleRecord);
				CcpJsonRepresentation _package = searchDataTransform.apply(executeHttpRequest);
				List<CcpJsonRepresentation> hits = _package.getAsJsonList("hits");
				scrollId = _package.getAsString("_scroll_id");
				for (CcpJsonRepresentation hit : hits) {
					consumer.accept(hit);
				}
				continue;
			}
			
			CcpJsonRepresentation flows = CcpConstants.EMPTY_JSON.put("200", CcpConstants.DO_BY_PASS).put("404", CcpConstants.RETURNS_EMPTY_JSON);
			CcpJsonRepresentation scrollRequest = CcpConstants.EMPTY_JSON.put("scroll", scrollTime).put("scroll_id", scrollId);
			
			ResponseHandlerToSearch searchDataTransform = new ResponseHandlerToSearch();
			CcpJsonRepresentation executeHttpRequest = dbUtils.executeHttpRequest("consumeQueryResult", "/_search/scroll", "POST", flows,  scrollRequest, CcpHttpResponseType.singleRecord);
			List<CcpJsonRepresentation> hits = searchDataTransform.apply(executeHttpRequest);
			for (CcpJsonRepresentation hit : hits) {
				consumer.accept(hit);
			}
		}
	}

	
	public long total(CcpDbQueryOptions elasticQuery, String[] resourcesNames) {
		CcpDbRequester dbUtils = CcpDependencyInjection.getDependency(CcpDbRequester.class);
		String indexes = this.getIndexes(resourcesNames);
		String url = indexes + "/_count";
		CcpJsonRepresentation executeHttpRequest = dbUtils.executeHttpRequest("getTotalRecords", url, "GET", 200, elasticQuery.json, CcpHttpResponseType.singleRecord);
		Long count = executeHttpRequest.getAsLongNumber("count");
		return count;
	}

	public String getIndexes(String[] resourcesNames) {
		String indexes = "/" + Arrays.asList(resourcesNames).toString().replace("[", "").replace("]", "");
		return indexes;
	}

	
	public List<CcpJsonRepresentation> getResultAsList(CcpDbQueryOptions elasticQuery, String[] resourcesNames, String... fieldsToSearch) {
		CcpJsonRepresentation executeHttpRequest = this.getResultAsPackage("/_search", "POST", 200, elasticQuery, resourcesNames, fieldsToSearch);
		
		ResponseHandlerToSearch searchDataTransform = new ResponseHandlerToSearch();
		List<CcpJsonRepresentation> hits = searchDataTransform.apply(executeHttpRequest);
		return hits;
	}

	
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

	
	public CcpJsonRepresentation getResultAsPackage(String url, String method, int expectedStatus, CcpDbQueryOptions elasticQuery, String[] resourcesNames, String... fieldsToSearch) {
		CcpJsonRepresentation _source = elasticQuery.json.put("_source", Arrays.asList(fieldsToSearch));
		CcpDbRequester dbUtils = CcpDependencyInjection.getDependency(CcpDbRequester.class);
		
		CcpJsonRepresentation executeHttpRequest = dbUtils.executeHttpRequest("getResultAsPackage", url, method, expectedStatus,  _source, resourcesNames, CcpHttpResponseType.singleRecord);
		return executeHttpRequest;
	}

	
	public CcpJsonRepresentation getMap(CcpDbQueryOptions elasticQuery, String[] resourcesNames, String field) {
		CcpJsonRepresentation aggregations = this.getAggregations(elasticQuery, resourcesNames);
		List<CcpJsonRepresentation> asMapList = aggregations.getAsJsonList(field);
		CcpJsonRepresentation retorno = CcpConstants.EMPTY_JSON;
		for (CcpJsonRepresentation md : asMapList) {
			Object value = md.get("value");
			String key = md.getAsString("key");
			retorno = retorno.put(key, value);
		}
		return retorno;
	}

	
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
				List<CcpJsonRepresentation> results = value.getAsJsonList("buckets");
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
