package com.tuan.elasticsearch.river.json;

import java.util.List;
import java.util.Map;

public class JsonRiverConfigure {
	/**
	 * the source url.
	 */
	String sourceURL;
	/**
	 * input indices
	 */
	List<String> inputIndex;
	/**
	 * input types
	 */
	List<String> inputType;
	/**
	 * output indices
	 */
	List<String> outputIndex;
	/**
	 * output types
	 */
	List<String> outputType;
	/**
	 * commit document's count per time
	 */
	int batchCommitCount=2000;
	/**
	 * RESTFul请求index开始位置
	 */
	int searchFrom = 0;
	/**
	 * RESTFul请求index大小
	 */
	int searchSize = 500;
	/**
	 * 每次请求等待时间(ms)
	 */
	int sleepTimeEveryReq = 100;
	
	public String getSourceURL() {
		return sourceURL;
	}

	public void setSourceURL(String sourceURL) {
		this.sourceURL = sourceURL;
	}

	public List<String> getInputIndex() {
		return inputIndex;
	}

	public void setInputIndex(List<String> inputIndex) {
		this.inputIndex = inputIndex;
	}

	public List<String> getInputType() {
		return inputType;
	}

	public void setInputType(List<String> inputType) {
		this.inputType = inputType;
	}

	public List<String> getOutputIndex() {
		return outputIndex;
	}

	public void setOutputIndex(List<String> outputIndex) {
		this.outputIndex = outputIndex;
	}

	public List<String> getOutputType() {
		return outputType;
	}

	public void setOutputType(List<String> outputType) {
		this.outputType = outputType;
	}

	public int getBatchCommitCount() {
		return batchCommitCount;
	}

	public void setBatchCommitCount(int batchCommitCount) {
		this.batchCommitCount = batchCommitCount;
	}

	public int getSearchFrom() {
		return searchFrom;
	}

	public void setSearchFrom(int searchFrom) {
		this.searchFrom = searchFrom;
	}

	public int getSearchSize() {
		return searchSize;
	}

	public void setSearchSize(int searchSize) {
		this.searchSize = searchSize;
	}

	public int getSleepTimeEveryReq() {
		return sleepTimeEveryReq;
	}

	public void setSleepTimeEveryReq(int sleepTimeEveryReq) {
		this.sleepTimeEveryReq = sleepTimeEveryReq;
	}

	@SuppressWarnings("unchecked")
	public static JsonRiverConfigure getInstance(Map<String, Object> settings) {
		JsonRiverConfigure configure = new JsonRiverConfigure();
		if(settings != null) {
			if(settings.containsKey("sourceURL"))
				configure.setSourceURL((String) settings.get("sourceURL"));
			if(settings.containsKey("inputIndex"))
				configure.setInputIndex((List<String>) settings.get("inputIndex"));
			if(settings.containsKey("inputType"))
				configure.setInputType((List<String>) settings.get("inputType"));
			if(settings.containsKey("outputIndex"))
				configure.setOutputIndex((List<String>) settings.get("outputIndex"));
			if(settings.containsKey("outputType"))
				configure.setOutputType((List<String>) settings.get("outputType"));
			if(settings.containsKey("batchCommitCount"))
				configure.setBatchCommitCount((int) settings.get("batchCommitCount"));
			if(settings.containsKey("searchFrom"))
				configure.setSearchFrom((int) settings.get("searchFrom"));
			if(settings.containsKey("searchSize"))
				configure.setSearchSize((int) settings.get("searchSize"));
			if(settings.containsKey("sleepTimeEveryReq"))
				configure.setSleepTimeEveryReq((int) settings.get("sleepTimeEveryReq"));
		}
		
		return configure;	
	}

	@Override
	public String toString() {
		return "JsonRiverConfigure [sourceURL=" + sourceURL + ", inputIndex="
				+ inputIndex + ", inputType=" + inputType + ", outputIndex="
				+ outputIndex + ", outputType=" + outputType
				+ ", batchCommitCount=" + batchCommitCount + ", searchFrom="
				+ searchFrom + ", searchSize=" + searchSize
				+ ", sleepTimeEveryReq=" + sleepTimeEveryReq + "]";
	}

}
