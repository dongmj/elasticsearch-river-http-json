package com.tuan.elasticsearch.river.json.input;

import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TransferQueue;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.ESLoggerFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentParser.Token;
import org.elasticsearch.common.xcontent.json.JsonXContent;

import com.tuan.elasticsearch.river.json.JsonRiverConfigure;
import com.tuan.elasticsearch.river.json.RiverProduct;

public class EsImporter implements Importer {

	private final JsonRiverConfigure jsonRiverConfigure;
	private final TransferQueue<RiverProduct> queue;
	private static final int TIMEOUT = 120 * 1000;
	private static final ESLogger logger = ESLoggerFactory.getLogger(EsImporter.class.getName());
	
	public EsImporter(JsonRiverConfigure jsonRiverConfigure, TransferQueue<RiverProduct> stream) {
		this.jsonRiverConfigure = jsonRiverConfigure;
		this.queue = stream;
	}

	private void checkParams() {
		if(jsonRiverConfigure.getSourceURL() == null)
			throw new ElasticsearchException("source url can't be empty!");
		if(jsonRiverConfigure.getInputIndex() == null || jsonRiverConfigure.getInputIndex().size() == 0)
			throw new ElasticsearchException("input indices can't be empty!");
		if(jsonRiverConfigure.getOutputIndex() != null && jsonRiverConfigure.getInputIndex().size() != jsonRiverConfigure.getOutputIndex().size())
			throw new ElasticsearchException("input indices' length must be equal to output indices' length!");
		if(jsonRiverConfigure.getInputType() != null && jsonRiverConfigure.getOutputType() != null 
				&& jsonRiverConfigure.getInputType().size() != jsonRiverConfigure.getOutputType().size())
			throw new ElasticsearchException("input indices' types's length must be equal to output indices' types length!");
	}
	
	@Override
	public RiverProductImport executeImport() {
		checkParams();
		RiverProductImport result = new RiverProductImport();
		for(int i = 0; i < jsonRiverConfigure.getInputIndex().size(); i++) {
			String inputIndex = jsonRiverConfigure.getInputIndex().get(i);
			String outputIndex = null;
			if( jsonRiverConfigure.getOutputIndex() != null)
				outputIndex = jsonRiverConfigure.getOutputIndex().get(i);
			if(jsonRiverConfigure.getInputType() != null) {
				for(int j = 0; j < jsonRiverConfigure.getInputType().size(); j++) {
					String inputType = jsonRiverConfigure.getInputType().get(j);
					String outputType = jsonRiverConfigure.getOutputType().get(j);
					String destURL = jsonRiverConfigure.getSourceURL()+"/" + inputIndex + "/" + inputType + "/_search";
					result.exportedProductCount += process(destURL, outputIndex, outputType);
				}
			} else {
				String destURL = jsonRiverConfigure.getSourceURL()+"/" + inputIndex + "/_search";
				result.exportedProductCount += process(destURL, outputIndex, null);
			}
		}
		
		return result;
	}
	
	private int process(String destUrl, String index, String type) {
		int total = 0;
		int offset = jsonRiverConfigure.getSearchFrom();
		int size = jsonRiverConfigure.getSearchSize();
		int sleepTime = jsonRiverConfigure.getSleepTimeEveryReq();
        // 本次循环处理doc个数
        int processedCount= 0;
		do{
	        XContentParser parser = null;
	        InputStream in = null;
			try {
	            in = getConnectionInputstream(destUrl, offset, size);
	            parser = JsonXContent.jsonXContent.createParser(in);
	
	            Map<String, Object> product = null;
	            @SuppressWarnings("unused")
	            Token token = null;
	            processedCount= 0;
	            String docType = type, docIndex = index, docId = null;
	            while ((token = parser.nextToken()) != null) {
	            	if("total".equals(parser.text())) {
	            		token = parser.nextToken();
	            		total = Integer.parseInt(parser.text());
	            	} else if(type == null && "_type".equals(parser.text())) {
	            		token = parser.nextToken();
	            		docType = parser.text();
	            	} else if("_id".equals(parser.text())) {
	            		token = parser.nextToken();
	            		docId = parser.text();
	            	} else if(index == null && "_index".equals(parser.text())) {
	            		token = parser.nextToken();
	            		docIndex = parser.text();
	            	} else if("_source".equals(parser.text())) {
	            		token = parser.nextToken();
	            		product = parser.map();
	            	}
	            	
	            	if(docIndex != null && docType != null && docId != null && product != null) {
	            		queue.add(RiverProduct.index(docIndex, docType, docId, product));
	            		processedCount++;
	            		docIndex = index;
	            		docType = type;
	            		docId = null;
	            		product = null;
	            	}
	            }
	        
			}catch(IOException e) {
				logger.error("Could not get content with index [{},{}]", e, index, type);
			} finally {
				closeQuietly(in);
			}
			offset += processedCount;
			logger.info("process [{}] documents of [{}].", offset, total);
			
			if(sleepTime > 0)
				try {
					TimeUnit.MILLISECONDS.sleep(sleepTime);
				} catch (InterruptedException e) {
					logger.error("exit without finish the task!", e);
					break;
				}
			
		} while(0 != processedCount);
		
		logger.info("process [{}] documents of [{},{}]", offset - jsonRiverConfigure.getSearchFrom(), index, type);
		
		return offset - jsonRiverConfigure.getSearchFrom();
	}
	
    public InputStream getConnectionInputstream(String destUrl, int offset, int size) throws IOException {
        URL url = new URL(destUrl);
        HttpURLConnection connection = null;

        connection = (HttpURLConnection) url.openConnection();
        connection.setUseCaches(false);
        connection.setConnectTimeout(TIMEOUT);
        connection.setReadTimeout(TIMEOUT);
        connection.setRequestMethod("POST");
        connection.setDoOutput(true);
        
        StringBuffer params = new StringBuffer();
        params.append("{")
        	.append("\"from\":").append(offset)
        	.append(",\"size\":").append(size)
        	.append(",\"sort\":\"_uid\"")
        .append("}");
        
        byte[] bytes = params.toString().getBytes();
        connection.getOutputStream().write(bytes);

        if (connection.getResponseCode() != 200) {
            String message = String.format("River endpoint problem for url %s: Connection response code was %s %s", url, connection.getResponseCode(), connection.getResponseMessage());
            throw new ElasticsearchException(message);
        }

        return connection.getInputStream();
    }

    private void closeQuietly(InputStream in) {
    	if(in != null)
			try {
				in.close();
			} catch (IOException e) {}
    }
}
