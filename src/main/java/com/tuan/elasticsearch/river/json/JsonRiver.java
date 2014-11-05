package com.tuan.elasticsearch.river.json;

import static org.elasticsearch.client.Requests.indexRequest;

import java.util.Map;
import java.util.concurrent.LinkedTransferQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TransferQueue;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.StopWatch;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.ESLoggerFactory;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.river.AbstractRiverComponent;
import org.elasticsearch.river.River;
import org.elasticsearch.river.RiverName;
import org.elasticsearch.river.RiverSettings;

import com.tuan.elasticsearch.river.json.input.EsImporter;
import com.tuan.elasticsearch.river.json.input.Importer;
import com.tuan.elasticsearch.river.json.input.RiverProductImport;

public class JsonRiver extends AbstractRiverComponent implements River {

    private final Client client;
    private final JsonRiverConfigure configuration;

    private volatile Thread slurperThread;
    private volatile Thread indexerThread;
    private volatile boolean closed;
    private volatile boolean downloadFinish = false;

    private final TransferQueue<RiverProduct> stream = new LinkedTransferQueue<RiverProduct>();

    @SuppressWarnings("unchecked")
	@Inject
    public JsonRiver(RiverName riverName, RiverSettings riverSettings, Client client) {
        super(riverName, riverSettings);
        this.client = client;
        if (!riverSettings.settings().containsKey("configuration")) {
            throw new IllegalArgumentException("no 'json' settings in river settings?" + settings.settings());
        }
        this.configuration = JsonRiverConfigure.getInstance((Map<String, Object>) riverSettings.settings().get("configuration"));
    }
    
    @Override
    public void start() {
        logger.info("Starting JSON river: configuration [{}}", configuration);

        try {
            slurperThread = EsExecutors.daemonThreadFactory("json_river_slurper").newThread(new Slurper());
            slurperThread.start();
            indexerThread = EsExecutors.daemonThreadFactory("json_river_indexer").newThread(new Indexer());
            indexerThread.start();
        } catch (ElasticsearchException e) {
            logger.error("Error starting indexer and slurper. River is not running", e);
            closed = true;
        }
    }

    @Override
    public void close() {
        if (closed) {
            return;
        }
        logger.info("closing json stream river");
        slurperThread.interrupt();
        indexerThread.interrupt();
        closed = true;
    }

    private class Slurper implements Runnable {

        private final ESLogger logger = ESLoggerFactory.getLogger(this.getClass().getName());

        @Override
        public void run() {
            Importer importer = new EsImporter(configuration, stream);
            logger.debug("Slurper run() started");
            try {
                RiverProductImport result = importer.executeImport();

                logger.info("Slurping [{}] documents with configure [{}]", result, configuration);
                downloadFinish = true;
            } catch (ElasticsearchException e) {
                logger.error("Failed to import data from json stream", e);
                closed = true;
            }
        }
    }


    private class Indexer implements Runnable {
        private final ESLogger logger = ESLoggerFactory.getLogger(this.getClass().getName());
        private int deletedDocuments = 0;
        private int insertedDocuments = 0;
        private BulkRequestBuilder bulk;
        private StopWatch sw;

        @Override
        public void run() {
            while (!closed || !downloadFinish) {
                logger.debug("Indexer run() started");
                sw = new StopWatch().start();
                deletedDocuments = 0;
                insertedDocuments = 0;

                try {
                    RiverProduct product = stream.take();
                    bulk = client.prepareBulk();
                    do {
                        addProductToBulkRequest(product);
                    } while ((product = stream.poll(250, TimeUnit.MILLISECONDS)) != null && deletedDocuments + insertedDocuments < configuration.getBatchCommitCount());
                } catch (InterruptedException e) {
                    continue;
                } finally {
                    bulk.execute().actionGet();
                }
                logStatistics();
            }
        }

        private void addProductToBulkRequest(RiverProduct riverProduct) {
            if (riverProduct.action == RiverProduct.Action.DELETE) {
                //bulk.add(deleteRequest(RIVER_INDEX).type(RIVER_TYPE).id(riverProduct.id));
                logger.info("DELETING {}/{}/{}", riverProduct.index, riverProduct.type, riverProduct.id);
                client.prepareDelete(riverProduct.index, riverProduct.type, riverProduct.id).execute().actionGet();
                deletedDocuments++;
            } else {
                logger.info("INDEXING {}/{}/{}", riverProduct.index, riverProduct.type, riverProduct.id);
                bulk.add(indexRequest(riverProduct.index).type(riverProduct.type).id(riverProduct.id).source(riverProduct.product));
                insertedDocuments++;
            }
        }

        private void logStatistics() {
            long totalDocuments = deletedDocuments + insertedDocuments;
            long totalTimeInSeconds = sw.stop().totalTime().seconds();
            long totalDocumentsPerSecond = (totalTimeInSeconds == 0) ? totalDocuments : totalDocuments / totalTimeInSeconds;
            logger.info("INDEXED {} documents, {} insertions/updates, {} deletions, {} documents per second", totalDocuments, insertedDocuments, deletedDocuments, totalDocumentsPerSecond);
            logger.info("Indexed {} documents, {} insertions/updates, {} deletions, {} documents per second", totalDocuments, insertedDocuments, deletedDocuments, totalDocumentsPerSecond);
        }
    }
}