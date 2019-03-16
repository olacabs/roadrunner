//************************************************************
// Copyright 2019 ANI Technologies Pvt. Ltd.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//************************************************************
package com.olacabs.roadrunner.impl;

import java.util.concurrent.ArrayBlockingQueue;

import com.olacabs.roadrunner.api.Record;
import com.olacabs.roadrunner.monitor.RoadRunnerMetricFactory;
import com.olacabs.roadrunner.utils.Logger;
import com.olacabs.roadrunner.utils.LoggerFactory;

public class RecordActorSystem {

    private GeoSpatialStoreImpl store;
    private static class RecordQueueConsumer implements Runnable {

    		private GeoSpatialStoreImpl store;
        ArrayBlockingQueue<Record> queue = null;
        static Logger logger = LoggerFactory.getLogger(RecordQueueConsumer.class);
        int partition = 99;
        boolean isDebug = logger.isDebugEnabled();

        private RecordQueueConsumer() {
        }

        private RecordQueueConsumer(GeoSpatialStoreImpl store, ArrayBlockingQueue<Record> queue, int partition) {
	        	this.store = store;
	        	this.queue = queue;
	        	this.partition = partition;
        }

        @Override
        public void run() {
            Record record = null;
            while (true) {
                try {

                    metrics.gauge("recordqueue_size", queue.size());
                    record = queue.poll();
                    if(record == null) {

                        if ( isDebug ) metrics.increment("recordqueue_" + partition + "_empty", 1);
                        continue;

                    } else {

                        metrics.increment("recordqueue_polled", 1);

                        if ( isDebug ) {
                            metrics.increment("recordqueue_" + partition + "_polled", 1);
                            metrics.increment("recordqueue_" + partition + "_polled_" + Thread.currentThread().getName() , 1);
                        }

                        tellToStore(record);

                    }

                } catch (Exception ex) {
                    metrics.increment("recordqueue_exception", 1);
                    if ( isDebug )  metrics.increment("recordqueue_" + partition + "_exception", 1);
                    logger.error("Error while processing message : {} ",  ex.getMessage(), ex);
                }
            }
        }

        private void tellToStore(Record record) throws Exception {
            store.onReceiveUpsert(record);
        }
    }

    private static RoadRunnerMetricFactory metrics = RoadRunnerMetricFactory.getInstance();
    static Logger logger = LoggerFactory.getLogger(RecordActorSystem.class.getName());

    private boolean writeAmplifyEnabled = true;
    private int writeAmplification = 1;
    private int queueSize = 10000;
    private ArrayBlockingQueue<Record>[] blockingQueues = null;
    private RecordQueueConsumer nonParallel = new RecordQueueConsumer();
    private Thread[] queueConsumersThreads = null;

    protected RecordActorSystem(GeoSpatialStoreImpl aStore, int parallel, int queueSize) {
	    	this.store = aStore;
	    	this.writeAmplifyEnabled = true;
	    	this.queueSize = queueSize;
	    	int cores = Runtime.getRuntime().availableProcessors();
	    	this.writeAmplification = ( parallel >= cores/2 ) ? cores/2 : parallel;
	    	if (this.writeAmplification < 1) this.writeAmplification = 1;
	
	    	logger.info("Write amplification set at {}", this.writeAmplification);
    }

    private void init() {

        if ( null != this.blockingQueues) {
            throw new RuntimeException("Record Queue is already initialized.");
        }

        logger.info("Initializing partition recordSystem {}", this.writeAmplification);

        this.blockingQueues = new ArrayBlockingQueue[this.writeAmplification];
        this.queueConsumersThreads = new Thread[this.writeAmplification];

        for (int i = 0; i< this.writeAmplification; i++) {

            final ArrayBlockingQueue<Record> blockingQueue = new ArrayBlockingQueue(this.queueSize);
            RecordQueueConsumer queueConsumer = new RecordQueueConsumer(store, blockingQueue, i);
            this.blockingQueues[i] = blockingQueue;
            this.queueConsumersThreads[i] = new Thread(queueConsumer);
            this.queueConsumersThreads[i].setName("RecordPartitionSystem-" + i);
            this.queueConsumersThreads[i].start();
        }
        logger.info("Partition recordSystem initialized {} , blockingQueueConsumers {} ",
        		this.writeAmplification, this.queueConsumersThreads);
    }

    public boolean put(String recordId, Record record, boolean isOKToMiss) throws Exception {

        if ( null == recordId) {
            metrics.increment("recordqueue_nullrecord", 1);
            return false;
        }

        /**
         * Parallel Execution - Lazy initializaton
         */
        if ( null == this.blockingQueues) {
            synchronized (RecordActorSystem.class.getName()) {
                if ( null == this.blockingQueues) init();
            }
        }

        int partition = (this.writeAmplification == 1 ) ? 0 : (recordId.hashCode() & Integer.MAX_VALUE) % this.writeAmplification;

        metrics.increment("recordqueue_pushed", 1);
        metrics.increment("recordqueue_" + partition + "_pushed", 1);


        ArrayBlockingQueue<Record> queue = blockingQueues[partition];
        boolean success = queue.offer(record);
        if(success) return success;

        metrics.increment("recordqueue_full", 1);
        logger.error("recordqueue_full", 60000, "Not able to offer a record, May happen at startup : " + record + ", recordid : " + recordId);
    
        if ( isOKToMiss ) return false;
        else queue.put(record);
        return true;
    }
}
