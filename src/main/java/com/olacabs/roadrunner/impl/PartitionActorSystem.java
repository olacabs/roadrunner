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

import com.olacabs.roadrunner.monitor.RoadRunnerMetricFactory;
import com.olacabs.roadrunner.utils.Logger;
import com.olacabs.roadrunner.utils.LoggerFactory;

public class PartitionActorSystem {

    public static int CLEANUP_RECORDS_INTERVAL = 5000;
    public static int CHECKSUM_RECORDS_INTERVAL = 50000;

    private static class PartitionActorQueueConsumer implements Runnable {

        ArrayBlockingQueue<PartitionBucketMessage> queue = null;
        static Logger logger = LoggerFactory.getLogger(PartitionActorQueueConsumer.class);
        int partition = 99;
        int cleanupInterval = 0;
        int checksumInterval = 0;
        boolean isDebug = logger.isDebugEnabled();

        private PartitionActorQueueConsumer() {
        }

        private PartitionActorQueueConsumer(ArrayBlockingQueue<PartitionBucketMessage> queue, int partition) {
            this.queue = queue;
            this.partition = partition;
        }

        @Override
        public void run() {
            PartitionBucketMessage message = null;
            while (true) {
                try {

                    metrics.gauge("s2queue_size", queue.size());
                    message = queue.poll();
                    if(message == null) {

                        if ( isDebug ) metrics.increment("s2queue_" + partition + "_empty", 1);
                        continue;

                    } else {

                        metrics.increment("s2queue_polled", 1);

                        if ( isDebug ) {
                            metrics.increment("s2queue_" + partition + "_polled", 1);
                            metrics.increment("s2queue_" + partition + "_polled_" + Thread.currentThread().getName() , 1);
                        }

                        tellToPartitionBucket(message);

                    }

                } catch (Exception ex) {
                    metrics.increment("s2queue_exception", 1);
                    metrics.increment("s2queue_exception_" + message.type.name(), 1);

                    if ( isDebug )  metrics.increment("s2queue_" + partition + "_exception", 1);

                    logger.error("Error while processing message : {} ",  message, ex);
                }
            }
        }

        private void tellToPartitionBucket(PartitionBucketMessage message) throws Exception {
            cleanupInterval++;
            checksumInterval++;

            if ( cleanupInterval > CLEANUP_RECORDS_INTERVAL ) {
                try {
                    message.partitionBucket.onReceive(new PartitionBucketMessage(message.partitionBucket).cleanup());
                } catch (Exception ex) {
                    logger.error("Error while cleanup {} ", message, ex);
                    metrics.increment("s2queue_cleanupfailed", 1);
                } finally {
                    cleanupInterval = 0;
                    isDebug = logger.isDebugEnabled();
                }
            }

            if ( checksumInterval > CHECKSUM_RECORDS_INTERVAL ) {
                try {
                    message.partitionBucket.onReceive(new PartitionBucketMessage(message.partitionBucket).checksum());
                } catch (Exception ex) {
                    logger.error("Error while processing checksum : {} ",  message, ex);
                    metrics.increment("s2queue_checksumfailed", 1);
                } finally {
                    checksumInterval = 0;
                }
            }

            message.partitionBucket.onReceive(message);
        }
    }

    private static RoadRunnerMetricFactory metrics = RoadRunnerMetricFactory.getInstance();
    static Logger logger = LoggerFactory.getLogger(PartitionActorSystem.class.getName());

    private boolean writeAmplifyEnabled = false;
    private int writeAmplification = 1;
    private int queueSize = 10000;
    private ArrayBlockingQueue<PartitionBucketMessage>[] blockingQueues = null;
    private PartitionActorQueueConsumer nonParallel = new PartitionActorQueueConsumer();
    private Thread[] queueConsumersThreads = null;


    protected PartitionActorSystem(int parallel, int queueSize) {
	    	if(parallel < 1) {
				this.writeAmplifyEnabled = false;
				return;
			}
			this.writeAmplifyEnabled = true;
			this.queueSize = queueSize;
	    int cores = Runtime.getRuntime().availableProcessors();
	    logger.info("TEMP: Number of cores: {}, parallelism factor: {}", cores, parallel);
	    this.writeAmplification = ( parallel >= cores/2 ) ? cores/2 : parallel;
	    logger.info("Write amplification set at {}", this.writeAmplification);
    }

    private void init() {

        if ( ! this.writeAmplifyEnabled ) {
            logger.info("Short circuiting the PartitionBucket Actor queue write.");
            return;
        }

        if ( null != this.blockingQueues) {
            throw new RuntimeException("PartitionBucket Queue is already initialized.");
        }

        logger.info("Initializing partition partitionBucketSystem {}", this.writeAmplification);

        this.blockingQueues = new ArrayBlockingQueue[this.writeAmplification];
        this.queueConsumersThreads = new Thread[this.writeAmplification];

        for (int i = 0; i< this.writeAmplification; i++) {

            final ArrayBlockingQueue<PartitionBucketMessage> blockingQueue = new ArrayBlockingQueue(this.queueSize);
            PartitionActorQueueConsumer queueConsumer = new PartitionActorQueueConsumer(blockingQueue, i);
            this.blockingQueues[i] = blockingQueue;
            this.queueConsumersThreads[i] = new Thread(queueConsumer);
            this.queueConsumersThreads[i].setName("PartitionSystem-" + i);
            this.queueConsumersThreads[i].start();
        }
        logger.info("Partition partitionBucketSystem initialized {} , blockingQueueConsumers {} ",
        		this.writeAmplification, this.queueConsumersThreads);
    }

    public boolean put(String partitionValue, PartitionBucketMessage message) throws Exception {
    		
        if ( null == partitionValue) {
            metrics.increment("s2queue_nullrecord", 1);
            return false;
        }

        /**
         * Non parallelized execution
         */
        if ( ! this.writeAmplifyEnabled ) {
        		this.nonParallel.tellToPartitionBucket(message);
            return true;
        }

        /**
         * Parallel Execution - Lazy initializaton
         */
        if ( null == this.blockingQueues) {
            synchronized (PartitionActorSystem.class.getName()) {
                if ( null == this.blockingQueues) init();
            }
        }

        int partition =  (this.writeAmplification == 1 ) ? 0 : (partitionValue.hashCode() & Integer.MAX_VALUE) % this.writeAmplification;

        metrics.increment("s2queue_pushed", 1);
        metrics.increment("s2queue_" + partition + "_pushed", 1);
        this.blockingQueues[partition].put(message);
        return true;
    }
}
