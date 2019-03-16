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
package com.olacabs.roadrunner.monitor;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class RoadrunnerMetricLocal implements IRoadrunnerMetric {

    public static class MethodCallTiming {
        AtomicLong timeTakenInNanoSucess = new AtomicLong();
        AtomicLong timeTakenInNanoFailure = new AtomicLong();
        AtomicInteger countSucess = new AtomicInteger();
        AtomicInteger countFailure = new AtomicInteger();

        void reset() {
            countSucess.set(0);
            countFailure.set(0);
            timeTakenInNanoSucess.set(0);
            timeTakenInNanoFailure.set(0);
        }
    }

    public static class VariableCounter {
        AtomicLong value = new AtomicLong();
        AtomicInteger collected = new AtomicInteger();
    }

    public Map<String, MethodCallTiming> methodCalls = new ConcurrentHashMap();
    public Map<String, AtomicLong> counters = new ConcurrentHashMap();
    public Map<String, VariableCounter> _collected = new ConcurrentHashMap();

    @Override
    public void measureMethodCall(String methodName, long timeTakenNano, boolean isSucess) {

        MethodCallTiming timing = methodCalls.get(methodName);
        if ( null == timing) methodCalls.put(methodName, new MethodCallTiming());
        timing = methodCalls.get(methodName);

        if ( isSucess ) {
            timing.countSucess.incrementAndGet();
            timing.timeTakenInNanoSucess.addAndGet(timeTakenNano);
        } else {
            timing.countFailure.incrementAndGet();
            timing.timeTakenInNanoFailure.addAndGet(timeTakenNano);
        }
    }

    @Override
    public void increment(String metric, int increment) {
        AtomicLong val = counters.get(metric);
        if ( null == val) {
            synchronized (metric) {
                if ( null == counters.get(metric)) {
                    counters.put(metric, new AtomicLong());
                }
            }
            val = counters.get(metric);
        }
        val.incrementAndGet();
    }

    @Override
    public void gauge(String metricName, long metric) {

        VariableCounter metricVariable = _collected.get(metricName);
        if ( null == metricVariable) _collected.put(metricName, new VariableCounter());
        metricVariable = _collected.get(metricName);
        metricVariable.collected.incrementAndGet();
        metricVariable.value.addAndGet(metric);
    }

    @Override
    public void printStatus() {

        StringBuilder sb = new StringBuilder(8092);
        for (Map.Entry<String, AtomicLong> entry : counters.entrySet()) {
            sb.append(entry.getKey()).append(":").append(entry.getValue().longValue()).append('\n');
            entry.getValue().set(0);
        }

        for ( Map.Entry<String, MethodCallTiming> entry: methodCalls.entrySet()) {
            int countFailure = entry.getValue().countFailure.get();
            sb.append(entry.getKey()  + "failurescount:").append(countFailure).append('\n');
            sb.append(entry.getKey() + "failuresnano:").append(entry.getValue().timeTakenInNanoFailure.get()).append('\n');

            int countSucess = entry.getValue().countSucess.get();
            sb.append(entry.getKey()  + "sucesscount:").append(countSucess).append('\n');
            long timeInNano = entry.getValue().timeTakenInNanoSucess.get();
            sb.append(entry.getKey() + "sucessnano:").append(timeInNano).append('\n');

            if ( timeInNano > 0 ) {
                Double rps = (1000000000.0 * countSucess / timeInNano);
                sb.append(entry.getKey() + "sucessrps:").append( rps.intValue() ).append('\n');
            }

            if ( countSucess > 0 ) {
                Long avg = (timeInNano / countSucess);
                sb.append(entry.getKey() + "avgnano:").append(avg).append('\n');
            }

            entry.getValue().reset();
        }

        for ( Map.Entry<String, VariableCounter> entry: _collected.entrySet()) {
            int collectedTimes = entry.getValue().collected.get();
            if(collectedTimes > 0) {
            		sb.append(entry.getKey() + ":").append(entry.getValue().value.get() / collectedTimes).append('\n');
            }
            entry.getValue().collected.set(0);
            entry.getValue().value.set(0);
        }

        System.out.println(sb.toString());

    }

	@Override
	public void measureSummary(String elementName, long volume) {
		// TODO Auto-generated method stub
		
	}

}
