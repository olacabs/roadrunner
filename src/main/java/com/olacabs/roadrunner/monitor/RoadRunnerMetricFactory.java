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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;

public class RoadRunnerMetricFactory extends TimerTask implements IRoadrunnerMetric {
	
    private static RoadRunnerMetricFactory instance = new RoadRunnerMetricFactory();

    public static RoadRunnerMetricFactory  getInstance() {
        return instance;
    }

    IRoadrunnerMetric[] plugins = null;

    List<IPullMetric> pullMetrics = new ArrayList<>();

    Timer timer;
    private RoadRunnerMetricFactory() {
        timer = new Timer("metriccollector");//create a new timer
        timer.scheduleAtFixedRate(this, 30000, 60000);//build timer in 30 seconds to gauge metrics in each 1 mins
    }

    public final RoadRunnerMetricFactory register(final IRoadrunnerMetric aPlugin) {
        System.out.println("Registering the plugin: " + aPlugin.getClass().getName());
        if ( null == plugins ) {
            plugins = new IRoadrunnerMetric[]{aPlugin};
        } else {
            IRoadrunnerMetric[] pluginsNew = new IRoadrunnerMetric[plugins.length + 1];
            System.arraycopy(plugins,0,pluginsNew,0, plugins.length);
            pluginsNew[plugins.length] = aPlugin;
            plugins = pluginsNew;
        }
        return this;
    }

    public RoadRunnerMetricFactory deregisterAll() {
        this.plugins = null;
        return this;
    }
    
    @Override
    public final void measureMethodCall(final String methodName, final long timeTakenNano, boolean isSucess) {
        if ( null != plugins) {
            for ( IRoadrunnerMetric aPlugin : plugins) {
                aPlugin.measureMethodCall(methodName, timeTakenNano, isSucess);
            }
        }
    }

    @Override
    public void increment(final String metric, int increment) {
        if ( null != plugins) {
            for ( IRoadrunnerMetric aPlugin : plugins) {
                aPlugin.increment(metric, increment);
            }
        }
    }

    @Override
    public void gauge(final String metricName, long metric) {
        if ( null != plugins) {
            for ( IRoadrunnerMetric aPlugin : plugins) {
                aPlugin.gauge(metricName, metric);
            }
        }
    }

    @Override
    public void printStatus() {
        if ( null != plugins) {
            for (IRoadrunnerMetric aPlugin : plugins) {
                aPlugin.printStatus();
            }
        }
    }

    public RoadRunnerMetricFactory register(IPullMetric metrics) {
        System.out.println("Registering the pull metric: " + metrics.getClass().getName());
        pullMetrics.add(metrics);
        return this;
    }

    @Override
    public void run() {
        for ( IPullMetric pullMetric : pullMetrics) {
            Map<String, Long> metrics = pullMetric.collectMetrics();
            if ( null != plugins) {
                for ( IRoadrunnerMetric aPlugin : plugins) {
                    for (Map.Entry<String, Long> aMetric : metrics.entrySet()) {
                        aPlugin.gauge(aMetric.getKey(), aMetric.getValue());
                    }
                }
            }
        }

    }

	@Override
	public void measureSummary(String elementName, long volume) {
		 if ( null != plugins) {
	            for ( IRoadrunnerMetric aPlugin : plugins) {
	                aPlugin.measureSummary(elementName, volume);
	            }
	        }
		
	}
}
