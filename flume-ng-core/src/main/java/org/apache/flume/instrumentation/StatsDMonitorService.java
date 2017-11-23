package org.apache.flume.instrumentation;


import java.net.InetAddress;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.flume.Context;
import org.apache.flume.instrumentation.util.JMXPollUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.timgroup.statsd.NonBlockingStatsDClient;
import com.timgroup.statsd.StatsDClient;

public class StatsDMonitorService implements MonitorService {

    private static final Logger logger = LoggerFactory
            .getLogger(StatsDMonitorService.class);
    protected final StatsDCollector metricsCollector;
    private ScheduledExecutorService service = Executors
            .newSingleThreadScheduledExecutor();
    private StatsDClient statsd;
    private final String CONF_STATSD_SERVER = "statsdServer";
    private final String CONF_STATSD_PORT = "statsdPort";
    private final String CONF_POLL_FREQUENCY = "pollFrequency";
    private final String CONF_STATSD_AGENT = "agent";
    private int pollFrequency = 60;

    public StatsDMonitorService() {
        metricsCollector = new StatsDCollector();
    }

    @Override
    public void start() {
        service.scheduleWithFixedDelay(metricsCollector, 0, pollFrequency,
                TimeUnit.SECONDS);
    }

    @Override
    public void stop() {
        try {
            statsd.stop();
        } catch (Exception ex) {
            logger.error("failed to stop statsd client", ex);
        }
    }

    @Override
    public void configure(Context context) {
        try {
            String hostName = InetAddress.getLocalHost().getHostName().split("\\.")[0];
            String agent = context.getString(CONF_STATSD_AGENT);
            String metricsPrefix = "flume." + hostName + "." + agent;
            pollFrequency = context.getInteger(CONF_POLL_FREQUENCY);
            String statsdServer = context.getString(CONF_STATSD_SERVER);
            int statsdPort = context.getInteger(CONF_STATSD_PORT, 8125);
            statsd =
                    new NonBlockingStatsDClient(metricsPrefix, statsdServer, statsdPort);
        } catch (Exception ex) {
            logger.error("failed to configure due to:", ex);
        }
    }

    protected class StatsDCollector implements Runnable {
        @Override
        public void run() {
            try {
                Map<String, Map<String, String>> metricsMap =
                        JMXPollUtil.getAllMBeans();
                for (String component : metricsMap.keySet()) {
                    Map<String, String> attributeMap = metricsMap.get(component);
                    for (String attribute : attributeMap.keySet()) {
                        try {
                            long value = Long.parseLong(attributeMap.get(attribute));
                            statsd.recordGaugeValue(component + "." + attribute, value);
                        } catch (Exception ex) {
//              logger.warn("value of " + attribute + " is not long:"
//                  + attributeMap.get(attribute) + ex);
                        }
                    }
                }
            } catch (Throwable t) {
                logger.error("Unexpected error", t);
            }
        }
    }
}
