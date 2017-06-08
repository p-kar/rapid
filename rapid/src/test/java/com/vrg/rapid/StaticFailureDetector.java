package com.vrg.rapid;

import com.google.common.net.HostAndPort;
import com.google.common.util.concurrent.SettableFuture;
import com.vrg.rapid.monitoring.ILinkFailureDetectorFactory;

import java.util.Map;
import java.util.Set;

/**
 * Used for testing.
 */
class StaticFailureDetector implements Runnable {
    private final Set<HostAndPort> blackList;
    private final Map<HostAndPort, SettableFuture<Void>> perMonitoreeNotifier;

    /**
     * The factory passes the blacklist over by reference and modifies it to influence failure detections by
     * the StaticFailureDetector instances.
     */
    private StaticFailureDetector(final Set<HostAndPort> blackList,
                                  final Map<HostAndPort, SettableFuture<Void>> perMonitoreeNotifier) {
        this.blackList = blackList;
        this.perMonitoreeNotifier = perMonitoreeNotifier;
    }

    @Override
    public void run() {
        this.perMonitoreeNotifier.forEach((monitoree, notifier) -> {
            if (blackList.contains(monitoree)) {
                perMonitoreeNotifier.get(monitoree).set(null);
            }
        });
    }

    static class Factory implements ILinkFailureDetectorFactory {
        private final Set<HostAndPort> blackList;

        Factory(final Set<HostAndPort> blackList) {
            this.blackList = blackList;
        }

        @Override
        public Runnable getInstance(final Map<HostAndPort, SettableFuture<Void>> perMonitoreeNotifier) {
            return new StaticFailureDetector(blackList, perMonitoreeNotifier);
        }

        public void addFailedNodes(final Set<HostAndPort> nodes) {
            blackList.addAll(nodes);
        }
    }
}