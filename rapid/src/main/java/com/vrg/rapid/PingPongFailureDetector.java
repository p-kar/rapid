/*
 * Copyright © 2016 - 2017 VMware, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the “License”); you may not use this file
 * except in compliance with the License. You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an “AS IS” BASIS, without warranties or conditions of any kind,
 * EITHER EXPRESS OR IMPLIED. See the License for the specific language governing
 * permissions and limitations under the License.
 */

package com.vrg.rapid;

import com.google.common.net.HostAndPort;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.SettableFuture;
import com.vrg.rapid.monitoring.ILinkFailureDetectorFactory;
import com.vrg.rapid.pb.NodeStatus;
import com.vrg.rapid.pb.ProbeMessage;
import com.vrg.rapid.pb.ProbeResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Represents a simple ping-pong failure detector. It is also aware of nodes that are added to the cluster
 * but are still bootstrapping.
 */
@NotThreadSafe
public class PingPongFailureDetector implements Runnable {
    private static final Logger LOG = LoggerFactory.getLogger(PingPongFailureDetector.class);
    private static final int FAILURE_THRESHOLD = 10;

    // Number of BOOTSTRAPPING status responses a node is allowed to return before we begin
    // treating that as a failure condition.
    private static final int BOOTSTRAP_COUNT_THRESHOLD = 30;
    private final HostAndPort address;
    private final Map<HostAndPort, AtomicInteger> failureCount;
    private final Map<HostAndPort, AtomicInteger> bootstrapResponseCount;
    private final RpcClient rpcClient;
    private final Map<HostAndPort, SettableFuture<Void>> monitoreeNotifiers;
    private final Map<HostAndPort, ProbeCallback> probeCallbacks;

    // A cache for probe messages. Avoids creating an unnecessary copy of a probe message each time.
    private final HashMap<HostAndPort, ProbeMessage> messageHashMap;

    private PingPongFailureDetector(final HostAndPort address, final RpcClient rpcClient,
                                    final Map<HostAndPort, SettableFuture<Void>> monitoreeNotifiers) {
        this.address = address;
        this.failureCount = new ConcurrentHashMap<>(monitoreeNotifiers.size());
        this.bootstrapResponseCount = new ConcurrentHashMap<>(monitoreeNotifiers.size());
        this.messageHashMap = new HashMap<>(monitoreeNotifiers.size());
        this.probeCallbacks = new ConcurrentHashMap<>(monitoreeNotifiers.size());
        this.rpcClient = rpcClient;
        final ProbeMessage.Builder builder = ProbeMessage.newBuilder();
        for (final Map.Entry<HostAndPort, SettableFuture<Void>> entry: monitoreeNotifiers.entrySet()) {
            failureCount.put(entry.getKey(), new AtomicInteger(0));
            messageHashMap.putIfAbsent(entry.getKey(), builder.setSender(address.toString()).build());
            probeCallbacks.put(entry.getKey(), new ProbeCallback(entry.getKey(), entry.getValue()));
        }
        this.monitoreeNotifiers = monitoreeNotifiers;
    }

    // Executed at monitor
    private void handleProbeOnSuccess(final HostAndPort monitoree) {
        if (!failureCount.containsKey(monitoree)) {
            LOG.trace("handleProbeOnSuccess at {} heard from a node we are not assigned to ({})", address, monitoree);
        }
        LOG.trace("handleProbeOnSuccess at {} from {}", address, monitoree);
    }

    // Executed at monitor
    private void handleProbeOnFailure(final Throwable throwable,
                                      final HostAndPort monitoree) {
        if (!failureCount.containsKey(monitoree)) {
            LOG.trace("handleProbeOnSuccess at {} heard from a node we are not assigned to ({})", address, monitoree);
        }
        failureCount.get(monitoree).incrementAndGet();
        LOG.trace("handleProbeOnFailure at {} from {}: {}", address, monitoree, throwable.getLocalizedMessage());
    }

    // Executed at monitor
    private boolean hasFailed(final HostAndPort monitoree) {
        if (!failureCount.containsKey(monitoree)) {
            LOG.trace("handleProbeOnSuccess at {} heard from a node we are not assigned to ({})",
                       address, monitoree);
        }
        return failureCount.get(monitoree).get() >= FAILURE_THRESHOLD;
    }

    @Override
    public void run() {
        monitoreeNotifiers.forEach((monitoree, notifierFuture) -> {
            LOG.trace("{} sending probe to {}", address, monitoree);
            final ProbeMessage probeMessage = messageHashMap.get(monitoree);
            Futures.addCallback(rpcClient.sendProbeMessage(monitoree, probeMessage), probeCallbacks.get(monitoree));
        });
    }

    private class ProbeCallback implements FutureCallback<ProbeResponse> {
        final HostAndPort monitoree;
        final SettableFuture<Void> failureNotifier;

        ProbeCallback(final HostAndPort monitoree, final SettableFuture<Void> completionEvent) {
            this.monitoree = monitoree;
            this.failureNotifier = completionEvent;
        }

        @Override
        public void onSuccess(@Nullable final ProbeResponse probeResponse) {
            if (probeResponse == null) {
                handleProbeOnFailure(new RuntimeException("null probe response received"), monitoree);
                return;
            }
            if (probeResponse.getStatus().equals(NodeStatus.BOOTSTRAPPING)) {
                final int numBootstrapResponses = bootstrapResponseCount.computeIfAbsent(monitoree,
                        (k) -> new AtomicInteger(0)).incrementAndGet();
                if (numBootstrapResponses > BOOTSTRAP_COUNT_THRESHOLD) {
                    handleProbeOnFailure(new RuntimeException("BOOTSTRAP_COUNT_THRESHOLD exceeded"), monitoree);
                    return;
                }
            }
            handleProbeOnSuccess(monitoree);
        }

        @Override
        public void onFailure(final Throwable throwable) {
            try {
                handleProbeOnFailure(throwable, monitoree);
            } finally {
                if (hasFailed(monitoree)) {
                    failureNotifier.set(null);
                }
            }
        }
    }

    static class Factory implements ILinkFailureDetectorFactory {
        private final HostAndPort address;
        private final RpcClient rpcClient;

        Factory(final HostAndPort address, final RpcClient rpcClient) {
            this.address = address;
            this.rpcClient = rpcClient;
        }

        @Override
        public Runnable getInstance(final Map<HostAndPort, SettableFuture<Void>> perMonitoreeNotifier) {
            return new PingPongFailureDetector(address, rpcClient, perMonitoreeNotifier);
        }
    }
}
