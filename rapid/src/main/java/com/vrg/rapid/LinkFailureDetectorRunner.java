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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

/**
 * A runnable that periodically executes a failure detector. In the future, the frequency of invoking this
 * function may be left to the LinkFailureDetector object itself.
 */
class LinkFailureDetectorRunner {
    static int FAILURE_DETECTOR_INTERVAL_IN_MS = 1000;
    static int FAILURE_DETECTOR_INITIAL_DELAY_IN_MS = 2000;
    private static final Logger LOG = LoggerFactory.getLogger(LinkFailureDetectorRunner.class);
    private final List<Consumer<HostAndPort>> linkFailureSubscriptions = new ArrayList<>();
    private final ScheduledExecutorService executorService;
    private final ILinkFailureDetectorFactory linkFailureDetectorFactory;
    @Nullable private ScheduledFuture<?> runningTask = null;

    LinkFailureDetectorRunner(final ILinkFailureDetectorFactory linkFailureDetectorFactory,
                              final ScheduledExecutorService executorService) {
        this.linkFailureDetectorFactory = linkFailureDetectorFactory;
        this.executorService = executorService;
    }

    /**
     * MembershipService invokes this whenever the set of monitorees to watch changes.
     *
     * @param newMonitorees the new set of monitorees for this node.
     */
    void updateMembership(final List<HostAndPort> newMonitorees) {
        final Map<HostAndPort, SettableFuture<Void>> notifiers = new ConcurrentHashMap<>(newMonitorees.size());
        newMonitorees.forEach(m -> {
            final SettableFuture<Void> notification = SettableFuture.create();
            Futures.addCallback(notification, new FailureCallback(m));
                notifiers.put(m, notification);
            });
        if (runningTask != null) {
            runningTask.cancel(true);
        }
        runningTask = executorService.scheduleAtFixedRate(linkFailureDetectorFactory.getInstance(notifiers),
                                                          FAILURE_DETECTOR_INITIAL_DELAY_IN_MS,
                                                          FAILURE_DETECTOR_INTERVAL_IN_MS, TimeUnit.MILLISECONDS);
    }

    /**
     * Register subscribe to link failed notifications.
     */
    void registerSubscription(final Consumer<HostAndPort> consumer) {
        linkFailureSubscriptions.add(consumer);
    }


    private class FailureCallback implements FutureCallback<Void> {
        private final HostAndPort node;

        FailureCallback(final HostAndPort node) {
            this.node = node;
        }

        @Override
        public void onSuccess(@Nullable final Void aVoid) {
            linkFailureSubscriptions.forEach(subscriber -> subscriber.accept(node));
        }

        @Override
        public void onFailure(final Throwable throwable) {
            throw new IllegalStateException("Should not happen");
        }
    }
}
