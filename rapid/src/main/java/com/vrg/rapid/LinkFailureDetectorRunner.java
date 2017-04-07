package com.vrg.rapid;


import com.google.common.net.HostAndPort;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.vrg.rapid.monitoring.ILinkFailureDetector;
import com.vrg.rapid.pb.ProbeMessage;
import com.vrg.rapid.pb.ProbeResponse;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.GuardedBy;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.function.Consumer;

/**
 * A runnable that periodically executes a failure detector. In the future, the frequency of invoking this
 * function may be left to the LinkFailureDetector object itself.
 */
public class LinkFailureDetectorRunner implements Runnable {
    private static final Logger LOG = LoggerFactory.getLogger(LinkFailureDetectorRunner.class);
    @GuardedBy("this") private Set<HostAndPort> monitorees = Collections.emptySet();
    @GuardedBy("this") private final Set<HostAndPort> faultyLinks = new HashSet<>();
    private final ILinkFailureDetector linkFailureDetector;
    private final RpcClient rpcClient;
    private final List<Consumer<HostAndPort>> linkFailureSubscriptions = new ArrayList<>();

    LinkFailureDetectorRunner(final ILinkFailureDetector linkFailureDetector,
                              final RpcClient rpcClient) {
        this.linkFailureDetector = linkFailureDetector;
        this.rpcClient = rpcClient;
    }

    /**
     * MembershipService invokes this whenever the set of monitorees to watch changes.
     *
     * @param newMonitorees the new set of monitorees for this node.
     */
    synchronized void updateMembership(final List<HostAndPort> newMonitorees) {
        this.monitorees = new HashSet<>(newMonitorees);
        this.faultyLinks.clear();
        rpcClient.updateLongLivedConnections(this.monitorees);
        this.linkFailureDetector.onMembershipChange(newMonitorees);
    }

    /**
     * Mark an edge as faulty. This is used by MembershipService for failure notification
     * reinforcement.
     *
     * @param faulty The edge to this node will be marked as faulty.
     */
    synchronized void markAsFaulty(final HostAndPort faulty) {
        faultyLinks.add(faulty);
    }

    /**
     * Receive a probe message from a remote failure detector.
     */
    void handleProbeMessage(final ProbeMessage probeMessage,
                            final StreamObserver<ProbeResponse> probeResponseObserver) {
        linkFailureDetector.handleProbeMessage(probeMessage, probeResponseObserver);
    }

    /**
     * Register subscribe to link failed notifications.
     */
    void registerSubscription(final Consumer<HostAndPort> consumer) {
        linkFailureSubscriptions.add(consumer);
    }

    /**
     * For every monitoree, first checkMonitoree if the link has failed. If not, send out a probe request
     * and handle the onSuccess and onFailure callbacks. If a link has failed, inform the MembershipService.
     */
    @Override
    public synchronized void run() {
        try {
            if (monitorees.size() == 0) {
                return;
            }
            final List<ListenableFuture<Void>> healthChecks = new ArrayList<>();
            for (final HostAndPort monitoree : monitorees) {
                if (linkFailureDetector.hasFailed(monitoree) || faultyLinks.contains(monitoree)) {
                    // Informs MembershipService and other subscribers, if any, about the failure.
                    linkFailureSubscriptions.forEach(subscriber -> subscriber.accept(monitoree));
                } else {
                    // Node is up, so send it a probe and attach the callbacks.
                    final ListenableFuture<Void> check = linkFailureDetector.checkMonitoree(monitoree);
                    healthChecks.add(check);
                }
            }

            // Failed requests will have their onFailure() events called. So it is okay to
            // only block for the successful ones here.
            Futures.successfulAsList(healthChecks).get();
        }
        catch (final ExecutionException | StatusRuntimeException e) {
            LOG.error("Potential link failures: some probe messages have failed.");
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
