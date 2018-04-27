package com.vrg.rapid;

import com.vrg.rapid.pb.Endpoint;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.errorprone.annotations.CanIgnoreReturnValue;
import com.vrg.rapid.messaging.IBroadcaster;
import com.vrg.rapid.messaging.IMessagingClient;
import com.vrg.rapid.pb.RapidRequest;
import com.vrg.rapid.pb.RapidResponse;
import com.vrg.rapid.pb.GossipUpdateMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.GuardedBy;
import java.lang.Math;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Simple gossip broadcaster based on memberlist.
 * https://godoc.org/github.com/hashicorp/memberlist
 */
final class GossipBroadcaster implements IBroadcaster {

    // RetransmitMult is the multiplier for the number of retransmissions
    // that are attempted for messages broadcasted over gossip. The actual
    // count of retransmissions is calculated using the formula:
    //
    //   Retransmits = RetransmitMult * log(N+1)
    //
    // This allows the retransmits to scale properly with cluster size. The
    // higher the multiplier, the more likely a failed broadcast is to converge
    // at the expense of increased bandwidth.
    private static int retransmitMult = 4;
    // If the cluster size is too small then this sets the minimum
    // gossip fanout.
    // private static int minRetransmitNum = 2;

    // PushPullInterval is the interval between complete state syncs.
    // Complete state syncs are done with a single node over TCP and are
    // quite expensive relative to standard gossiped messages. Setting this
    // to zero will disable state push/pull syncs completely.
    //
    // Setting this interval lower (more frequent) will increase convergence
    // speeds across larger clusters at the expense of increased bandwidth
    // usage.
    // PushPullInterval time.Duration

    // ProbeInterval and ProbeTimeout are used to configure probing
    // behavior for memberlist.
    //
    // ProbeInterval is the interval between random node probes. Setting
    // this lower (more frequent) will cause the memberlist cluster to detect
    // failed nodes more quickly at the expense of increased bandwidth usage.
    //
    // ProbeTimeout is the timeout to wait for an ack from a probed node
    // before assuming it is unhealthy. This should be set to 99-percentile
    // of RTT (round-trip time) on your network.
    // ProbeInterval time.Duration
    // ProbeTimeout  time.Duration

    // DisableTcpPings will turn off the fallback TCP pings that are attempted
    // if the direct UDP ping fails. These get pipelined along with the
    // indirect UDP pings.
    // private static boolean DisableTcpPings;

    // AwarenessMaxMultiplier will increase the probe interval if the node
    // becomes aware that it might be degraded and not meeting the soft real
    // time requirements to reliably probe other nodes.
    // private static int AwarenessMaxMultiplier;

    // GossipInterval (in ms) and GossipNodes are used to configure the gossip
    // behavior of memberlist.
    //
    // GossipInterval is the interval between sending messages that need
    // to be gossiped that haven't been able to piggyback on probing messages.
    // If this is set to zero, non-piggyback gossip is disabled. By lowering
    // this value (more frequent) gossip messages are propagated across
    // the cluster more quickly at the expense of increased bandwidth.
    //
    // GossipNodes is the number of random nodes to send gossip messages to
    // per GossipInterval. Increasing this number causes the gossip messages
    // to propagate across the cluster more quickly at the expense of
    // increased bandwidth.
    //
    // GossipToTheDeadTime is the interval after which a node has died that
    // we will still try to gossip to it. This gives it a chance to refute.
    private static int gossipInterval = 200;
    private static int gossipNodes = 3;
    // GossipToTheDeadTime time.Duration

    // GossipVerifyIncoming controls whether to enforce encryption for incoming
    // gossip. It is used for upshifting from unencrypted to encrypted gossip on
    // a running cluster.
    // private static boolean gossipVerifyIncoming;

    // GossipVerifyOutgoing controls whether to enforce encryption for outgoing
    // gossip. It is used for upshifting from unencrypted to encrypted gossip on
    // a running cluster.
    // private static boolean gossipVerifyOutgoing;


    private static final Logger LOG = LoggerFactory.getLogger(GossipBroadcaster.class);
    private final IMessagingClient messagingClient;
    private final Lock broadcastLock = new ReentrantLock();
    private final ScheduledFuture<?> gossipJob;

    private static class GossipMessageStruct {
        public int msgId;
        public int numRetransmitsLeft;
        public RapidRequest msg;

        public GossipMessageStruct(final RapidRequest m, final List<Endpoint> recipients) {
            msgId = m.toString().hashCode();
            numRetransmitsLeft =  (int) Math.ceil(retransmitMult * Math.log10((float) recipients.size() + 1));
            switch (m.getContentCase()) {
                case PREJOINMESSAGE:
                    msg = Utils.toRapidRequest(m.getPreJoinMessage());
                    break;
                case JOINMESSAGE:
                    msg = Utils.toRapidRequest(m.getJoinMessage());
                    break;
                case BATCHEDLINKUPDATEMESSAGE:
                    msg = Utils.toRapidRequest(m.getBatchedLinkUpdateMessage());
                    break;
                case GOSSIPUPDATEMESSAGE:
                    msg = Utils.toRapidRequest(m.getGossipUpdateMessage());
                    break;
                case PROBEMESSAGE:
                    msg = Utils.toRapidRequest(m.getProbeMessage());
                    break;
                case FASTROUNDPHASE2BMESSAGE:
                    msg = Utils.toRapidRequest(m.getFastRoundPhase2BMessage());
                    break;
                case PHASE1AMESSAGE:
                    msg = Utils.toRapidRequest(m.getPhase1AMessage());
                    break;
                case PHASE1BMESSAGE:
                    msg = Utils.toRapidRequest(m.getPhase1BMessage());
                    break;
                case PHASE2AMESSAGE:
                    msg = Utils.toRapidRequest(m.getPhase2AMessage());
                    break;
                case PHASE2BMESSAGE:
                    msg = Utils.toRapidRequest(m.getPhase2BMessage());
                    break;
                case CONTENT_NOT_SET:
                default:
                    throw new IllegalArgumentException("Unidentified RapidRequest type " + m.getContentCase());
            }
        }
    }

    @GuardedBy("broadcastLock")
    private List<GossipMessageStruct> sendQueue;
    @GuardedBy("broadcastLock")
    private List<GossipMessageStruct> doneQueue;
    @GuardedBy("broadcastLock")
    private List<Endpoint> recipients = Collections.emptyList();

    /**
     * Implements Runnable interface will be responsible for periodically
     * gossiping broadcast messages
     */
    private class GossipBroadcastBackgroundService implements Runnable {

        @Override
        public void run() {
            broadcastLock.lock();
            try {
                if (!sendQueue.isEmpty()) {
                    final List<RapidRequest> msgs = new ArrayList<RapidRequest>(sendQueue.size());
                    for (Iterator<GossipMessageStruct> it = sendQueue.listIterator(); it.hasNext(); ) {
                        final GossipMessageStruct msgInfo = it.next();
                        if (msgInfo.numRetransmitsLeft == 0) {
                            doneQueue.add(msgInfo);
                            it.remove();
                        }
                    }
                    for (int i = 0; i < sendQueue.size(); ++i) {
                        msgs.add(sendQueue.get(i).msg);
                        sendQueue.get(i).numRetransmitsLeft--;
                    }
                    final GossipUpdateMessage gossipMsg = GossipUpdateMessage.newBuilder()
                            .addAllMessages(msgs)
                            .build();
                    Collections.shuffle(recipients, ThreadLocalRandom.current());
                    for (int i = 0; i < Math.min(gossipNodes, recipients.size()); ++i) {
                        messagingClient.sendMessageBestEffort(recipients.get(i), Utils.toRapidRequest(gossipMsg));
                    }
                }
            } finally {
                broadcastLock.unlock();
            }
        }
    }

    GossipBroadcaster(final IMessagingClient messagingClient, final SharedResources sharedResources) {
        this.messagingClient = messagingClient;
        this.sendQueue = new ArrayList<GossipMessageStruct>();
        this.doneQueue = new ArrayList<GossipMessageStruct>();
        this.gossipJob = sharedResources.getScheduledTasksExecutor()
                            .scheduleAtFixedRate(new GossipBroadcastBackgroundService(),
                0, gossipInterval, TimeUnit.MILLISECONDS);
    }

    public void shutdown() {
        this.gossipJob.cancel(true);
    }

    @Override
    @CanIgnoreReturnValue
    public synchronized List<ListenableFuture<RapidResponse>> broadcast(final RapidRequest msg) {
        broadcastLock.lock();
        try {
            final int hashCode = msg.toString().hashCode();
            for (final GossipMessageStruct msginfo: doneQueue) {
                if (hashCode == msginfo.msgId) {
                    return null;
                }
            }
            for (final GossipMessageStruct msginfo: sendQueue) {
                if (hashCode == msginfo.msgId) {
                    return null;
                }
            }
            sendQueue.add(new GossipMessageStruct(msg, recipients));
        }
        finally {
            broadcastLock.unlock();
        }
        return null;
    }

    @Override
    public synchronized void setMembership(final List<Endpoint> recipients) {
        LOG.trace("setMembership {}", recipients);
        // Randomize the sequence of nodes that will receive a broadcast from this node for each configuration
        final List<Endpoint> arr = new ArrayList<>(recipients);
        Collections.shuffle(arr, ThreadLocalRandom.current());
        broadcastLock.lock();
        try {
            this.recipients = arr;
            this.sendQueue.clear();
            this.doneQueue.clear();
        }
        finally {
            broadcastLock.unlock();
        }
    }
}