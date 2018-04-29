package com.vrg.rapid;

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.errorprone.annotations.CanIgnoreReturnValue;
import com.vrg.rapid.messaging.IBroadcaster;
import com.vrg.rapid.messaging.IMessagingClient;
import com.vrg.rapid.pb.RapidRequest;
import com.vrg.rapid.pb.RapidResponse;
import com.vrg.rapid.pb.Endpoint;
import com.vrg.rapid.pb.GossipUpdateMessage;
import com.vrg.rapid.pb.GossipResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;
import java.lang.Math;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.HashMap;
import java.util.concurrent.ThreadLocalRandom;
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
    private static int gossipInterval = 100;
    private static int gossipNodes = 3;
    // GossipToTheDeadTime time.Duration


    private static final Logger LOG = LoggerFactory.getLogger(GossipBroadcaster.class);
    private final Endpoint myAddr;
    private final IMessagingClient messagingClient;
    private final Lock broadcastLock = new ReentrantLock();
    private final Thread gossipThread;
    private boolean shutdown;

    private static class GossipMessageStruct {
        public int numRetransmitsLeft;
        public RapidRequest msg;

        public GossipMessageStruct(final RapidRequest m, final List<Endpoint> recipients) {
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
    private Map<Integer, GossipMessageStruct> sendQueue;
    @GuardedBy("broadcastLock")
    private Map<Integer, GossipMessageStruct> doneQueue;
    @GuardedBy("broadcastLock")
    private List<Endpoint> recipients = Collections.emptyList();

    /**
     * Implements Runnable interface will be responsible for periodically
     * gossiping broadcast messages
     */
    private class GossipBroadcastBackgroundService implements Runnable {

        @Override
        public void run() {
            while (true) {
                if (shutdown) {
                    break;
                }
                broadcastLock.lock();
                try {
                    if (!sendQueue.isEmpty()) {
                        final List<RapidRequest> msgs = new ArrayList<>(sendQueue.size());
                        final Iterator<Map.Entry<Integer, GossipMessageStruct>> it = sendQueue.entrySet().iterator();
                        while (it.hasNext()) {
                            final Map.Entry<Integer, GossipMessageStruct> pair = it.next();
                            if (pair.getValue().numRetransmitsLeft == 0) {
                                doneQueue.put(pair.getKey(), pair.getValue());
                                it.remove();
                            }
                        }
                        sendQueue.forEach((hash, gossipMsgStruct) -> {
                            msgs.add(gossipMsgStruct.msg);
                            gossipMsgStruct.numRetransmitsLeft--;
                        });
                        final GossipUpdateMessage gossipMsg = GossipUpdateMessage.newBuilder()
                                .addAllMessages(msgs)
                                .build();
                        final RapidRequest msgToSend = Utils.toRapidRequest(gossipMsg);
                        Collections.shuffle(recipients, ThreadLocalRandom.current());
                        for (int i = 0; i < Math.min(gossipNodes, recipients.size()); ++i) {
                            Futures.addCallback(messagingClient.sendMessageBestEffort(recipients.get(i), msgToSend),
                                new GossipCallback());
                        }
                    }
                } finally {
                    broadcastLock.unlock();
                    try {
                        Thread.sleep(gossipInterval);
                    } catch (final InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }
        }
    }

    private class GossipCallback implements FutureCallback<RapidResponse> {

        GossipCallback() {
        }

        @Override
        public void onSuccess(@Nullable final RapidResponse response) {
            if (response == null) {
                LOG.trace("Received null gossip response at {}", myAddr);
                return;
            }
            final GossipResponse gossipResponse = response.getGossipResponse();
            for (int i = 0; i < gossipResponse.getKnownGossipsList().size(); i++) {
                if (gossipResponse.getKnownGossips(i)) {
                    final int msgHash = gossipResponse.getGossipHashes(i);
                    if (ThreadLocalRandom.current().nextInt(0, 3) == 0) {
                        broadcastLock.lock();
                        try {
                            if (sendQueue.containsKey(msgHash)) {
                                doneQueue.put(msgHash, sendQueue.get(msgHash));
                                sendQueue.remove(msgHash);
                            }
                        }
                        finally {
                            broadcastLock.unlock();
                        }
                    }
                }
            }
        }

        @Override
        public void onFailure(final Throwable throwable) {
            LOG.trace("Failed gossip message at {} : {}", myAddr, throwable.getLocalizedMessage());
        }


    }

    GossipBroadcaster(final IMessagingClient messagingClient, final Endpoint myAddr) {
        this.messagingClient = messagingClient;
        this.myAddr = myAddr;
        this.sendQueue = new HashMap<>();
        this.doneQueue = new HashMap<>();
        this.shutdown = false;
        this.gossipThread = new Thread(new GossipBroadcastBackgroundService());
        this.gossipThread.start();
    }

    public void shutdown() {
        this.shutdown = true;
        try {
            this.gossipThread.join();
        }
        catch (final InterruptedException e) {
            e.printStackTrace();
        }
    }

    public boolean containsMessage(final int msgHash) {
        broadcastLock.lock();
        try {
            return (sendQueue.containsKey(msgHash) || doneQueue.containsKey(msgHash));
        }
        finally {
            broadcastLock.unlock();
        }
    }

    @Override
    @CanIgnoreReturnValue
    public synchronized List<ListenableFuture<RapidResponse>> broadcast(final RapidRequest msg) {
        broadcastLock.lock();
        try {
            final int hashCode = msg.toString().hashCode();
            if (doneQueue.containsKey(hashCode) || sendQueue.containsKey(hashCode)) {
                return null;
            }
            sendQueue.put(hashCode, new GossipMessageStruct(msg, recipients));
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