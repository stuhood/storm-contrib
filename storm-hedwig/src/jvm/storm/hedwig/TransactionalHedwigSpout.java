package storm.hedwig;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseTransactionalSpout;
import backtype.storm.transactional.TransactionAttempt;
import backtype.storm.transactional.ITransactionalSpout;
import backtype.storm.coordination.BatchOutputCollector;
import backtype.storm.tuple.Fields;
import backtype.storm.utils.Utils;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.PeekingIterator;
import com.google.protobuf.ByteString;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.List;
import java.util.Map;
import org.apache.hedwig.client.api.MessageHandler;
import org.apache.hedwig.client.api.Subscriber;
import org.apache.hedwig.client.HedwigClient;
import org.apache.hedwig.protocol.PubSubProtocol;
import org.apache.hedwig.protocol.PubSubProtocol.Message;
import org.apache.hedwig.util.Callback;


public class TransactionalHedwigSpout extends BaseTransactionalSpout<BatchMeta> {
    public static final String ATTEMPT_FIELD = TransactionalHedwigSpout.class.getCanonicalName() + "/attempt";
    
    HedwigConfig _config;
    
    public TransactionalHedwigSpout(HedwigConfig config) {
        _config = config;
    }
    
    class Coordinator implements ITransactionalSpout.Coordinator<BatchMeta> {
        @Override
        public BatchMeta initializeTransaction(java.math.BigInteger txid, BatchMeta prevMetadata) {

        }

        @Override
        public void close() {
        }
    }
    
    class Emitter implements ITransactionalSpout.Emitter<BatchMeta> {
        private final HedwigClient client;
        private final Subscriber subscriber;
        private final ByteString topicId;
        private final ByteString subscriberId;
        private final QueueingMessageHandler messageHandler;
        
        public Emitter() {
            this.client = new HedwigClient(_config.clientConfig);
            this.subscriber = client.getSubscriber();
            this.topicId = ByteString.copyFromUtf8(_config.topic);
            // TODO: completely arbitrary subscriber id: config should specify whether
            // to create/attach an id, and allow it to be specified
            this.subscriberId = ByteString.copyFromUtf8("arbitraryid!-" + this.hashCode());
            // TODO: blocking subscribe: consider async subscribe, followed by subscription
            // confirmation in emitBatch
            this.subscriber.subscribe(
                topicId,
                subscriberId,
                PubSubProtocol.SubscribeRequest.CreateOrAttach.CREATE_OR_ATTACH);

            // begin delivering messages into a bounded buffer
            this.messageHandler =
                new QueueingMessageHandler(
                    new ArrayBlockingQueue<Message>(_config.messageReadaheadCount));
            this.subscriber.startDelivery(topicId, subscriberId, messageHandler);
        }
        
        @Override
        public void cleanupBefore(java.math.BigInteger txid) {
            // TODO: takes a TxId: should probably take <generic>/BatchMeta?
            throw new RuntimeException("FIXME: should clear local buffer up to the given position," +
                " and send a 'consume' message on the hedwig connection");
        }

        /** TODO: MessageSeqId comparison is using only local components: sufficient? */
        @Override
        public BatchMeta emitBatch(TransactionAttempt attempt, BatchMeta lastMeta, BatchOutputCollector collector) {
            if (!subscriber.hasSubscription(topicId, subscriberId))
                // TODO: determine conditions under which this could happen
                throw new RuntimeException("Lost subscription for " + _config.topic);

            // skip to the end of the last batch (past messages waiting for cleanupBefore())
            PeekingIterator<Message> iter =
                Iterators.peekingIterator(messageHandler.queue.iterator());
            while (iter.hasNext() &&
                    iter.peek().getMsgId().getLocalComponent() < lastMeta.nextOffset) {
                iter.next();
            }

            // emit while there are more messages
            if (!iter.hasNext())
                throw new RuntimeException("TODO: does the coordinator trigger batches?" +
                    " does an empty batch cause backoff?");

            long offset = iter.peek().getMsgId().getLocalComponent();
            while (true) {
                Message msg = iter.next();
                if (iter.hasNext()) {
                    // there is at least one more message in the buffer: this one is safe to emit
                    emit(attempt, collector, msg);
                } else {
                    // this is the last message, for now: record it as the first in the next batch
                    long nextOffset = msg.getMsgId().getLocalComponent();
                    return new BatchMeta(offset, nextOffset);
                }
            }
        }
        
        private void emit(TransactionAttempt attempt, BatchOutputCollector collector, Message msg) {
            // TODO: yuck... lots o' copying going on here.
            List<Object> values = _config.scheme.deserialize(msg.getBody().toByteArray());
            List<Object> toEmit = Lists.newArrayListWithCapacity(values.size() + 1);
            toEmit.add(attempt);
            toEmit.addAll(values);
            collector.emit(toEmit);
        }

        @Override
        public void close() {
            subscriber.unsubscribe(topicId, subscriberId);
        }
    }

    /** Consumes messages into the given (bounded) queue. */
    private static class QueueingMessageHandler implements MessageHandler {
        public final BlockingQueue<Message> queue;

        public QueueingMessageHandler(BlockingQueue<Message> queue) {
            this.queue = queue;
        }

        @Override
        public void deliver(ByteString thisTopic, ByteString subscriberId,
                Message msg, Callback<Void> callback, Object context) {
            System.out.println("Got message from src-region: " + msg.getSrcRegion() + " with seq-id: "
                          + msg.getMsgId());
            this.queue.put(msg);
            callback.operationFinished(context, null);
        }
    }

    @Override
    public ITransactionalSpout.Coordinator getCoordinator(Map conf, TopologyContext context) {
        return new Coordinator();
    }

    @Override
    public ITransactionalSpout.Emitter getEmitter(Map conf, TopologyContext context) {
        return new Emitter();
    }    
    
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        List<String> fields = Lists.newArrayList(_config.scheme.getOutputFields().toList());
        fields.add(0, ATTEMPT_FIELD);
        declarer.declare(new Fields(fields));
    }
    
    @Override
    public Map<String, Object> getComponentConfiguration() {
        backtype.storm.Config conf = new backtype.storm.Config();
        conf.registerSerialization(BatchMeta.class);
        return conf;
    }
}
