package storm.hedwig;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.spout.Scheme;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.transactional.TransactionalTopologyBuilder;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;

public class TestTopology {
    public static class PrinterBolt extends BaseBasicBolt {
        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
        }

        @Override
        public void execute(Tuple tuple, BasicOutputCollector collector) {
            System.out.println(tuple.toString());
        }
    }
    
    public static void main(String [] args) throws Exception {
        HedwigConfig hedwigConf = HedwigConfig.fromURL(new URL("TODO"), "test");
        hedwigConf.scheme = new StringScheme();
        LocalCluster cluster = new LocalCluster();
        TransactionalTopologyBuilder builder = new TransactionalTopologyBuilder("id", "spout",
                new TransactionalHedwigSpout(hedwigConf), 1);
        builder.setBolt("printer", new PrinterBolt())
                .shuffleGrouping("spout");
        Config config = new Config();
        
        cluster.submitTopology("hedwig-test", config, builder.buildTopology());
        
        Thread.sleep(600000);
    }

    public static class StringScheme implements Scheme {
        public List<Object> deserialize(byte[] bytes) {
            // nb: assuming utf-8: it's only a test
            return new Values(new String(bytes));
        }

        public Fields getOutputFields() {
            return new Fields("str");
        }
    }
}
