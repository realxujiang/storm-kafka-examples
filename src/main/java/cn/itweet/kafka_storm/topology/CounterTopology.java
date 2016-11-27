package cn.itweet.kafka_storm.topology;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.topology.TopologyBuilder;
import cn.itweet.kafka_storm.bolt.CheckOrderBolt;
import cn.itweet.kafka_storm.bolt.CounterBolt;
import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.StringScheme;
import storm.kafka.ZkHosts;

public class CounterTopology {

	/**
	 * @param args
	 * http://www.programcreek.com/java-api-examples/index.php?api=storm.kafka.KafkaSpout
	 */
	public static void main(String[] args) {
		try{
			//设置喷发节点并分配并发数，该并发数将会控制该对象在集群中的线程数（6个）
			String zkhost = "wxb-1:2181,wxb-2:2181,wxb-3:2181";
			String topic = "order";
			String groupId = "id";
			int spoutNum = 3;
			int boltNum = 1;
			ZkHosts zkHosts = new ZkHosts(zkhost);//kafaka所在的zookeeper
			SpoutConfig spoutConfig = new SpoutConfig(zkHosts, topic, "/order", groupId);  // create /order /id
			spoutConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
			KafkaSpout kafkaSpout = new KafkaSpout(spoutConfig);

	        TopologyBuilder builder = new TopologyBuilder();
	        builder.setSpout("spout", kafkaSpout, spoutNum);
			builder.setBolt("check", new CheckOrderBolt(), boltNum).shuffleGrouping("spout");
	        builder.setBolt("counter", new CounterBolt(),boltNum).shuffleGrouping("check");

	        Config config = new Config();
	        config.setDebug(true);
	        
	        if(args!=null && args.length > 0) {
	            config.setNumWorkers(2);
	            StormSubmitter.submitTopology(args[0], config, builder.createTopology());
	        } else {        
	            config.setMaxTaskParallelism(2);
	
	            LocalCluster cluster = new LocalCluster();
	            cluster.submitTopology("Wordcount-Topology", config, builder.createTopology());

	            Thread.sleep(500000);

	            cluster.shutdown();
	        }
		}catch (Exception e) {
			e.printStackTrace();
		}
	}

}