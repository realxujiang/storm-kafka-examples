package cn.itweet.kafka_storm.bolt;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import cn.itweet.kafka_storm.utils.DateUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

import java.util.List;

public class CounterBolt extends BaseBasicBolt {
	private static final long serialVersionUID = -5508421065181891596L;

	private static Logger logger = Logger.getLogger(CounterBolt.class);

	private static long counter = 0;

	@Override
	public void execute(Tuple tuple, BasicOutputCollector collector) {
		List<Object> data = tuple.getValues();

		String id = (String) data.get(0);
		String memberid = (String) data.get(1);
		String totalprice = (String) data.get(2);
		String preprice = (String) data.get(3);
		String sendpay = (String) data.get(4);
		String createdate = (String) data.get(5);
		collector.emit(new Values(id,memberid,totalprice,preprice,sendpay,createdate));
		logger.info("+++++++++++++++++++++++++++++++++Valid+++++++++++++++++++++++++++++++++");
		logger.info("msg = "+data+" ----@-@-@-@-@--------counter = "+(counter++));
		logger.info("+++++++++++++++++++++++++++++++++Valid+++++++++++++++++++++++++++++++++");
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("id","memberid","totalprice","preprice","sendpay","createdate"));
	}
}
