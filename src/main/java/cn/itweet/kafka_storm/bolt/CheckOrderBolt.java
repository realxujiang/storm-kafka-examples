package cn.itweet.kafka_storm.bolt;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import cn.itweet.kafka_storm.utils.DateUtils;
import org.apache.commons.lang.StringUtils;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Created by Andrew on 2015-5-18.
 */
public class CheckOrderBolt extends BaseBasicBolt {
	private static final long serialVersionUID = 8602883858475659429L;

	@Override
	public void execute(Tuple tuple, BasicOutputCollector collector) {
		SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd");//设置日期格式
		String nowData = df.format(new Date()); // new Date()为获取当前系统时间,检测是否为最新数据

		String data = tuple.getString(0);
		//订单号		用户id	     原金额	                      优惠价	          标示字段		下单时间
		//id		memberid  	totalprice					preprice		sendpay		createdate
		if(data!=null && data.length()>0) {
			String[] values = data.split("\t");
			if(values.length==6) {
				String id = values[0];
				String memberid = values[1];
				String totalprice = values[2];
				String preprice = values[3];
				String sendpay = values[4];
				String createdate = values[5];
				
				if(StringUtils.isNotEmpty(id)&&StringUtils.isNotEmpty(memberid)&&StringUtils.isNotEmpty(totalprice)) {
					if(DateUtils.isValidDate(createdate, nowData)) {
						collector.emit(new Values(id,memberid,totalprice,preprice,sendpay,createdate));
					}
				}
			}
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("id","memberid","totalprice","preprice","sendpay","createdate"));
	}
	
	public static void main(String[] args) {
		SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");//设置日期格式
		String nowData = df.format(new Date()); // new Date()为获取当前系统时间
		System.out.println(nowData);
		String data = "3343434	4	4	5	4	2015-05-19 13:33:20";
		//订单号		用户id	     原金额	                      优惠价	          标示字段		下单时间
		//id		memberid  totalprice	preprice	sendpay		createdate
		if(data!=null && data.length()>0) {
			String[] values = data.split("\t");
			if(values.length==6) {
				String id = values[0];
				String memberid = values[1];
				String totalprice = values[2];
				String preprice = values[3];
				String sendpay = values[4];
				String createdate = values[5];
				
				if(StringUtils.isNotEmpty(id)&&StringUtils.isNotEmpty(memberid)&&StringUtils.isNotEmpty(totalprice)) {
					if(DateUtils.isValidDate(createdate, "2015-04-19")) {
						System.out.println("true"+"  "+id+" "+memberid+" "+preprice+" "+sendpay+" "+createdate);
					}
				}
			}
		}
	}
}
