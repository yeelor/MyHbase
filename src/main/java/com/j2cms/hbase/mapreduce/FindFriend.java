package com.j2cms.hbase.mapreduce;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;

import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Job;

public class FindFriend {

	public static class Mapper extends TableMapper<ImmutableBytesWritable, ImmutableBytesWritable> {

		public void map(ImmutableBytesWritable row, Result values, Context context) throws IOException, InterruptedException {
			ImmutableBytesWritable value = null;
			String[] tags = null;
			for (KeyValue kv : values.list()) {
				if ("author".equals(Bytes.toString(kv.getFamily())) && "nickname".equals(Bytes.toString(kv.getQualifier()))) {
					value = new ImmutableBytesWritable(kv.getValue());
				}
				if ("article".equals(Bytes.toString(kv.getFamily())) && "tags".equals(Bytes.toString(kv.getQualifier()))) {
					tags = Bytes.toString(kv.getValue()).split(",");
				}
			}
			if ((tags != null) && (value != null))//不加的话在value为null时会报错
				for (int i = 0; i < tags.length; i++) {
					ImmutableBytesWritable key = new ImmutableBytesWritable(Bytes.toBytes(tags[i].toLowerCase()));
					System.out.println("key=" + key);
					System.out.println("value=" + value);
					context.write(key, value);
				}

		}

	}

	public static class Reducer extends TableReducer<ImmutableBytesWritable, ImmutableBytesWritable, ImmutableBytesWritable> {
		public void reduce(ImmutableBytesWritable key, Iterable<ImmutableBytesWritable> values, Context context) throws IOException, InterruptedException {
			String friends = "";
			for (ImmutableBytesWritable val : values) {
				friends += (friends.length() > 0 ? "," : "") + Bytes.toString(val.get());
			}
			Put put = new Put(key.get());
			put.add(Bytes.toBytes("person"), Bytes.toBytes("nicknames"), Bytes.toBytes(friends));
			context.write(key, put);
		}
	}

	public static void main(String[] args) throws Exception {
//		Configuration conf = new Configuration();
//		conf.set("mapred.job.tracker", "lenovo0:9001");

//		conf = HBaseConfiguration.create(conf);
		Configuration conf = HBaseConfiguration.create();
//		conf.addResource("/usr/local/hbase/conf/hbase-site.xml");

//		conf.set("hbase.zookeeper.property.clientPort", "2181");//默认值，不用设置
//		conf.set("hbase.zookeeper.quorum", "218.193.154.182,218.193.154.183,218.193.154.184,218.193.154.185,218.193.154.186,218.193.154.187,218.193.154.188,218.193.154.189");

//		conf.set("hbase.nameserver.address", "218.193.154.181");
		conf.set("hbase.zookeeper.quorum", "lenovo1,lenovo2,lenovo3,lenovo4,lenovo5,lenovo6,lenovo7,lenovo8");

		
		Job job = new Job(conf, "HBase_FindFriend");
		job.setJarByClass(FindFriend.class);
		Scan scan = new Scan();
		scan.addColumn(Bytes.toBytes("author"), Bytes.toBytes("nickname"));
		scan.addColumn(Bytes.toBytes("article"), Bytes.toBytes("tags"));

		TableMapReduceUtil.initTableMapperJob("blog", scan, FindFriend.Mapper.class, ImmutableBytesWritable.class, ImmutableBytesWritable.class, job);
		TableMapReduceUtil.initTableReducerJob("tag_friend", FindFriend.Reducer.class, job);

		System.exit(job.waitForCompletion(true) ? 0 : 1);

		// job.setMapperClass(Mapper.class);
		// job.setReducerClass(Reducer.class);
		// job.setInputFormatClass(TableInputFormat.class);
		// job.setOutputFormatClass(TableOutputFormat.class);

	}

}
