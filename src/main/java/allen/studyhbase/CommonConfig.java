package allen.studyhbase;



import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.hbase.HBaseConfiguration;

import org.apache.hadoop.hbase.client.HBaseAdmin;

import org.apache.hadoop.hbase.client.HTablePool;



/**

 * CommonConfig.

 * */

public class CommonConfig {



	private static HTablePool pool;

	private static HBaseAdmin hbaseAdmin;

	private static Configuration conf;



	static {

		try {

			conf = HBaseConfiguration.create();

			pool = new HTablePool(conf, 50);

			hbaseAdmin = new HBaseAdmin(conf);

		} catch (Exception e) {

			throw new RuntimeException(e);

		}

	};



	public static Configuration getConfiguration() {

		return conf;

	}



	public static HBaseAdmin getHBaseAdmin() {

		return hbaseAdmin;

	}



	public static HTablePool getHTablePool() {

		return pool;

	}

}


