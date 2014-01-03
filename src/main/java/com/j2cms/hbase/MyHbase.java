package com.j2cms.hbase;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

public class MyHbase {

	// public static Configuration conf = new Configuration();

	public static Configuration conf;
	// HTable负责跟记录相关的操作如增删改查等
	public static HTable table;
	public static String tableName = "blog";
	static {
		conf = HBaseConfiguration.create();
		// configuration.set("hbase.zookeeper.property.clientPort", "2181");
		// configuration.set("hbase.zookeeper.quorum", "192.168.1.100");
		// configuration.set("hbase.master", "192.168.1.100:600000");
		conf.addResource("hbase-site.xml");
		try {
			table = new HTable(conf, tableName);
		} catch (IOException e) {
			e.printStackTrace();
		}// instantiate HTable
	}

	/**
	 * 创建表
	 * 
	 * @param tableName
	 * @param cfs
	 * @throws IOException
	 */
	public static void createTable(String tableName, String[] cfs) throws IOException {
		HBaseAdmin admin = new HBaseAdmin(conf);
		if (admin.tableExists(tableName)) {
			System.out.println("table already exists");
		} else {
			HTableDescriptor descriptor = new HTableDescriptor(tableName);
			for (int i = 0; i < cfs.length; ++i) {
				descriptor.addFamily(new HColumnDescriptor(cfs[i]));
			}
			admin.createTable(descriptor);
		}
	}

	/**
	 * 根据rowKey查找记录
	 * 
	 * @param rowKey
	 * @throws IOException
	 */
	public static void searchByRowKey(String rowKey) throws IOException {
		HTable table = new HTable(conf, tableName);

		// 查询数据
		Get get = new Get(Bytes.toBytes(rowKey));
//		get.setMaxVersions(3);  // will return last 3 versions of row
		Result result = table.get(get);
//		byte[] b = result.getValue(Bytes.toBytes("article"), Bytes.toBytes("tags"));  // returns current version of value
		for (KeyValue kv : result.list()) {
			System.out.println("family:" + Bytes.toString(kv.getFamily()));
			System.out.println("qualifier:" + Bytes.toString(kv.getQualifier()));
			System.out.println("value:" + Bytes.toString(kv.getValue()));
			System.out.println("Timestamp:" + kv.getTimestamp());
		}
	}

	/**
	 * 遍历查询与迭代
	 * 
	 * @throws IOException
	 */
	public static void scanAll() throws IOException {
		Scan scan = new Scan();
		ResultScanner rs = table.getScanner(scan);
		for (Result r : rs) {
			for (KeyValue kv : r.list()) {
				System.out.println("family:" + Bytes.toString(kv.getFamily()));
				System.out.println("qualifier:" + Bytes.toString(kv.getQualifier()));
				System.out.println("value:" + Bytes.toString(kv.getValue()));
				System.out.println("Timestamp:" + kv.getTimestamp());
			}
		}
		rs.close();
	}

	/**
	 * 遍历查询与迭代
	 * 
	 * @param startRowKey
	 * @param stopRowKey
	 * @param family
	 * @param qualifier
	 * @throws IOException
	 */
	public static void scan(String startRowKey, String stopRowKey, String family, String qualifier) throws IOException {

		Scan scan = new Scan();
		scan.addColumn(Bytes.toBytes(family), Bytes.toBytes(qualifier));
		scan.setStartRow(Bytes.toBytes(startRowKey)); // start key is inclusive
		scan.setStopRow(Bytes.toBytes(stopRowKey)); // stop key is exclusive
		ResultScanner rs = table.getScanner(scan);
		try {
			for (Result r = rs.next(); r != null; r = rs.next()) {
				for (KeyValue kv : r.list()) {
					System.out.println("row:" + Bytes.toString(kv.getRow()));
					System.out.println("family:" + Bytes.toString(kv.getFamily()));
					System.out.println("qualifier:" + Bytes.toString(kv.getQualifier()));
					System.out.println("value:" + Bytes.toString(kv.getValue()));
					System.out.println("Timestamp:" + kv.getTimestamp());
				}
			}
			// process result...
		} finally {
			rs.close(); // always close the ResultScanner!
		}

	}

	/**
	 * 删除指定列
	 * 
	 * @param rowKey
	 * @param family
	 * @param qualifier
	 * @throws IOException
	 */
	public static void deleteColumn(String rowKey, String family, String qualifier) throws IOException {
		Delete delete = new Delete(Bytes.toBytes(rowKey));
		delete.deleteColumn(Bytes.toBytes(family), Bytes.toBytes(qualifier));
		table.delete(delete);
	}

	/**
	 * 删除指定行的所有列
	 * 
	 * @param rowKey
	 * @param family
	 * @param qualifier
	 * @throws IOException
	 */
	public static void deleteColumn(String rowKey) throws IOException {
		Delete delete = new Delete(Bytes.toBytes(rowKey));
		table.delete(delete);
	}

	// 创建表测试
	public static void createTest() throws IOException {
		// HBaseAdmin负责跟表相关的操作如 create ,drop等
		HBaseAdmin admin = new HBaseAdmin(conf);

		/* 创建表* */
		HTableDescriptor desc = new HTableDescriptor(tableName);
		desc.addFamily(new HColumnDescriptor("article"));
		desc.addFamily(new HColumnDescriptor("author"));

		if (!admin.tableExists(tableName))
			admin.createTable(desc);

	}

	// 插入数据测试
	public static void insertTest() throws IOException {

		// HTable table = new HTable(conf, tableName);

		Put put = new Put(Bytes.toBytes("1"));
		put.add(Bytes.toBytes("article"), Bytes.toBytes("title"), Bytes.toBytes("Haad First Hbase"));
		put.add(Bytes.toBytes("article"), Bytes.toBytes("content"), Bytes.toBytes("HBase database"));
		put.add(Bytes.toBytes("article"), Bytes.toBytes("tags"), Bytes.toBytes("Hadoop,Hbase,NoSQL"));
		put.add(Bytes.toBytes("author"), Bytes.toBytes("name"), Bytes.toBytes("yeelor"));
		put.add(Bytes.toBytes("author"), Bytes.toBytes("nickname"), Bytes.toBytes("涛声依旧"));
		table.put(put);
	}

	public static void updateTest() {
		Get get2 = new Get(Bytes.toBytes("1"));
		get2.addColumn(Bytes.toBytes("author"), Bytes.toBytes("nickname"));
		// assertThat(Bytes.toString(table.get(get2).list().get(0).getValue())，("一叶渡江"));

	}

	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception {

		String[] cfs = new String[] { "article", "author" };
		// createTable(tableName, cfs);
//		 insertTest();
		// searchByRowKey("1");
		// scanAll();
		// deleteColumn("1","author","nickname");
//		 scanAll();
//		deleteColumn("1");
		scan("1", "2", "article", "tags");
	}
}
