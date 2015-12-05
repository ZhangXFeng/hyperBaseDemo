package io.transwarp.hyperbase.examples;

import io.transwarp.hyperbase.examples.objectstore.HbaseUtil;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

public class HyperbaseDemo {
	private static Log LOG = LogFactory.getLog(HyperbaseDemo.class);
	private Configuration conf = HBaseConfiguration.create();
	private static String tableName = "hyperbaseDemo";
	private static String columnFamily = "cf";
	private static byte[] bytesCf = Bytes.toBytes(columnFamily);
	private static byte[] bytesColumn1 = Bytes.toBytes("c1");
	private static byte[] bytesColumn2 = Bytes.toBytes("c2");
	private HTable table;
	private static Map<Integer, String> map = new HashMap<Integer, String>();

	static {
		map.put(0, "a");
		map.put(1, "d");
		map.put(2, "g");
		map.put(3, "m");
		map.put(4, "t");
		map.put(5, "w");
		map.put(6, "z");
	}

	private void createTable() {
		HBaseAdmin admin = null;
		try {
			TableName name = TableName.valueOf(Bytes.toBytes(tableName));
			HTableDescriptor descriptor = new HTableDescriptor(name);
			HColumnDescriptor columnDescriptor = new HColumnDescriptor(
					Bytes.toBytes(columnFamily));
			descriptor.addFamily(columnDescriptor);
			admin = new HBaseAdmin(conf);
			byte[] splitKey1 = Bytes.toBytes("d");
			byte[] splitKey2 = Bytes.toBytes("g");
			byte[] splitKey3 = Bytes.toBytes("m");
			byte[] splitKey4 = Bytes.toBytes("t");
			byte[] splitKey5 = Bytes.toBytes("w");
			byte[] splitKey6 = Bytes.toBytes("z");
			byte[][] splitKeys = new byte[6][];
			splitKeys[0] = splitKey1;
			splitKeys[1] = splitKey2;
			splitKeys[2] = splitKey3;
			splitKeys[3] = splitKey4;
			splitKeys[4] = splitKey5;
			splitKeys[5] = splitKey6;

			admin.createTable(descriptor, splitKeys);

		} catch (Exception e) {
			LOG.error(e.getMessage(), e);
		} finally {
			if (null != admin) {
				try {
					admin.close();
				} catch (IOException e) {
					LOG.error("HBaseAdmin close error. ", e);
				}
			}
		}
	}

	private void putRecord() {
		try {
			table = new HTable(conf, tableName);
			table.setAutoFlushTo(true);
			List<Put> puts = new ArrayList<Put>();
			for (int i = 0; i < 70; i++) {
				String prefix = map.get(i % 7);
				String rowkey = prefix + i;
				Put put = new Put(Bytes.toBytes(rowkey));
				put.add(bytesCf, bytesColumn1, Bytes.toBytes("value" + i));
				put.add(bytesCf, bytesColumn2, Bytes.toBytes("value" + i));
				puts.add(put);
			}
			table.put(puts);
		} catch (Exception e) {
			LOG.error(e.getMessage(), e);
		}
	}

	private void getRecord() {
		try {
			table = new HTable(conf, tableName);
			Scan scan = new Scan();
			ResultScanner rs = table.getScanner(scan);
			Result result = rs.next();
			while (null != result) {
				String c1 = Bytes.toString(result.getValue(bytesCf,
						bytesColumn1));
				String c2 = Bytes.toString(result.getValue(bytesCf,
						bytesColumn2));
				LOG.info("c1=" + c1 + ",c2=" + c2);
				result = rs.next();
			}
		} catch (Exception e) {
			LOG.error(e.getMessage(), e);
		}
	}

	/**
	 * object store write example
	 */
	public void insert() {
		HbaseUtil hbaseUtil = new HbaseUtil();
		hbaseUtil.init("objectstore");
		hbaseUtil.insert("/home/sean/Pictures/lujin.png", "cf", "f1");

	}

	/**
	 * object store read example
	 */
	public void get() {
		HbaseUtil hbaseUtil = new HbaseUtil();
		hbaseUtil.init("objectstore");
		hbaseUtil.get("objectstore", "/media/sean/0001464B000495BB/",
				"lujin.png");
	}

	public static void main(String[] args) {
		HyperbaseDemo hbd = new HyperbaseDemo();
		hbd.createTable();
		// hbd.putRecord();
		// hbd.getRecord();
		// hbd.insert();
		// hbd.get();

	}
}
