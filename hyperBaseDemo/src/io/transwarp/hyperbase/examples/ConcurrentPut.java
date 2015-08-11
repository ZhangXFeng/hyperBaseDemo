package io.transwarp.hyperbase.examples;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

public class ConcurrentPut {
	private static Log LOG = LogFactory.getLog(ConcurrentPut.class);
	private static Configuration conf = HBaseConfiguration.create();
	private static String tableName = "hyperbaseDemo";
	private static String columnFamily = "cf";
	private static byte[] bytesCf = Bytes.toBytes(columnFamily);
	private static byte[] bytesColumn1 = Bytes.toBytes("c1");
	private static byte[] bytesColumn2 = Bytes.toBytes("c2");

	private static Map<String, PutThread> map = new HashMap<String, PutThread>();
	private static Map<Integer, String> map2 = new HashMap<Integer, String>();

	static {
		map2.put(0, "a");
		map2.put(1, "d");
		map2.put(2, "g");
		map2.put(3, "m");
		map2.put(4, "t");
		map2.put(5, "w");
		map2.put(6, "z");
	}

	private static void init() {
		PutThread aThread = new PutThread();
		map.put("a", aThread);
		PutThread dThread = new PutThread();
		map.put("d", dThread);
		PutThread gThread = new PutThread();
		map.put("g", gThread);
		PutThread mThread = new PutThread();
		map.put("m", mThread);
		PutThread tThread = new PutThread();
		map.put("t", tThread);
		PutThread wThread = new PutThread();
		map.put("w", wThread);
		PutThread zThread = new PutThread();
		map.put("z", zThread);
	}

	private static int random() {
		return (int) (Math.random() * 7);
	}

	private static String rowKeyBuilder(String prefix) {
		return prefix + UUID.randomUUID().toString();
	}

	private static void start() {
		init();

		Collection<PutThread> threads = map.values();
		for (Iterator<PutThread> iterator = threads.iterator(); iterator
				.hasNext();) {
			PutThread putThread = (PutThread) iterator.next();
			putThread.start();

		}

		while (true) {
			String prefix = map2.get(random());
			String rowKey = rowKeyBuilder(prefix);
			Record record = new Record(rowKey, "v1", "v2");
			PutThread putThread = map.get(prefix);
			putThread.put(record);

		}

	}

	static class PutThread extends Thread {

		LinkedBlockingQueue<Record> queue = new LinkedBlockingQueue<Record>();

		void put(Record record) {
			try {
				queue.put(record);
			} catch (InterruptedException e) {
				LOG.error(e.getMessage(), e);
			}

		}

		@Override
		public void run() {
			try {
				HTable table = new HTable(conf, tableName);
				table.setAutoFlushTo(true);
				List<Put> puts = new ArrayList<Put>();
				Record record;
				while (true) {
					record = queue.take();
					if (null != record) {
						Put put = new Put(Bytes.toBytes(record.getRowKey()));
						put.add(bytesCf, bytesColumn1,
								Bytes.toBytes(record.getC1()));
						put.add(bytesCf, bytesColumn2,
								Bytes.toBytes(record.getC2()));
						puts.add(put);
					}
					if (puts.size() > 1000) {
						table.put(puts);
						puts = new ArrayList<Put>();
					}
				}

			} catch (Exception e) {
				LOG.error(e.getMessage(), e);
			}

		}

	}

	static class Record {
		String rowKey;
		String c1;
		String c2;

		public Record(String rowKey, String c1, String c2) {
			this.rowKey = rowKey;
			this.c1 = c1;
			this.c2 = c2;
		}

		public String getRowKey() {
			return rowKey;
		}

		public void setRowKey(String rowKey) {
			this.rowKey = rowKey;
		}

		public String getC1() {
			return c1;
		}

		public void setC1(String c1) {
			this.c1 = c1;
		}

		public String getC2() {
			return c2;
		}

		public void setC2(String c2) {
			this.c2 = c2;
		}

	}

//	static class Range {
//		public byte[] startKey;
//		public byte[] endKey;
//
//		public Range(byte[] startKey, byte[] endKey) {
//			this.startKey = startKey;
//			this.endKey = endKey;
//		}
//
//	}

	public static void main(String[] args) {
		start();

	}
}
