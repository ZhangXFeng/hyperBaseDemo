package io.transwarp.hyperbase.examples;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

public class ConcurrentPuter {
	private static Log LOG = LogFactory.getLog(ConcurrentPuter.class);
	private static Configuration conf = HBaseConfiguration.create();
	private String tableName = "";
	private String columnFamily = "";
	private static byte[] bytesColumn1 = Bytes.toBytes("c1");
	private static byte[] bytesColumn2 = Bytes.toBytes("c2");

	private static Map<Integer, String> map2 = new HashMap<Integer, String>();

	private Map<ServerName, Set<HRegionInfo>> map4 = new HashMap<ServerName, Set<HRegionInfo>>();
	private Map<ServerName, PutThread> putThreads = new HashMap<ServerName, ConcurrentPuter.PutThread>();

	public ConcurrentPuter(String tableName, String columnFamily) {
		this.tableName = tableName;
		this.columnFamily = columnFamily;
	}

	static {
		map2.put(0, "a");
		map2.put(1, "d");
		map2.put(2, "g");
		map2.put(3, "m");
		map2.put(4, "t");
		map2.put(5, "w");
		map2.put(6, "z");
	}

	public void init() {
		HTable hTable = null;
		try {
			hTable = new HTable(conf, tableName);
			Map<HRegionInfo, ServerName> map3 = hTable.getRegionLocations();
			Set<Map.Entry<HRegionInfo, ServerName>> entries = map3.entrySet();
			for (Iterator<Entry<HRegionInfo, ServerName>> iterator = entries
					.iterator(); iterator.hasNext();) {
				Entry<HRegionInfo, ServerName> entry = (Entry<HRegionInfo, ServerName>) iterator
						.next();
				ServerName sn = entry.getValue();
				HRegionInfo hRegionInfo = entry.getKey();
				Set<HRegionInfo> infos = map4.get(sn);
				if (null == infos) {
					infos = new HashSet<HRegionInfo>();
					map4.put(sn, infos);
				}
				infos.add(hRegionInfo);

			}

			Set<ServerName> names = map4.keySet();
			for (Iterator<ServerName> iterator = names.iterator(); iterator
					.hasNext();) {
				ServerName serverName = (ServerName) iterator.next();
				PutThread putThread = new PutThread(tableName, columnFamily);
				putThreads.put(serverName, putThread);
			}

		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			if (hTable != null) {
				try {
					hTable.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
	}

	private static int random() {
		return (int) (Math.random() * 7);
	}

	private static String rowKeyBuilder(String prefix) {
		return prefix + UUID.randomUUID().toString();
	}

	public void start() {

		Collection<PutThread> threads = putThreads.values();
		for (Iterator<PutThread> iterator = threads.iterator(); iterator
				.hasNext();) {
			PutThread putThread = (PutThread) iterator.next();
			putThread.start();

		}
		Set<Map.Entry<ServerName, Set<HRegionInfo>>> set = map4.entrySet();
		while (true) {
			String prefix = map2.get(random());
			String rowKey = rowKeyBuilder(prefix);
			Record record = new Record(rowKey, "v1", "v2");

			for (Iterator<Entry<ServerName, Set<HRegionInfo>>> iterator = set
					.iterator(); iterator.hasNext();) {
				Entry<ServerName, Set<HRegionInfo>> entry = (Entry<ServerName, Set<HRegionInfo>>) iterator
						.next();

				ServerName serverName = entry.getKey();
				Set<HRegionInfo> infos = entry.getValue();
				if (contains(infos, record)) {
					PutThread putThread = putThreads.get(serverName);
					putThread.put(record);
					break;
				}

			}

		}

	}

	static boolean contains(Set<HRegionInfo> infos, Record record) {
		for (Iterator<HRegionInfo> iterator = infos.iterator(); iterator
				.hasNext();) {
			HRegionInfo hRegionInfo = (HRegionInfo) iterator.next();
			if (hRegionInfo.containsRow(Bytes.toBytes(record.getRowKey()))) {
				return true;
			}

		}
		return false;
	}

	static class PutThread extends Thread {

		LinkedBlockingQueue<Record> queue = new LinkedBlockingQueue<Record>();

		private byte[] bytesCf;
		private String tableName;

		public PutThread(String tableName, String columnFamily) {
			this.tableName = tableName;
			this.bytesCf = Bytes.toBytes(columnFamily);
		}

		void put(Record record) {
			try {
				queue.put(record);
			} catch (InterruptedException e) {
				LOG.error(e.getMessage(), e);
			}

		}

		@Override
		public void run() {
			HTable table = null;
			try {
				table = new HTable(conf, tableName);
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
			} finally {
				try {
					table.close();
				} catch (IOException e) {
					LOG.error("HTable close error.", e);
				}
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

	public static void main(String[] args) {
		ConcurrentPuter puter = new ConcurrentPuter("hyperbaseDemo", "cf");
		puter.init();
		puter.start();
	}
}
