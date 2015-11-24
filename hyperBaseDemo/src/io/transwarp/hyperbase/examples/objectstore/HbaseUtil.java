package io.transwarp.hyperbase.examples.objectstore;

import java.io.File;
import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.IndexHTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.compress.Compression.Algorithm;
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding;
import org.apache.hadoop.hbase.secondaryindex.GlobalIndexAdmin;
import org.apache.hadoop.hbase.secondaryindex.indexbuilder.SecondaryIndexUtil;
import org.apache.hadoop.hbase.util.Bytes;

public class HbaseUtil {

	private Configuration conf;
	private HTableInterface htable;

	public void init(String tableName) {
		try {
			conf = HBaseConfiguration.create();
			// conf.set("hbase.client.keyvalue.maxsize", "209715200");
			GlobalIndexAdmin admin = new GlobalIndexAdmin(conf);
			@SuppressWarnings("deprecation")
			HTableDescriptor table = new HTableDescriptor(tableName);
			HColumnDescriptor f1 = new HColumnDescriptor("cf");
			f1.setValue(
					HConstants.HREGION_MEMSTORE_SPECIAL_COLUMN_FAMILY_FLUSHSIZE_KEY,
					1024 * 1024 * 64 + "");

			boolean withCompress = true;
			if (withCompress) {
				f1.setDataBlockEncoding(DataBlockEncoding.PREFIX_TREE);
				f1.setCompressionType(Algorithm.SNAPPY);
			}

			int ttl = 24 * 60 * 60 * 365;
			f1.setTimeToLive(ttl);
			table.addFamily(f1);

			if (!admin.isTableExists(new String(tableName).getBytes())) {
				admin.createTable(table);
				admin.addLOBForExistFamily(new String(tableName).getBytes(),
						new String("cf").getBytes(),
						new String("lobf").getBytes());
				System.out.println(tableName + " is not exists");
			} else {
				System.out.println(tableName + " is exists");
			}
			htable = new IndexHTable(conf, tableName);
			admin.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public synchronized void insert(String path, String cf, String cq) {
		DataDto data = new DataDto();
		String fileName = new Path(path).getName();
		data.setRowKey(fileName.getBytes());
		data.setColumnFamily(cf.getBytes());
		data.setColumnQualifier(cq.getBytes());
		data.setValue(FileByteArrayUtil.getByteFromFile(path));
		insert(data);
	}

	public synchronized void insert(DataDto data) {
		Put p = new Put(data.getRowKey());
		p.setDurability(Durability.SKIP_WAL);
		System.out.println(data.getValue());
		p.add(data.getColumnFamily(), data.getColumnQualifier(),
				data.getValue());
		try {
			htable.put(p);
			System.out.println("put successed");
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public void insert(List<Put> list) {
		try {
			htable.put(list);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public void get(String tablename, String path, String rowkey) {
		HTable table = null;
		Configuration conf = HBaseConfiguration.create();
		try {
			table = new HTable(conf, Bytes.toBytes(tablename));
			Scan scan = new Scan();
			// 只取一行
			scan.setStartRow(Bytes.toBytes(rowkey));
			byte[] stopkey = Bytes.toBytes(rowkey);
			SecondaryIndexUtil.IncreaseBytes(stopkey);
			scan.setStopRow(stopkey);

			ResultScanner resultSet = table.getScanner(scan);
			for (Result result : resultSet) {
				byte[] rowkeybytes = result.getRow();
				byte[] picbytes = result.getValue(Bytes.toBytes("cf"),
						Bytes.toBytes("f1"));
				FileByteArrayUtil.getFileFromByte(picbytes, path
						+ File.separator + new String(rowkeybytes));
			}
			table.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public void close() {
		if (htable != null) {
			try {
				htable.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
}