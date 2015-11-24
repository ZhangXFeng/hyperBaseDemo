package io.transwarp.hyperbase.examples.globalindex;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.IndexHTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.expr.ExprFactory;
import org.apache.hadoop.hbase.expr.ExprInterface;
import org.apache.hadoop.hbase.expr.result.BytesResult;
import org.apache.hadoop.hbase.expr.result.CollectionResult;
import org.apache.hadoop.hbase.filter.ExprFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.secondaryindex.GlobalIndexAdmin;
import org.apache.hadoop.hbase.secondaryindex.indexbuilder.CombineIndex;
import org.apache.hadoop.hbase.secondaryindex.indexbuilder.SecondaryIndex;
import org.apache.hadoop.hbase.secondaryindex.indexbuilder.SecondaryIndexUtil;
import org.apache.hadoop.hbase.util.Bytes;

public class GlobalIndexDemo {
	private static Log LOG = LogFactory.getLog(GlobalIndexDemo.class);
	private String tableName;
	private String columnFamily;
	private String columnName;
	public String C1_INDEX;
	private String indexName;

	private Configuration conf = HBaseConfiguration.create();

	public GlobalIndexDemo(String tableName, String columnFamily,
			String columnName) {
		super();
		this.tableName = tableName;
		this.columnFamily = columnFamily;
		this.columnName = columnName;
		this.C1_INDEX = CombineIndex.class.getName() + "|INDEXED="
				+ columnFamily + ":" + columnName
				+ ":9|rowKey:rowKey:7,DCOP=true,UPDATE=true";
		this.indexName = tableName + "_" + columnName;
	}

	public void init() {
		GlobalIndexAdmin admin = null;
		try {
			admin = new GlobalIndexAdmin(conf);
			@SuppressWarnings("deprecation")
			HTableDescriptor table = new HTableDescriptor(tableName);
			HColumnDescriptor f1 = new HColumnDescriptor("cf");
			table.addFamily(f1);
			if (!admin.isTableExists(new String(tableName).getBytes())) {
				admin.createTable(table);
				LOG.info(tableName + " is not exists");
			} else {
				LOG.info(tableName + " is exists");
			}

			SecondaryIndex index = SecondaryIndexUtil
					.createSecondaryIndexFromPattern(C1_INDEX);
			admin.addGlobalIndex(Bytes.toBytes(tableName), index,
					Bytes.toBytes(indexName), null, false);

		} catch (Exception e) {
			LOG.error(e.getMessage(), e);
		} finally {
			if (null != admin) {
				try {
					admin.close();
				} catch (IOException e) {
					LOG.error("GlobalIndexAdmin close error");
				}
			}
		}
	}

	public void initData() {
		IndexHTable hTable = null;
		try {
			hTable = new IndexHTable(conf, tableName);
			hTable.setAutoFlushTo(true);
			List<Put> puts = new ArrayList<Put>();
			byte[] familyBytes = columnFamily.getBytes();
			byte[] qualifier = columnName.getBytes();
			for (int i = 0; i < 100; i++) {
				Put put = new Put(Bytes.toBytes("row" + i));
				put.add(familyBytes, qualifier, ("value" + i).getBytes());
				puts.add(put);
			}
			hTable.put(puts);
			hTable.flushCommits();
		} catch (Exception e) {
			LOG.error(e.getMessage(), e);
		} finally {
			try {
				hTable.close();
			} catch (IOException e) {
				LOG.error("htable close error");
			}
		}

	}

	public void scan() {
		IndexHTable hTable = null;
		try {
			hTable = new IndexHTable(conf, tableName);
			Scan scan = new Scan();
			scan.setFilter(getInFilter(columnFamily.getBytes(),
					columnName.getBytes(), "value3".getBytes(), "value9".getBytes()));
			scan.setIndexColumn(Bytes.toBytes(indexName));
			ResultScanner rs = hTable.getScanner(scan);
			Result result = null;
			while (null != (result = rs.next())) {
				System.out.println("================"+Bytes.toString(result.getRow()));
			}

		} catch (Exception e) {
		}
	}

	public static Filter getInFilter(byte[] family, byte[] qualifier,
			byte[]... c1) {
		ExprInterface kobhVarExpr = ExprFactory.createVariableExpr(family,
				qualifier);
		Collection<org.apache.hadoop.hbase.expr.result.Result> colls = new ArrayList<org.apache.hadoop.hbase.expr.result.Result>();
		for (byte[] c : c1) {
			colls.add(BytesResult.getInstance(c));
		}
		org.apache.hadoop.hbase.expr.result.Result result = CollectionResult
				.getInstance(colls);
		ExprInterface kkbhValueExpr = ExprFactory.createConstExpr(result);
		ExprInterface funcExpr = ExprFactory.createInExpr(kobhVarExpr,
				kkbhValueExpr);
		ExprFilter kkbhExprFilter = new ExprFilter(funcExpr);
		return kkbhExprFilter;
	}

	public static void main(String[] args) {
		GlobalIndexDemo indexDemo = new GlobalIndexDemo("globalindexdemo",
				"cf", "c1");
		// indexDemo.init();
		// indexDemo.initData();
		indexDemo.scan();
	}
}
