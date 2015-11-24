package io.transwarp.hyperbase.examples.localindex;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTableInterface;
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
import org.apache.hadoop.hbase.secondaryindex.indexbuilder.CombineIndex;
import org.apache.hadoop.hbase.secondaryindex.indexbuilder.SecondaryIndex;
import org.apache.hadoop.hbase.secondaryindex.indexbuilder.SecondaryIndexUtil;
import org.apache.hadoop.hbase.util.Bytes;

public class LocalIndexDemo {
	private static Log LOG = LogFactory.getLog(LocalIndexDemo.class);
	private String tableName;
	private String columnFamily;
	private String columnName;
	private Configuration conf = HBaseConfiguration.create();
	public String UP_COMBINE_INDEX;
	HTable htable = null;
	HBaseAdmin admin = null;

	public LocalIndexDemo(String tableName, String columnFamily,
			String columnName) {
		super();
		this.tableName = tableName;
		this.columnFamily = columnFamily;
		this.columnName = columnName;
		UP_COMBINE_INDEX = CombineIndex.class.getName() + "|INDEXED="
				+ columnFamily + ":" + columnName
				+ ":6|rowKey:rowKey:7,DCOP=true,UPDATE=true";
	}

	public void init() {
		try {
			admin = new HBaseAdmin(conf);
			HTableDescriptor desc = new HTableDescriptor(
					TableName.valueOf(tableName));
			HColumnDescriptor family = new HColumnDescriptor(
					Bytes.toBytes(columnFamily));
			desc.addFamily(family);
			if (!admin.tableExists(tableName)) {
				admin.createTable(desc);
			}

			admin.disableTable(tableName);
			SecondaryIndex index = SecondaryIndexUtil
					.createSecondaryIndexFromPattern(UP_COMBINE_INDEX);
			HColumnDescriptor indexFamly = new HColumnDescriptor(
					Bytes.toBytes("IDX"));
			indexFamly.setLocalIndex(index.toPb().toByteArray());
			admin.addColumn(tableName, indexFamly);
			admin.enableTable(tableName);

		} catch (Exception e) {
			LOG.error(e.getMessage(), e);
		}
	}

	public void initData() {
		try {
			htable = new HTable(conf, tableName);
			htable.setAutoFlushTo(false);
			byte[] cfBytes = columnFamily.getBytes();
			byte[] cnBytes = columnName.getBytes();
			for (int i = 0; i < 1024; i++) {
				Put put = new Put(Bytes.toBytes("row" + i));
				put.add(cfBytes, cnBytes, Bytes.toBytes("value" + i));
				htable.put(put);
			}
			htable.flushCommits();
		} catch (Exception e) {
			LOG.error(e.getMessage(), e);
		} finally {
			try {
				htable.close();
			} catch (Exception e) {
				LOG.error(e.getMessage(), e);

			}
		}
	}

	private List<Result> getLocalIndexCount(byte[] value) {
		List<Result> results = new ArrayList<Result>();
		try {
			htable = new HTable(conf, tableName);

			Scan scan = new Scan();
			scan.setUseLocalIndex(true);
			scan.setFilter(getInFilter(columnFamily.getBytes(),
					columnName.getBytes(), value));
			ResultScanner rs = htable.getScanner(scan);
			Iterator<Result> iter = rs.iterator();
			while (iter.hasNext()) {
				results.add(iter.next());
			}
		} catch (Exception e) {
			LOG.error(e.getMessage(), e);
		}
		return results;
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
		LocalIndexDemo indexDemo = new LocalIndexDemo("localindexdemo", "cf",
				"c1");
		// indexDemo.initData();

		List<Result> results = indexDemo.getLocalIndexCount("value99"
				.getBytes());
		for (Iterator iterator = results.iterator(); iterator.hasNext();) {
			Result result = (Result) iterator.next();
			System.out.println(Bytes.toString(result.getRow()));
		}
	}
}
