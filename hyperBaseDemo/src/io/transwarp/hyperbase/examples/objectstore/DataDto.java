package io.transwarp.hyperbase.examples.objectstore;

import java.util.Arrays;

public class DataDto {
	private byte[] rowKey;
	private byte[] columnFamily;
	private byte[] columnQualifier;
	private byte[] value;
	private long timestamp;

	public DataDto() {
		super();
	}

	public DataDto(byte[] rowKey, byte[] columnFamily, byte[] columnQualifier,
			byte[] value, long timestamp) {
		super();
		this.rowKey = rowKey;
		this.columnFamily = columnFamily;
		this.columnQualifier = columnQualifier;
		this.value = value;
		this.timestamp = timestamp;
	}

	public byte[] getRowKey() {
		return rowKey;
	}

	public void setRowKey(byte[] rowKey) {
		this.rowKey = rowKey;
	}

	public byte[] getColumnFamily() {
		return columnFamily;
	}

	public void setColumnFamily(byte[] columnFamily) {
		this.columnFamily = columnFamily;
	}

	public byte[] getColumnQualifier() {
		return columnQualifier;
	}

	public void setColumnQualifier(byte[] columnQualifier) {
		this.columnQualifier = columnQualifier;
	}

	public byte[] getValue() {
		return value;
	}

	public void setValue(byte[] value) {
		this.value = value;
	}

	public long getTimestamp() {
		return timestamp;
	}

	public void setTimestamp(long timestamp) {
		this.timestamp = timestamp;
	}

	@Override
	public String toString() {
		return "DataDto [rowKey=" + Arrays.toString(rowKey) + ", columnFamily="
				+ Arrays.toString(columnFamily) + ", columnQualifier="
				+ Arrays.toString(columnQualifier) + ", value="
				+ Arrays.toString(value) + ", timestamp=" + timestamp + "]";
	}
}