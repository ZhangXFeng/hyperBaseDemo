package io.transwarp.hyperbase.examples.objectstore;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;

public class FileByteArrayUtil {
	static byte[] getByteFromFile(String path) {
		byte[] bytes = null;
		FileInputStream fis = null;
		try {
			fis = new FileInputStream(new File(path));
			int length = fis.available();
			bytes = new byte[length];
			fis.read(bytes);
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			if (fis != null) {
				try {
					fis.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
				fis = null;
			}
		}
		return bytes;
	}

	static File getFileFromByte(byte[] bytes, String path) {
		if (bytes == null) {
			return null;
		}
		File f = new File(path);
		FileOutputStream fos = null;
		try {
			fos = new FileOutputStream(f);
			fos.write(bytes);
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			if (fos != null) {
				try {
					fos.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
				fos = null;
			}
		}
		return f;
	}
}
