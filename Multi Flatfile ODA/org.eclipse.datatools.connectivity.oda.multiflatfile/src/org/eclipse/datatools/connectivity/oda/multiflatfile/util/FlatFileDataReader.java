/*******************************************************************************
 * Copyright (c) 2004, 2007 Actuate Corporation.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License v1.0
 * which accompanies this distribution, and is available at
 * http://www.eclipse.org/legal/epl-v10.html
 * 
 * Contributors:
 *  Actuate Corporation - initial API and implementation
 *  
 *******************************************************************************/

package org.eclipse.datatools.connectivity.oda.multiflatfile.util;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.eclipse.datatools.connectivity.oda.IResultSetMetaData;
import org.eclipse.datatools.connectivity.oda.OdaException;
import org.eclipse.datatools.connectivity.oda.multiflatfile.CommonConstants;
import org.eclipse.datatools.connectivity.oda.multiflatfile.Connection;
import org.eclipse.datatools.connectivity.oda.multiflatfile.FlatFileQuery.FlatFileBufferedReader;
import org.eclipse.datatools.connectivity.oda.multiflatfile.ResourceInputStream;
import org.eclipse.datatools.connectivity.oda.multiflatfile.ResultSetMetaDataHelper;
import org.eclipse.datatools.connectivity.oda.multiflatfile.i18n.Messages;

/**
 * FlatFileDataSourceReader is a utility class that help read and parse the raw
 * data from flat file
 */

public class FlatFileDataReader {

	private String currentTableName;
	private String charSet;
	private FlatFileBufferedReader flatFileBufferedReader = null;
	private IResultSetMetaData rsmd;
	private ResultSetMetaDataHelper rsmdHelper;
	private int statementMaxRows = 0;
	private int resultSetMaxRows = 0;
	private int maxRowsToRead = 0;
	private String[] originalColumnNames;
	private boolean isFirstTimeToReadSourceData = true;
	private boolean isFirstTimeToCallReadLine = true;
	private int fetchCounter = 0;
	private int[] selectColumIndexes;
	private Connection conn;
	private List<String> fileNameList;
	private boolean includeAllFiles;
	private int index = 0;

	private List<String> nextDataLine;

	// Max number of rows fetched each time from data source
	public static final int MAX_ROWS_PER_FETCH = 20000;

	/**
	 * Constructor
	 * 
	 * @param connProperties
	 *            Connection properties
	 * @param currentTableName
	 *            The current table name of this connection
	 * @param fileNames
	 * @param includeAllFiles
	 * @param statementMaxRows
	 *            The max number of rows specified in the query
	 * @param rsmd
	 *            ResultSet meta-data
	 * @param rsmdHelper
	 *            ResultSet meta-data helper
	 * @throws OdaException
	 */
	public FlatFileDataReader(Connection connection, String currentTableName,
			boolean includeAllFiles, String fileNames, int statementMaxRows,
			IResultSetMetaData rsmd, ResultSetMetaDataHelper rsmdHelper)
			throws OdaException {
		this.rsmd = rsmd;
		this.rsmdHelper = rsmdHelper;
		this.statementMaxRows = statementMaxRows;
		this.currentTableName = currentTableName;
		this.conn = connection;
		this.includeAllFiles = includeAllFiles;
		this.fileNameList = getFileNameList(fileNames);
		this.currentTableName = fileNameList.get(index++);

		this.charSet = conn.getCharSet();
	}

	private List<String> getFileNameList(String fileNames) throws OdaException {
		List<String> fileList = new ArrayList<String>();
		fileList.add(currentTableName);
		if (includeAllFiles) {
			File dir = new File(conn.getHomeFolder());
			for (File file : dir.listFiles()) {
				if (file.isDirectory() || !isValidFileName(file.getName()))
					continue;
				if (!fileList.contains(file.getName()))
					fileList.add(file.getName());
			}
		} else if (fileNames != null) {
			String[] fileNameArray = fileNames.split(";");
			for (int index = 0; index < fileNameArray.length; index++) {
				if (!fileList.contains(fileNameArray[index]))
					fileList.add(fileNameArray[index]);
			}
		}

		return fileList;
	}

	private boolean isValidFileName(String name) {
		return name.endsWith(CommonConstants.FILE_EXTENSION_CSV)
				|| name.endsWith(CommonConstants.FILE_EXTENSION_TSV)
				|| name.endsWith(CommonConstants.FILE_EXTENSION_PSV)
				|| name.endsWith(CommonConstants.FILE_EXTENSION_SSV);
	}

	private void initNameIndexMap() throws OdaException {
		assert originalColumnNames != null;
		HashMap<String, Integer> originalColumnNameIndexMap = new HashMap<String, Integer>();
		for (int i = 0; i < originalColumnNames.length; i++) {
			originalColumnNameIndexMap.put(originalColumnNames[i].trim()
					.toUpperCase(), Integer.valueOf(i));
		}
		selectColumIndexes = new int[rsmd.getColumnCount()];

		for (int i = 0; i < rsmd.getColumnCount(); i++) {
			selectColumIndexes[i] = findIndex(
					rsmdHelper.getOriginalColumnName(rsmd.getColumnName(i + 1)),
					originalColumnNameIndexMap);
		}
	}

	public boolean getTrailNullColumns() {
		return conn.trailNullColumns();
	}

	/**
	 * 
	 * @return
	 * @throws OdaException
	 */
	public String[][] getSourceData() throws OdaException {
		createBufferedReader();
		List<String[]> v = fetchQueriedDataFromFileToList();
		return copyDataFromListToTwoDimensionArray(v);
	}

	/**
	 * Read a row from the source data
	 * 
	 * @return
	 * @throws OdaException
	 * @throws IOException
	 */
	public List<String> readLine() throws OdaException {
		if (isFirstTimeToCallReadLine) {
			createBufferedReader();
			isFirstTimeToCallReadLine = false;
		}
		List<String> line = flatFileBufferedReader.readLine();
		try {
			while (line == null && index < fileNameList.size()) {
				flatFileBufferedReader.close();
				flatFileBufferedReader = null;
				currentTableName = fileNameList.get(index++);
				createBufferedReader();
				if (flatFileBufferedReader == null)
					continue;

				line = flatFileBufferedReader.readLine();
			}
		} catch (IOException e) {
			throw new OdaException(e);
		}

		return line;
	}

	/**
	 * 
	 * @throws OdaException
	 */
	private void createBufferedReader() throws OdaException {
		try {
			if (this.flatFileBufferedReader == null) {
				examCharset(getInputStream());

				this.flatFileBufferedReader = new FlatFileBufferedReader(
						getInputStream(), this.charSet, getDelimeter());
			}

		} catch (IOException e) {
			throw new OdaException(e.getMessage());
		}
	}

	private char getDelimeter() throws OdaException {
		if (this.currentTableName.endsWith(CommonConstants.FILE_EXTENSION_CSV))
			return CommonConstants.getDelimiterValue(
					CommonConstants.DELIMITER_COMMA).charAt(0);
		else if (this.currentTableName
				.endsWith(CommonConstants.FILE_EXTENSION_PSV))
			return CommonConstants.getDelimiterValue(
					CommonConstants.DELIMITER_PIPE).charAt(0);
		else if (this.currentTableName
				.endsWith(CommonConstants.FILE_EXTENSION_SSV))
			return CommonConstants.getDelimiterValue(
					CommonConstants.DELIMITER_SEMICOLON).charAt(0);
		else if (this.currentTableName
				.endsWith(CommonConstants.FILE_EXTENSION_TSV))
			return CommonConstants.getDelimiterValue(
					CommonConstants.DELIMITER_TAB).charAt(0);
		else
			throw new OdaException("Invalid file extension.");
	}

	/**
	 * 
	 * @param resultSetMaxRows
	 * @return
	 */
	public int getMaxRowsToRead(int resultSetMaxRows) {
		this.resultSetMaxRows = resultSetMaxRows;
		return this.maxRowsToRead = ((this.statementMaxRows != 0 && this.statementMaxRows < this.resultSetMaxRows) || this.resultSetMaxRows == 0) ? this.statementMaxRows
				: this.resultSetMaxRows;
	}

	/**
	 * 
	 *
	 */
	public void clearBufferedReader() {
		try {
			if (this.flatFileBufferedReader != null)
				this.flatFileBufferedReader.close();
		} catch (IOException e) {

		}
		this.flatFileBufferedReader = null;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.lang.Object#finalize()
	 */
	public void finalize() {
		this.clearBufferedReader();
	}

	/**
	 * If Charset is not set (that is, of null or empty value), test whether the
	 * file is encoded with "UTF-16LE" or "UTF-16BE". If neither, then treat
	 * file as using default "UTF-8"
	 */
	private void examCharset(InputStream fis) throws OdaException, IOException {
		if (this.charSet != null && this.charSet.length() > 0)
			return;
		byte[] byteMarker = new byte[2];
		fis.read(byteMarker);
		// file encoded using UTF-16LE sometimes have two bytes prefix
		// -1, -2
		// file encoded using UTF-16BE sometimes have two bytes prefix
		// -2, -1
		if (byteMarker[0] == -1 && byteMarker[1] == -2)
			this.charSet = "UTF-16LE"; //$NON-NLS-1$
		else if (byteMarker[0] == -2 && byteMarker[1] == -1)
			this.charSet = "UTF-16BE"; //$NON-NLS-1$
		else
			this.charSet = CommonConstants.CONN_DEFAULT_CHARSET;
		fis.close();
	}

	/**
	 * 
	 * @param connProperties
	 * @param tableName
	 * @return
	 * @throws OdaException
	 */
	public int getColumnCount() throws OdaException {
		int count;
		ResourceInputStream fis = null;
		try {
			fis = getInputStream();
			examCharset(fis);
			fis = getInputStream();
			FlatFileBufferedReader br = new FlatFileBufferedReader(fis,
					this.charSet, getDelimeter());
			List<String> columnLine;
			while (isEmptyRow(columnLine = br.readLine())) {
				continue;
			}
			count = columnLine.size();
			br.close();
			fis.close();
		} catch (IOException e) {
			throw new OdaException(Messages.getString("query_IO_EXCEPTION") //$NON-NLS-1$
					+ fis.getLocation());
		}

		return count;
	}

	public ResourceInputStream getInputStream() throws OdaException {
		return conn.getInputStream(this.currentTableName);
	}

	/**
	 * See if this row is empty or not
	 * 
	 * @param row
	 * @return
	 * @throws OdaException
	 */
	public static boolean isEmptyRow(List<String> line) throws OdaException {
		if (line == null)
			throw new OdaException(
					Messages.getString("query_INVALID_FLAT_FILE")); //$NON-NLS-1$

		return line.isEmpty() || (line.size() == 1 && line.get(0).equals("")); //$NON-NLS-1$
	}

	/**
	 * 
	 * @return
	 * @throws OdaException
	 */
	private List<String[]> fetchQueriedDataFromFileToList() throws OdaException {
		List<String[]> result = new ArrayList<String[]>();
		if (isFirstTimeToReadSourceData) {
			// make a copy of column names if there are
			if (conn.hasColumnNames()) {
				List<String> columeNameLine;
				while (isEmptyRow(columeNameLine = readLine())) {
					continue;
				}
				this.originalColumnNames = getColumnNameArray(columeNameLine);
				initNameIndexMap();
			}

			// skip Type information. The type information is in the second
			// line
			// of file
			if (conn.hasTypeLine()) {
				while (isEmptyRow(readLine()))
					continue;
			}

			if (!conn.hasColumnNames()) {
				while (isEmptyRow(nextDataLine = readLine())) {
					continue;
				}
				this.originalColumnNames = createTempColumnNames(nextDataLine);
				initNameIndexMap();
			} else {
				nextDataLine = readLine();
			}
			isFirstTimeToReadSourceData = false;
		}

		// temporary variable which is used to store the data of a row
		// fetched from a flat file

		int counterLimitPerFetch = fetchCounter + MAX_ROWS_PER_FETCH;

		while ((this.maxRowsToRead <= 0 ? true
				: this.fetchCounter < this.maxRowsToRead)
				&& this.fetchCounter < counterLimitPerFetch
				&& nextDataLine != null) {
			if (!isEmptyRow(nextDataLine)) {
				fetchCounter++;
				result.add(fetchQueriedDataFromRow(nextDataLine));
			}
			nextDataLine = readLine();
		}

		return result;
	}

	/**
	 * Extract the column name from the line into the format of string array
	 * 
	 * @param line
	 * @param isFirstLine
	 * @return
	 * @throws OdaException
	 */
	public String[] getColumnNameArray(List<String> line) throws OdaException {
		if (line == null)
			throw new OdaException(
					Messages.getString("common_CANNOT_FIND_COLUMN")); //$NON-NLS-1$
		return getStringArrayFromList(line);
	}

	/**
	 * Put the contants of the list into a string array
	 * 
	 * @param list
	 * @return
	 */
	public static String[] getStringArrayFromList(List<String> list) {
		String[] array = null;
		if (list != null) {
			array = new String[list.size()];
			for (int i = 0; i < list.size(); i++)
				array[i] = (String) list.get(i);
		}
		return array;
	}

	/**
	 * Feed the row data from a List to a two-dimension array. The string value
	 * is trimmed before being copied into array.
	 * 
	 * @param v
	 * @return a String two dimension array with each horizontal array contains
	 *         a row
	 * @throws OdaException
	 */
	private String[][] copyDataFromListToTwoDimensionArray(List<String[]> v)
			throws OdaException {
		String[][] rowSet = new String[v.size()][this.rsmd.getColumnCount()];
		for (int i = 0; i < v.size(); i++) {
			String[] temp = (String[]) v.get(i);
			for (int j = 0; j < temp.length; j++) {
				if (temp[j] != null)
					rowSet[i][j] = temp[j].trim();
				else if (conn.trailNullColumns()) {
					continue;
				} else {
					throw new OdaException(
							Messages.getString("data_read_error")); //$NON-NLS-1$
				}
			}
		}
		return rowSet;
	}

	/**
	 * 
	 * @param columnCount
	 * @return
	 * @throws OdaException
	 */
	private String[] createTempColumnNames(List<String> aRow)
			throws OdaException {
		String[] tempColumnNames = new String[aRow.size()];

		for (int i = 0; i < aRow.size(); i++) {
			tempColumnNames[i] = "COLUMN_" + (i + 1); //$NON-NLS-1$
		}

		return tempColumnNames;
	}

	/**
	 * Fetch data from a row.
	 * 
	 * @param aRow
	 *            a row read from table
	 * @return an array of data values for each specified column names from a
	 *         row. The "specified column names" are obtained from meta data
	 * @throws OdaException
	 */
	private String[] fetchQueriedDataFromRow(List<String> aRow)
			throws OdaException {
		String[] sArray = new String[rsmd.getColumnCount()];
		for (int i = 0; i < sArray.length; i++) {
			int location = selectColumIndexes[i];
			if (location != -1) {
				if (location >= aRow.size()) {
					if (conn.trailNullColumns())
						sArray[i] = null;
					else
						throw new OdaException(
								Messages.getString("query_INVALID_FLAT_FILE")); //$NON-NLS-1$
				} else {
					sArray[i] = aRow.get(location).toString();
				}
			}
		}
		return sArray;
	}

	/**
	 * Return the 0-based position of a value in the given array
	 * 
	 * @param value
	 * @param array
	 * @return
	 */
	private int findIndex(String value,
			HashMap<String, Integer> originalColumnNameIndexMap) {
		Integer index = originalColumnNameIndexMap.get(value.trim()
				.toUpperCase());
		if (index == null) {
			return -1;
		} else {
			return index.intValue();
		}
	}
}
