package org.eclipse.birt.report.data.oda.excel.impl.util;

import java.util.ArrayList;
import java.util.List;

public class XlsxRowCallBack implements RowCallBack {
	private ArrayList<String[]> xlsxRowData = new ArrayList<String[]>();
	private int rowNum;

	public void handleRow(List<Object> values) {
		++rowNum;

		if (values == null || values.size() == 0) {
			return;
		}
		String[] valArray = new String[values.size()];
		values.toArray(valArray);

		xlsxRowData.add(valArray);
		for(Object val : values)
		System.out.print(val + " ");
		System.out.println();
	}

	public ArrayList<String> initArrayList(String[] strings) {
		ArrayList<String> list = new ArrayList<String>();
		for (String i : strings) {
			list.add(i);
		}
		return list;
	}

	public int getMaxRowsInSheet() {
		return (xlsxRowData.size());
	}

	public ArrayList<String> getRow(int rownum) {
		return (initArrayList(xlsxRowData.get(rownum)));
	}
}