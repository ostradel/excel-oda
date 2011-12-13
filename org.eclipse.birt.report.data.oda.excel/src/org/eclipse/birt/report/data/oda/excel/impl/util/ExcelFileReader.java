package org.eclipse.birt.report.data.oda.excel.impl.util;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.poi.hssf.usermodel.HSSFWorkbook;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.FormulaEvaluator;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.eclipse.birt.report.data.oda.excel.ExcelODAConstants;

public class ExcelFileReader {

	private FileInputStream fis;
	private String fileExtension;
	private List<String> workSheetList;
	private int currentSheetIndex = 0;

	private Workbook workBook;
	private Sheet sheet;
	private FormulaEvaluator formulaEvaluator;

	private boolean isInitialised;

	private int maxRowsInAllSheet;
	private int maxRowsInThisSheet;
	private int currentRowIndex = 0;
	private int maxColumnIndex = 0;

	public void setMaxColumnIndex(int maxColumnIndex) {
		this.maxColumnIndex = maxColumnIndex;
	}

	private DateFormat dateFormat;

	public ExcelFileReader(FileInputStream fis, String fileExtension,
			List<String> sheetNameList, String dateFormatString) {
		this.fis = fis;
		this.fileExtension = fileExtension;
		this.workSheetList = sheetNameList;
		this.dateFormat = new SimpleDateFormat(dateFormatString);
	}

	public List<String> readLine() throws IOException {
		if (!isInitialised)
			initialise();

		if (currentRowIndex >= maxRowsInThisSheet) {
			if (!initialiseNextSheet())
				return null;
		}

		Row row = sheet.getRow(currentRowIndex);

		List<String> rowData = new ArrayList<String>();

		if (maxColumnIndex == 0)
			maxColumnIndex = row.getLastCellNum();
		
		for (short colIx = 0; colIx < maxColumnIndex; colIx++) {
			Cell cell = row.getCell(colIx);
			rowData.add(getCellValue(cell));
		}

		currentRowIndex++;
		return rowData;
	}

	public void close() throws IOException {
		this.fis.close();
	}

	private void initialise() throws IOException {
		workBook = isXlsxFile(fileExtension) ? new XSSFWorkbook(fis)
				: new HSSFWorkbook(fis);
		formulaEvaluator = workBook.getCreationHelper()
				.createFormulaEvaluator();
		workBook.setMissingCellPolicy(Row.RETURN_NULL_AND_BLANK);
		sheet = workBook.getSheet(workSheetList.get(currentSheetIndex));
		maxRowsInThisSheet = sheet.getPhysicalNumberOfRows();

		for (String sheetName : workSheetList) {
			Sheet localSheet = workBook.getSheet(sheetName);
			maxRowsInAllSheet += localSheet.getPhysicalNumberOfRows();
		}

		isInitialised = true;
	}

	private boolean initialiseNextSheet() {
		if (workSheetList.size() <= ++currentSheetIndex) {
			return false;
		}

		do {
			sheet = workBook.getSheet(workSheetList.get(currentSheetIndex));
			maxRowsInThisSheet = sheet.getPhysicalNumberOfRows();
		} while (maxRowsInThisSheet == 0
				&& (workSheetList.size() < ++currentSheetIndex));

		if (maxRowsInThisSheet == 0)
			return false;

		currentRowIndex = 0;
		return true;
	}

	private static boolean isXlsxFile(String extension) {
		return !extension.equals(ExcelODAConstants.XLS_FORMAT);
	}

	public String getCellValue(Cell cell) {
		if (cell == null)
			return ExcelODAConstants.EMPTY_STRING;

		if (cell.getCellType() == Cell.CELL_TYPE_FORMULA) {
			return resolveFormula(cell);
		}

		if (cell.getCellType() == Cell.CELL_TYPE_NUMERIC) {
			if (org.apache.poi.ss.usermodel.DateUtil.isCellDateFormatted(cell)) {
				Date date = cell.getDateCellValue();
				return dateFormat.format(date);
			}

			return ((Double) cell.getNumericCellValue()).toString();
		}

		return cell.toString();
	}

	private String resolveFormula(Cell cell) {
		if (formulaEvaluator == null)
			return cell.toString();

		switch (formulaEvaluator.evaluateFormulaCell(cell)) {
		case Cell.CELL_TYPE_BOOLEAN:
			return ((Boolean) cell.getBooleanCellValue()).toString();

		case Cell.CELL_TYPE_NUMERIC:
			if (org.apache.poi.ss.usermodel.DateUtil.isCellDateFormatted(cell)) {
				Date date = cell.getDateCellValue();
				return dateFormat.format(date);
			}

			return ((Double) cell.getNumericCellValue()).toString();

		case Cell.CELL_TYPE_STRING:
			return cell.getStringCellValue();

		default:
			return null;
		}
	}

	public int getMaxRows() throws IOException {
		if (!isInitialised)
			initialise();
		return maxRowsInAllSheet;
	}

	public static List<String> getSheetNamesInExcelFile(File file) {
		String extension = file.getName();
		extension = extension.substring(extension.lastIndexOf(".") + 1,
				extension.length());
		FileInputStream fis;
		List<String> sheetNames = new ArrayList<String>();
		try {
			fis = new FileInputStream(file);
			Workbook workBook = isXlsxFile(extension) ? new XSSFWorkbook(fis)
					: new HSSFWorkbook(fis);
			for (int i = 0; i < workBook.getNumberOfSheets(); i++) {
				sheetNames.add(workBook.getSheetName(i));
			}
		} catch (FileNotFoundException e) {
			// do nothing
		} catch (IOException e) {
			// do nothing
		}
		return sheetNames;
	}
}
