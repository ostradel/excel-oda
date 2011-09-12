package org.eclipse.birt.report.data.oda.excel.ui.wizards;

import java.io.File;
import java.util.Properties;

import org.eclipse.birt.report.data.oda.excel.ExcelODAConstants;
import org.eclipse.birt.report.data.oda.excel.ui.i18n.Messages;
import org.eclipse.birt.report.data.oda.excel.ui.util.IHelpConstants;
import org.eclipse.birt.report.data.oda.excel.ui.util.Utility;
import org.eclipse.datatools.connectivity.oda.design.ui.nls.TextProcessorWrapper;
import org.eclipse.jface.dialogs.IMessageProvider;
import org.eclipse.jface.preference.PreferencePage;
import org.eclipse.jface.wizard.WizardPage;
import org.eclipse.swt.SWT;
import org.eclipse.swt.events.ModifyEvent;
import org.eclipse.swt.events.ModifyListener;
import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.layout.GridData;
import org.eclipse.swt.layout.GridLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.DirectoryDialog;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Text;

public class ExcelDataSourcePageHelper {

	static final String DEFAULT_MESSAGE = Messages
			.getString("wizard.defaultMessage.selectFolder"); //$NON-NLS-1$

	private WizardPage wizardPage;
	private PreferencePage propertyPage;

	private transient Text folderLocation = null;
	private transient Button typeLineCheckBox = null;
	private transient Button browseFolderButton = null;
	private transient Button columnNameLineCheckBox = null;

	private static final int CORRECT_FOLDER = 0;
	private static final int ERROR_FOLDER = 1;
	private static final int ERROR_EMPTY_PATH = 2;
	private static final String EMPTY_STRING = ""; //$NON-NLS-1$

	public ExcelDataSourcePageHelper(ExcelDataSourceWizardPage page) {
		wizardPage = page;
	}

	public ExcelDataSourcePageHelper(
			ExcelDataSourcePropertyPage excelDataSourcePropertyPage) {
		propertyPage = excelDataSourcePropertyPage;
	}

	/**
	 * 
	 * @param parent
	 */
	void createCustomControl(Composite parent) {
		Composite content = new Composite(parent, SWT.NULL);
		GridLayout layout = new GridLayout(3, false);
		content.setLayout(layout);

		// GridData data;
		setupFolderLocation(content);

		setupColumnNameLineCheckBox(content);

		setupTypeLineCheckBox(content);

		Utility.setSystemHelp(getControl(),
				IHelpConstants.CONEXT_ID_DATASOURCE_EXCEL);
	}

	private void setupFolderLocation(Composite composite) {
		Label label = new Label(composite, SWT.NONE);
		label.setText(Messages.getString("label.selectFolder")); //$NON-NLS-1$

		GridData data = new GridData(GridData.FILL_HORIZONTAL);

		folderLocation = new Text(composite, SWT.BORDER);
		folderLocation.setLayoutData(data);
		setPageComplete(false);
		folderLocation.addModifyListener(new ModifyListener() {

			public void modifyText(ModifyEvent e) {
				verifyFileLocation();
			}

		});

		browseFolderButton = new Button(composite, SWT.NONE);
		browseFolderButton.setText(Messages
				.getString("button.selectFolder.browse")); //$NON-NLS-1$
		browseFolderButton.addSelectionListener(new SelectionAdapter() {

			/*
			 * @see
			 * org.eclipse.swt.events.SelectionAdapter#widgetSelected(org.eclipse
			 * .swt.events.SelectionEvent)
			 */
			public void widgetSelected(SelectionEvent e) {
				DirectoryDialog dialog = new DirectoryDialog(folderLocation
						.getShell());
				String folderLocationValue = getFolderLocationString();
				if (folderLocationValue != null
						&& folderLocationValue.trim().length() > 0) {
					dialog.setFilterPath(folderLocationValue);
				}

				dialog.setMessage(DEFAULT_MESSAGE);
				String selectedLocation = dialog.open();
				if (selectedLocation != null) {
					setFolderLocationString(selectedLocation);
				}
			}
		});
	}

	/**
	 * 
	 * @param composite
	 */
	private void setupColumnNameLineCheckBox(Composite composite) {
		Label labelFill = new Label(composite, SWT.NONE);
		labelFill.setText(""); //$NON-NLS-1$

		columnNameLineCheckBox = new Button(composite, SWT.CHECK);
		columnNameLineCheckBox.setToolTipText(Messages
				.getString("tooltip.columnnameline")); //$NON-NLS-1$
		GridData gd = new GridData();
		gd.horizontalSpan = 3;
		columnNameLineCheckBox.setLayoutData(gd);
		columnNameLineCheckBox.setText(Messages
				.getString("label.includeColumnNameLine")); //$NON-NLS-1$
		columnNameLineCheckBox.setSelection(true);
		columnNameLineCheckBox.addSelectionListener(new SelectionAdapter() {

			public void widgetSelected(SelectionEvent e) {
				if (columnNameLineCheckBox.getSelection())
					typeLineCheckBox.setEnabled(true);
				else {
					typeLineCheckBox.setSelection(false);
					typeLineCheckBox.setEnabled(false);
				}
			}
		});

	}

	/**
	 * @param composite
	 */
	private void setupTypeLineCheckBox(Composite composite) {
		typeLineCheckBox = new Button(composite, SWT.CHECK);
		typeLineCheckBox.setToolTipText(Messages.getString("tooltip.typeline")); //$NON-NLS-1$
		GridData data = new GridData();
		data.horizontalSpan = 3;
		typeLineCheckBox.setLayoutData(data);
		typeLineCheckBox.setText(Messages.getString("label.includeTypeLine")); //$NON-NLS-1$
		if (columnNameLineCheckBox.getSelection())
			typeLineCheckBox.setEnabled(true);
		else {
			typeLineCheckBox.setSelection(false);
			typeLineCheckBox.setEnabled(false);
		}
	}

	/**
	 * 
	 * @param props
	 * @return
	 */
	Properties collectCustomProperties(Properties props) {
		if (props == null)
			props = new Properties();

		// set custom driver specific properties
		props.setProperty(ExcelODAConstants.CONN_HOME_DIR_PROP,
				getFolderLocation().trim());
		props.setProperty(ExcelODAConstants.CONN_INCLCOLUMNNAME_PROP,
				getWhetherUseFirstLineAsColumnNameLine());
		props.setProperty(ExcelODAConstants.CONN_INCLTYPELINE_PROP,
				getWhetherUseSecondLineAsTypeLine());

		return props;
	}

	/**
	 * 
	 * @return
	 */
	String getFolderLocation() {
		if (folderLocation == null)
			return EMPTY_STRING;
		return getFolderLocationString();
	}

	/**
	 * 
	 * @return
	 */
	String getWhetherUseFirstLineAsColumnNameLine() {
		if (columnNameLineCheckBox == null
				|| !columnNameLineCheckBox.getEnabled())
			return EMPTY_STRING;
		return columnNameLineCheckBox.getSelection() ? ExcelODAConstants.INC_COLUMN_NAME_YES
				: ExcelODAConstants.INC_COLUMN_NAME_NO;
	}

	/**
	 * 
	 * @return
	 */
	String getWhetherUseSecondLineAsTypeLine() {
		if (typeLineCheckBox == null)
			return EMPTY_STRING;
		return typeLineCheckBox.getSelection() ? ExcelODAConstants.INC_TYPE_LINE_YES
				: ExcelODAConstants.INC_TYPE_LINE_NO;
	}

	/**
	 * 
	 * @param profileProps
	 */
	void initCustomControl(Properties profileProps) {
		if (profileProps == null || profileProps.isEmpty()
				|| folderLocation == null)
			return; // nothing to initialize

		String folderPath = profileProps
				.getProperty(ExcelODAConstants.CONN_HOME_DIR_PROP);
		if (folderPath == null)
			folderPath = EMPTY_STRING;
		setFolderLocationString(folderPath);

		String hasColumnNameLine = profileProps
				.getProperty(ExcelODAConstants.CONN_INCLCOLUMNNAME_PROP);
		if (hasColumnNameLine == null)
			hasColumnNameLine = ExcelODAConstants.INC_COLUMN_NAME_YES;
		if (hasColumnNameLine
				.equalsIgnoreCase(ExcelODAConstants.INC_COLUMN_NAME_YES)) {
			columnNameLineCheckBox.setSelection(true);

			String useSecondLine = profileProps
					.getProperty(ExcelODAConstants.CONN_INCLTYPELINE_PROP);
			if (useSecondLine == null)
				useSecondLine = EMPTY_STRING;
			typeLineCheckBox.setEnabled(true);
			typeLineCheckBox.setSelection(useSecondLine
					.equalsIgnoreCase(ExcelODAConstants.INC_TYPE_LINE_YES));
		} else {
			columnNameLineCheckBox.setSelection(false);
			typeLineCheckBox.setSelection(false);
			typeLineCheckBox.setEnabled(false);
		}

		verifyFileLocation();
	}

	/**
	 * 
	 * @param complete
	 */
	private void setPageComplete(boolean complete) {
		if (wizardPage != null)
			wizardPage.setPageComplete(complete);
		else if (propertyPage != null)
			propertyPage.setValid(complete);
	}

	/**
	 * 
	 * @return
	 */
	private int verifyFileLocation() {
		int result = CORRECT_FOLDER;
		String folderLocationValue = getFolderLocationString();
		if (folderLocationValue.trim().length() > 0) {
			File f = new File(folderLocationValue.trim());
			if (f.exists()) {
				setMessage(DEFAULT_MESSAGE, IMessageProvider.NONE);
				setPageComplete(true);
			} else {
				setMessage(
						Messages.getString("error.selectFolder"), IMessageProvider.ERROR); //$NON-NLS-1$
				setPageComplete(false);
				result = ERROR_FOLDER;
			}
		} else {
			setMessage(
					Messages.getString("error.emptyPath"), IMessageProvider.ERROR); //$NON-NLS-1$
			setPageComplete(false);
			result = ERROR_EMPTY_PATH;
		}
		if (result == CORRECT_FOLDER)
			return result;

		if (wizardPage == null) {
			setPageComplete(true);
			setMessage(
					Messages.getString("error.invalidFlatFilePath"), IMessageProvider.ERROR); //$NON-NLS-1$
		}
		return result;
	}

	/**
	 * 
	 * @return
	 */
	private String getFolderLocationString() {
		return TextProcessorWrapper.deprocess(folderLocation.getText());
	}

	/**
	 * 
	 * @param folderPath
	 */
	private void setFolderLocationString(String folderPath) {
		folderLocation.setText(TextProcessorWrapper.process(folderPath));
	}

	/**
	 * 
	 * @param newMessage
	 * @param newType
	 */
	private void setMessage(String newMessage, int newType) {
		if (wizardPage != null)
			wizardPage.setMessage(newMessage, newType);
		else if (propertyPage != null)
			propertyPage.setMessage(newMessage, newType);
	}

	private Control getControl() {
		if (wizardPage != null)
			return wizardPage.getControl();
		if (propertyPage != null)
			return propertyPage.getControl();

		return null;
	}
}
