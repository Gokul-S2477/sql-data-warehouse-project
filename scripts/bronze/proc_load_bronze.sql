CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN
	DECLARE @starttime DATETIME ,@endtime DATETIME;

	
	BEGIN TRY 
	PRINT '===================================================='
	PRINT 'LOADING BRONZE LAYER'
	PRINT '===================================================='


	PRINT '----------------------------------------------------'	
	PRINT 'LAODING CRM TABLES'
	PRINT '----------------------------------------------------'

	SET @starttime= GETDATE();
	TRUNCATE TABLE [bronze].[crm_cust_info]  
	BULK INSERT [bronze].[crm_cust_info]
	FROM 'C:\GOKUL\projects\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
	with(
		FIRSTROW = 2,
		FIELDTERMINATOR = ',',
		TABLOCK
	);

	TRUNCATE TABLE [bronze].[crm_prd_info]  
	BULK INSERT [bronze].[crm_prd_info]
	FROM 'C:\GOKUL\projects\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'	
	with(
		FIRSTROW = 2,
		FIELDTERMINATOR = ',',
		TABLOCK
	);

	TRUNCATE TABLE [bronze].[crm_sales_details]  
	BULK INSERT [bronze].[crm_sales_details]
	FROM 'C:\GOKUL\projects\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
	with(
		FIRSTROW = 2,
		FIELDTERMINATOR = ',',
		TABLOCK
	);
	
	PRINT '----------------------------------------------------'
	PRINT 'LOADING ERP TABELS'
	PRINT '----------------------------------------------------'
	
	TRUNCATE TABLE [bronze].[erp_cust_az12]
	BULK INSERT [bronze].[erp_cust_az12]
	FROM 'C:\GOKUL\projects\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
	with(
		FIRSTROW = 2,
		FIELDTERMINATOR = ',',
		TABLOCK

	);


	TRUNCATE TABLE [bronze].[erp_loc_a101]  
	BULK INSERT [bronze].[erp_loc_a101]
	FROM 'C:\GOKUL\projects\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
	with(
		FIRSTROW = 2,
		FIELDTERMINATOR = ',',
		TABLOCK
	);


	TRUNCATE TABLE [bronze].[erp_px_cat_g1v2]  
	BULK INSERT [bronze].[erp_px_cat_g1v2]
	FROM 'C:\GOKUL\projects\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
	with(
		FIRSTROW = 2,
		FIELDTERMINATOR = ',',
		TABLOCK
	);
	set @endtime = getdate();
	print 'Time taken to load ' + cast(datediff(second,@starttime,@endtime) as nvarchar) + ' seconds';

	END TRY
	
	BEGIN CATCH
		PRINT 'ERROR OCCURED';
		PRINT ERROR_MESSAGE();
		PRINT ERROR_LINE();
		PRINT ERROR_NUMBER();
	END CATCH



END;
