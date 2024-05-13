
	MERGE [dm].[F_PURCHASES_CREDIT_NOTES] trg
		USING [dm_prl].[F_PURCHASES_CREDIT_NOTES_hist] src
		 ON [src].[Company_SK]								= [trg].[Company_SK]
		 And [src].[21_Document_SK]							= [trg].[21_Document_SK]		
		 And [src].[Fiscal_Year]								= [trg].[Fiscal_Year]
		 and [src].[29_Document_SK]				= [trg].[29_Document_SK]
		 and [src].[21_Accounting_Document_Line_ID]			= [trg].[21_Accounting_Document_Line_ID]
		 and [src].[29_Accounting_Document_Line_ID]			= [trg].[29_Accounting_Document_Line_ID]


	WHEN NOT MATCHED BY TARGET
	THEN INSERT (
			 [Company_SK]
			,[Sales_Organization_SK]
			,[21_Document_SK]
			,[21_Accounting_Document_Line_ID]
			,[Fiscal_Year]
			,[PostingDatekey] 
			,[FinalAmount]
			,[Material_SK]
			,[Plant_SK]
			,[Vendor_SK]
			,[29_Document_SK]
			,[29_Accounting_Document_Line_ID]
            ,[ETL_Reference_DT]
			,[ETL_Batch_ID]
	)
	VALUES (
		 src.[Company_SK]
		,src.[Sales_Organization_SK]
		,src.[21_Document_SK]
		,src.[21_Accounting_Document_Line_ID]
		,src.[Fiscal_Year]
		,src.[PostingDatekey] 
		,src.[FinalAmount]
		,src.[Material_SK]
		,src.[Plant_SK]
		,src.[Vendor_SK]
		,src.[29_Document_SK]
		,src.[29_Accounting_Document_Line_ID]
        ,src.[ETL_Reference_DT]
        ,src.[ETL_Batch_ID]
	);
