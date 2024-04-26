
	MERGE [dm].[F_PURCHASE_RETURNS] trg
		USING [dm_prl].[F_PURCHASE_RETURNS] src
		 ON [src].[Material_Document_SK]								= [trg].[Material_Document_SK]
		 And [src].[Material_Document_Item]							= [trg].[Material_Document_Item]
		 and [src].[Material_Document_Year]							= [trg].[Material_Document_Year]
		 and [src].[Purchase_Invoice_SK]								= [trg].[Purchase_Invoice_SK]
		 and [src].[Purchase_Invoice_Item_ID]							= [trg].[Purchase_Invoice_Item_ID]

WHEN MATCHED THEN
	UPDATE SET  
			 [Purchase_Order_SK]                            = SRC. [Purchase_Order_SK]   
            ,[Purchasing_Document_Item_Number]              = SRC.[Purchasing_Document_Item_Number]
            ,[Document_DT]                                  = SRC.[Document_DT]
            ,[Movement_Type]                                = SRC.[Movement_Type]
            ,[Document_Type_ID]                             = SRC.[Document_Type_ID]
            ,[Return_Status]                                = SRC.[Return_Status]
            ,[Purchase_Invoice_SK]                          = src.[Purchase_Invoice_SK] 
            ,[Purchase_Invoice_Item_ID]                     = src.[Purchase_Invoice_Item_ID]
            ,[User_Name]                                    = SRC.[User_Name]
            ,[Material_Document_SK]                         = SRC.[Material_Document_SK]
            ,[Material_Document_Item]                       = SRC.[Material_Document_Item]
            ,[Material_Document_Year]                       = SRC.[Material_Document_Year]
            ,[Quantity]                                     = SRC.[Quantity]
            ,[Local_Currency_Amount]                        = SRC.[Local_Currency_Amount]
            ,[Count_CreditNotes]                            = SRC.[Count_CreditNotes]
            ,[ETL_Reference_DT]                             = SRC.[ETL_Reference_DT]
            ,[ETL_Batch_ID]                                 = SRC.[ETL_Batch_ID]

	WHEN NOT MATCHED BY TARGET
	THEN INSERT (
		  [Purchase_Order_SK] 
		 ,[Purchasing_Document_Item_Number]
		 ,[Document_DT]
		 ,[Movement_Type]
		 ,[Document_Type_ID]
		 ,[Return_Status]
		 ,[Purchase_Invoice_SK] 
		 ,[Purchase_Invoice_Item_ID]
		 ,[User_Name]
		 ,[Material_Document_SK]
		 ,[Material_Document_Item]
		 ,[Material_Document_Year]
		 ,[Quantity]
		 ,[Local_Currency_Amount]
		 ,[Count_CreditNotes]
         ,[ETL_Reference_DT]
		 ,[ETL_Batch_ID]
	)
	VALUES (
		  src.[Purchase_Order_SK] 
		 ,src.[Purchasing_Document_Item_Number]
		 ,src.[Document_DT]
		 ,src.[Movement_Type]
		 ,src.[Document_Type_ID]
		 ,src.[Return_Status]
		 ,isnull(src.[Purchase_Invoice_SK],-1) 
		 ,isnull(src.[Purchase_Invoice_Item_ID],-1)
		 ,src.[User_Name]
		 ,src.[Material_Document_SK]
		 ,src.[Material_Document_Item]
		 ,src.[Material_Document_Year]
		 ,src.[Quantity]
		 ,src.[Local_Currency_Amount]
		 ,src.[Count_CreditNotes]
         ,src.[ETL_Reference_DT]
		 ,src.[ETL_Batch_ID]
	);


