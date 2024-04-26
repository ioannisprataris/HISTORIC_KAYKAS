
	MERGE [dm].[F_PURCHASE_RECEIVALS] trg
		USING [dm_prl].[F_PURCHASE_RECEIVALS] src
		 ON [src].[Purchasing_Document_SK]								= [trg].[Purchasing_Document_SK]
		 And [src].[Purchasing_Document_Item_ID]							= [trg].[Purchasing_Document_Item_ID]		
		 And [src].[Account_Assignment_Sequential_ID]						= [trg].[Account_Assignment_Sequential_ID]	
		 And [src].[Purchase_Order_History_Transaction_Event_Type_ID]		= [trg].[Purchase_Order_History_Transaction_Event_Type_ID]		
		 And [src].[Material_Document_Year]								= [trg].[Material_Document_Year]	
		 And [src].[Material_Document_SK]									= [trg].[Material_Document_SK] 
		 And [src].[Material_Document_Item_ID]							= [trg].[Material_Document_Item_ID] 

	WHEN MATCHED THEN 
	UPDATE SET 
	             [Purchasing_Document_SK]		=  src.[Purchasing_Document_SK]
                ,[Purchasing_Document_Item_ID]  =  src.[Purchasing_Document_Item_ID]
                ,[Account_Assignment_Sequential_ID]  =  src.[Account_Assignment_Sequential_ID]
                ,[Purchase_Order_History_Transaction_Event_Type_ID]  =  src.[Purchase_Order_History_Transaction_Event_Type_ID]
                ,[Material_Document_Year]  =  src.[Material_Document_Year]
                ,[Material_Document_SK]  =  src.[Material_Document_SK]
                ,[Material_Document_Item_ID]  =  src.[Material_Document_Item_ID]
                ,[DeliveredQty]  =  src.[DeliveredQty]
                ,[OrderLineDeliveryStatus]  =  src.[OrderLineDeliveryStatus]
                ,[OrderCharacterization]  =  src.[OrderCharacterization]
                ,[DeliveryDt]  =  src.[DeliveryDt]
                ,[CPUDt]  =  src.[CPUDt]
                ,[ETL_Reference_DT]  =  src.[ETL_Reference_DT]
                ,[ETL_Batch_ID]  =  src.[ETL_Batch_ID]

	WHEN NOT MATCHED BY TARGET
	THEN INSERT (
		[Purchasing_Document_SK]
      ,[Purchasing_Document_Item_ID]
      ,[Account_Assignment_Sequential_ID]
      ,[Purchase_Order_History_Transaction_Event_Type_ID]
      ,[Material_Document_Year]
      ,[Material_Document_SK]
      ,[Material_Document_Item_ID]
      ,[DeliveredQty]
      ,[OrderLineDeliveryStatus]
      ,[OrderCharacterization]
      ,[DeliveryDt]
      ,[CPUDt]
      ,[ETL_Reference_DT]
      ,[ETL_Batch_ID]
	)
	VALUES (
	   src.[Purchasing_Document_SK]
      ,src.[Purchasing_Document_Item_ID]
      ,src.[Account_Assignment_Sequential_ID]
      ,src.[Purchase_Order_History_Transaction_Event_Type_ID]
      ,src.[Material_Document_Year]
      ,src.[Material_Document_SK]
      ,src.[Material_Document_Item_ID]
      ,src.[DeliveredQty]
      ,src.[OrderLineDeliveryStatus]
      ,src.[OrderCharacterization]
      ,src.[DeliveryDt]
      ,src.[CPUDt]
      ,src.[ETL_Reference_DT]
      ,src.[ETL_Batch_ID]
	);
