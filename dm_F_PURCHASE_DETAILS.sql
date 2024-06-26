MERGE [dm].[F_PURCHASE_DETAILS] trg
		USING [dm_prl].[F_PURCHASE_DETAILS_hist] src
		 ON [src].[Purchasing_Document_SK]								= [trg].[Purchasing_Document_SK]
		 And [src].[Purchasing_Document_Item_ID]							= [trg].[Purchasing_Document_Item_ID]		

	WHEN MATCHED THEN
	UPDATE SET 
	       [Purchasing_Document_SK]   =  src.[Purchasing_Document_SK]
          ,[Client_ID]   =  src.[Client_ID]
          ,[Purchasing_Document_ID]   =  src.[Purchasing_Document_ID]
          ,[Company_SK]   =  src.[Company_SK]
          ,[Sales_Organization_SK]   =  src.[Sales_Organization_SK]
          ,[Purchasing_Document_Type_ID]   =  src.[Purchasing_Document_Type_ID]
          ,[Purchasing_document_Type_Control_IND]   =  src.[Purchasing_document_Type_Control_IND]
          ,[Purchasing_Document_Status]   =  src.[Purchasing_Document_Status]
          ,[Created_DT]   =  src.[Created_DT]
          ,[Created_By]   =  src.[Created_By]
          ,[Vendor_SK]   =  src.[Vendor_SK]
          ,[Return_Reason_ID]   =  src.[Return_Reason_ID]
          ,[Purpose_Of_Movement_ID]   =  src.[Purpose_Of_Movement_ID]
          ,[Purchasing_Document_Item_ID]   =  src.[Purchasing_Document_Item_ID]
          ,[Material_SK]   =  src.[Material_SK]
          ,[Plant_SK]   =  src.[Plant_SK]
          ,[Purchase_Order_Quantity]   =  src.[Purchase_Order_Quantity]
          ,[Net_Order_Value]   =  src.[Net_Order_Value]
          ,[Delivery_Completed_IND]   =  src.[Delivery_Completed_IND]
          ,[Vendor_Material_Number]   =  src.[Vendor_Material_Number]
          ,[Purchasing_Document_Deletion_IND]   =  src.[Purchasing_Document_Deletion_IND]
          ,[Returns_Item_IND]   =  src.[Returns_Item_IND]
          ,[Purchasing_Document_DT]   =  src.[Purchasing_Document_DT]
          ,[Purchasing_Document_Deletion_FLG]   =  src.[Purchasing_Document_Deletion_FLG]
          ,[Payment_Terms_ID]   =  src.[Payment_Terms_ID]
          ,[Purchasing_Group_ID]   =  src.[Purchasing_Group_ID]
          ,[Supplying_Plant_SK]   =  src.[Supplying_Plant_SK]
          ,[Header_Plant_SK]   =  src.[Header_Plant_SK]
          ,[Purchasing_Document_Item_Change_DT]   =  src.[Purchasing_Document_Item_Change_DT]
          ,[Storage_Location_SK]   =  src.[Storage_Location_SK]
          ,[Purchasing_Document_Net_Price]   =  src.[Purchasing_Document_Net_Price]
          ,[Price_Unit]   =  src.[Price_Unit]
          ,[Gross_Order_Value]   =  src.[Gross_Order_Value]
          ,[Effective_Item_Value]   =  src.[Effective_Item_Value]
          ,[Purchase_Requisition_SK]   =  src.[Purchase_Requisition_SK]
          ,[Purchase_Requisition_Item_ID]   =  src.[Purchase_Requisition_Item_ID]
          ,[Requisitioner_Requester_Name]   =  src.[Requisitioner_Requester_Name]
          ,[Planned_Delivery_Time_in_Days]   =  src.[Planned_Delivery_Time_in_Days]
          ,[Stock_Transport_Order_Rejection_Reasons]   =  src.[Stock_Transport_Order_Rejection_Reasons]
          ,[Price_Determination_DT]   =  src.[Price_Determination_DT]
          ,[ETL_Reference_DT]   =  src.[ETL_Reference_DT]
          ,[ETL_Batch_ID]   =  src.[ETL_Batch_ID]

	WHEN NOT MATCHED BY TARGET
	THEN INSERT (
		 [Purchasing_Document_SK]
      ,[Client_ID]
      ,[Purchasing_Document_ID]
      ,[Company_SK]
      ,[Sales_Organization_SK]
      ,[Purchasing_Document_Type_ID]
      ,[Purchasing_document_Type_Control_IND]
      ,[Purchasing_Document_Status]
      ,[Created_DT]
      ,[Created_By]
      ,[Vendor_SK]
      ,[Return_Reason_ID]
      ,[Purpose_Of_Movement_ID]
      ,[Purchasing_Document_Item_ID]
      ,[Material_SK]
      ,[Plant_SK]
      ,[Purchase_Order_Quantity]
      ,[Net_Order_Value]
      ,[Delivery_Completed_IND]
      ,[Vendor_Material_Number]
      ,[Purchasing_Document_Deletion_IND]
      ,[Returns_Item_IND]
      ,[Purchasing_Document_DT]
      ,[Purchasing_Document_Deletion_FLG]
      ,[Payment_Terms_ID]
      ,[Purchasing_Group_ID]
      ,[Supplying_Plant_SK]
      ,[Header_Plant_SK]
      ,[Purchasing_Document_Item_Change_DT]
      ,[Storage_Location_SK]
      ,[Purchasing_Document_Net_Price]
      ,[Price_Unit]
      ,[Gross_Order_Value]
      ,[Effective_Item_Value]
      ,[Purchase_Requisition_SK]
      ,[Purchase_Requisition_Item_ID]
      ,[Requisitioner_Requester_Name]
      ,[Planned_Delivery_Time_in_Days]
      ,[Stock_Transport_Order_Rejection_Reasons]
      ,[Price_Determination_DT]
      ,[ETL_Reference_DT]
      ,[ETL_Batch_ID]
	)
	VALUES (
	   src.[Purchasing_Document_SK]
      ,src.[Client_ID]
      ,src.[Purchasing_Document_ID]
      ,src.[Company_SK]
      ,src.[Sales_Organization_SK]
      ,src.[Purchasing_Document_Type_ID]
      ,src.[Purchasing_document_Type_Control_IND]
      ,src.[Purchasing_Document_Status]
      ,src.[Created_DT]
      ,src.[Created_By]
      ,src.[Vendor_SK]
      ,src.[Return_Reason_ID]
      ,src.[Purpose_Of_Movement_ID]
      ,src.[Purchasing_Document_Item_ID]
      ,src.[Material_SK]
      ,src.[Plant_SK]
      ,src.[Purchase_Order_Quantity]
      ,src.[Net_Order_Value]
      ,src.[Delivery_Completed_IND]
      ,src.[Vendor_Material_Number]
      ,src.[Purchasing_Document_Deletion_IND]
      ,src.[Returns_Item_IND]
      ,src.[Purchasing_Document_DT]
      ,src.[Purchasing_Document_Deletion_FLG]
      ,src.[Payment_Terms_ID]
      ,src.[Purchasing_Group_ID]
      ,src.[Supplying_Plant_SK]
      ,src.[Header_Plant_SK]
      ,src.[Purchasing_Document_Item_Change_DT]
      ,src.[Storage_Location_SK]
      ,src.[Purchasing_Document_Net_Price]
      ,src.[Price_Unit]
      ,src.[Gross_Order_Value]
      ,src.[Effective_Item_Value]
      ,src.[Purchase_Requisition_SK]
      ,src.[Purchase_Requisition_Item_ID]
      ,src.[Requisitioner_Requester_Name]
      ,src.[Planned_Delivery_Time_in_Days]
      ,src.[Stock_Transport_Order_Rejection_Reasons]
      ,src.[Price_Determination_DT]
      ,src.[ETL_Reference_DT]
      ,src.[ETL_Batch_ID]
	);

