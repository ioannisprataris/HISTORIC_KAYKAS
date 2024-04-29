
DROP TABLE IF EXISTS [temp].[F_PURCHASES_DET_LOG];

Create table [temp].[F_PURCHASES_DET_LOG] ([Purchasing_Document_SK] int not null primary key)



Insert into [temp].[F_PURCHASES_DET_LOG] ([Purchasing_Document_SK])

Select [Purchasing_Document_SK] from [dbo].[PURCHASE_HEADER] where ETL_Batch_ID = -9999 
union
Select [Purchasing_Document_SK] from [dbo].[PURCHASE_DETAIL] where ETL_Batch_ID = -9999 



INSERT INTO [dm_prl].[F_PURCHASE_DETAILS] 
(		 
		 [Purchasing_Document_SK], 
		 [Client_ID], 
		 [Purchasing_Document_ID], 
		 [Company_SK], 
		 [Sales_Organization_SK],
		 [Purchasing_Document_Type_ID], 
		 [Purchasing_document_Type_Control_IND], 
		 [Purchasing_Document_Status], 
		 [Created_DT], 
		 [Created_By], 
		 [Vendor_SK], 
		 [Return_Reason_ID], 
		 [Purpose_Of_Movement_ID], 
		 [Purchasing_Document_Item_ID], 
		 [Material_SK], 
		 [Plant_SK], 
		 [Purchase_Order_Quantity], 
		 [Net_Order_Value],
		 [Delivery_Completed_IND], 
		 [Vendor_Material_Number],
		 [Purchasing_Document_Deletion_IND], 
		 [Returns_Item_IND], 
		 [Purchasing_Document_DT], 
		 [Purchasing_Document_Deletion_FLG], 
		 [Payment_Terms_ID], 
		 [Purchasing_Group_ID], 
		 [Supplying_Plant_SK], 
		 [Header_Plant_SK],
		 [Purchasing_Document_Item_Change_DT], -- AEDAT detail
		 [Storage_Location_SK], -- LGORT Detail
		 [Purchasing_Document_Net_Price], -- NETPR Detail
		 [Price_Unit], --PEINH detail
		 [Gross_Order_Value], -- BRTWR detail
		 [Effective_Item_Value], -- EFFWR detail
		 [Purchase_Requisition_SK], -- FK gia BANFN 
		 [Purchase_Requisition_Item_ID], -- BNFPO detail
		 [Requisitioner_Requester_Name], -- AFNAM detail 
		 [Planned_Delivery_Time_in_Days], -- From MARC table
		 [Stock_Transport_Order_Rejection_Reasons], -- ZZ_STO_REJRES
		 [Price_Determination_DT], -- PRDAT
		 [ETL_Reference_DT],			
		 [ETL_Batch_ID]
)
 SELECT 
	  ph.[Purchasing_Document_SK] -- FK gia EBELN
	 ,ph.[Client_ID] -- MANDT
	 ,ph.[Purchasing_Document_ID] -- EBELN
	 ,ph.[Company_SK] -- BUKRS
	 ,pla.[Sales_Organization_SK]
	 ,ph.[Purchasing_Document_Type_ID] -- BSART
	 ,ph.[Purchasing_document_Type_Control_IND]
	 ,ph.[Purchasing_Document_Status]
	 ,ph.[Created_DT] --AEDAT
	 ,ph.[Created_By] -- ERNAM
	 ,ph.[Vendor_SK] -- FK gia LIFNR
	 ,ph.[Return_Reason_ID] -- ZZ_TEXT
	 ,ph.[Purpose_Of_Movement_ID] -- ZZ_RET_MOVEMENT
	 ,pd.[Purchasing_Document_Item_ID] -- EBELP
	 ,pd.[Material_SK] -- FK gia MATNR
	 ,pd.[Plant_SK] -- FK gia WERKS
	 ,pd.[Purchase_Order_Quantity] -- MENGE
	 ,pd.[Net_Order_Value] -- NETWR
	 ,pd.[Delivery_Completed_IND] -- ELIKZ
	 ,pd.[Vendor_Material_Number] -- IDNLF
	 ,pd.[Purchasing_Document_Deletion_IND] -- LOEKZ 
	 ,pd.[Returns_Item_IND] -- RETPO 
	 ,ph.[Purchasing_Document_DT] -- BEDAT
	 ,ph.[Purchasing_Document_Deletion_FLG] -- LOEKZ
	 ,ph.[Payment_Terms_ID] -- ZTERM
	 ,ph.[Purchasing_Group_ID] -- EKGRP
	 ,ph.[Supplying_Plant_SK] -- FK gia RESWK
	 ,ph.[Plant_SK] -- FK gia ZZ_WERKS
	 ,pd.[Purchasing_Document_Item_Change_DT] -- AEDAT detail
	 ,pd.[Storage_Location_SK] -- LGORT Detail
	 ,pd.[Purchasing_Document_Net_Price] -- NETPR Detail
	 ,pd.[Price_Unit] --PEINH detail
	 ,pd.[Gross_Order_Value] -- BRTWR detail
	 ,pd.[Effective_Item_Value] -- EFFWR detail
	 ,pd.[Purchase_Requisition_SK] -- FK gia BANFN 
	 ,pd.[Purchase_Requisition_Item_ID] -- BNFPO detail
	 ,pd.[Requisitioner_Requester_Name] -- AFNAM detail 
	 ,st.[Planned_Delivery_Time_in_Days]
	 ,pd.[Stock_Transport_Order_Rejection_Reasons] -- ZZ_STO_REJRES
	 ,pd.Price_Determination_DT -- PRDAT
	 ,'2024-04-28'				 AS [ETL_Reference_DT]							
	 ,-9999					 AS [ETL_Batch_ID]
 FROM [dbo].[PURCHASE_HEADER] ph

 inner join [temp].[F_PURCHASES_DET_LOG] delta -- Inner join to keep purchase documents included in the current batch
 on delta.[Purchasing_Document_SK]=ph.[Purchasing_Document_SK]

 inner join [dbo].[PURCHASE_DETAIL] pd
 on ph.[Purchasing_Document_SK]=pd.Purchasing_Document_SK

 left join [dbo].[PLANT_STOCK] st
 on st.[Plant_SK]=pd.[Plant_SK]
 and st.[Material_SK]=pd.[Material_SK]

 left join [dbo].PLANT pla
 on pla.Plant_SK=pd.Plant_SK

 where  ph.ETL_Batch_ID = -9999 
 and pd.ETL_Batch_ID = -9999     

