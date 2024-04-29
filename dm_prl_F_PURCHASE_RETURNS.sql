
Select 
	det.[Purchase_Order_SK], -- EBELN FK purchase
	det.[Purchasing_Document_Item_Number], -- EBELP
	det.[Material_Document_SK], -- MBLNR FK finance
	det.[Material_Document_Item], -- ZEILE
	det.[Inventory_Management_Movement_Type], -- BWART
	det.[Debit_Credit_IND], 
	case when det.[Debit_Credit_IND] = 'S' then det.[Quantity] else det.[Quantity]*(-1) end Quantity, -- MENGE
	case when det.[Debit_Credit_IND] = 'S' then det.[Local_Currency_Amount] else det.[Local_Currency_Amount]*(-1) end [Local_Currency_Amount], -- DMBTR
	det.[Currency_Key], -- WAERS
	det.[Base_Unit_Of_Measure], -- MEINS
	det.[Material_Document_Year], -- MJAHR
	det.[Plant_SK], -- WERKS FK
	det.[Vendor_SK],-- LIFNR FK
	hea.[Posting_DT]
	into #mseg_161
from [dbo].[PRODUCT_POSTINGS_DETAIL] det
inner join [dbo].[PRODUCT_POSTINGS_HEADER] hea
on hea.[Material_Document_SK]=det.[Material_Document_SK]
where [Inventory_Management_Movement_Type] = '161'
and det.ETL_Batch_ID = -9999 






Select 
	[Purchase_Order_SK], -- EBELN FK purchase
	[Purchasing_Document_Item_Number], -- EBELP
	[Reference_Document_SK] as [Material_Document_SK_161], -- LFBNR FK finance
 [Reference_Document_Item] as [Material_Document_Item_161], -- LFPOS
	[Material_Document_SK], -- MBLNR FK finance
	[Material_Document_Item], -- ZEILE
	[Inventory_Management_Movement_Type], -- BWART
	[Debit_Credit_IND], 
	case when [Debit_Credit_IND] = 'S' then [Quantity] else [Quantity]*(-1) end Quantity, -- MENGE
	case when [Debit_Credit_IND] = 'S' then [Local_Currency_Amount] else [Local_Currency_Amount]*(-1) end [Local_Currency_Amount], -- DMBTR
	[Currency_Key], -- WAERS
	[Base_Unit_Of_Measure], -- MEINS
	[Material_Document_Year], -- MJAHR
	[Plant_SK], -- WERKS FK
	[Vendor_SK]-- LIFNR FK
	into #mseg_162
from [dbo].[PRODUCT_POSTINGS_DETAIL]
where [Inventory_Management_Movement_Type] = '162'
and ETL_Batch_ID = -9999   




SELECT 

	det.[Reference_Material_Document_SK] as [Material_Document_SK_161], -- LFBNR FK
	det.[Reference_Document_Item_ID] as [Material_Document_Item_161], -- LFPOS
	det.[Purchase_Invoice_SK], -- BELNR FK
	det.[Purchase_Invoice_Item_ID], -- BUZEI
	det.[Fiscal_Year], -- GJAHR
	det.[Purchasing_Document_SK], -- EBELN FK
	det.[Purchasing_Document_Item_ID], -- EBELP
	det.[Material_SK], -- MATNR
	det.[Company_SK], -- BUKRS
	det.[Plant_SK], -- WERKS
	case when det.[Debit_Credit_IND] = 'S' then det.[Document_Currency_Amount] else det.[Document_Currency_Amount]*(-1) end [Document_Currency_Amount], -- WRBTR
	det.[Debit_Credit_IND], -- SHKZG
	det.[Purchases_Code_Tax], -- MWSKZ
	case when det.[Debit_Credit_IND] = 'S' then det.[Quantity] else det.[Quantity]*(-1) end Quantity, -- MENGE
	det.[Purchase_Order_Unit_Of_Measure], -- BSTME 
	det.[Total_Valuated_Stock], -- LBKUM
	det.[Purchase_Order_History_IND], -- EXKBE
	det.[Purchase_Order_Delivery_Costs_Update_IND], -- XEKBZ 
	det.[Reference_Document_Number], -- XBLNR 
	det.[Document_Posted_To_Previous_Period_IND], -- XRUEB 
	det.[Condition_Type], -- KSCHL 
	det.[Total_Valuated_Stock_Value], -- SALK3 
	det.[Reference_Material_Document_SK], -- LFBNR FK
	det.[Current_Period_Fiscal_Year], -- LFGJA 
    det.[Reference_Document_Item_ID], -- LFPOS
	hea.[Document_Type_ID], -- BLART
	hea.[Document_DT], -- BLDAT
	hea.[User_Name] -- USNAM 
	into #RSEG_RBKP
 FROM [dbo].[PURCHASE_INVOICE_DETAIL] det
 inner join [dbo].[PURCHASE_INVOICE_HEADER] Hea
 on hea.[Purchase_Invoice_SK]=det.[Purchase_Invoice_SK]
 and det.ETL_Batch_ID = -9999 
and hea.ETL_Batch_ID = -9999  







 Select 
		m161.[Purchase_Order_SK] [161_Purchase_Order_SK],
		m161.[Purchasing_Document_Item_Number] [161_Purchasing_Document_Item_Number],
		m161.[Material_Document_SK] [161_Material_Document_SK],
		m161.[Material_Document_Item] [161_Material_Document_Item],
		m161.[Inventory_Management_Movement_Type] [161_Inventory_Management_Movement_Type],
		m161.[Debit_Credit_IND] [161_Debit_Credit_IND], 
		m161.[Quantity] [161_Quantity],
		m161.[Local_Currency_Amount] [161_Local_Currency_Amount],
		m161.[Currency_Key] [161_Currency_Key], 
		m161.[Base_Unit_Of_Measure] [161_Base_Unit_Of_Measure], 
		m161.[Material_Document_Year] [161_Material_Document_Year],
		m161.[Plant_SK] [161_Plant_SK],
		m161.[Vendor_SK] [161_Vendor_SK],
		m161.[Posting_DT] [161_Posting_DT],

		m162.[Purchase_Order_SK] [162_Purchase_Order_SK],
		m162.[Purchasing_Document_Item_Number] [162_Purchasing_Document_Item_Number],
		--m162.[Material_Document_SK_161],
		--m162.[Material_Document_Item_161],
		m162.[Material_Document_SK] [162_Material_Document_SK],
		m162.[Material_Document_Item] [162_Material_Document_Item],
		m162.[Inventory_Management_Movement_Type] [162_Inventory_Management_Movement_Type],
		m162.[Debit_Credit_IND] [162_Debit_Credit_IND], 
		m162.[Quantity] [162_Quantity],
		m162.[Local_Currency_Amount] [162_Local_Currency_Amount],
		m162.[Currency_Key] [162_Currency_Key],
		m162.[Base_Unit_Of_Measure] [162_Base_Unit_Of_Measure],
		m162.[Material_Document_Year] [162_Material_Document_Year],
		m162.[Plant_SK] [162_Plant_SK], 
		m162.[Vendor_SK] [162_Vendor_SK],

		isnull(inv.[Purchase_Invoice_SK],-1) [inv_Purchase_Invoice_SK], -- BELNR FK
		isnull(inv.[Purchase_Invoice_Item_ID],-1) [inv_Purchase_Invoice_Item_ID], -- BUZEI
		inv.[Fiscal_Year] [inv_Fiscal_Year], -- GJAHR
		inv.[Purchasing_Document_SK] [inv_Purchasing_Document_SK], -- EBELN FK
		inv.[Purchasing_Document_Item_ID] [inv_Purchasing_Document_Item_ID], -- EBELP
		inv.[Material_SK] [inv_Material_SK], -- MATNR
		inv.[Company_SK] [inv_Company_SK], -- BUKRS
		inv.[Plant_SK] [inv_Plant_SK], -- WERKS
		inv.[Document_Currency_Amount] [inv_Document_Currency_Amount], -- WRBTR
		inv.[Debit_Credit_IND] [inv_Debit_Credit_IND], -- SHKZG
		inv.[Purchases_Code_Tax] [inv_Purchases_Code_Tax], -- MWSKZ
		inv.[Quantity] [inv_Quantity], -- MENGE
		inv.[Purchase_Order_Unit_Of_Measure] [inv_Purchase_Order_Unit_Of_Measure], -- BSTME 
		inv.[Total_Valuated_Stock] [inv_Total_Valuated_Stock], -- LBKUM
		inv.[Purchase_Order_History_IND] [inv_Purchase_Order_History_IND], -- EXKBE
		inv.[Purchase_Order_Delivery_Costs_Update_IND] [inv_Purchase_Order_Delivery_Costs_Update_IND], -- XEKBZ 
		inv.[Reference_Document_Number] [inv_Reference_Document_Number], -- XBLNR 
		inv.[Document_Posted_To_Previous_Period_IND] [inv_Document_Posted_To_Previous_Period_IND], -- XRUEB 
		inv.[Condition_Type] [inv_Condition_Type], -- KSCHL 
		inv.[Total_Valuated_Stock_Value] [inv_Total_Valuated_Stock_Value], -- SALK3 
		inv.[Reference_Material_Document_SK] [inv_Reference_Material_Document_SK], -- LFBNR FK
		inv.[Current_Period_Fiscal_Year] [inv_Current_Period_Fiscal_Year], -- LFGJA 
		inv.[Reference_Document_Item_ID] [inv_Reference_Document_Item_ID], -- LFPOS
		inv.[Document_Type_ID] [inv_Document_Type_ID], -- BLART
		inv.[Document_DT] [inv_Document_DT], -- BLDAT
		inv.[User_Name] [inv_User_Name],-- USNAM

case when isnull(m162.[Material_Document_SK],'')='' and isnull(inv.[Purchase_Invoice_SK],'')='' then N'Εκκρεμής'
			 when m162.[Material_Document_SK] is not null and isnull(inv.[Purchase_Invoice_SK],'')='' then N'Ολοκληρωμένη'
			 when m162.[Material_Document_SK] is null and inv.[Purchase_Invoice_SK] is not null then N'Ολοκληρωμένη'
			 when m162.[Material_Document_SK] is not null and inv.[Purchase_Invoice_SK] is not null then N'Επιστρ.&Τιμολ.'
		else 'Εκκρεμής' end [Return_Status]


		into #stage
	From #mseg_161 m161

	left join #mseg_162 m162
	on m161.[Material_Document_SK]=m162.[Material_Document_SK_161]
	and m161.[Material_Document_Item]=m162.[Material_Document_Item_161]

	Left join #RSEG_RBKP inv
	on inv.[Material_Document_SK_161] = m161.[Material_Document_SK]
	and inv.[Material_Document_Item_161] = m161.[Material_Document_Item]





INSERT INTO [dm_prl].[F_PURCHASE_RETURNS]
(		 
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
 
select	distinct 
		stage.[161_Purchase_Order_SK], -- KEY_EBELN_EBELP
		stage.[161_Purchasing_Document_Item_Number], -- KEY_EBELN_EBELP
		stage.[inv_Document_DT], -- Return_Dt
		stage.[161_Inventory_Management_Movement_Type], --Kvd. Typos Kinhshs
		stage.[inv_Document_Type_ID], -- T003T
		stage.[Return_Status],
		stage.[inv_Purchase_Invoice_SK], -- Pistvtiko FK -- óáí business ìðïñåé íá åéíáé null (åðéóôñïöåò, ìðïñåé íá ìçí åêäïèåé ðáñáóôáôéêï )
		stage.[inv_Purchase_Invoice_Item_ID], -- Line Pistvtikoy --óáí business ìðïñåé íá åéíáé null
		stage.[inv_User_Name], -- Pistvtiko apo xrhsth
		stage.[161_Material_Document_SK], -- Parastatiko epistrofhs
		stage.[161_Material_Document_Item], -- Line parastatikoy epistrofhs
		stage.[161_Material_Document_Year], -- Logistiko etos parastatikoy epistrofhs
		stage.[161_Quantity], -- Posothta epistrofvn
		stage.[161_Local_Currency_Amount], -- Ajia epistrofvn
		count(*) over (partition by [161_Material_Document_SK], [161_Material_Document_Item]) Count_CreditNotes,
		'2024-04-28' AS [ETL_Reference_DT],		  				
	 -9999			 AS [ETL_Batch_ID]                  
from #stage stage
