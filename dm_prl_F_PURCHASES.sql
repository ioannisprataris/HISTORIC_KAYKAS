
TRUNCATE TABLE [dm_prl].[F_PURCHASES_hist];

DROP TABLE IF EXISTS [temp].[F_PURCHASES_DET_LOG];

Create table [temp].[F_PURCHASES_DET_LOG] ([Purchasing_Document_SK] int not null primary key)


Insert into [temp].[F_PURCHASES_DET_LOG] ([Purchasing_Document_SK])

Select distinct [Purchasing_Document_SK] from [dbo].[PURCHASE_HISTORY] where ETL_Batch_ID = -2999 





SELECT 
	  phist.[Purchasing_Document_SK] -- key for EBELN
	 ,phist.[Purchasing_Document_Item_ID]
	 ,phist.[Account_Assignment_Sequential_ID]
	 ,phist.[Purchase_Order_History_Transaction_Event_Type_ID] -- VGABE
	 ,phist.[Material_Document_Year] -- GJHAR
	 ,phist.[Material_Document_SK] -- BELNR
	 ,phist.[Material_Document_Item_ID]
	 ,phist.[Client_ID]
	 ,phist.[Purchase_Order_History_Category_ID] -- BEWTP
	 ,phist.[Document_Posting_DT] -- BUDAT
	 ,phist.[Quantity] -- MENGE
	 ,case when phist.[Purchase_Order_History_Category_ID] = 'Q' and phist.[Debit_Credit_IND]='S' then phist.[Quantity]
			when phist.[Purchase_Order_History_Category_ID] = 'Q' and phist.[Debit_Credit_IND]<>'S' then phist.[Quantity]*(-1)
			else null
	  end FinalQuantity
	 ,phist.[Purchase_Order_Price_Unit_Quantity]
	 ,phist.[Local_Currency_Amount]
	 ,case when phist.[Debit_Credit_IND]='S' then phist.[Local_Currency_Amount] 
			else phist.[Local_Currency_Amount]*(-1) 
	  end																					as FinalAmount
	 ,phist.[Debit_Credit_IND] -- SHKZG
	 ,phist.[Material_SK]
	 ,phist.[Plant_SK]
	 ,va.Company_SK
	 ,p.[Sales_Organization_SK]
	 ,phist.[Material_Document_ID]															as Purchase_doc
	 ,phist.[Material_Document_ID]+cast(year(phist.[Document_Posting_DT]) as nvarchar(4))	as ReferenceKey

	 ,phist.[Document_DT] -- BLDAT
	 ,phist.[Entry_DT] -- CPUDT
	 ,phist.[Entry_TM] -- CPUTM
	 ,phist.[Inventory_Management_Movement_Type_ID] -- BWART
	 ,ph.Vendor_SK
	 into #Purchase_stage
 FROM [dbo].[PURCHASE_HISTORY]								as phist
 inner join [temp].[F_PURCHASES_DET_LOG] delta -- Inner join to keep purchase documents included in the current batch
 on delta.Purchasing_Document_SK=phist.Purchasing_Document_SK
 inner join	dbo.PLANT												as P 
 on P.Plant_SK=phist.Plant_SK
 LEFT JOIN [dbo].[VALUATION_AREA]									as va 
 ON P.[Valuation_Area_SK] = va.[Valuation_SK]
 left join [dbo].[PURCHASE_HEADER] ph
 on ph.Purchasing_Document_SK=phist.[Purchasing_Document_SK]
 where	(phist.[Purchase_Order_History_Transaction_Event_Type_ID]='2' 
 or	phist.[Purchase_Order_History_Transaction_Event_Type_ID]='3')
 and [Material_Document_ID] not like '54%' 
 and phist.ETL_Batch_ID = -3999 








Select distinct	accd.[Company_SK],
		accd.[Accounting_Document_SK],
		accd.[Fiscal_Year],
		accd.[Vendor_SK],
		acch.[Reference_Key],
		fin.Finance_ID
	 into #Accounting_info
from [dbo].[ACCOUNTING_DOC_DETAIL] accd

inner join [dbo].[ACCOUNTING_DOC_HEADER] acch
on accd.Accounting_Document_SK=acch.Accounting_Document_SK
and accd.Company_SK=acch.Company_SK
and accd.Fiscal_Year=acch.Fiscal_Year
inner join [sgk].[FINANCE] fin
on fin.Finance_SK=accd.Accounting_Document_SK
inner join #Purchase_stage delta -- keep logistic entries only for the purchases that where loaded in the current batch
on delta.ReferenceKey=acch.[Reference_Key]
Where accd.Account_Type='K'
and fin.Finance_ID >= '0020000000' 
and fin.Finance_ID <= '0020999999'
 and accd.ETL_Batch_ID = -3999 








Select	cast(format(pur.[Document_Posting_DT],'yyyyMMdd') as int) PostingDatekey,
		pur.[Purchasing_Document_SK],
		pur.[Purchasing_Document_Item_ID],
		pur.[Account_Assignment_Sequential_ID],
		pur.[Purchase_Order_History_Transaction_Event_Type_ID],
		pur.[Material_Document_Year],
		pur.[Material_Document_SK],
		pur.[Material_Document_Item_ID],
		pur.[Purchase_Order_History_Category_ID],
		pur.[FinalQuantity],
		pur.[FinalAmount],
		pur.[Material_SK],
		pur.[Plant_SK],
		pur.[Company_SK],
		pur.[Sales_Organization_SK],
		pur.Purchase_doc,
		coalesce(acc.Vendor_SK, pur.vendor_sk) as [Vendor_SK], -- an den yparxei promhueyths fernei ayton ths entolhs agoras
		cast(format(pur.[Document_DT],'yyyyMMdd') as int) [DocumentDatekey], -- BLDAT
		cast(format(pur.[Entry_DT],'yyyyMMdd') as int) [EntryDatekey], -- CPUDT
		pur.[Entry_TM], -- CPUTM
		pur.[Inventory_Management_Movement_Type_ID] -- BWART
		into #src
from #Purchase_stage pur
left join #Accounting_info acc
on acc.Reference_Key=pur.ReferenceKey





INSERT INTO [dm_prl].[F_PURCHASES_hist]
(		 
		 [Purchasing_Document_SK], 
		 [Purchasing_Document_Item_ID], 
		 [Account_Assignment_Sequential_ID], 
		 [Purchase_Order_History_Transaction_Event_Type_ID], 
		 [Material_Document_Year], 
		 [Material_Document_SK], 
		 [Material_Document_ID],
		 [Material_Document_Item_ID], 
		 [PostingDatekey], 
		 [Purchase_Order_History_Category_ID], 
		 [FinalQuantity], 
		 [FinalAmount], 
		 [Material_SK], 
		 [Plant_SK], 
		 [Vendor_SK],
		 [Company_SK],
		 [Sales_Organization_SK],
		 [DocumentDatekey],
		 [EntryDatekey],
		 [Entry_TM],
		 [Inventory_Management_Movement_Type_ID],
		 [ETL_Reference_DT],			
		 [ETL_Batch_ID]
)
SELECT 
	 src.[Purchasing_Document_SK], 
	 src.[Purchasing_Document_Item_ID], 
	 src.[Account_Assignment_Sequential_ID], 
	 src.[Purchase_Order_History_Transaction_Event_Type_ID], 
	 src.[Material_Document_Year], 
	 src.[Material_Document_SK], 
	 src.[Purchase_doc],
	 src.[Material_Document_Item_ID], 
	 src.[PostingDatekey], 
	 src.[Purchase_Order_History_Category_ID], 
	 src.[FinalQuantity], 
	 src.[FinalAmount], 
	 src.[Material_SK], 
	 src.[Plant_SK], 
	 src.[Vendor_SK],
	 src.[Company_SK],
	 src.[Sales_Organization_SK],
	 src.[DocumentDatekey],
	 src.[EntryDatekey],
	 src.[Entry_TM],
	 src.[Inventory_Management_Movement_Type_ID],
	 '2022-12-31'				 AS [ETL_Reference_DT],						
	 -3999					 AS [ETL_Batch_ID]
FROM #src src;
