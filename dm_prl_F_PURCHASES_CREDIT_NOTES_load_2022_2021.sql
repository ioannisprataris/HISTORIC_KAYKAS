TRUNCATE TABLE [dm_prl].[F_PURCHASES_CREDIT_NOTES_hist];



Select	
accd.[Company_SK],
accd.[Accounting_Document_SK],
accd.[Accounting_Document_Line_ID] [29_Accounting_Document_Line_ID],
accd.[Accounting_Document_Line_ID] [21_Accounting_Document_Line_ID],
accd.[Fiscal_Year],
accd.[Vendor_SK],
acch.[Posting_DT],
case when accd.[Debit_Credit_IND]='S' then accd.Local_Currency_Amount else accd.Local_Currency_Amount*(-1) end FinalAmount,
accd.Material_SK,
accd.Plant_SK,
pla.Sales_Organization_SK
into #acc_29_21	 
from [dbo].[ACCOUNTING_DOC_DETAIL] accd
inner join [dbo].[ACCOUNTING_DOC_HEADER] acch
on accd.Accounting_Document_SK=acch.Accounting_Document_SK
and accd.Company_SK=acch.Company_SK
and accd.Fiscal_Year=acch.Fiscal_Year
inner join [sgk].[FINANCE] fin
on fin.Finance_SK=accd.Accounting_Document_SK
left join [dbo].PLANT pla
on pla.Plant_SK=accd.Plant_SK
where accd.etl_batch_id = -3999




Select acc_29_21.[Company_SK],
	 acc_29_21.[Accounting_Document_SK] [21_Document_SK],
	 acc_29_21.[21_Accounting_Document_Line_ID],
	 acc_29_21.[Fiscal_Year],
	 cast(format(acc_29_21.[Posting_DT],'yyyyMMdd') as int) [PostingDatekey],
	 acc_29_21.[Vendor_SK],
	 acc_29_21.[Accounting_Document_SK] [29_Document_SK],
	 acc_29_21.[29_Accounting_Document_Line_ID],
	 acc_29_21.[FinalAmount],
	 acc_29_21.[Material_SK],
	 acc_29_21.[Plant_SK],
	 acc_29_21.[Sales_Organization_SK]
into #src
from #acc_29_21	AS acc_29_21


-- ========================
-- Load Preloading table
-- ========================

INSERT INTO [dm_prl].[F_PURCHASES_CREDIT_NOTES]
(		 

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

SELECT 
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
	 ,isnull(src.[29_Document_SK],-1)
	 ,isnull(src.[29_Accounting_Document_Line_ID],-1)
	,'2022-12-31'			 AS [ETL_Reference_DT],				
	 -3999		     AS [ETL_Batch_ID]
FROM  #src as src;