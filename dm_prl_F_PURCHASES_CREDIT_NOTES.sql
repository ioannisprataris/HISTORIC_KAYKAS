
TRUNCATE TABLE [dm_prl].[F_PURCHASES_CREDIT_NOTES_hist];





Select	accd.[Company_SK],
		accd.[Accounting_Document_SK],
		accd.[Accounting_Document_Line_ID] [29_Accounting_Document_Line_ID],
		accd.[Fiscal_Year],
		case when accd.[Debit_Credit_IND]='S' then accd.Local_Currency_Amount else accd.Local_Currency_Amount*(-1) end FinalAmount,
		accd.Material_SK,
		accd.Plant_SK,
		cast(accd.[Company_SK] as nvarchar(10)) + '-' + accd.[Item_Text] + '-' + cast(accd.[Fiscal_Year] as nvarchar(4)) aac_key,
		pla.Sales_Organization_SK
	 into #acc_29
from [dbo].[ACCOUNTING_DOC_DETAIL] accd
inner join [sgk].[FINANCE] fin
on fin.Finance_SK=accd.Accounting_Document_SK
left join [dbo].PLANT pla
on pla.Plant_SK=accd.Plant_SK
Where Material_SK is not null 
and fin.Finance_ID >= '2900000000' 
and fin.Finance_ID <= '2999999999'
and accd.ETL_Batch_ID = -3999 





Select	accd.[Company_SK],
		accd.[Accounting_Document_SK],
		accd.[Accounting_Document_Line_ID] [21_Accounting_Document_Line_ID],
		accd.[Fiscal_Year],
		accd.[Vendor_SK],
		acch.[Posting_DT],
		cast(accd.[Company_SK] as nvarchar(10)) + '-' + fin.Finance_ID + '-' + cast(accd.[Fiscal_Year] as nvarchar(4)) aac_key
		
	 into #acc_21
from [dbo].[ACCOUNTING_DOC_DETAIL] accd
inner join [dbo].[ACCOUNTING_DOC_HEADER] acch  
on accd.Accounting_Document_SK=acch.Accounting_Document_SK
and accd.Company_SK=acch.Company_SK
and accd.Fiscal_Year=acch.Fiscal_Year
inner join [sgk].[FINANCE] fin
on fin.Finance_SK=accd.Accounting_Document_SK
Where accd.Account_Type='K'
and fin.Finance_ID >= '2100000000' 
and fin.Finance_ID <= '2199999999'
and accd.ETL_Batch_ID = -3999



Select acc_21.[Company_SK],
	 acc_21.[Accounting_Document_SK] [21_Document_SK],
	 acc_21.[21_Accounting_Document_Line_ID],
	 acc_21.[Fiscal_Year],
	 cast(format(acc_21.[Posting_DT],'yyyyMMdd') as int) [PostingDatekey],
	 acc_21.[Vendor_SK],
	 acc_29.[Accounting_Document_SK] [29_Document_SK],
	 acc_29.[29_Accounting_Document_Line_ID],
	 acc_29.[FinalAmount],
	 acc_29.[Material_SK],
	 acc_29.[Plant_SK],
	 acc_29.[Sales_Organization_SK]
into #src
from #acc_21 as acc_21
left join #acc_29 as acc_29
on acc_21.aac_key=acc_29.aac_key



INSERT INTO [dm_prl].[F_PURCHASES_CREDIT_NOTES_hist]
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
	 -3999				 AS [ETL_Batch_ID]
FROM #src src;
