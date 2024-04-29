
TRUNCATE TABLE [dm_prl].[F_PURCHASE_RECEIVALS];




Select 
	 ph.[Purchasing_Document_SK] -- EBELN
 ,pd.[Purchasing_Document_Item_ID] -- EBELP
	 ,pd.[Material_SK] -- MATNR
 ,pd.[Plant_SK] -- WERKS
	 ,ph.[Created_DT] -- AEDAT
	 ,sum(pd.[Purchase_Order_Quantity]) OrderedQty -- MENGE
--	 ,count([Purchasing_Document_Item_ID]) over (partition by ph.[Purchasing_Document_SK]) OrderLines
into #entoles
from [dbo].[PURCHASE_HEADER] ph
inner join [dbo].[PURCHASE_DETAIL] pd
on ph.[Purchasing_Document_SK]=pd.Purchasing_Document_SK
Where ph.[Purchasing_Document_Type_ID]<>'ZNE' -- BSART
and ph.[vendor_sk]<>0 -- LIFNR
and pd.[Purchasing_Document_Deletion_IND]<>'L' -- LOEKZ
and pd.[Returns_Item_IND]<>'X' -- RETPO
and ph.ETL_Batch_ID = -9999 
and pd.ETL_Batch_ID = -9999 
group by ph.[Purchasing_Document_SK] -- EBELN
		,pd.[Purchasing_Document_Item_ID] -- EBELP
		,pd.[Material_SK] -- MATNR
		,pd.[Plant_SK] -- WERKS
		,ph.[Created_DT] -- AEDAT





SELECT 
	 [Purchasing_Document_SK] -- EBELN
 ,[Purchasing_Document_Item_ID] -- EBELP
	 ,[Account_Assignment_Sequential_ID]
	 ,[Purchase_Order_History_Transaction_Event_Type_ID]
	 ,[Material_Document_Year]
	 ,[Material_SK] -- MATNR
 ,[Plant_SK] -- WERKS
	 ,[Quantity] -- MENGE
 ,[Material_Document_SK] -- BELNR
 ,[Material_Document_Item_ID] -- BUZEI
	 ,[Document_Posting_DT] -- BUDAT
	 ,[Document_DT] -- BLDAT
	 ,[Entry_DT] -- CPUDT
	 into #paradoseis
 FROM [dbo].[PURCHASE_HISTORY]
 where [Purchase_Order_History_Category_ID] = 'E' 
 And [Inventory_Management_Movement_Type_ID] = '101'
 and ETL_Batch_ID = -9999



Select	e.*, 
		p.[Quantity] [DeliveredQty],
		p.[Document_Posting_DT] DeliveryDt,
		p.[Entry_DT] CPUDt,
	 p.[Account_Assignment_Sequential_ID]
	 ,p.[Purchase_Order_History_Transaction_Event_Type_ID]
	 ,p.[Material_Document_Year]
	 ,p.[Material_Document_SK]
	 ,p.[Material_Document_Item_ID]
	 into #MinDates
from #entoles e
inner join #paradoseis p
on e.[Purchasing_Document_SK]		= p.[Purchasing_Document_SK]
and e.[Purchasing_Document_Item_ID]	= p.[Purchasing_Document_Item_ID]
and e.[Material_SK]					= p.[Material_SK]
and e.[Plant_SK]				 = p.[Plant_SK]





Select	[Purchasing_Document_SK], 
		[Purchasing_Document_Item_ID],
		[Account_Assignment_Sequential_ID],
	 [Purchase_Order_History_Transaction_Event_Type_ID],
	 [Material_Document_Year],
	 [Material_Document_SK],
	 [Material_Document_Item_ID],
		[DeliveredQty],
		[DeliveryDt],
		[CPUDt],
		case when isnull(sum([DeliveredQty]) over (partition by [Purchasing_Document_SK], [Purchasing_Document_Item_ID], [Material_SK], [Plant_SK]),0)=0
			 then 'Undelivered'
			 when isnull(sum([DeliveredQty]) over (partition by [Purchasing_Document_SK], [Purchasing_Document_Item_ID], [Material_SK], [Plant_SK]),0)>=[OrderedQty]
			 then 'Full Delivery'
			 when isnull(sum([DeliveredQty]) over (partition by [Purchasing_Document_SK], [Purchasing_Document_Item_ID], [Material_SK], [Plant_SK]),0)<[OrderedQty]
			 then 'Partial Delivery'
		end OrderLineDeliveryStatus -- Elegxos an exei paradouei plhrvs h posothta kaue grammhs
		into #DaysDelayed
from #MinDates





select *,		
	case when sum(case when [OrderLineDeliveryStatus]='Full Delivery' then 1 else 0 end) over (partition by [Purchasing_Document_SK])
					=
				 count([Purchasing_Document_SK]) over (partition by [Purchasing_Document_SK])
			 then N'Πράσινη'
			 when sum(case when [OrderLineDeliveryStatus]='Undelivered' then 1 else 0 end) over (partition by [Purchasing_Document_SK])
			 		=
				 count([Purchasing_Document_SK]) over (partition by [Purchasing_Document_SK])
			 then N'Κόκκινη'
			 else N'Κίτρινη'
		end OrderCharacterization -- Prosuetoyme xarakthrismo katastashs entolhs agoras analoga tis paradoseis kaue grammhs ayths
		into #src
from #DaysDelayed



INSERT INTO [dm_prl].[F_PURCHASE_RECEIVALS]
(		 
		 [Purchasing_Document_SK], 
		 [Purchasing_Document_Item_ID], 
		 [Account_Assignment_Sequential_ID], 
		 [Purchase_Order_History_Transaction_Event_Type_ID], 
		 [Material_Document_Year], 
		 [Material_Document_SK], 
		 [Material_Document_Item_ID], 
		 [DeliveredQty],
		 [OrderLineDeliveryStatus],
		 [OrderCharacterization],
		 [DeliveryDt],
		 [CPUDt],
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
	 src.[Material_Document_Item_ID], 
	 src.[DeliveredQty],
	 src.[OrderLineDeliveryStatus],
	 src.[OrderCharacterization],
	 src.[DeliveryDt],
	 src.[CPUDt],
	 '2024-04-28'				 AS [ETL_Reference_DT],				
	 -9999					 AS [ETL_Batch_ID]
FROM #src src;
