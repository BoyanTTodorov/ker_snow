CREATE OR REPLACE FORCE EDITIONABLE VIEW "SMART_BI"."CRT_KER_01_INBOUND" ("PK_FACT_INBOUND", "ENVIRONMENT", "QUALITY", "UDM", "SKU", "INBOUND_REFERENCE", "ASN", "FORECAST_QUANTITY", "ACTUAL_QUANTITY", "EAN", "MODEL", "PART", "COLOUR", "SIZE", "DROP", "PRODUCT_CATEGORY", "PRODUCT_SUBCATEGORY", "TRUCK_PLATE", "BUILDING", "DOCK", "TRUCK_GATE_ARRIVAL", "TRUCK_BAY_ARRIVAL", "TRUCK_BAY_DEPARTURE", "TRUCK_GATE_DEPARTURE", "UDM_RECEIVING", "UDM_POSITIONING", "Appointment_Date", "UPDATED_DATE", "ISDELETED") AS 
  select 
RAWTOHEX(standard_hasH('B'||'-'||YY.DEPOT ||'-'|| YY.ACTIVITY || '/' || YY.ITEM||'-'||YY.GRADE||'-'||YY.SUPPORT||'-'||YY.INBOUNDREF,'MD5'))		AS "PK_FACT_INBOUND" ,
'Reflex WEB B' 																																	AS "ENVIRONMENT" ,
YY.GRADE 																																		AS "QUALITY" ,
YY.SUPPORT																																		AS "UDM" ,
YY.ITEM																																			AS "SKU" ,
YY.INBOUNDREF																																	AS "INBOUND_REFERENCE" ,
YY.ASN                                                                                                                                          AS "ASN",
YY.QTY_EXPECT																																	AS "FORECAST_QUANTITY" ,
YY.QTY_CONFIRM 																																	AS "ACTUAL_QUANTITY" ,
VI2.VICIVL																																		as "EAN" ,
CA.CALMOD																																		as "MODEL",
CA.CAPART																																		as "PART" ,
CA.CACOLC																																		as "COLOUR" ,
CA.CASIZC																																		as "SIZE",
CA.CADROP																																		as "DROP",
CA.CAPRCA																																		as "PRODUCT_CATEGORY" ,
CA.CAPRFA																																		as "PRODUCT_SUBCATEGORY" , 
YY.Lplate		 																																AS "TRUCK_PLATE" ,
'B'				 																																AS "BUILDING" ,
YY.DOCK																																			AS "DOCK",
CASE WHEN YY.Dat_Gate = 	timestamp'1900-01-01 00:00:01' THEN Null ELSE YY.Dat_Gate END														AS "TRUCK_GATE_ARRIVAL" ,
CASE WHEN YY.Dat_Dock_in =  timestamp'1900-01-01 00:00:01'  THEN Null ELSE YY.Dat_Dock_in END	 												AS "TRUCK_BAY_ARRIVAL" ,
CASE WHEN YY.Dat_Dock_out = timestamp'1900-01-01 00:00:01'  THEN NULL ELSE YY.Dat_Dock_out END	 												AS "TRUCK_BAY_DEPARTURE" ,
CASE WHEN YY.Dat_Depart   = timestamp'1900-01-01 00:00:01'  THEN NULL ELSE YY.Dat_Depart END	 												AS "TRUCK_GATE_DEPARTURE" ,
CASE WHEN YY.DATE_CNF     = timestamp'1900-01-01 00:00:01'  THEN Null ELSE YY.DATE_CNF END		 											AS "UDM_RECEIVING" ,
CASE WHEN YY.DATE_PUT     = timestamp'1900-01-01 00:00:01' THEN Null ELSE YY.DATE_PUT END		 												AS "UDM_POSITIONING" ,
CASE WHEN YY.Appointment_Date   = timestamp'1900-01-01 00:00:01'  THEN Null ELSE YY.Appointment_Date END		 								AS "Appointment_Date" ,
YY.LASTUPDATE	 																																AS "UPDATED_DATE" ,
'0'  																																			AS "ISDELETED"
from (  
		select
		XX.INBOUNDREF 																																			as INBOUNDREF,
        XX.ASN                                                                                                                                                  AS ASN,
		XX.DEPOT																																				AS DEPOT,
		XX.ACTIVITY																																				AS ACTIVITY,
		XX.ITEM 																																				as ITEM, 
		max(XX.GRADE)																																			AS GRADE	,		
		XX.SUPPORT																																				AS SUPPORT,
		sum(case when XX.QTYEXP is null then 0 else XX.QTYEXP end) 																								as QTY_EXPECT,
		sum(case when XX.QTYCNF is null then 0 else XX.QTYCNF end) 																								as QTY_CONFIRM,
		max(XX.DAT_CNF)  																																		as DATE_CNF,
		max(XX.DAT_PUT)																																			AS DATE_PUT,
		max(XX.LASTUPDATE)																																		AS LASTUPDATE,
		max(XX.Dat_Gate )																																		AS Dat_Gate,
		max(XX.Dat_Dock_in)																																		AS Dat_Dock_in,
		max(XX.Dat_Dock_out)																																	AS Dat_Dock_out,
		max(XX.Dat_Depart)																																		AS Dat_Depart,
		max(XX.DOCK)																																			AS DOCK,
		max(XX.Lplate)																																			AS Lplate,
		max(XX.Appointment_Date)																																AS Appointment_Date
		from (
			-- -- Expected Qty EDI Received
			select 
				I22.I22NRC 																		    as INBOUNDREF, 
                I22.I22RFRC 																		as ASN, 
				I22.I22WHOU																			AS DEPOT,
				I22.I22CPNY																			AS ACTIVITY,
				I22.I22IDPA																			AS SUPPORT,
				I22.I22CITE 																		as ITEM, 
				NULL--I22.I22GREX
																									AS GRADE	,
				sum(I22.I22QTEX) 																	as QTYEXP,
				0 																					as QTYCNF,
				(TIMESTAMP'1900-01-01 00:00:01')													as DAT_CNF,
				(TIMESTAMP'1900-01-01 00:00:01')													AS DAT_PUT ,
				(TIMESTAMP'1900-01-01 00:00:01')													AS Dat_Gate,
			    (TIMESTAMP'1900-01-01 00:00:01')													AS Dat_Dock_in,
			    (TIMESTAMP'1900-01-01 00:00:01')													AS Dat_Dock_out,
			    (TIMESTAMP'1900-01-01 00:00:01')													AS Dat_Depart,
			    (TIMESTAMP'1900-01-01 00:00:01')													AS Appointment_Date,
			    Null																				AS DOCK,
			    Null																				AS Lplate,
			    max( GREATEST(
			 		   nvl(I22.GGS_UPDATED,TIMESTAMP'1900-01-01 00:00:01'), nvl(I22.GGS_CREATED ,TIMESTAMP'1900-01-01 00:00:01')
			 		   ))																																		AS LASTUPDATE
				from GG_KERI_PRD.xprc03p I22
				where I22WHOU = '001' and I22CPNY ='100' AND I22.GGS_DELETED IS null 
				group by I22.I22NRC, I22.I22RFRC,I22.I22WHOU,I22.I22CPNY,I22.I22IDPA, I22.I22CITE,I22.I22GREX	
			UNION ALL
			-- -- Received qty at moment IPG is created
				select
				RE.RENBLF 																																		as INBOUNDREF,
                RE.RERREC                                                                                                                                       AS ASN,
				VG.VGCDPO 																																		AS DEPOT,
				VG.VGCACT 																																		AS ACTIVITY,
				VG.VGNSUP 																																		AS SUPPORT,
				VG.VGCART 																																		as ITEM, 
				VG.VGCQAL 																																		AS GRADE,
				0 																																				as QTYEXP,
				sum(VG.VGQMVG) 																																	as QTYCNF,
				max(
					TO_TIMESTAMP( case when RE.RETRVA = '0' then '1900-01-01 00:00:01' else  lpad(RE.RESVAL, 2, '0') ||  lpad(RE.REAVAL, 2, '0') ||'-'||lpad(RE.REmVAL, 2, '0') ||'-'||  lpad(RE.REJVAL, 2, '0') || ' '||
						SUBSTR(lpad(RE.REHVAR,6,0),1,2)||':'|| SUBSTR(lpad(RE.REHVAR,6,0),3,2)||':'|| SUBSTR(lpad(RE.REHVAR,6,0),5,2) end ,'YYYY-MM-DD HH24:MI:SS' )
					)																																			as DAT_CNF,
				max(CH.CHDAE9)																																	AS DAT_PUT,
			   (min(YMA.MOArrivalDate)) 																														AS Dat_Gate,
			   (min(YMB.MOBayDate)) 																															AS Dat_Dock_in,
			   (min(YMBE.MODepartureDate)) 																														AS Dat_Dock_out,
			   (min(YMD.MODepartureDate)) 																														AS Dat_Depart,
	 max(TO_TIMESTAMP( CASE WHEN RE.resrec = 0 THEN '1900-01-01 00:00:01' ELSE  lpad(RE.resrec, 2, '0') ||  lpad(RE.rearec, 2, '0') ||'-'||lpad(RE.REmrec, 2, '0') ||'-'||  lpad(RE.REJrec, 2, '0') || ' '||
						SUBSTR(lpad(RE.REHrec,6,0),1,2)||':'|| SUBSTR(lpad(RE.REHrec,6,0),3,2)||':'|| SUBSTR(lpad(RE.REHrec,6,0),5,2) END,'YYYY-MM-DD HH24:MI:SS' )
					 	)																																		AS Appointment_Date,
			   min(YMB.DOCK)																																	AS DOCK,
			   min(YMA.MOLICENCEPLATE1) 																														AS Lplate,
			   MAX(GREATEST(
			 		   nvl(RE.GGS_UPDATED,TIMESTAMP'1900-01-01 00:00:01'), nvl(RE.GGS_CREATED ,TIMESTAMP'1900-01-01 00:00:01'),
   			 		   nvl(VG.GGS_UPDATED,TIMESTAMP'1900-01-01 00:00:01'), nvl(VG.GGS_CREATED ,TIMESTAMP'1900-01-01 00:00:01'),
  			 		   nvl(CH.GGS_UPDATED,TIMESTAMP'1900-01-01 00:00:01'), nvl(CH.GGS_CREATED ,TIMESTAMP'1900-01-01 00:00:01')
			   ))																																				AS LASTUPDATE

				from GG_KERI_PRD.hlrecpp RE
				inner join GG_KERI_PRD.hlmvtgp VG on recdpo = vgcdpo and recact= vgcact and renann = vgnann and vgnrec = renrec  and vgctvg = '100'
				LEFT outer join GG_KERI_PRD.kbrechp CH on CH.CHNANN=RE.RENANN and RE.RENREC = CH.CHNREC and VG.VGNSUP =CH.CHNUDM AND CH.CHNANF=RE.RENANN and RE.RENREC = CH.CHNRFI  AND trunc(CH.GGS_DELETED) IS NULL
								AND CH.CHCDPO = RE.RECDPO  AND CH.CHCACT = RE.RECACT 
				LEFT outer JOIN (
						 SELECT 
						 UP.UPCACT ,UP.UPNANN , UP.UPNREC , UP.UPCDPO ,

						 max(LPAD(NVL(U6.U6NARV,'0'), 2, '0') || LPAD(NVL(U6.U6NRDV,'0'), 9, '0')) AS APPOINTMENT_NR,
						 Max (U6.U6NARV) AS U6NARV,
						 max(U6.U6NRDV) AS U6NRDV, 
						 MAX(U6.U6CTMR) AS CARRIER,
						  max(U6.U6LRDV) AS U6LRDV
						 FROM  GG_KERI_PRD.HLRDVRP UP  
						 LEFT JOIN GG_KERI_PRD.HLRDVTP U6  ON U6.U6CDPO= UP.UPCDPO AND UP.UPNARV= U6.U6NARV AND UP.UPNRDV= U6.U6NRDV   AND TRUNC(U6.GGS_DELETED) IS NULL 
						 WHERE 1=1 AND  trunc(UP.GGS_DELETED) IS NULL
						 GROUP BY UP.UPCACT ,UP.UPNANN , UP.UPNREC , UP.UPCDPO 
				 )U6A ON RE.RECACT= U6A.UPCACT AND RE.RENANN= U6A.UPNANN AND RE.RENREC= U6A.UPNREC AND RE.RECDPO= U6A.UPCDPO 

				--INNER JOIN GG_KERIYMS_PRD.Historymovement YM ON YM.IDSite = '1' AND YM.IDDepot = '4' AND YM.MOAppointment = U6LRDV AND  
				--			YM.MOState = 20  -- state 20 arrive au porte cabine
				LEFT outer JOIN (
						SELECT YM1.IDSite,YM1.IDDepot,YM1.MOState,YM1.MOAppointment,min (MOArrivalDate) AS MOArrivalDate , max(YM1.MOLICENCEPLATE1) AS MOLICENCEPLATE1
						FROM GG_KERIYMS_PRD.Historymovement YM1 
						WHERE  YM1.IDSite = '1' AND YM1.IDDepot = '4' AND YM1.MOState = 20
						GROUP BY YM1.IDSite,YM1.IDDepot,YM1.MOState,YM1.MOAppointment
						) YMA ON YMA.MOAppointment = U6A.U6LRDV 
				LEFT outer JOIN (
						SELECT YM2.IDSite,YM2.IDDepot,YM2.MOState,YM2.MOAppointment,min (YM2.HYTIMESTAMP) AS MOBayDate , Min(d.DONAME) AS DOCK
						FROM GG_KERIYMS_PRD.Historymovement YM2 
						INNER JOIN GG_KERIYMS_PRD.DOCK d ON  d.IDDOCK = YM2.IDDOCK 
						WHERE  YM2.IDSite = '1' AND YM2.IDDepot = '4' AND YM2.MOState = 60 AND YM2.IDSTATUS = 3
						GROUP BY YM2.IDSite,YM2.IDDepot,YM2.MOState,YM2.MOAppointment
						) YMB ON YMB.MOAppointment = U6A.U6LRDV 
				LEFT outer JOIN (
						SELECT YM4.IDSite,YM4.IDDepot,YM4.MOState,YM4.MOAppointment,min (HYTIMESTAMP) AS MODEPARTUREDATE  , Min(d.DONAME) AS DOCK
						FROM GG_KERIYMS_PRD.Historymovement YM4 
						INNER JOIN GG_KERIYMS_PRD.DOCK d ON  d.IDDOCK = YM4.IDDOCK 
						WHERE  YM4.IDSite = '1' AND YM4.IDDepot = '4' AND YM4.MOState = 60 AND YM4.IDSTATUS = 11
						GROUP BY YM4.IDSite,YM4.IDDepot,YM4.MOState,YM4.MOAppointment
						) YMBE ON YMBE.MOAppointment = U6A.U6LRDV 
		    	LEFT outer JOIN (
						SELECT YM3.IDSite,YM3.IDDepot,YM3.MOState,YM3.MOAppointment,min (YM3.MODEPARTUREDATE ) AS MODepartureDate
						FROM GG_KERIYMS_PRD.Historymovement YM3 
						WHERE  YM3.IDSite = '1' AND YM3.IDDepot = '4' AND YM3.MOState = 99
						GROUP BY YM3.IDSite,YM3.IDDepot,YM3.MOState,YM3.MOAppointment
						) YMD ON YMD.MOAppointment = U6A.U6LRDV 
				where RE.recact = '100' and RE.recdpo ='001' AND RE.GGS_DELETED IS NULL AND VG.GGS_DELETED IS NULL
				and RE.RESVAL !=0

				group by RE.RENBLF, RE.RERREC, VG.VGCDPO,VG.VGCACT,VG.VGNSUP, VGCART,VG.VGCQAL
			) XX
		WHERE 1=1	
		group by XX.INBOUNDREF,XX.ASN,XX.DEPOT,XX. ACTIVITY,XX.SUPPORT , XX.ITEM-- ,XX.GRADE

) YY
LEFT OUTER  JOIN GG_KERI_PRD.KBCARTP CA ON CA.cacact = YY.ACTIVITY AND CA.caskuc = YY.ITEM  AND CA.GGS_DELETED IS null
--LEFT OUTER JOIN GG_KERI_PRD.HLVLIDP VI on VI.VICTYI = 'EAN13' AND VI.VICVLA='01' AND VI.VICACT = cacact AND VI.VICART =caskuc AND VI.GGS_DELETED IS null
LEFT  JOIN (
      SELECT h1.*
      FROM GG_KERI_PRD.HLVLIDP h1
      INNER JOIN (
          SELECT MAX(VIACRE || LPAD(VIMCRE, 2, '0') || LPAD(VIJCRE, 2, '0') || LPAD(VIHCRE, 6, '0')) AS max_value, VICART
          FROM GG_KERI_PRD.HLVLIDP
          WHERE VICTYI = 'EAN13'  and trunc(GGS_DELETED) is null
          GROUP BY VICART
      ) h2
      ON (h1.VIACRE || LPAD(h1.VIMCRE, 2, '0') || LPAD(h1.VIJCRE, 2, '0') || LPAD(h1.VIHCRE, 6, '0')) = h2.max_value
      AND h1.VICART = h2.VICART
      WHERE h1.VICTYI = 'EAN13' and trunc(h1.GGS_DELETED) is null) VI2 ON VICACT=CACACT AND VICART=CASKUC
where 1=1 

--AND YY.INBOUNDREF = '0181254821' --'8099413591'  
AND YY.LASTUPDATE	  >= (SELECT CAST(or2.LAST_UPDATE - interval '4' hour AS DATE) FROM SMART_BI.OBJECT_RUN_KERING_B or2 WHERE "OBJECT" = '01_INBOUND');