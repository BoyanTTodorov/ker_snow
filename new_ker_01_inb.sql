CREATE OR REPLACE VIEW "SMART_BI"."CRT_KER_01_INBOUND" (
    "PK_FACT_INBOUND", "ENVIRONMENT", "QUALITY", "UDM", "SKU", "INBOUND_REFERENCE", "ASN", "FORECAST_QUANTITY", 
    "ACTUAL_QUANTITY", "EAN", "MODEL", "PART", "COLOUR", "SIZE", "DROP", "PRODUCT_CATEGORY", "PRODUCT_SUBCATEGORY", 
    "TRUCK_PLATE", "BUILDING", "DOCK", "TRUCK_GATE_ARRIVAL", "TRUCK_BAY_ARRIVAL", "TRUCK_BAY_DEPARTURE", 
    "TRUCK_GATE_DEPARTURE", "UDM_RECEIVING", "UDM_POSITIONING", "Appointment_Date", "UPDATED_DATE", "ISDELETED"
) AS
select
    MD5('B' || '-' || YY.DEPOT || '-' || YY.ACTIVITY || '/' || YY.ITEM || '-' || YY.GRADE || '-' || YY.SUPPORT || '-' || YY.INBOUNDREF) AS "PK_FACT_INBOUND",
    'Reflex WEB B' AS "ENVIRONMENT",
    YY.GRADE AS "QUALITY",
    YY.SUPPORT AS "UDM",
    YY.ITEM AS "SKU",
    YY.INBOUNDREF AS "INBOUND_REFERENCE",
    YY.ASN AS "ASN",
    YY.QTY_EXPECT AS "FORECAST_QUANTITY",
    YY.QTY_CONFIRM AS "ACTUAL_QUANTITY",
    VI2.VICIVL AS "EAN",
    CA.CALMOD AS "MODEL",
    CA.CAPART AS "PART",
    CA.CACOLC AS "COLOUR",
    CA.CASIZC AS "SIZE",
    CA.CADROP AS "DROP",
    CA.CAPRCA AS "PRODUCT_CATEGORY",
    CA.CAPRFA AS "PRODUCT_SUBCATEGORY",
    YY.Lplate AS "TRUCK_PLATE",
    'B' AS "BUILDING",
    YY.DOCK AS "DOCK",
    CASE WHEN YY.Dat_Gate = TO_TIMESTAMP('1900-01-01 00:00:01') THEN NULL ELSE YY.Dat_Gate END AS "TRUCK_GATE_ARRIVAL",
    CASE WHEN YY.Dat_Dock_in = TO_TIMESTAMP('1900-01-01 00:00:01') THEN NULL ELSE YY.Dat_Dock_in END AS "TRUCK_BAY_ARRIVAL",
    CASE WHEN YY.Dat_Dock_out = TO_TIMESTAMP('1900-01-01 00:00:01') THEN NULL ELSE YY.Dat_Dock_out END AS "TRUCK_BAY_DEPARTURE",
    CASE WHEN YY.Dat_Depart = TO_TIMESTAMP('1900-01-01 00:00:01') THEN NULL ELSE YY.Dat_Depart END AS "TRUCK_GATE_DEPARTURE",
    CASE WHEN YY.DATE_CNF = TO_TIMESTAMP('1900-01-01 00:00:01') THEN NULL ELSE YY.DATE_CNF END AS "UDM_RECEIVING",
    CASE WHEN YY.DATE_PUT = TO_TIMESTAMP('1900-01-01 00:00:01') THEN NULL ELSE YY.DATE_PUT END AS "UDM_POSITIONING",
    CASE WHEN YY.Appointment_Date = TO_TIMESTAMP('1900-01-01 00:00:01') THEN NULL ELSE YY.Appointment_Date END AS "Appointment_Date",
    YY.LASTUPDATE AS "UPDATED_DATE",
    '0' AS "ISDELETED"
from (
    select
        XX.INBOUNDREF,
        XX.ASN,
        XX.DEPOT,
        XX.ACTIVITY,
        XX.ITEM,
        max(XX.GRADE) AS GRADE,
        XX.SUPPORT,
        sum(COALESCE(XX.QTYEXP, 0)) as QTY_EXPECT,
        sum(COALESCE(XX.QTYCNF, 0)) as QTY_CONFIRM,
        max(XX.DAT_CNF) as DATE_CNF,
        max(XX.DAT_PUT) AS DATE_PUT,
        max(XX.LASTUPDATE) AS LASTUPDATE,
        max(XX.Dat_Gate) AS Dat_Gate,
        max(XX.Dat_Dock_in) AS Dat_Dock_in,
        max(XX.Dat_Dock_out) AS Dat_Dock_out,
        max(XX.Dat_Depart) AS Dat_Depart,
        max(XX.DOCK) AS DOCK,
        max(XX.Lplate) AS Lplate,
        max(XX.Appointment_Date) AS Appointment_Date
    from (
        -- Expected Qty EDI Received
        select
            I22.I22NRC AS INBOUNDREF,
            I22.I22RFRC AS ASN,
            I22.I22WHOU AS DEPOT,
            I22.I22CPNY AS ACTIVITY,
            I22.I22IDPA AS SUPPORT,
            I22.I22CITE as ITEM,
            NULL AS GRADE, --I22.I22GREX
            sum(I22.I22QTEX) as QTYEXP,
            0 as QTYCNF,
            TO_TIMESTAMP('1900-01-01 00:00:01') as DAT_CNF,
            TO_TIMESTAMP('1900-01-01 00:00:01') AS DAT_PUT,
            TO_TIMESTAMP('1900-01-01 00:00:01') AS Dat_Gate,
            TO_TIMESTAMP('1900-01-01 00:00:01') AS Dat_Dock_in,
            TO_TIMESTAMP('1900-01-01 00:00:01') AS Dat_Dock_out,
            TO_TIMESTAMP('1900-01-01 00:00:01') AS Dat_Depart,
            TO_TIMESTAMP('1900-01-01 00:00:01') AS Appointment_Date,
            NULL AS DOCK,
            NULL AS Lplate,
            max(I22.HVR_CHANGE_TIME) AS LASTUPDATE
        from MODELS.KERING_GLOBE.xprc03p I22
        where I22WHOU = '001' and I22CPNY ='100' AND I22.HVR_IS_DELETED = 0 -- VERIFY THIS COLUMN
        group by I22.I22NRC, I22.I22RFRC, I22.I22WHOU, I22.I22CPNY, I22.I22IDPA, I22.I22CITE, I22.I22GREX
        
        UNION ALL
        
        -- Received qty at moment IPG is created
        select
            RE.RENBLF AS INBOUNDREF,
            RE.RERREC AS ASN,
            VG.VGCDPO AS DEPOT,
            VG.VGCACT AS ACTIVITY,
            VG.VGNSUP AS SUPPORT,
            VG.VGCART as ITEM,
            VG.VGCQAL AS GRADE,
            0 as QTYEXP,
            sum(VG.VGQMVG) as QTYCNF,
            max(TRY_TO_TIMESTAMP(
                CASE WHEN RE.RETRVA = '0' THEN '1900-01-01 00:00:01'
                     ELSE LPAD(RE.RESVAL, 2, '0') || LPAD(RE.REAVAL, 2, '0') || '-' || LPAD(RE.REmVAL, 2, '0') || '-' || LPAD(RE.REJVAL, 2, '0') || ' ' ||
                          SUBSTR(LPAD(RE.REHVAR, 6, 0), 1, 2) || ':' || SUBSTR(LPAD(RE.REHVAR, 6, 0), 3, 2) || ':' || SUBSTR(LPAD(RE.REHVAR, 6, 0), 5, 2)
                END, 'YYYY-MM-DD HH24:MI:SS')
            ) as DAT_CNF,
            max(CH.CHDAE9) AS DAT_PUT,
            min(YMA.MOArrivalDate) AS Dat_Gate,
            min(YMB.MOBayDate) AS Dat_Dock_in,
            min(YMBE.MODepartureDate) AS Dat_Dock_out,
            min(YMD.MODepartureDate) AS Dat_Depart,
            max(TRY_TO_TIMESTAMP(
                CASE WHEN RE.resrec = 0 THEN '1900-01-01 00:00:01'
                     ELSE LPAD(RE.resrec, 2, '0') || LPAD(RE.rearec, 2, '0') || '-' || LPAD(RE.REmrec, 2, '0') || '-' || LPAD(RE.REJrec, 2, '0') || ' ' ||
                          SUBSTR(LPAD(RE.REHrec, 6, 0), 1, 2) || ':' || SUBSTR(LPAD(RE.REHrec, 6, 0), 3, 2) || ':' || SUBSTR(LPAD(RE.REHrec, 6, 0), 5, 2)
                END, 'YYYY-MM-DD HH24:MI:SS')
            ) AS Appointment_Date,
            min(YMB.DOCK) AS DOCK,
            min(YMA.MOLICENCEPLATE1) AS Lplate,
            MAX(GREATEST(
                COALESCE(RE.HVR_CHANGE_TIME, TO_TIMESTAMP('1900-01-01 00:00:01')),
                COALESCE(VG.HVR_CHANGE_TIME, TO_TIMESTAMP('1900-01-01 00:00:01')),
                COALESCE(CH.HVR_CHANGE_TIME, TO_TIMESTAMP('1900-01-01 00:00:01'))
            )) AS LASTUPDATE
        from MODELS.KERING_GLOBE.hlrecpp RE
        inner join MODELS.KERING_GLOBE.hlmvtgp VG on RE.recdpo = VG.vgcdpo and RE.recact = VG.vgcact and RE.renann = VG.vgnann and RE.vgnrec = VG.renrec and VG.vgctvg = '100'
        LEFT outer join MODELS.KERING_GLOBE.kbrechp CH on CH.CHNANN = RE.RENANN and RE.RENREC = CH.CHNREC and VG.VGNSUP = CH.CHNUDM AND CH.CHNANF = RE.RENANN and RE.RENREC = CH.CHNRFI AND CH.HVR_IS_DELETED = 0 -- VERIFY THIS COLUMN
                                            AND CH.CHCDPO = RE.RECDPO AND CH.CHCACT = RE.RECACT
        LEFT outer JOIN (
            SELECT
                UP.UPCACT, UP.UPNANN, UP.UPNREC, UP.UPCDPO,
                max(LPAD(COALESCE(U6.U6NARV, '0'), 2, '0') || LPAD(COALESCE(U6.U6NRDV, '0'), 9, '0')) AS APPOINTMENT_NR,
                Max (U6.U6NARV) AS U6NARV,
                max(U6.U6NRDV) AS U6NRDV,
                MAX(U6.U6CTMR) AS CARRIER,
                max(U6.U6LRDV) AS U6LRDV
            FROM MODELS.KERING_GLOBE.HLRDVRP UP
            LEFT JOIN MODELS.KERING_GLOBE.HLRDVTP U6 ON U6.U6CDPO = UP.UPCDPO AND UP.UPNARV = U6.U6NARV AND UP.UPNRDV = U6.U6NRDV AND U6.HVR_IS_DELETED = 0 -- VERIFY THIS COLUMN
            WHERE UP.HVR_IS_DELETED = 0 -- VERIFY THIS COLUMN
            GROUP BY UP.UPCACT, UP.UPNANN, UP.UPNREC, UP.UPCDPO
        ) U6A ON RE.RECACT = U6A.UPCACT AND RE.RENANN = U6A.UPNANN AND RE.RENREC = U6A.UPNREC AND RE.RECDPO = U6A.UPCDPO
        LEFT outer JOIN (
            SELECT YM1.IDSite, YM1.IDDepot, YM1.MOState, YM1.MOAppointment, min(MOArrivalDate) AS MOArrivalDate, max(YM1.MOLICENCEPLATE1) AS MOLICENCEPLATE1
            FROM Historymovement YM1
            WHERE YM1.IDSite = '1' AND YM1.IDDepot = '4' AND YM1.MOState = 20
            GROUP BY YM1.IDSite, YM1.IDDepot, YM1.MOState, YM1.MOAppointment
        ) YMA ON YMA.MOAppointment = U6A.U6LRDV
        LEFT outer JOIN (
            SELECT YM2.IDSite, YM2.IDDepot, YM2.MOState, YM2.MOAppointment, min(YM2.HYTIMESTAMP) AS MOBayDate, Min(d.DONAME) AS DOCK
            FROM Historymovement YM2
            INNER JOIN DOCK d ON d.IDDOCK = YM2.IDDOCK
            WHERE YM2.IDSite = '1' AND YM2.IDDepot = '4' AND YM2.MOState = 60 AND YM2.IDSTATUS = 3
            GROUP BY YM2.IDSite, YM2.IDDepot, YM2.MOState, YM2.MOAppointment
        ) YMB ON YMB.MOAppointment = U6A.U6LRDV
        LEFT outer JOIN (
            SELECT YM4.IDSite, YM4.IDDepot, YM4.MOState, YM4.MOAppointment, min(HYTIMESTAMP) AS MODEPARTUREDATE, Min(d.DONAME) AS DOCK
            FROM Historymovement YM4
            INNER JOIN DOCK d ON d.IDDOCK = YM4.IDDOCK
            WHERE YM4.IDSite = '1' AND YM4.IDDepot = '4' AND YM4.MOState = 60 AND YM4.IDSTATUS = 11
            GROUP BY YM4.IDSite, YM4.IDDepot, YM4.MOState, YM4.MOAppointment
        ) YMBE ON YMBE.MOAppointment = U6A.U6LRDV
        LEFT outer JOIN (
            SELECT YM3.IDSite, YM3.IDDepot, YM3.MOState, YM3.MOAppointment, min(YM3.MODEPARTUREDATE) AS MODepartureDate
            FROM Historymovement YM3
            WHERE YM3.IDSite = '1' AND YM3.IDDepot = '4' AND YM3.MOState = 99
            GROUP BY YM3.IDSite, YM3.IDDepot, YM3.MOState, YM3.MOAppointment
        ) YMD ON YMD.MOAppointment = U6A.U6LRDV
        where RE.recact = '100' and RE.recdpo = '001' AND RE.HVR_IS_DELETED = 0 AND VG.HVR_IS_DELETED = 0 AND RE.RESVAL != 0 -- VERIFY THESE COLUMNS
        group by RE.RENBLF, RE.RERREC, VG.VGCDPO, VG.VGCACT, VG.VGNSUP, VGCART, VG.VGCQAL
    ) XX
    group by XX.INBOUNDREF, XX.ASN, XX.DEPOT, XX.ACTIVITY, XX.SUPPORT, XX.ITEM
) YY
LEFT OUTER JOIN MODELS.KERING_GLOBE.KBCARTP CA ON CA.cacact = YY.ACTIVITY AND CA.caskuc = YY.ITEM AND CA.HVR_IS_DELETED = 0 -- VERIFY THIS COLUMN
LEFT JOIN (
    -- Rewritten subquery to use QUALIFY for better performance and readability in Snowflake
    SELECT *
    FROM MODELS.KERING_GLOBE.HLVLIDP
    WHERE VICTYI = 'EAN13' AND HVR_IS_DELETED = 0 -- VERIFY THIS COLUMN
    QUALIFY ROW_NUMBER() OVER (PARTITION BY VICART ORDER BY VIACRE DESC, VIMCRE DESC, VIJCRE DESC, VIHCRE DESC) = 1
) VI2 ON VICACT = CACACT AND VICART = CASKUC
where 1=1
-- Incremental Load Condition
AND YY.LASTUPDATE >= (
    SELECT (DATEADD(hour, -4, or2.LAST_UPDATE))::DATE 
    FROM OBJECT_RUN or2 
    WHERE "OBJECT" = '01_INBOUND'
);