SELECT
  INTEGRATION.ccej_tdivision.divisionnm AS ディビジョン
  , INTEGRATION.ccej_tdivision.regionnm AS リジョン
  , INTEGRATION.ccej_tdivision.zkzpdwerks AS 商流拠点
  , INTEGRATION.ccej_tdivision.zkzpdwerksnm AS 商流拠点名
  , INTEGRATION.ccej_tzktsd0004.zkzpdcroute AS 商流ルート
  , INTEGRATION.kna1.kunnr AS 得意先CD
  , INTEGRATION.kna1.name2 AS 得意先名
  , INTEGRATION.ccej_tzktsd0004.ZRESALCOD AS 代表得意先CD
  , kna11.name2 AS 代表得意先名
  ,case when INTEGRATION.ccej_tzktsd0004.ZRESALCOD is null then ''
        when INTEGRATION.ccej_tzktsd0004.ZRESALCOD = '' then ''
        when INTEGRATION.ccej_tzktsd0004.ZRESALCOD = INTEGRATION.ccej_tzktsd0004.zkzkunnr then ''
		when INTEGRATION.ccej_tzktsd0004.ZRESALCOD <> INTEGRATION.ccej_tzktsd0004.zkzkunnr then 'X'
		end AS 得意先と代表得意先異なるフラグ
  , INTEGRATION.kna1.ccc_season_start1 AS 取引開始日
  , INTEGRATION.kna1.ccc_season_end1 AS 取引終了日
  ,case when INTEGRATION.kna1.kdkg5 = 'M3' then 'RS'
        when INTEGRATION.kna1.kdkg5 = 'M4' then 'FS'
		end AS `RS/FS`
--   , INTEGRATION.ccej_tdivision.oldbottler1cd
--   , INTEGRATION.ccej_tdivision.divisioncd
--   , INTEGRATION.ccej_tdivision.regioncd 
FROM
  INTEGRATION.ccej_tzktsd0004 
  INNER JOIN INTEGRATION.kna1 
    ON INTEGRATION.ccej_tzktsd0004.zkzkunnr = INTEGRATION.kna1.kunnr 
  INNER JOIN INTEGRATION.ccej_tdivision 
    ON INTEGRATION.ccej_tdivision.zkzpdwerks = INTEGRATION.ccej_tzktsd0004.zkzpdwerks 
  LEFT JOIN INTEGRATION.kna1 kna11 
    ON INTEGRATION.ccej_tzktsd0004.ZRESALCOD = kna11.kunnr
where 
-- INTEGRATION.ccej_tdivision.oldbottler1cd = '21'
-- or 
-- INTEGRATION.ccej_tdivision.oldbottler1cd = '22'
-- or 
-- INTEGRATION.ccej_tdivision.oldbottler1cd = '13'
-- or 

-- INTEGRATION.ccej_tdivision.oldbottler1cd = '23'
-- or 
-- INTEGRATION.ccej_tdivision.oldbottler1cd = '39'
-- or
-- INTEGRATION.ccej_tdivision.oldbottler1cd = '42'
-- or 
INTEGRATION.ccej_tdivision.oldbottler1cd = '35'
