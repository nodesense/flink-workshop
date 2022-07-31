
INSERT INTO TradeAggregates
SELECT CONCAT('NIFTY 50 EQ','') as `asset` ,
        st,
        et,
        DT,
        sum(V) as V,
        sum(TA) as TA,
        avg(OI) as OI,
        sum(OIDiff) as OIDiff
FROM TempCandles
where DT='EQ' and  N50=1
GROUP BY DT, st, et;