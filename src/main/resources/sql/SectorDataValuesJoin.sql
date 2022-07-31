SELECT
    company,
    industry,
    sect.asset,
    series ,
    isbn,

     name ,
     tag,
     ts
     `value`
FROM DataValues dv
LEFT JOIN Sectors sect
ON dv.asset = sect.asset