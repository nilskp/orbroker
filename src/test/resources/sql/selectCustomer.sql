[#ftl]

SELECT 
  * 
FROM
  Customer

[#if id??]
WHERE
  ID = :id
[/#if]
