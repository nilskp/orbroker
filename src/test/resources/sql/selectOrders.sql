SELECT
  o.*,
  c.name AS CustomerName,
  oi.itemID,
  i.name AS ItemName,
  i.price
FROM CustOrder o
JOIN Customer c
ON o.CustomerID = c.ID
JOIN OrderItem oi
ON o.ID = oi.OrderID
JOIN Item i
ON oi.ItemID = i.ID
#if ($cust) 
  WHERE o.CustomerID = :cust.id 
#end
ORDER BY
 o.ID
