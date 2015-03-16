
SELECT * FROM Item

#if ($ids)
	WHERE
	ID #IN("ids")
#end
