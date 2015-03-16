

[#ftl]

SELECT * FROM Item

[#if ids??]
	WHERE
	ID [@IN seq="ids"/]
[/#if]
