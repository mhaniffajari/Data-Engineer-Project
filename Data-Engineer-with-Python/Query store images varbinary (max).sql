
IF OBJECT_ID(N'dbo.Images', N'U') IS NULL
CREATE TABLE dbo.Images
(id int,
img varbinary(max))
GO

insert into dbo.Images values(
1,(select * from Openrowset (BULK 'C:\Users\Hanif\Data Science\Data Engineer\Data Engineer with Python\horse-or-human\humans\human01-00.png', Single_Blob)as T)
)

select * from dbo.Images


