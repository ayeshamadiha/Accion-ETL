create table sales.accion_sales_error(
SalesDate date not null,
ProductId varchar not null,
StoreId varchar not null,
SalesTypeId int not null,
Quantity varchar not null,
Revenue varchar not null,
Time_Stamp timestamp,
SourceFile varchar,
err varchar
);

