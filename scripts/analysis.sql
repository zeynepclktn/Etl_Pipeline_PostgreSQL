-- Rapor tablosunu oluştur
DROP TABLE IF EXISTS DIRTY_CAFE_SALES;

CREATE TABLE DIRTY_CAFE_SALES (
    Transaction_ID varchar(15), 
    ID varchar(20),
    Quantity float,
    Price_Per_Unit float,
    Total_Spent float ,
    Payment_Method varchar(25),
    Location varchar(20),
    Transaction_Date timestamp 
);

ALTER TABLE DIRTY_CAFE_SALES ADD PRIMARY KEY (Transaction_ID);
-- Rapor tablosunu kontrol et

