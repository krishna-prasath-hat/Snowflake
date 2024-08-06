create or replace view Indivial_traders_block as select DATE, SYMBOL, SECURITY_NAME, CLIENT_NAME, BUY_OR_SELL, replace(QUANTITY_TRADED,',','') as QUANTITY_TRADED, TRADE_PRICE_OR_WGHT_AVG_PRICE, REMARKS  from TRADING_APP.TRADING_APP.Block_DEALS where client_name not like '%LTD%' and client_name not like '%LIMITED%' and client_name not like '%LLP%' and client_name not like '%TRUST%' and client_name not like '%FUND%' and client_name not like '%INVESTMENTS%' and client_name not like '%OPPORTUNITIES%' and client_name not like '%LLC%' and  client_name not like '%HUF%' and client_name not like '%INC%' and client_name not like '%ETF%' and client_name not like '%SECURITIES%' and client_name not like '%PRIVATE%' and client_name not like '%CAPITAL%';

select * from Indivial_traders_block;

select *,SUM(REPLACE(b.QUANTITY_TRADED, ',', '')::INTEGER) from bulk_deals where client_name like '%FLEX%';

select distinct(client_name) from Indivial_traders;


select DATE, SYMBOL, SECURITY_NAME, CLIENT_NAME, BUY_OR_SELL, replace(QUANTITY_TRADED,',',''), TRADE_PRICE_VS_WGHT_AVG_PRICE, REMARKS from Indivial_traders where client_name = 'RAJESHBHAI PATEL';

1.
create table HNI_trading_block as
select * from (select client_name,SUM(CASE WHEN BUY_OR_SELL = 'BUY' THEN QUANTITY_TRADED ELSE 0 END) -
    SUM(CASE WHEN BUY_OR_SELL = 'SELL' THEN QUANTITY_TRADED ELSE 0 END) AS NET_QUANTITY from Indivial_traders_block 
    group by CLIENT_NAME) where net_quantity> 10000000;

2.
create table stocks_purchased_block as
SELECT 
    client_name,
    SUM(CASE WHEN BUY_OR_SELL = 'BUY' THEN REPLACE(QUANTITY_TRADED, ',', '')::INTEGER ELSE 0 END) AS Stocks_purchased,
    SUM(CASE WHEN BUY_OR_SELL = 'SELL' THEN REPLACE(QUANTITY_TRADED, ',', '')::INTEGER ELSE 0 END) AS Stocks_Sold
FROM 
    Indivial_traders_block
GROUP BY 
    client_name;

3)

create table Current_Holding_block as
select client_name,Stocks_purchased - Stocks_Sold as current_holding from (SELECT 
    client_name,
    SUM(CASE WHEN BUY_OR_SELL = 'BUY' THEN REPLACE(QUANTITY_TRADED, ',', '')::INTEGER ELSE 0 END) AS Stocks_purchased,
    SUM(CASE WHEN BUY_OR_SELL = 'SELL' THEN REPLACE(QUANTITY_TRADED, ',', '')::INTEGER ELSE 0 END) AS Stocks_Sold
FROM 
    block_deals
GROUP BY 
    client_name
) where Stocks_purchased >= Stocks_Sold ;


4)

create table Cumulative_sum_block as 
SELECT 
    DATE,
    SYMBOL,
    SECURITY_NAME,
    CLIENT_NAME,
    BUY_OR_SELL,
    QUANTITY_TRADED,
    TRADE_PRICE_OR_WGHT_AVG_PRICE,
    SUM(REPLACE(QUANTITY_TRADED, ',', '')::INTEGER) OVER (
        PARTITION BY CLIENT_NAME
        ORDER BY DATE
    ) AS cumulative_quantity_traded
FROM 
    block_deals
ORDER BY 
    CLIENT_NAME, DATE;


SELECT *
FROM bulk_deals
limit 10 ;


5)

create table Data_cleansing_block as
SELECT 
    DATE,
    SYMBOL,
    SECURITY_NAME,
    CLIENT_NAME,
    BUY_OR_SELL,
    QUANTITY_TRADED,
    TRADE_PRICE_OR_WGHT_AVG_PRICE,
    REMARKS,
    CASE 
        WHEN client_name RLIKE '[^a-zA-Z\\s]' THEN 'Unknown'
        ELSE client_name
    END AS cleansed_client_name
FROM block_deals;

6)

create table top_10_bulk_deals as
SELECT SECURITY_NAME, SUM(REPLACE(QUANTITY_TRADED, ',', '')::INTEGER) AS Most_Quantity
FROM bulk_deals
WHERE BUY_OR_SELL = 'BUY'
GROUP BY SECURITY_NAME
ORDER BY Most_Quantity DESC
LIMIT 10;

create table least_10_bulk_deals as 
SELECT SECURITY_NAME, SUM(REPLACE(QUANTITY_TRADED, ',', '')::INTEGER) AS least_Quantity
FROM bulk_deals
WHERE BUY_OR_SELL = 'BUY'
GROUP BY SECURITY_NAME
ORDER BY least_Quantity ASC
LIMIT 10;


7) Overlapping_participants in both Bulk&Block deal

create table Overlapping_participants as 
SELECT 
    COALESCE(b.client_name, c.client_name) AS client_name,
    COALESCE(SUM(REPLACE(b.QUANTITY_TRADED, ',', '')::INTEGER), 0) AS bulk_quantity_traded,
    COALESCE(SUM(REPLACE(c.QUANTITY_TRADED, ',', '')::INTEGER), 0) AS block_quantity_traded
FROM 
    bulk_deals b
JOIN 
    block_deals c ON b.client_name = c.client_name
GROUP BY 
    COALESCE(b.client_name, c.client_name)
ORDER BY 
    client_name;



select SUM(REPLACE(QUANTITY_TRADED, ',', '')::INTEGER) from bulk_deals where security_name like 'Dish%' and buy_or_sell = 'BUY'