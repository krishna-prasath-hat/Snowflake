create or replace TABLE EXCEPTIONHANDLER.EXCEPTIONHANDLER.ITEM_TABLE (
	ITEM_CODE VARCHAR(16777216),
	ITEM_NAME VARCHAR(16777216),
	ITEM_QUALITY NUMBER(38,0),
	PRICE_PER_QUANTITY NUMBER(38,0)
);


create or replace TABLE EXCEPTIONHANDLER.EXCEPTIONHANDLER.CASH_TABLE (
	BILL_NO NUMBER(38,0) NOT NULL autoincrement start 1 increment 1 noorder,
	BILL_DATE TIMESTAMP,
	ITEM_NAME VARCHAR(16777216),
	BILL_AMOUNT NUMBER(38,0),
	primary key (BILL_NO)
);


INSERT INTO EXCEPTIONHANDLER.EXCEPTIONHANDLER.ITEM_TABLE (ITEM_CODE, ITEM_NAME, ITEM_QUALITY, PRICE_PER_QUANTITY)
VALUES ('A1001', 'HORLICKS', 150, 250);

INSERT INTO EXCEPTIONHANDLER.EXCEPTIONHANDLER.ITEM_TABLE (ITEM_CODE, ITEM_NAME, ITEM_QUALITY, PRICE_PER_QUANTITY)
VALUES ('B2002', 'BOOST', 100, 300);

INSERT INTO EXCEPTIONHANDLER.EXCEPTIONHANDLER.ITEM_TABLE (ITEM_CODE, ITEM_NAME, ITEM_QUALITY, PRICE_PER_QUANTITY)
VALUES ('C3003', 'COMPLAN', 200, 350);



CREATE OR REPLACE PROCEDURE proc_item_master(
    IN_ITEM_CODE STRING,
    IN_ITEM_QUANTITY STRING
)
RETURNS STRING
LANGUAGE JAVASCRIPT
AS
$$
var sql_command;
var item_name;
var item_quality;
var price_per_quantity;
var bill_amount;
var bill_no;

var quantity = Number(IN_ITEM_QUANTITY);

sql_command = `SELECT ITEM_NAME, ITEM_QUALITY, PRICE_PER_QUANTITY 
               FROM EXCEPTIONHANDLER.EXCEPTIONHANDLER.ITEM_TABLE 
               WHERE ITEM_CODE = '${IN_ITEM_CODE}'`;
var statement1 = snowflake.createStatement({sqlText: sql_command});
var result1 = statement1.execute();
if (result1.next()) {
    item_name = result1.getColumnValue("ITEM_NAME");
    item_quality = Number(result1.getColumnValue("ITEM_QUALITY")); 
    price_per_quantity = Number(result1.getColumnValue("PRICE_PER_QUANTITY")); 
} else {
    return "Item code not found.";
}

item_quality -= quantity;
sql_command = `UPDATE EXCEPTIONHANDLER.EXCEPTIONHANDLER.ITEM_TABLE 
               SET ITEM_QUALITY = ${item_quality} 
               WHERE ITEM_CODE = '${IN_ITEM_CODE}'`;
var statement2 = snowflake.createStatement({sqlText: sql_command});
statement2.execute();

bill_amount = quantity * price_per_quantity;

sql_command = `SELECT COALESCE(MAX(BILL_NO), 0) AS MAX_BILL_NO 
               FROM EXCEPTIONHANDLER.EXCEPTIONHANDLER.CASH_TABLE`;
var statement3 = snowflake.createStatement({sqlText: sql_command});
var result3 = statement3.execute();
if (result3.next()) {
    bill_no = Number(result3.getColumnValue("MAX_BILL_NO")) + 1; 
} else {
    bill_no = 1;
}
sql_command = `INSERT INTO EXCEPTIONHANDLER.EXCEPTIONHANDLER.CASH_TABLE (BILL_NO, BILL_DATE, ITEM_NAME, BILL_AMOUNT) 
               VALUES (${bill_no}, DATE_TRUNC('SECOND', CURRENT_TIMESTAMP()), '${item_name}', ${bill_amount})`;
var statement4 = snowflake.createStatement({sqlText: sql_command});
statement4.execute();
return "Item updated and bill recorded successfully.";
$$;



CALL proc_item_master('A1001', '5');


select * from item_table;
select  * from cash_table;