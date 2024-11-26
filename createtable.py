from logging import exception
import pymysql
import json

   

timeout = 10
connection = pymysql.connect(
  charset="utf8mb4",
  connect_timeout=timeout,
  cursorclass=pymysql.cursors.DictCursor,
  db="iia_group2",
  host="world-athletics-world-athletics.h.aivencloud.com",
  password="AVNS_q4qfMZEYhnIhreAFwYi",
  read_timeout=timeout,
  port=24936,
  user="avnadmin",
  write_timeout=timeout,
)

try:
    cursor = connection.cursor()
    cursor.execute("use iia_group2")
    # cursor.execute("create table GST_Payers_PerYear_Data(id int auto_increment, state varchar(255), keyy varchar(255), start_date date, end_date date, value float, primary key (id))")
    # cursor.execute("create table GST_Info_PerYear_Data (id int auto_increment, state varchar(255), keyy varchar(255), start_date date, end_date date, value float, primary key (id))")
    # cursor.execute("create table GST_Info_PerMonth (id int auto_increment, state varchar(255), keyy varchar(255), datee date, value float, primary key (id))")
    # cursor.execute("create table GST_ReturnType_Info_Value (id int auto_increment, state varchar(255), keyy varchar(255), datee date, value float, primary key (id))")
    # cursor.execute("create table Banking_Statistics (id int auto_increment, state varchar(255), start_date date, end_date date, bank_type  varchar(255), keyy varchar(255), value float, primary key (id))")
    # cursor.execute("create table Outstanding_Liabilities_Info (id int auto_increment, state varchar(255), start_date date, end_date date, keyy varchar(255), value float, primary key (id))")
    # cursor.execute("create table Loans_Taken_by_States (id int auto_increment, state varchar(255), start_date date, end_date date, provider varchar(255), loan_amount float, primary key (id))")
    # cursor.execute("create table Fiscal_Indicator_of_States (id int auto_increment, state varchar(255), start_date date, end_date date, keyy varchar(255), value float, primary key (id))")
    # cursor.execute("create table States (id int auto_increment, state varchar(255), primary key (id))")
    # cursor.execute("create table Providers (id int auto_increment, provider varchar(255), primary key (id))")
    # cursor.execute("create table BankTypes (id int auto_increment, bank_type varchar(255), primary key (id))")
    # cursor.execute("create table GSTPayers_Key (id int auto_increment, keyy varchar(255), primary key (id))")
    # cursor.execute("create table GSTInfo_Key (id int auto_increment, keyy varchar(255), primary key (id))")
    # cursor.execute("create table GSTInfo_PerMonth_Key (id int auto_increment, keyy varchar(255), primary key (id))")
    # cursor.execute("create table BankStats_Key (id int auto_increment, keyy varchar(255), primary key (id))")
    # cursor.execute("create table Liabiity_Keys (id int auto_increment, keyy varchar(255), primary key (id))")
    # cursor.execute("create table Fiscal_Keys (id int auto_increment, keyy varchar(255), primary key (id))")
    
    # cursor.execute("create table GST_Payers_PerYear_Data_NS (id int auto_increment,state varchar(255), start_date date, end_date date, total_eligible_payers float, total_registered_payers float, normal_tax_payers float, composite_tax_payer float, casual_tax_payer float, nri_tax_payers float, primary key (id))")
    # cursor.execute("create table GST_Info_PerYear_Data_NS (id int auto_increment, state varchar(255), start_date date, end_date date, input_service_distributors float, UIN_holders float, settlement_IGST float, primary key (id))")
    # cursor.execute("create table GST_PerMonth_Info_NS (id int auto_increment, state varchar(255), datee date, settlement_IGST float, primary key (id))")
    # cursor.execute("create table GST_Payers_Info_PerMonth_NS (id int auto_increment, state varchar(255), datee date, return_type varchar(255), eligible_payers float, registered_payers float, primary key (id))")
    # cursor.execute("create table Banking_Statistics_IndianStates_NS (id int auto_increment, state varchar(255), start_date date, end_date date, bank_type varchar(255), agricultural_credit float, personal_loans float, industrial_credits float, credits float, deposits float, credits_deposit_ratio float, primary key (id))")
    # cursor.execute("create table Outstanding_Liabilities_Info_NS (id int auto_increment, state varchar(255), start_date date, end_date date, total_outstanding_liabilities float, contigency_funds float, provident_funds float, reserve_funds float, deposits_and_advances float, internal_debt float, power_bonds float, state_development_loans float, primary key (id))")
    # cursor.execute("create table Loans_Taken_by_States_NS (id int auto_increment, state varchar(255), start_date date, end_date date, provider varchar(255), loan_amount float, primary key (id))")
    # cursor.execute("create table Fiscal_Indicator_of_States_NS (id int auto_increment, state varchar(255), start_date date, end_date date, revenue_deficit float, pension_expenditure float, primary_deficit float, gross_fiscal_deficit float, capital_outlay float, social_sector_expenditure float, own_tax_revenue float, interest_payment float, capital_expenditure float, primary key (id))")
finally:
  connection.commit()
  connection.close()