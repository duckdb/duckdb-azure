CREATE OR REPLACE SECRET az_data ( 
  TYPE azure, 
  PROVIDER credential_chain,
  ACCOUNT_NAME 'duckdblabstestdatablob',
  SCOPE 'az://'
);
