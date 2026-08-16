# Cloud tests
Tests in this directory all depend on an Azure container being set up that is populated with the data from `./data`. 
The environment each test requires varies, and is specified by a set of env vars specified at the start of each test
file through `require-env` statements.

