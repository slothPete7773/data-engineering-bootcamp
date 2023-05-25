# Note

1. If the table is not exists yet, and use the load to table from uri, it will raise error Not Found Table, which is quite obvious. \
Additionally, it is recommend not to create table inline in the same insert request [with this reference](https://stackoverflow.com/questions/30348384/not-found-table-for-new-bigquery-table)