# Noiz.DataManagement.PostgresDataAdapter

For postgresSql bulk data ingestion, this builds on https://github.com/PostgreSQLCopyHelper/PostgreSQLCopyHelper it automates the process of building a copy helper and also if need be creating a table.

To run the acceptance tests please complete the connection string at the top of the file.

Usage is as follows, see below
```C#
public ulong Save(IEnumerable<TestDataObject> entities)
{
  using var connection = new NpgsqlConnection(DbConnectionString);

  connection.Open();
  var createTableSql = PostgresBulkCopyUtility.CreatePostgresTable<TestDataObject>(Tablename);

  connection.ExecuteScalar(createTableSql);

  var mapping = PostgresBulkCopyUtility.GetPostgreSQLCopyHelper<TestDataObject>(Tablename);

  return mapping.SaveAll(connection, entities);
}
```
