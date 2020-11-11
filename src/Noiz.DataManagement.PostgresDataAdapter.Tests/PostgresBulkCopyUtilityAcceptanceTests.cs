using System;
using System.Collections.Generic;
using System.Linq;
using Xunit;
using Npgsql;
using Noiz.DataManagement.PostgresDataAdapter.Tests.Specimens;
using Dapper;

namespace Noiz.DataManagement.PostgresDataAdapter.Tests
{
	public class PostgresBulkCopyUtilityAcceptanceTests
	{
		private const string DbConnectionString = 
			"Server=127.0.0.1;Port=5432;Database=betfair_greyhounds;User Id=<Please fill in details>;Password=<Please fill in details>;";

		private const string Tablename = "postgres_bulk_util_test";

		private const int NumberOfEntities = 237;

		[Fact]
		public void CreateInsertAndVerifyInserts()
		{
			using var connection = new NpgsqlConnection(DbConnectionString);

			connection.Open();
			var createTableSql = PostgresBulkCopyUtility.CreatePostgresTable<TestDataObject>(Tablename);

			connection.ExecuteScalar(createTableSql);

			var mapping = PostgresBulkCopyUtility.GetPostgreSQLCopyHelper<TestDataObject>(Tablename);

			var entities = CreateEntities(NumberOfEntities).ToList();

			var saved = mapping.SaveAll(connection, entities);

			connection.ExecuteScalar($"Drop Table {Tablename}; VACUUM;");

			Assert.Equal(NumberOfEntities, (int)saved);
		}

		private IEnumerable<TestDataObject> CreateEntities(int count)
		{
			for (int i = 0; i < count; i++)
			{
				yield return new TestDataObject
				{
					TestDataObjectDate = DateTime.Today.AddDays(-1 * i),
					TestDataObjectId = i,
					TestDataObjectName = $"Item {i}",
					TestDataObjectValue = i%2 == 0? null: i + Math.Log(i + Math.PI)
				};
			}
		}
	}
}
