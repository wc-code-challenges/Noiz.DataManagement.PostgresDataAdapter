using System;
using System.Collections.Generic;
using System.Linq;
using Xunit;
using Npgsql;
using Noiz.DataManagement.PostgresDataAdapter.Tests.Specimens;
using Dapper;
using System.Data;
using System.Threading;

namespace Noiz.DataManagement.PostgresDataAdapter.Tests
{
	public class PostgresBulkCopyUtilityAcceptanceTests
	{
		private readonly string DbConnectionString =
            $"Server=127.0.0.1;Port=5432;Database={DbName};User Id={DbUsername};Password={DbPassword};";

		private const string DbName = "<Enter Database Name Here>";
		private const string DbUsername = "<Enter Username Here>";
		private const string DbPassword = "<Enter Password Here>";

		private const string Tablename = "postgres_bulk_util_test";

		private const int NumberOfEntities = 237;

        public PostgresBulkCopyUtilityAcceptanceTests()
        {
            Thread.Sleep(1000);
        }

        [Fact]
		public void CreateInsertAndVerifyInsertsUsingAttributes()
		{
			PostgresBulkCopyUtility.UsePascalCase = true;
			using var connection = new NpgsqlConnection(DbConnectionString);

			connection.Open();
			var createTableSql = PostgresBulkCopyUtility.CreatePostgresTable<TestDataObject>(Tablename);

			connection.ExecuteScalar(createTableSql);

			var mapping = PostgresBulkCopyUtility.GetPostgreSQLCopyHelper<TestDataObject>(Tablename);

			var entities = CreateEntities(NumberOfEntities).ToList();

			var saved = mapping.SaveAll(connection, entities);

			connection.ExecuteScalar($"Drop Table {Tablename};");

			Assert.Equal(NumberOfEntities, (int)saved);
		}

		[Fact]
		public void CreateTempTableThenUpsertAndVerify()
		{
			PostgresBulkCopyUtility.UsePascalCase = true;
			using var connection = new NpgsqlConnection(DbConnectionString);
			connection.Open();
			var constraintColumns = new List<string> { "TestDataObjectName" };

			var createTableSql = PostgresBulkCopyUtility.CreatePostgresTable<TestDataObject>(Tablename);
            connection.ExecuteScalar(createTableSql);
            var mapping = PostgresBulkCopyUtility.GetPostgreSQLCopyHelper<TestDataObject>(Tablename);

            var insertEntities = CreateEntities(NumberOfEntities).ToList();
            var upsertEntities = CreateEntities(NumberOfEntities + 1).ToList();

            var saved = mapping.SaveAll(connection, insertEntities);

			var upsert = PostgresBulkCopyUtility.Upsert<TestDataObject>(Tablename, upsertEntities, connection, constraintColumns);

            if (connection.State != ConnectionState.Open)
                connection.Open();

            connection.ExecuteScalar($"Drop Table {Tablename};");

            Assert.Equal(NumberOfEntities + 1, upsert);
        }

        [Fact]
		public void CreateInsertAndVerifyInsertsNotUsingAttributes()
		{
			using var connection = new NpgsqlConnection(DbConnectionString);

			connection.Open();
			PostgresBulkCopyUtility.UsePascalCase = false;
			var createTableSql = PostgresBulkCopyUtility.CreatePostgresTable<TestDataObjectNoAttributes>(Tablename, primaryKeyColumnNameOrConstraintSql: "test_data_object_id");

			connection.ExecuteScalar(createTableSql);

			var mapping = PostgresBulkCopyUtility.GetPostgreSQLCopyHelper<TestDataObjectNoAttributes>(Tablename);

			var entities = CreateEntitiesWithNoAttributes(NumberOfEntities).ToList();

			var saved = mapping.SaveAll(connection, entities);

			connection.ExecuteScalar($"Drop Table {Tablename};");

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

		private IEnumerable<TestDataObjectNoAttributes> CreateEntitiesWithNoAttributes(int count)
		{
			for (int i = 0; i < count; i++)
			{
				yield return new TestDataObjectNoAttributes
				{
					TestDataObjectDate = DateTime.Today.AddDays(-1 * i),
					TestDataObjectId = i,
					TestDataObjectName = $"Item {i}",
					TestDataObjectValue = i % 2 == 0 ? null : i + Math.Log(i + Math.PI),
					ChangeValue = (i % 3) switch
					{
						0 => TestEnumeration.test1,
						1 => TestEnumeration.test2,
						2 => TestEnumeration.test3,
						_ => throw new NotImplementedException(),
					}
				};
			}
		}
	}
}
