using Noiz.DataManagement.PostgresDataAdapter.Tests.Specimens;
using Xunit;
using System.Collections.Generic;
using PostgreSQLCopyHelper.Model;
using System.Linq;

namespace Noiz.DataManagement.PostgresDataAdapter.Tests
{
	public class PostgresBulkCopyUtilityTests
	{
		public PostgresBulkCopyUtilityTests()
		{
			PostgresBulkCopyUtility.UsePascalCase = false;
		}

		[Fact]
		public void GetPostgreSQLCopyHelper_AllPropertyDataTypes_TypesTranslateAndNameIsMatched()
		{
            var actual = PostgresBulkCopyUtility.GetPostgreSQLCopyHelper<TestDataObject>("table_2").TargetTable;

			Assert.Equal("table_2", actual.GetFullyQualifiedTableName());

			Assert.Equal(4, actual.Columns.Count);
			VerifyColumn("test_data_object_id", "Integer", actual.Columns);
			VerifyColumn("test_data_object_date", "Timestamp", actual.Columns);
			VerifyColumn("test_data_object_value", "Double", actual.Columns);
			VerifyColumn("test_data_object_name", "Varchar", actual.Columns);
		}

		[Fact]
		public void CreatePostgresTable_AllPropertyDataTypes_AppropriateDmlCreated()
		{
			var sql = PostgresBulkCopyUtility.CreatePostgresTable<TestDataObject>("table_2");

			var expected = @"Drop Table IF EXISTS table_2;

CREATE TABLE table_2 (
  test_data_object_id integer  null,
  test_data_object_date timestamp  null,
  test_data_object_value double precision  null,
  test_data_object_name varchar(50) UNIQUE null,
  test_data_object_serial serial PRIMARY KEY 
);
";
			Assert.Equal(expected, sql);
		}

		[Fact]
		public void GenerateUpsertQueryTest()
		{
			List<string> constraints = new List<string>();
			constraints.Add("test_data_object_name");

			var sql = PostgresBulkCopyUtility.GenerateUpsertQuery<TestDataObject>("original", "temp", constraints);

			var expected = @"INSERT INTO 
							original (test_data_object_id,test_data_object_date,test_data_object_value,test_data_object_name) 
							select test_data_object_id,test_data_object_date,test_data_object_value,test_data_object_name 
							from temp
							on conflict (test_data_object_name) do 
							update set test_data_object_id = EXCLUDED.test_data_object_id,test_data_object_date = EXCLUDED.test_data_object_date,test_data_object_value = EXCLUDED.test_data_object_value;";

			Assert.Equal(expected, sql);
        }

		[Fact]
		public void CreatePostgresTable_AllPropertyDataTypes_AppropriateDmlCreated_NoAttributesWithEnum()
		{
			var sql = PostgresBulkCopyUtility.CreatePostgresTable<TestDataObjectNoAttributes>("postgres_bulk_util_test", primaryKeyColumnNameOrConstraintSql: "test_data_object_id");

			var expected = @"Drop Table IF EXISTS postgres_bulk_util_test;

CREATE TABLE postgres_bulk_util_test (
  test_data_object_id integer NOT NULL,
  test_data_object_date timestamp NULL,
  test_data_object_value double precision NULL,
  test_data_object_name varchar(245) NULL,
  change_value varchar(245) NOT NULL
);
ALTER TABLE postgres_bulk_util_test ADD PRIMARY KEY (test_data_object_id);
";
			Assert.Equal(expected, sql);
		}

		[Fact]
		public void CreatePostgresTable_PkConstraint_ColumnGeneratedCorrectly()
		{
			var sql = PostgresBulkCopyUtility.CreatePostgresTable<ProvidedPkTestObject>("table_2");

			var expected = @"Drop Table IF EXISTS table_2;

CREATE TABLE table_2 (
  id integer PRIMARY KEY ,
  number_of_items integer  null
);
";
			Assert.Equal(expected, sql);
		}

		private void VerifyColumn(string name, string dataType, IReadOnlyList<TargetColumn> columns)
		{
			var column = columns.First(x => string.Equals(x.ColumnName, name));

			Assert.Equal(dataType.ToLower(), column.DbType.ToString().ToLower());
		}
	}
}
