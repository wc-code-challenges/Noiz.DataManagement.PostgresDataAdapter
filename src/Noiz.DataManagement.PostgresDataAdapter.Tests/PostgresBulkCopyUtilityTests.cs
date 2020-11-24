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

			var expected = @"Drop Table IF EXISTS table_2; VACUUM;

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
		public void CreatePostgresTable_PkConstraint_ColumnGeneratedCorrectly()
		{
			var sql = PostgresBulkCopyUtility.CreatePostgresTable<ProvidedPkTestObject>("table_2");

			var expected = @"Drop Table IF EXISTS table_2; VACUUM;

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
