using System;
using System.Data;
using System.Linq;
using System.Reflection;
using System.Text;
using Noiz.DataManagement.PostgresDataAdapter.Definitions;
using PostgreSQLCopyHelper;

namespace Noiz.DataManagement.PostgresDataAdapter
{
	public static class PostgresBulkCopyUtility
	{
		/// <summary>
		/// This method creates a PostgreSQLCopyHelper for any type on all Properties that can be read.
		/// It converts property names from Came or Pascal case to using lower case seperated by undersore _
		/// </summary>
		/// <typeparam name="T">The type of entity to wrap</typeparam>
		/// <param name="entityType">The type definition for the entity</param>
		/// <param name="tableName">Thye table name to bulk insert to</param>
		/// <returns>An instance of PostgreSQLCopyHelper that has been mapped for the specified type</returns>
		public static PostgreSQLCopyHelper<T> GetPostgreSQLCopyHelper<T>(string tableName)
		{
			var entityType = typeof(T);
			var mapping = new PostgreSQLCopyHelper<T>(tableName);

			foreach (var propertyInfo in entityType.GetProperties(BindingFlags.Instance | BindingFlags.Public))
				if(propertyInfo.CanRead) mapping.MapProperty(propertyInfo);

			return mapping;
		}

		/// <summary>
		/// Generate DDL for a class definition
		/// </summary>
		/// <typeparam name="T">The type to generate table DDL statements for</typeparam>
		/// <param name="tableName">The name to use for the table</param>
		/// <param name="drop">whether or not to drop the table</param>
		/// <returns></returns>
		public static string CreatePostgresTable<T>(string tableName, bool drop = true)
		{
			var sql = new StringBuilder();

			if (drop) sql.AppendLine($"Drop Table {tableName}; VACUUM;");

			sql.AppendLine($"CREATE TABLE accounts (");

			var entityType = typeof(T);
			var properties = entityType.GetProperties(BindingFlags.Instance | BindingFlags.Public);
			for (int i = 0; i < properties.Length; i++)
			{
				var propertyInfo = properties[i];
				sql.AppendLine(string.Format("  {0} {1}", GetColumnCreateSql(propertyInfo), i < properties.Length - 1 ? "," : string.Empty));
			}

			sql.AppendLine(");");

			return sql.ToString();
		}

		internal static PostgreSQLCopyHelper<T> MapProperty<T>(this PostgreSQLCopyHelper<T> postgreSQLCopyHelper, PropertyInfo propertyInfo)
		{
			return propertyInfo.PropertyType.Name.ToLower() switch
			{
				"string" => postgreSQLCopyHelper.MapVarchar(GetColumnNameFromPascalCaseOrCamelCasePropertyName(propertyInfo.Name)
					, x => (string)propertyInfo.GetValue(x)),
				"char" => postgreSQLCopyHelper.MapCharacter(GetColumnNameFromPascalCaseOrCamelCasePropertyName(propertyInfo.Name)
					, x => propertyInfo.GetValue(x).ToString()),
				"datetime" => postgreSQLCopyHelper.MapTimeStamp(GetColumnNameFromPascalCaseOrCamelCasePropertyName(propertyInfo.Name)
					, x => (DateTime)propertyInfo.GetValue(x)),
				"double" => postgreSQLCopyHelper.MapDouble(GetColumnNameFromPascalCaseOrCamelCasePropertyName(propertyInfo.Name)
					, x => (double)propertyInfo.GetValue(x)),
				"int32" => postgreSQLCopyHelper.MapInteger(GetColumnNameFromPascalCaseOrCamelCasePropertyName(propertyInfo.Name)
					, x => (int)propertyInfo.GetValue(x)),
				"int64" => postgreSQLCopyHelper.MapBigInt(GetColumnNameFromPascalCaseOrCamelCasePropertyName(propertyInfo.Name)
					, x => (long)propertyInfo.GetValue(x)),
				"decimal" => postgreSQLCopyHelper.MapNumeric(GetColumnNameFromPascalCaseOrCamelCasePropertyName(propertyInfo.Name)
					, x => (decimal)propertyInfo.GetValue(x)),
				"boolean" => postgreSQLCopyHelper.MapBoolean(GetColumnNameFromPascalCaseOrCamelCasePropertyName(propertyInfo.Name)
					, x => (bool)propertyInfo.GetValue(x)),
				_ => throw new NotImplementedException($"Error on property '{propertyInfo.Name}' The type conversion to postgres for .NET type {propertyInfo.PropertyType.FullName} is not implemented in this library."),
			};
		}

		internal static string GetColumnNameFromPascalCaseOrCamelCasePropertyName(string propertyName)
			=> System.Text.RegularExpressions.Regex.Replace(propertyName, "(\\B[A-Z])", "_$1").ToLower();

		internal static string GetColumnCreateSql(PropertyInfo propertyInfo)
		{
			var columnInfo = propertyInfo.GetCustomAttributes(typeof(PostgresColumnAttribute)).Cast<PostgresColumnAttribute>().FirstOrDefault();
			if (columnInfo == null) 
				throw new Exception($"The property '{propertyInfo.Name}' has no data type defined for generating the column SQL");

			var columnName = columnInfo?.Name?? GetColumnNameFromPascalCaseOrCamelCasePropertyName(propertyInfo.Name);

			var dataType = columnInfo.DataType switch
			{
				PostgresDataType.Serial => " serial PRIMARY KEY ",
				PostgresDataType.BigSerial => " bigserial PRIMARY KEY ",
				PostgresDataType.BigInt => " bigint ",
				PostgresDataType.Int => " integer ",
				PostgresDataType.DoublePrecision => " double precision ",
				PostgresDataType.Real => " real ",
				PostgresDataType.SmallInt => " smallint ",
				PostgresDataType.Timestamp => " timestamp ",
				PostgresDataType.Varchar => $" varchar({columnInfo.Size}) ",
				PostgresDataType.Boolean => " boolean ",

				_ => throw new Exception($"The property '{propertyInfo.Name}' has no data type defined for generating the column SQL")
			};

			var nullable = string.Empty;
			if (columnInfo.DataType != PostgresDataType.BigSerial && columnInfo.DataType != PostgresDataType.Serial)
				nullable = columnInfo.IsNullable ? " null " : " not null ";

			return $"{columnName} {dataType} {nullable}";
		}
	}
}
