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
		/// <returns>String representing the table DDL</returns>
		public static string CreatePostgresTable<T>(string tableName, bool drop = true)
		{
			var sql = new StringBuilder();

			if (drop) sql.AppendLine($"Drop Table IF EXISTS {tableName}; VACUUM;");

			sql.AppendLine();

			sql.AppendLine($"CREATE TABLE {tableName} (");

			var entityType = typeof(T);
			var properties = entityType.GetProperties(BindingFlags.Instance | BindingFlags.Public);
			for (int i = 0; i < properties.Length; i++)
			{
				var propertyInfo = properties[i];
				sql.AppendLine(string.Format("  {0}{1}", GetColumnCreateSql(propertyInfo), i < properties.Length - 1 ? "," : string.Empty));
			}

			sql.AppendLine(");");

			return sql.ToString();
		}

		internal static PostgreSQLCopyHelper<T> MapProperty<T>(this PostgreSQLCopyHelper<T> postgreSQLCopyHelper, PropertyInfo propertyInfo)
		{
			var dataType = propertyInfo.GetCustomAttribute<PostgresColumnAttribute>()?.DataType;
			if (dataType == PostgresDataType.BigSerial || dataType == PostgresDataType.Serial)//serial is not inserted
				return postgreSQLCopyHelper;

			return (propertyInfo.PropertyType.Name.ToLower()
				, propertyInfo.PropertyType.IsGenericType
				, propertyInfo.PropertyType.GenericTypeArguments.FirstOrDefault()?.Name?.ToLower() ?? propertyInfo.PropertyType.Name.ToLower()) switch
			{
				("string", _, _) => postgreSQLCopyHelper.MapVarchar(GetColumnNameFromPascalCaseOrCamelCasePropertyName(propertyInfo.Name)
					, x => (string)propertyInfo.GetValue(x)),
				("char", false, _) => postgreSQLCopyHelper.MapCharacter(GetColumnNameFromPascalCaseOrCamelCasePropertyName(propertyInfo.Name)
					, x => propertyInfo.GetValue(x).ToString()),
				(_, true, "char") => postgreSQLCopyHelper.MapCharacter(GetColumnNameFromPascalCaseOrCamelCasePropertyName(propertyInfo.Name)
					, x => propertyInfo.GetValue(x).ToString()),
				("datetime", _, _) => postgreSQLCopyHelper.MapTimeStamp(GetColumnNameFromPascalCaseOrCamelCasePropertyName(propertyInfo.Name)
					, x =>  (DateTime)propertyInfo.GetValue(x)),
				(_, true, "datetime") => postgreSQLCopyHelper.MapTimeStamp(GetColumnNameFromPascalCaseOrCamelCasePropertyName(propertyInfo.Name)
					, x => (DateTime?)propertyInfo.GetValue(x)),
				("double", _, _) => postgreSQLCopyHelper.MapDouble(GetColumnNameFromPascalCaseOrCamelCasePropertyName(propertyInfo.Name)
					, x => (double)propertyInfo.GetValue(x)),
				(_, true, "double") => postgreSQLCopyHelper.MapDouble(GetColumnNameFromPascalCaseOrCamelCasePropertyName(propertyInfo.Name)
					, x => (double?)propertyInfo.GetValue(x)),
				(_, true, "int32") => postgreSQLCopyHelper.MapInteger(GetColumnNameFromPascalCaseOrCamelCasePropertyName(propertyInfo.Name)
					, x => (int?)propertyInfo.GetValue(x)),
				("int32", _, _) => postgreSQLCopyHelper.MapInteger(GetColumnNameFromPascalCaseOrCamelCasePropertyName(propertyInfo.Name)
				, x => (int)propertyInfo.GetValue(x)),
				("int64", _, _) => postgreSQLCopyHelper.MapBigInt(GetColumnNameFromPascalCaseOrCamelCasePropertyName(propertyInfo.Name)
					, x => (long)propertyInfo.GetValue(x)),
				(_, true, "int64") => postgreSQLCopyHelper.MapBigInt(GetColumnNameFromPascalCaseOrCamelCasePropertyName(propertyInfo.Name)
					, x => (long?)propertyInfo.GetValue(x)),
				("decimal", _, _) => postgreSQLCopyHelper.MapNumeric(GetColumnNameFromPascalCaseOrCamelCasePropertyName(propertyInfo.Name)
					, x => (decimal)propertyInfo.GetValue(x)),
				(_, true, "decimal") => postgreSQLCopyHelper.MapNumeric(GetColumnNameFromPascalCaseOrCamelCasePropertyName(propertyInfo.Name)
					, x => (decimal?)propertyInfo.GetValue(x)),
				("boolean", _, _) => postgreSQLCopyHelper.MapBoolean(GetColumnNameFromPascalCaseOrCamelCasePropertyName(propertyInfo.Name)
					, x => (bool)propertyInfo.GetValue(x)),
				(_, true, "boolean") => postgreSQLCopyHelper.MapBoolean(GetColumnNameFromPascalCaseOrCamelCasePropertyName(propertyInfo.Name)
					, x => (bool?)propertyInfo.GetValue(x)),
				_ => throw new NotImplementedException($"Error on property '{propertyInfo.Name}' The type conversion to postgres for .NET type {propertyInfo.PropertyType.FullName} is not implemented in this library implemented types are common value types [int, long, double, decimal, char, string, datetime]."),
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
				PostgresDataType.Serial => "serial",
				PostgresDataType.BigSerial => "bigserial ",
				PostgresDataType.BigInt => "bigint",
				PostgresDataType.Int => "integer",
				PostgresDataType.DoublePrecision => "double precision",
				PostgresDataType.Real => "real",
				PostgresDataType.SmallInt => "smallint",
				PostgresDataType.Timestamp => "timestamp",
				PostgresDataType.Varchar => $"varchar({columnInfo.Size})",
				PostgresDataType.Boolean => "boolean",

				_ => throw new Exception($"The property '{propertyInfo.Name}' has no data type defined for generating the column SQL")
			};

			var constraints = (columnInfo.DataType, columnInfo.Constraint) switch
			{
				(PostgresDataType.BigSerial, _) => "PRIMARY KEY",
				(PostgresDataType.Serial, _) => "PRIMARY KEY",
				(_, PostgresConstraint.PrimaryKey) => "PRIMARY KEY",
				(_, PostgresConstraint.Unique) => "UNIQUE",
				(_, PostgresConstraint.None) => string.Empty,
				_ => string.Empty,
			};
			

			var nullable = string.Empty;
			if (columnInfo.DataType != PostgresDataType.BigSerial && columnInfo.DataType != PostgresDataType.Serial
					&& columnInfo.Constraint != PostgresConstraint.PrimaryKey)
				nullable = columnInfo.IsNullable ? "null" : "not null";

			return $"{columnName} {dataType} {constraints} {nullable}";
		}
	}
}
