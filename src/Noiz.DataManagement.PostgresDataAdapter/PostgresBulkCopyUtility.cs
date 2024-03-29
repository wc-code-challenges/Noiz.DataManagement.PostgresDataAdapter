﻿using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Reflection;
using System.Text;
using Noiz.DataManagement.PostgresDataAdapter.Definitions;
using Npgsql;
using PostgreSQLCopyHelper;
using Dapper;

namespace Noiz.DataManagement.PostgresDataAdapter
{
	public static class PostgresBulkCopyUtility
	{
		/// <summary>
		/// Pascal case  if true = ColumnName otherwise if false column_name.
		/// When true then C# property names and column names match. By default is set to true
		/// </summary>
		public static bool UsePascalCase { get; set; } = true;

		/// <summary>
		/// This method creates a PostgreSQLCopyHelper for any type on all Properties that can be read.
		/// It converts property names from Came or Pascal case to using lower case seperated by undersore _ if UsePascalCase is false
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
		/// It converts property names from Camel or Pascal case to using lower case seperated by undersore _ if UsePascalCase is false
		/// </summary>
		/// <typeparam name="T">The type to generate table DDL statements for</typeparam>
		/// <param name="tableName">The name to use for the table</param>
		/// <param name="drop">whether or not to drop the table</param>
		/// <returns>String representing the table DDL</returns>
		public static string CreatePostgresTable<T>(string tableName, bool drop = true, string primaryKeyColumnNameOrConstraintSql = null, bool isTempTable = false)
		{
			var sql = new StringBuilder();

			if (drop) sql.AppendLine($"Drop Table IF EXISTS {tableName};");

			sql.AppendLine();

			if (isTempTable)
                sql.AppendLine($"CREATE TEMPORARY TABLE {tableName} (");
            else
                sql.AppendLine($"CREATE TABLE {tableName} (");

			var entityType = typeof(T);
			var properties = entityType.GetProperties(BindingFlags.Instance | BindingFlags.Public);
			for (int i = 0; i < properties.Length; i++)
			{
				var propertyInfo = properties[i];
				sql.AppendLine(string.Format("  {0}{1}", GetColumnCreateSql(propertyInfo), i < properties.Length - 1 ? "," : string.Empty));
			}

			sql.AppendLine(");");

			if (!string.IsNullOrWhiteSpace(primaryKeyColumnNameOrConstraintSql))
			{
				if (primaryKeyColumnNameOrConstraintSql.StartsWith("Alter Table", StringComparison.OrdinalIgnoreCase))
					sql.AppendLine(primaryKeyColumnNameOrConstraintSql);
				else
					sql.AppendLine($"ALTER TABLE {tableName} ADD PRIMARY KEY ({primaryKeyColumnNameOrConstraintSql});");
			}

			return sql.ToString();
		}

		/// <summary>
		/// Pre-condition: the target table must exist and must match the type "T" passed.
		/// This will do a bulk insert into a temporary table named temp_[tableName].
		/// Then perform an upsert query from the temporary table into the target table.
		/// Finally it will drop the temporary table.
		/// </summary>
		/// <typeparam name="T">Data type matching the table</typeparam>
		/// <param name="tableName">Table name</param>
		/// <param name="values">Data to be saved</param>
		/// <param name="connection">Valid connection to the database</param>
		/// <param name="constraintColumns">Ordered list of the columns in the unique constraint</param>
		/// <returns>Number of database changes</returns>
		public static int Upsert<T>(string tableName, IEnumerable<T> values, NpgsqlConnection connection, IList<string> constraintColumns)
		{
            var tempTableName = $"temp_{tableName}";
            if (connection.State != ConnectionState.Open)
                connection.Open();

            try
			{
				var createTempTableSql = CreatePostgresTable<T>(tempTableName, isTempTable: true);
				connection.ExecuteScalar(createTempTableSql);

				var mapping = GetPostgreSQLCopyHelper<T>(tempTableName);
				var savedEntities = mapping.SaveAll(connection, values);

				var upsertQuerySql = GenerateUpsertQuery<T>(tableName, tempTableName, constraintColumns);
				var upsertResult = connection.Execute(upsertQuerySql);

				return upsertResult;
			}
			finally
			{
                connection.ExecuteScalar($"Drop Table {tempTableName};");
                connection.Close();
            }
		}

		internal static string GenerateUpsertQuery<T>(string targetTable, string tempTable, IList<string> constraintColumns)
		{
			List<string> updateColumnList = new();
			var dataColumns = typeof(T).GetProperties(BindingFlags.Instance | BindingFlags.Public);
			foreach (var propertyInfo in dataColumns)
			{
                var columnInfo = propertyInfo.GetCustomAttributes(typeof(PostgresColumnAttribute)).Cast<PostgresColumnAttribute>().FirstOrDefault();
                if (columnInfo != null)
				{
					if (columnInfo.DataType == PostgresDataType.BigSerial || columnInfo.DataType == PostgresDataType.Serial)
						continue;

					var columnName = columnInfo?.Name ?? GetColumnNameFromPascalCaseOrCamelCasePropertyName(propertyInfo.Name);
					updateColumnList.Add(columnName);
				}
				else
				{
					var columnName = GetColumnNameFromPascalCaseOrCamelCasePropertyName(propertyInfo.Name);
					updateColumnList.Add(columnName);
                }
            }
            var colummnCsv = string.Join(',', updateColumnList);
            var updateColumnSql = string.Join(',', updateColumnList
				.Where(x => !constraintColumns.Contains(x))
				.Select(x => $"{x} = EXCLUDED.{x}"));

            return $@"INSERT INTO 
							{targetTable} ({colummnCsv}) 
							select {colummnCsv} 
							from {tempTable}
							on conflict ({string.Join(',', constraintColumns)}) do 
							update set {updateColumnSql};";
        }

		internal static PostgreSQLCopyHelper<T> MapProperty<T>(this PostgreSQLCopyHelper<T> postgreSQLCopyHelper, PropertyInfo propertyInfo)
		{
			var dataType = propertyInfo.GetCustomAttribute<PostgresColumnAttribute>()?.DataType;
			if (dataType == PostgresDataType.BigSerial || dataType == PostgresDataType.Serial)//serial is not inserted
				return postgreSQLCopyHelper;

			if (propertyInfo.PropertyType.IsEnum)
			{
				return postgreSQLCopyHelper.MapVarchar(GetColumnNameFromPascalCaseOrCamelCasePropertyName(propertyInfo.Name), x => propertyInfo.GetValue(x)?.ToString());
			}

			return (propertyInfo.PropertyType.Name.ToLower()
				, propertyInfo.PropertyType.IsGenericType
				, propertyInfo.PropertyType.GenericTypeArguments.FirstOrDefault()?.Name?.ToLower() ?? propertyInfo.PropertyType.Name.ToLower()) switch
			{
				("string", _, _) => postgreSQLCopyHelper.MapVarchar(GetColumnNameFromPascalCaseOrCamelCasePropertyName(propertyInfo.Name)
					, x => (string)propertyInfo.GetValue(x)),
				("char", false, _) => postgreSQLCopyHelper.MapCharacter(GetColumnNameFromPascalCaseOrCamelCasePropertyName(propertyInfo.Name)
					, x => propertyInfo.GetValue(x).ToString()),
				(_, true, "char") => postgreSQLCopyHelper.MapCharacter(GetColumnNameFromPascalCaseOrCamelCasePropertyName(propertyInfo.Name)
					, x => propertyInfo.GetValue(x)?.ToString()),
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
			=> UsePascalCase ? propertyName : System.Text.RegularExpressions.Regex.Replace(propertyName, "(\\B[A-Z])", "_$1").ToLower();

		internal static string GetColumnCreateSql(PropertyInfo propertyInfo)
		{
			var columnInfo = propertyInfo.GetCustomAttributes(typeof(PostgresColumnAttribute)).Cast<PostgresColumnAttribute>().FirstOrDefault();
			if (columnInfo == null)
				return GenerateColumnSqlFromPropertyInfo(propertyInfo);

			return GenerateColumnSqlFromAttribute(columnInfo, propertyInfo);
		}

		private static string GenerateColumnSqlFromAttribute(PostgresColumnAttribute columnInfo, PropertyInfo propertyInfo)
		{
			var columnName = columnInfo?.Name ?? GetColumnNameFromPascalCaseOrCamelCasePropertyName(propertyInfo.Name);

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
				PostgresDataType.Uuid => "Uuid",

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

		private static string GenerateColumnSqlFromPropertyInfo(PropertyInfo propertyInfo, int defaultSize = 245)
		{
			string nullStatus_null = "NULL",  nullStatus_not_null = "NOT NULL";
			return (propertyInfo.PropertyType.Name.ToLower()
				, propertyInfo.PropertyType.IsGenericType
				, propertyInfo.PropertyType.GenericTypeArguments.FirstOrDefault()?.Name?.ToLower() ?? propertyInfo.PropertyType.Name.ToLower()
				, propertyInfo.PropertyType.IsEnum) switch
			{
				(_, true, _, true) => $"{GetColumnNameFromPascalCaseOrCamelCasePropertyName(propertyInfo.Name)} varchar({defaultSize}) {nullStatus_null}",
				(_, false, _, true) => $"{GetColumnNameFromPascalCaseOrCamelCasePropertyName(propertyInfo.Name)} varchar({defaultSize}) {nullStatus_not_null}",
				("string", _, _, _) => $"{GetColumnNameFromPascalCaseOrCamelCasePropertyName(propertyInfo.Name)} varchar({defaultSize}) {nullStatus_null}",
				("char", false, _, _) => $"{GetColumnNameFromPascalCaseOrCamelCasePropertyName(propertyInfo.Name)} varchar({1}) {nullStatus_not_null}",
				(_, true, "char", _) => $"{GetColumnNameFromPascalCaseOrCamelCasePropertyName(propertyInfo.Name)} varchar({1}) {nullStatus_null}",
				("datetime", false, _, _) => $"{GetColumnNameFromPascalCaseOrCamelCasePropertyName(propertyInfo.Name)} timestamp {nullStatus_not_null}",
				(_, true, "datetime", _) => $"{GetColumnNameFromPascalCaseOrCamelCasePropertyName(propertyInfo.Name)} timestamp {nullStatus_null}",
				("double", false, _, _) => $"{GetColumnNameFromPascalCaseOrCamelCasePropertyName(propertyInfo.Name)} double precision {nullStatus_not_null}",
				(_, true, "double", _) => $"{GetColumnNameFromPascalCaseOrCamelCasePropertyName(propertyInfo.Name)} double precision {nullStatus_null}",
				(_, true, "int32", _) => $"{GetColumnNameFromPascalCaseOrCamelCasePropertyName(propertyInfo.Name)} integer {nullStatus_null}",
				("int32", false, _, _) => $"{GetColumnNameFromPascalCaseOrCamelCasePropertyName(propertyInfo.Name)} integer {nullStatus_not_null}",
				("int64", false, _, _) => $"{GetColumnNameFromPascalCaseOrCamelCasePropertyName(propertyInfo.Name)} bigint {nullStatus_not_null}",
				(_, true, "int64", _) => $"{GetColumnNameFromPascalCaseOrCamelCasePropertyName(propertyInfo.Name)} bigint {nullStatus_null}",
				("decimal", false, _, _) => $"{GetColumnNameFromPascalCaseOrCamelCasePropertyName(propertyInfo.Name)} numeric(18,9) {nullStatus_not_null}",
				(_, true, "decimal", _) => $"{GetColumnNameFromPascalCaseOrCamelCasePropertyName(propertyInfo.Name)} numeric(18,9) {nullStatus_null}",
				("boolean", false, _, _) => $"{GetColumnNameFromPascalCaseOrCamelCasePropertyName(propertyInfo.Name)} boolean {nullStatus_not_null}",
				(_, true, "boolean", _) => $"{GetColumnNameFromPascalCaseOrCamelCasePropertyName(propertyInfo.Name)} boolean {nullStatus_null}",
				("guid", false, _, _) => $"{GetColumnNameFromPascalCaseOrCamelCasePropertyName(propertyInfo.Name)} Uuid {nullStatus_not_null}",
				(_, true, "guid", _) => $"{GetColumnNameFromPascalCaseOrCamelCasePropertyName(propertyInfo.Name)} Uuid {nullStatus_null}",
				_ => throw new NotImplementedException($"Error on property '{propertyInfo.Name}' The type conversion to postgres for .NET type {propertyInfo.PropertyType.FullName} is not implemented in this library implemented types are common value types [int, long, double, decimal, char, string, datetime].")
			};
		}
	}
}
