using System;

namespace Noiz.DataManagement.PostgresDataAdapter.Definitions
{
	[AttributeUsage(AttributeTargets.Property)]
	public class PostgresColumnAttribute: Attribute
	{
		public PostgresDataType DataType { get; init; }

		public bool IsNullable { get; init; }

		public int Size { get; init; }

		public string Name { get; init; }

		public PostgresColumnAttribute(PostgresDataType dataType, bool isNullable = true, int size = 250)
		{
			DataType = dataType;
			IsNullable = isNullable;
		}
	}
}
