using System;
using Noiz.DataManagement.PostgresDataAdapter.Definitions;

namespace Noiz.DataManagement.PostgresDataAdapter.Tests.Specimens
{
	public class TestDataObject
	{
		[PostgresColumn(PostgresDataType.Int)]
		public int TestDataObjectId { get; set; }

		[PostgresColumn(PostgresDataType.Timestamp)]
		public DateTime? TestDataObjectDate { get; set; }

		[PostgresColumn(PostgresDataType.DoublePrecision)]
		public double? TestDataObjectValue { get; set; }

		[PostgresColumn(PostgresDataType.Varchar, Size = 50)]
		public string TestDataObjectName { get; set; }

		[PostgresColumn(PostgresDataType.Serial)]
		public int TestDataObjectSerial { get; set; }
	}
}
