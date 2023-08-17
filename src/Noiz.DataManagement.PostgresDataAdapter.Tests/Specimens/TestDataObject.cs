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

		[PostgresColumn(PostgresDataType.Varchar, Size = 50, Constraint = PostgresConstraint.Unique)]
		public string TestDataObjectName { get; set; }

		[PostgresColumn(PostgresDataType.Serial)]
		public int TestDataObjectSerial { get; set; }
	}

	public class TestDataObjectNoAttributes
	{
		public int TestDataObjectId { get; set; }

		public DateTime? TestDataObjectDate { get; set; }

		public double? TestDataObjectValue { get; set; }

		public string TestDataObjectName { get; set; }

		public TestEnumeration ChangeValue { get; set; }
	}

    public enum TestEnumeration
	{
		test1,
		test2,
		test3
	}
}
