using Noiz.DataManagement.PostgresDataAdapter.Definitions;

namespace Noiz.DataManagement.PostgresDataAdapter.Tests.Specimens
{
	public record ProvidedPkTestObject
	{
		[PostgresColumn(PostgresDataType.Int, Constraint =PostgresConstraint.PrimaryKey)]
		public int Id { get; init; }

		[PostgresColumn(PostgresDataType.Int)]
		public int? NumberOfItems { get; init; }
	}
}
