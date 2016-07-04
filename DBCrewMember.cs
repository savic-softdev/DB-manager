using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;

namespace Project.DatabaseManager
{
	public class DBCrewMember : DBItemBase
	{
		#region Fields

		public static string TableName = "CrewMember";
		public static List<CrewMemberColumn> Keys = new List<CrewMemberColumn>() { CrewMemberColumn.CrewMemberID };

		#endregion Fields

		public DBCrewMember()
		{
			data = new List<DBColumnItem>();
			data.Add(new DBColumnItem(DbType.Int64, (short)CrewMemberColumn.CrewMemberID, null));
			data.Add(new DBColumnItem(DbType.String, (short)CrewMemberColumn.CrewMemberName, null, 100));
			data.Add(new DBColumnItem(DbType.String, (short)CrewMemberColumn.CrewMemberPhone, null, 255));
			data.Add(new DBColumnItem(DbType.String, (short)CrewMemberColumn.CrewMemberType, null, 100));
			data.Add(new DBColumnItem(DbType.Boolean, (short)CrewMemberColumn.CrewMemberIsLead, null));
		}

		#region Public methods

		public object GetColumnValue(CrewMemberColumn column)
		{
			return base.GetColumnValue((short)column);
		}

		public void SetColumnValue(CrewMemberColumn column, object value)
		{
			base.SetColumnValue((short)column, value);
		}

		#endregion Public methods

		#region Override methods

		public override string GetTableName()
		{
			return TableName;
		}

		public override List<string> GetKeys()
		{
			return Keys.Select(x => ColumnHelper.GetCrewMemberColName(x)).ToList();
		}

		public override List<string> GetColumnNames()
		{
			return Enum.GetValues(typeof(CrewMemberColumn)).Cast<CrewMemberColumn>().Select(x => ColumnHelper.GetCrewMemberColName(x)).ToList();
		}

		public override List<string> GetCaseSensitiveColumnNames()
		{
			return Enum.GetValues(typeof(CrewMemberColumn)).Cast<CrewMemberColumn>().Select(x => ColumnHelper.GetColumnNameCaseSensitive(x)).ToList();
		}

		public override List<string> GetColumnNamesNoKey()
		{
			List<CrewMemberColumn> columnNames = Enum.GetValues(typeof(CrewMemberColumn)).Cast<CrewMemberColumn>().ToList();
			Keys.ForEach(x => columnNames.Remove(x));
			return columnNames.Select(x => ColumnHelper.GetCrewMemberColName(x)).ToList();
		}

		public override List<IDbDataParameter> GetParameterList(IDbCommand command)
		{
			List<IDbDataParameter> ret = new List<IDbDataParameter>();
			foreach (DBColumnItem item in data)
			{
				if (item.DataType == DbType.DateTime)
				{
					ret.Add(CreateDateTimeParameter(command, GetParamName(ColumnHelper.GetCrewMemberColName((CrewMemberColumn)item.Column)), item.Value));
				}
				else
				{
					ret.Add(CreateParameter(command, GetParamName(ColumnHelper.GetCrewMemberColName((CrewMemberColumn)item.Column)), item.DataType, item.Value));
				}
			}

			return ret;
		}

		#endregion Override methods
	}
}