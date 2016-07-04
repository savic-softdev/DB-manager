using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Data;
using System.Data.SqlTypes;
using Project.CustomLogger;
using System.Text.RegularExpressions;

namespace Project.DatabaseManager
{
	/// <summary>
	/// Abstract class for all database items
	/// </summary>
	public abstract class DBItemBase
	{
		protected List<DBColumnItem> data;

		public abstract string GetTableName();
		public abstract List<string> GetKeys();
		public abstract List<string> GetColumnNames();
		public abstract List<string> GetCaseSensitiveColumnNames();
		public abstract List<string> GetColumnNamesNoKey();
		public abstract List<IDbDataParameter> GetParameterList(IDbCommand command);

		protected object GetColumnValue(short column)
		{
			return data.Find(x => x.Column == column).Value;
		}

		protected void SetColumnValue(short column, object value)
		{
			DBColumnItem item = data.Find(x => x.Column == column);
			if ((item.DataType == DbType.String || item.DataType == DbType.StringFixedLength) &&
				item.MaxLength > 0 && (value as string) != null && (value as string).Length > item.MaxLength)
			{
				item.Value = (value as string).Substring(0, item.MaxLength);
			}
			else
			{
				item.Value = value;
			}
		}

		public List<string> GetParameterNames()
		{
			return GetColumnNames().Select(x => GetParamName(x)).ToList();
		}

		public List<string> GetParameterNamesNoKey()
		{
			return GetColumnNamesNoKey().Select(x => GetParamName(x)).ToList();
		}

		public List<string> GetKeysParam()
		{
			return GetKeys().Select(x => GetParamName(x)).ToList();
		}

		protected string GetParamName(string columnName)
		{
			return Regex.Replace(columnName, "[!?@#$%-]", "_");
		}

		protected IDbDataParameter CreateParameter(IDbCommand command, string name, DbType type, object value)
		{
			IDbDataParameter parameter = command.CreateParameter();
			parameter.ParameterName = name;
			parameter.DbType = type;
			parameter.Value = value != null ? value : DBNull.Value;

			return parameter;
		}

		protected IDbDataParameter CreateDateTimeParameter(IDbCommand command, string name, object dateTime)
		{
			try
			{
				IDbDataParameter parameter = command.CreateParameter();
				parameter.ParameterName = name;
				parameter.DbType = System.Data.DbType.DateTime;

				if (dateTime == null)
				{
					parameter.Value = DBNull.Value;
				}
				else
				{
					DateTime dt = ((DateTime)dateTime).ToUniversalTime();

					if (dt < SqlDateTime.MinValue.Value)
						dt = SqlDateTime.MinValue.Value;

					if (dt > SqlDateTime.MaxValue.Value)
						dt = SqlDateTime.MaxValue.Value;

					// SQLServer DateTime type has limited precision
					DateTime roundedDateTime = new DateTime(dt.Year, dt.Month, dt.Day, dt.Hour, dt.Minute, dt.Second, DateTimeKind.Utc);
					parameter.Value = roundedDateTime;
				}

				return parameter;
			}
			catch (Exception ex)
			{
				string errorMessage = String.Format("Warning: Failed to create DateTime parameter. Database Type = SQLClient.");
				CustomLogger.Log(CustomLogger.LogLevel.DebugLog, errorMessage + ex.Message);
				throw new Exception(errorMessage);
			}
		}

		public List<string> GetParametersQueryValue()
		{
			List<string> result = new List<string>();
			foreach (DBColumnItem item in data)
			{
				if (item.DataType == DbType.DateTime)
				{

					if (item.Value == null)
					{
						result.Add("NULL");
					}
					else
					{
						DateTime dt = ((DateTime)item.Value).ToUniversalTime();

						if (dt < SqlDateTime.MinValue.Value)
							dt = SqlDateTime.MinValue.Value;

						if (dt > SqlDateTime.MaxValue.Value)
							dt = SqlDateTime.MaxValue.Value;

						// SQLServer DateTime type has limited precision
						DateTime roundedDateTime = new DateTime(dt.Year, dt.Month, dt.Day, dt.Hour, dt.Minute, dt.Second, DateTimeKind.Utc);
						result.Add("'" + roundedDateTime.ToString("yyyy-MM-dd HH:mm:ss") + "'");
					}
				}
				else if (item.DataType == DbType.DateTime2)
				{
					if (item.Value == null)
					{
						result.Add("NULL");
					}
					else
					{
						result.Add("'" + ((DateTime)item.Value).ToString("yyyy-MM-dd HH:mm:ss") + "'");
					}
				}
				else if (item.DataType == DbType.String || item.DataType == DbType.Boolean)
				{
					result.Add("'" + item.Value.ToString().Replace("'", "''") + "'");
				}
				else
				{
					result.Add(item.Value.ToString());
				}
			}

			return result;
		}

		public List<object> GetDataValues()
		{
			List<object> values = new List<object>();
			for (short i = 0; i < data.Count; i++)
			{
				values.Add(GetColumnValue(i));
			}
			return values;
		}
	}
}
