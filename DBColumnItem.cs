using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Data;

namespace Project.DatabaseManager
{
	public class DBColumnItem
	{
		private DbType dataType;
		private short column;
		private short maxLength;
		private object value;

		public DBColumnItem(DbType dataType, short column, object value, short maxLength = 0)
		{
			this.dataType = dataType;
			this.column = column;
			this.maxLength = maxLength;
			this.value = value;
		}

		public object Value
		{
			get { return value; }
			set { this.value = value; }
		}

		public DbType DataType
		{
			get { return dataType; }
			set { this.dataType = value; }
		}

		public short Column
		{
			get { return column; }
			set { this.column = value; }
		}

		public short MaxLength
		{
			get { return maxLength; }
			set { this.maxLength = value; }
		}
	}
}
