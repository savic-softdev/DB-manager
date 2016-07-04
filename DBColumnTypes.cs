using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Linq;
using System.Reflection;
using System.Text;

namespace Project.DatabaseManager
{
	public enum CrewMemberColumn : short
	{
		[DescriptionAttribute("CrewMemberID")]
		CrewMemberID,
		[DescriptionAttribute("CrewMemberName")]
		CrewMemberName,
		[DescriptionAttribute("CrewMemberPhone")]
		CrewMemberPhone,
		[DescriptionAttribute("CrewMemberType")]
		CrewMemberType,
		[DescriptionAttribute("CrewMemberIsLead")]
		CrewMemberIsLead
	}

	public class ColumnHelper
	{
		public static string GetCrewMemberColName(CrewMemberColumn column)
		{
			return column.ToString().ToLower();
		}
	};
}
