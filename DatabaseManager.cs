using System;
using System.Data;
using System.Data.SqlClient;
using System.Text;
using System.Transactions;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Reflection;

namespace Project.DatabaseManager
{
	public class DatabaseManager
	{
		private string connectionString;
		private bool isDBInitialized = false;
		private const string logPrefix = "[Project] DatabaseManager:";

		/// <summary>
		/// Initializes a new instance of the DatabaseManager class with connection string.
		/// </summary>
		/// <param name="connectionString">Connection string</param>
		public DatabaseManager(string connectionString)
		{
			if (!string.IsNullOrEmpty(connectionString))
			{
				CustomLogger.Log(CustomLogger.LogLevel.DebugLog, string.Format("{0} Initializing database connection with connection string: {1}", logPrefix, connectionString));
				this.connectionString = connectionString;
				InitializeBasicData();
			}
		}

		/// <summary>
		/// Initialize basic data
		/// </summary>
		public void InitializeBasicData()
		{
			try
			{
				using (IDbConnection connection = CreateConnection())
				{
					connection.Open();
					string query = @"SELECT 'Server Collation', SERVERPROPERTY('Collation')
									UNION
									SELECT 'Computer BIOS Name', SERVERPROPERTY('ComputerNamePhysicalNetBIOS')
									UNION 
									SELECT 'Server Edition', SERVERPROPERTY('Edition')
									UNION 
									SELECT 'Instance Name', SERVERPROPERTY('InstanceName')
									UNION 
									SELECT 'Server Name', SERVERPROPERTY('ServerName')
									UNION 
									SELECT 'Current Schema', SCHEMA_NAME()
									UNION 
									SELECT 'Current Database', DB_NAME()";

					using (IDbCommand command = CreateCommand(query, connection))
					{
						IDataReader rdr = command.ExecuteReader();
					}

					CustomLogger.Log(CustomLogger.LogLevel.DebugLog, string.Format("{0} Getting SQL Server Info succeeded.", logPrefix));
					connection.Close();

					isDBInitialized = true;
				}
			}
			catch (SqlException sqlex)
			{
				string message = string.Format("Failed to get SQL Server Info. {0}\tServer: {1}{0}\tError Number: {2}, Severity: {3}, State: {4}, Message: {5}{0}\tSource: {6}", Environment.NewLine, sqlex.Server, sqlex.Number, sqlex.Class, sqlex.State, sqlex.Message, sqlex.Source);
				CustomLogger.Log(CustomLogger.LogLevel.Error, string.Format("{0} {1}", logPrefix, message));
			}
			catch (Exception ex)
			{
				string message = string.Format("{0} Failed to get SQL Server Info.", logPrefix);
				CustomLogger.Log(CustomLogger.LogLevel.Error, message);
				CustomLogger.Log(CustomLogger.LogLevel.Error, ex.Message);
			}
		}

		/// <summary>
		/// Create connection to DB
		/// </summary>
		private IDbConnection CreateConnection()
		{
			try
			{
				return new SqlConnection(this.connectionString);
			}
			catch (Exception ex)
			{
				string errorMessage = "Failed to create connection.";
				CustomLogger.Log(CustomLogger.LogLevel.Error, string.Format("{0} {1} Error: {2}", logPrefix, errorMessage, ex.Message));
				throw new DBModelException(ErrorCode.NoDBConnection, errorMessage);
			}
		}

		/// <summary>
		/// Create command
		/// </summary>
		private IDbCommand CreateCommand(string commandText, IDbConnection connection)
		{
			try
			{
				return new SqlCommand(commandText, (SqlConnection)connection) { CommandTimeout = 300 };
			}
			catch (Exception ex)
			{
				CustomLogger.Log(CustomLogger.LogLevel.Error, string.Format("{0} Failed to create command. Message: {1}", logPrefix, ex.Message));
				throw;
			}
		}

		/// <summary>
		/// Check if DB is initialized
		/// </summary>
		public bool IsDBInitialized
		{
			get { return isDBInitialized; }
			set { isDBInitialized = value; }
		}

		/// <summary>
		/// Save multiple data to database
		/// </summary>
		/// <param name="dbItems"></param>
		/// <param name="insertNew"></param>
		public void SaveToDatabase(List<DBItemBase> dbItems, bool insertNew)
		{
			if (dbItems == null || dbItems.Count == 0)
				return;

			try
			{
				CustomLogger.Log(CustomLogger.LogLevel.Verbose, "{0} Saving {1} custom data. Number of items: {2}", logPrefix, dbItems.First().GetTableName(), dbItems.Count);
				using (IDbConnection connection = CreateConnection())
				{
					connection.Open();
					IDbTransaction transaction = null;
					using (Transaction.Current == null ? transaction = connection.BeginTransaction() : transaction = null)
					{
						foreach (DBItemBase item in dbItems)
						{
							SaveDatabaseItem(item, connection, transaction, insertNew);
						}

						try
						{
							if (transaction != null)
							{
								transaction.Commit();
							}
						}
						catch (Exception)
						{
							transaction.Rollback();
							throw;
						}
					}
				}
			}
			catch (Exception ex)
			{
				CustomLogger.Log(CustomLogger.LogLevel.Error, string.Format("{0} Failed to save custom data to database. Message: {1}", logPrefix, ex.Message));
				throw;
			}
		}

		/// <summary>
		/// Save data to database
		/// </summary>
		/// <param name="dbItem"></param>
		/// <param name="insertNew"></param>
		public void SaveToDatabase(DBItemBase dbItem, bool insertNew)
		{
			if (dbItem == null)
				return;

			try
			{
				CustomLogger.Log(CustomLogger.LogLevel.Verbose, string.Format("{0} Saving {1} custom data.", logPrefix, dbItem.GetTableName()));
				using (IDbConnection connection = CreateConnection())
				{
					connection.Open();
					IDbTransaction transaction = null;
					using (Transaction.Current == null ? transaction = connection.BeginTransaction() : transaction = null)
					{
						SaveDatabaseItem(dbItem, connection, transaction, insertNew);

						try
						{
							if (transaction != null)
							{
								transaction.Commit();
							}
						}
						catch (Exception)
						{
							transaction.Rollback();
							throw;
						}
					}
				}
			}
			catch (Exception ex)
			{
				CustomLogger.Log(CustomLogger.LogLevel.Error, string.Format("{0} Failed to save custom data to database. Message: {1}", logPrefix, ex.Message));
				throw;
			}
		}

		/// <summary>
		/// Insert or update database item
		/// </summary>
		/// <param name="dbItem"></param>
		/// <param name="connection"></param>
		/// <param name="transaction"></param>
		/// <param name="insertNew"></param>
		private void SaveDatabaseItem(DBItemBase dbItem, IDbConnection connection, IDbTransaction transaction, bool insertNew)
		{
			CustomLogger.Log(CustomLogger.LogLevel.Info, "{0} {1} is called from {2} thread with {3} action.",
														logPrefix,
														MethodInfo.GetCurrentMethod().Name,
														Thread.CurrentThread.Name,
														insertNew ? "Insert" : "Update");
			try
			{
				string query;
				if (insertNew)
				{
					CustomLogger.Log(CustomLogger.LogLevel.Info, "{0} Insert new database item from Thread: {1}", logPrefix, Thread.CurrentThread.Name);

					List<string> columnNames = dbItem.GetColumnNames().Select(x => string.Format("[{0}]", x)).ToList();
					List<string> columnNamesParam = dbItem.GetParameterNames().Select(x => string.Format("@{0}", x)).ToList();
					query = string.Format(@"INSERT INTO {0} ( {1} ) VALUES ( {2} )",
						dbItem.GetTableName(),
						string.Join(", ", columnNames),
						string.Join(", ", columnNamesParam));
				}
				else
				{
					CustomLogger.Log(CustomLogger.LogLevel.Info, "{0} Update database item from Thread: {1}", logPrefix, Thread.CurrentThread.Name);

					List<string> columnNamesNoKey = dbItem.GetColumnNamesNoKey();
					List<string> columnNamesNoKeyParam = dbItem.GetParameterNamesNoKey();
					for (int i = 0; i < columnNamesNoKey.Count; i++)
					{
						columnNamesNoKey[i] = string.Format("[{0}] = @{1}", columnNamesNoKey[i], columnNamesNoKeyParam[i]);
					}
					List<string> keys = dbItem.GetKeys();
					List<string> keysParam = dbItem.GetKeysParam();
					for (int i = 0; i < keys.Count; i++)
					{
						keys[i] = string.Format("[{0}] = @{1}", keys[i], keysParam[i]);
					}

					query = string.Format(@"UPDATE {0} SET {1} WHERE {2}",
						dbItem.GetTableName(),
						string.Join(", ", columnNamesNoKey),
						string.Join(" AND ", keys));
				}

				using (IDbCommand commandQuery = CreateCommand(query, connection))
				{
					commandQuery.Transaction = transaction;

					List<IDbDataParameter> commandParams = dbItem.GetParameterList(commandQuery);
					commandParams.ForEach(x => commandQuery.Parameters.Add(x));

					int affected = commandQuery.ExecuteNonQuery();
					CustomLogger.Log(CustomLogger.LogLevel.Verbose, "{0} Executed SQL query: {1}", logPrefix, commandQuery.CommandText);
					CustomLogger.Log(CustomLogger.LogLevel.Info, string.Format("{0} Rows affected in database: {1}.", logPrefix, affected));
				}
				CustomLogger.Log(CustomLogger.LogLevel.Info, string.Format("{0} Thread finished SaveDatabaseItem method: {1}", logPrefix, Thread.CurrentThread.Name));
			}
			catch (Exception ex)
			{
				CustomLogger.Log(CustomLogger.LogLevel.Error, string.Format("{0} Failed to {1} item in database. Message: {2}", logPrefix, insertNew ? "insert" : "update", ex.Message));
				throw;
			}
		}

		/// <summary>
		/// Insert multi rows in one table
		/// </summary>
		/// <param name="dbItem"></param>
		/// <param name="connection"></param>
		/// <param name="transaction"></param>
		/// <param name="insertNew"></param>
		private void InsertDatabaseItems(List<DBItemBase> dbItems, IDbConnection connection, IDbTransaction transaction)
		{
			CustomLogger.Log(CustomLogger.LogLevel.Info, "{0} {1} is called from {2} thread with Insert action.",
														logPrefix,
														MethodInfo.GetCurrentMethod().Name,
														Thread.CurrentThread.Name);
			if (dbItems == null || dbItems.Count == 0)
				return;
			try
			{
				CustomLogger.Log(CustomLogger.LogLevel.Info, "{0} Insert new database items from Thread: {1}", logPrefix, Thread.CurrentThread.Name);

				DBItemBase header = dbItems.First();
				List<string> columnNames = header.GetCaseSensitiveColumnNames();
				string tableName = header.GetTableName();

				DataTable table = new DataTable(tableName);
				columnNames.ForEach(p => table.Columns.Add(p));

				for (int i = 0; i < dbItems.Count; i++)
				{
					List<object> recordValues = dbItems[i].GetDataValues();
					DataRow row = table.NewRow();
					for (int j = 0; j < columnNames.Count && j < recordValues.Count; j++)
					{
						row[columnNames[j]] = recordValues[j] ?? DBNull.Value;
					}
					table.Rows.Add(row);
				}

				using (var bulkCopy = new SqlBulkCopy((connection as SqlConnection), SqlBulkCopyOptions.Default, transaction as SqlTransaction))
				{
					bulkCopy.DestinationTableName = tableName;
					columnNames.ForEach(p => bulkCopy.ColumnMappings.Add(p, p));

					try
					{
						bulkCopy.WriteToServer(table);
					}
					catch (Exception ex)
					{
						string message = string.Format("Error occured while executing bulk copy to table: {0}. Message: {1}", bulkCopy.DestinationTableName, ex.Message);
						CustomLogger.Log(CustomLogger.LogLevel.Error, message);
						//throw new Exception(message + " " + ex.Message);
					}
				}

				CustomLogger.Log(CustomLogger.LogLevel.Info, string.Format("{0} Thread finished SaveDatabaseItem method: {1}", logPrefix, Thread.CurrentThread.Name));
			}
			catch (Exception ex)
			{
				string message = string.Format("Failed to insert item in database.");
				CustomLogger.Log(CustomLogger.LogLevel.Error, string.Format("{0} {1}", logPrefix, message));
				CustomLogger.Log(CustomLogger.LogLevel.Error, string.Format("{0} {1}", logPrefix, ex.Message));
				throw new Exception(message + " " + ex.Message);
			}
		}

		/// <summary>
		/// Remove items from database
		/// </summary>
		/// <param name="dbItems"></param>
		public void RemoveFromDatabase(List<DBItemBase> dbItems)
		{
			if (dbItems == null || dbItems.Count == 0)
				return;

			try
			{
				CustomLogger.Log(CustomLogger.LogLevel.Verbose, string.Format("{0} Removing {1} custom data. Number of items: {2}", logPrefix, dbItems.First().GetTableName(), dbItems.Count));
				using (IDbConnection connection = CreateConnection())
				{
					connection.Open();
					IDbTransaction transaction = null;
					using (Transaction.Current == null ? transaction = connection.BeginTransaction() : transaction = null)
					{
						foreach (DBItemBase item in dbItems)
						{
							RemoveDatabaseItem(item, connection, transaction);
						}

						try
						{
							if (transaction != null)
							{
								transaction.Commit();
							}
							else
							{
								CustomLogger.Log(CustomLogger.LogLevel.DebugLog, "{0} Skipping Commit operation for remove. Transaction is null.", logPrefix);
							}
						}
						catch (Exception)
						{
							transaction.Rollback();
							throw;
						}
					}
				}
			}
			catch (Exception)
			{
				CustomLogger.Log(CustomLogger.LogLevel.Error, string.Format("{0} Failed to remove custom data from database.", logPrefix));
				throw;
			}
		}

		/// <summary>
		/// Remove item from database
		/// </summary>
		/// <param name="dbItem"></param>
		public void RemoveFromDatabase(DBItemBase dbItem)
		{
			if (dbItem == null)
				return;

			try
			{
				CustomLogger.Log(CustomLogger.LogLevel.Verbose, string.Format("{0} Removing {1} custom data.", logPrefix, dbItem.GetTableName()));
				using (IDbConnection connection = CreateConnection())
				{
					connection.Open();
					IDbTransaction transaction = null;
					using (Transaction.Current == null ? transaction = connection.BeginTransaction() : transaction = null)
					{
						RemoveDatabaseItem(dbItem, connection, transaction);

						try
						{
							if (transaction != null)
							{
								transaction.Commit();
							}
							else
							{
								CustomLogger.Log(CustomLogger.LogLevel.DebugLog, "{0} Skipping Commit operation for remove. Transaction is null.", logPrefix);
							}
						}
						catch (Exception)
						{
							transaction.Rollback();
							throw;
						}
					}
				}
			}
			catch (Exception ex)
			{
				CustomLogger.Log(CustomLogger.LogLevel.Error, string.Format("{0} Failed to remove custom data from database. Message: {1}", logPrefix, ex.Message));
				throw;
			}
		}

		/// <summary>
		/// Remove database item
		/// </summary>
		/// <param name="dbItem"></param>
		/// <param name="connection"></param>
		/// <param name="transaction"></param>
		private void RemoveDatabaseItem(DBItemBase dbItem, IDbConnection connection, IDbTransaction transaction)
		{
			try
			{
				List<string> keys = dbItem.GetKeys();
				List<string> keysParam = dbItem.GetKeysParam();
				for (int i = 0; i < keys.Count; i++)
				{
					keys[i] = string.Format("[{0}] = @{1}", keys[i], keysParam[i]);
				}
				string query = string.Format(@"DELETE FROM {0} WHERE {1}",
					dbItem.GetTableName(),
					string.Join(" AND ", keys));

				using (IDbCommand commandQuery = CreateCommand(query, connection))
				{
					commandQuery.Transaction = transaction;

					List<IDbDataParameter> commandParams = dbItem.GetParameterList(commandQuery);
					commandParams.ForEach(x => commandQuery.Parameters.Add(x));

					int affected = commandQuery.ExecuteNonQuery();
					
					CustomLogger.Log(CustomLogger.LogLevel.Verbose, "{0} Executed SQL query: {1}", logPrefix, commandQuery.CommandText);
					CustomLogger.Log(CustomLogger.LogLevel.Info, string.Format("{0} Rows affected in database: {1}.", logPrefix, affected));
				}
			}
			catch (Exception ex)
			{
				CustomLogger.Log(CustomLogger.LogLevel.Error, string.Format("{0} Failed to delete item in database. Message: ", logPrefix, ex.Message));
				throw;
			}
		}

		/// <summary>
		/// Truncate database table
		/// </summary>
		/// <param name="tableName"></param>
		/// <param name="connection"></param>
		/// <param name="transaction"></param>
		public void EmptyTables(List<string> tableNames)
		{
			if (tableNames == null || tableNames.Count == 0)
				return;

			string strTables = string.Join(", ", tableNames);
			try
			{
				CustomLogger.Log(CustomLogger.LogLevel.Verbose, string.Format("{0} Removing data from tables: {1}.", logPrefix, strTables));
				using (IDbConnection connection = CreateConnection())
				{
					connection.Open();
					IDbTransaction transaction = null;
					using (Transaction.Current == null ? transaction = connection.BeginTransaction() : transaction = null)
					{
						foreach (string table in tableNames)
						{
							using (IDbCommand commandQuery = CreateCommand(String.Format("DELETE FROM {0}", table), connection))
							{
								commandQuery.Transaction = transaction;
								commandQuery.ExecuteNonQuery();
							}
						}

						try
						{
							if (transaction != null)
							{
								transaction.Commit();
							}
							else
							{
								CustomLogger.Log(CustomLogger.LogLevel.DebugLog, "{0} Skipping Commit operation for empty tables. Transaction is null.", logPrefix);
							}
						}
						catch (Exception)
						{
							transaction.Rollback();
							throw;
						}
					}
				}
			}
			catch (Exception ex)
			{
				CustomLogger.Log(CustomLogger.LogLevel.Error, string.Format("{0} Failed to remove data from tables: {1}. Message: {2}", logPrefix, strTables, ex.Message));
				throw;
			}
		}

		/// <summary>
		/// Check if database item exist, provide only key
		/// </summary>
		/// <param name="dbItem"></param>
		/// <returns></returns>
		public bool CheckDatabaseItemExist(DBItemBase dbItem)
		{
			string firstKey = dbItem.GetKeys().First();
			bool result = GetDatabaseItemValue(dbItem, firstKey) != null;

			return result;
		}

		/// <summary>
		/// Get database item value
		/// </summary>
		/// <param name="dbItem"></param>
		/// <param name="column"></param>
		/// <returns></returns>
		public object GetDatabaseItemValue(DBItemBase dbItem, string column)
		{
			object ret = null;

			try
			{
				List<string> keys = dbItem.GetKeys();
				List<string> keysParam = dbItem.GetKeysParam();
				for (int i = 0; i < keys.Count; i++)
				{
					keys[i] = string.Format("[{0}] = @{1}", keys[i], keysParam[i]);
				}
				string query = string.Format(@"SELECT * FROM {0} WHERE {1}",
					dbItem.GetTableName(),
					string.Join(" AND ", keys));

				using (IDbConnection connection = CreateConnection())
				{
					connection.Open();
					IDbCommand command = CreateCommand(query, connection);

					List<IDbDataParameter> commandParams = dbItem.GetParameterList(command);
					commandParams.ForEach(x => command.Parameters.Add(x));

					using (IDataReader reader = command.ExecuteReader())
					{
						if (reader.Read())
						{
							ret = reader[column];
						}

						if (ret == null)
							CustomLogger.Log(CustomLogger.LogLevel.Info, "{0} Query: {1}", logPrefix, command.CommandText);

						reader.Close();
					}
				}

				return ret;
			}
			catch (Exception ex)
			{
				CustomLogger.Log(CustomLogger.LogLevel.Error, string.Format("{0} Failed to select item in database table {1}. Message: {2}", logPrefix, dbItem.GetTableName(), ex.Message));
				throw;
			}
		}

		/// <summary>
		/// Get column names for data reader
		/// </summary>
		static List<string> GetDataReaderColumnNames(IDataReader rdr)
		{
			var columnNames = new List<string>();
			for (int i = 0; i < rdr.FieldCount; i++)
			{
				columnNames.Add(string.Format("{0}:{1}", rdr.GetName(i), rdr.GetValue(i)));
			}
			return columnNames;
		}

		/// <summary>
		/// Get database items
		/// </summary>
		/// <param name="table"></param>
		/// <param name="column"></param>
		/// <param name="whereColumn"></param>
		/// <param name="whereValue"></param>
		/// <returns></returns>
		public List<object> GetDatabaseItems(string table, string column, string whereColumn, DbType whereType, object whereValue)
		{
			List<object> ret = new List<object>();

			try
			{
				string query = string.Format(@"SELECT {0} FROM {1} WHERE [{2}] = @{2}", column, table, whereColumn);

				using (IDbConnection connection = CreateConnection())
				{
					connection.Open();
					IDbCommand command = CreateCommand(query, connection);

					IDbDataParameter parameter = command.CreateParameter();
					parameter.ParameterName = whereColumn;
					parameter.DbType = whereType;
					parameter.Value = whereValue != null ? whereValue : DBNull.Value;
					command.Parameters.Add(parameter);

					using (IDataReader reader = command.ExecuteReader())
					{
						while (reader.Read())
						{
							ret.Add(reader[column]);
						}

						reader.Close();
					}
				}

				return ret;
			}
			catch (Exception ex)
			{
				CustomLogger.Log(CustomLogger.LogLevel.Error, string.Format("{0} Failed to select items in database table {1}. Message: {2}", logPrefix, table, ex.Message));
				throw;
			}
		}

		public void RepopulateTablesAsTransaction(List<DBItemBase> itemsToInsert, List<string> allTablesChanging)
		{
			using (IDbConnection connection = CreateConnection())
			{
				connection.Open();
				IDbTransaction transaction = null;
				using (Transaction.Current == null ? transaction = connection.BeginTransaction() : transaction = null)
				{
					EmptyTablesWithoutCommit(allTablesChanging, connection, transaction);
					CustomLogger.Log(CustomLogger.LogLevel.Verbose, "{0} Done emptying {1} tables", logPrefix, String.Join(", ", allTablesChanging));

					foreach (var item in allTablesChanging)
					{
						InsertDatabaseItems(itemsToInsert.Where(x => x.GetTableName().Equals(item)).ToList(), connection, transaction);
					}

					try
					{
						if (transaction != null)
						{
							transaction.Commit();
							CustomLogger.Log(CustomLogger.LogLevel.Verbose, "{0} Completed repopulation of all tables.", logPrefix);
						}
					}
					catch (Exception ex)
					{
						CustomLogger.DumpNonFatalExceptionToLog(ex);
						transaction.Rollback();
						CustomLogger.Log(CustomLogger.LogLevel.DebugLog, "{0} Rollback all changes to all tables.", logPrefix);
						throw ex;
					}
				}
			}
		}
	}
}
