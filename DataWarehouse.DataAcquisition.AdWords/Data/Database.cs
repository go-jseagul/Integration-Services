using DataWarehouse.DataAcquisition.AdWords.Properties;
using Microsoft.Practices.EnterpriseLibrary.Data.Sql;
using System.Data;
using System.Data.Common;

namespace DataWarehouse.DataAcquisition.AdWords.Data
{
    /// <summary>
    /// The Database
    /// </summary>
    public class Database
    {
        /// <summary>
        /// The database
        /// </summary>
        private readonly SqlDatabase database;

        /// <summary>
        /// Initializes a new instance of the <see cref="Database"/> class.
        /// </summary>
        /// <param name="connectionString">The connection string.</param>
        internal Database(string connectionString)
        {
            this.database = new SqlDatabase(connectionString);
        }

        /// <summary>
        /// Gets the connection string.
        /// </summary>
        /// <value>
        /// The connection string.
        /// </value>
        public string ConnectionString
        {
            get { return this.database.ConnectionString; }
        }

        /// <summary>
        /// Gets the connection string without credentials.
        /// </summary>
        /// <value>
        /// The connection string without credentials.
        /// </value>
        public string ConnectionStringWithoutCredentials
        {
            get { return this.database.ConnectionStringWithoutCredentials; }
        }

        /// <summary>
        /// Adds the in parameter.
        /// </summary>
        /// <param name="command">The command.</param>
        /// <param name="name">Name of the parameter.</param>
        /// <param name="sqlDbType">Type of the parameter.</param>
        /// <param name="value">The value.</param>
        public void AddInParameter(DbCommand command, string name, SqlDbType sqlDbType, object value)
        {
            this.database.AddInParameter(command, name, sqlDbType, value);
        }

        /// <summary>
        /// Adds the out parameter.
        /// </summary>
        /// <param name="command">The command.</param>
        /// <param name="name">Name of the parameter.</param>
        /// <param name="sqlDbType">Type of the parameter.</param>
        public void AddOutParameter(DbCommand command, string name, SqlDbType sqlDbType)
        {
            this.AddOutParameter(command, name, sqlDbType, 0);
        }

        /// <summary>
        /// Adds the out parameter.
        /// </summary>
        /// <param name="command">The command.</param>
        /// <param name="name">Name of the parameter.</param>
        /// <param name="sqlDbType">Type of the parameter.</param>
        /// <param name="size">The size.</param>
        public void AddOutParameter(DbCommand command, string name, SqlDbType sqlDbType, int size)
        {
            this.database.AddOutParameter(command, name, sqlDbType, size);
        }

        /// <summary>
        /// Creates the connection.
        /// </summary>
        /// <returns><see cref="DbConnection"/></returns>
        public DbConnection CreateConnection()
        {
            return this.database.CreateConnection();
        }

        /// <summary>
        /// Executes the non query.
        /// </summary>
        /// <param name="command">The command.</param>
        /// <returns>Integer indicating success/failure of query execution.</returns>
        public int ExecuteNonQuery(DbCommand command)
        {
            return this.database.ExecuteNonQuery(command);
        }

        /// <summary>
        /// Executes the non query.
        /// </summary>
        /// <param name="command">The command.</param>
        /// <param name="transaction">The transaction.</param>
        /// <returns>Integer indicating success/failure of query execution.</returns>
        public int ExecuteNonQuery(DbCommand command, DbTransaction transaction)
        {
            return this.database.ExecuteNonQuery(command, transaction);
        }

        /// <summary>
        /// Executes the query.
        /// </summary>
        /// <param name="command">The command.</param>
        /// <returns>The first column of the first row in the result set returned by the query.</returns>
        public int ExecuteScalar(DbCommand command)
        {
            return (int)System.Convert.ToDecimal(this.database.ExecuteScalar(command));
        }

        /// <summary>
        /// Executes the reader.
        /// </summary>
        /// <param name="command">The command.</param>
        /// <returns><see cref="IDataReader"/></returns>
        public IDataReader ExecuteReader(DbCommand command)
        {
            return this.database.ExecuteReader(command);
        }

        /// <summary>
        /// Executes the reader.
        /// </summary>
        /// <param name="command">The command.</param>
        /// <param name="transaction">The transaction.</param>
        /// <returns><see cref="IDataReader"/></returns>
        public IDataReader ExecuteReader(DbCommand command, DbTransaction transaction)
        {
            return this.database.ExecuteReader(command, transaction);
        }

        /// <summary>
        /// Gets the parameter value.
        /// </summary>
        /// <param name="command">The command.</param>
        /// <param name="name">The name.</param>
        /// <returns>Response from the command execution.</returns>
        public object GetParameterValue(DbCommand command, string name)
        {
            return this.database.GetParameterValue(command, name);
        }

        /// <summary>
        /// Gets the class parameter value.
        /// </summary>
        /// <typeparam name="T">Generic class</typeparam>
        /// <param name="command">The command.</param>
        /// <param name="name">The name.</param>
        /// <returns>Parameter value.</returns>
        public T GetClassParameterValue<T>(DbCommand command, string name) where T : class
        {
            object temp = this.GetParameterValue(command, name);
            if (temp == System.DBNull.Value)
                return null;

            return (T)temp;
        }

        /// <summary>
        /// Gets the structure parameter value.
        /// </summary>
        /// <typeparam name="T">Generic structure.</typeparam>
        /// <param name="command">The command.</param>
        /// <param name="name">The name.</param>
        /// <returns>Parameter value.</returns>
        public T? GetParameterValue<T>(DbCommand command, string name) where T : struct
        {
            object temp = this.GetParameterValue(command, name);
            if (temp == System.DBNull.Value)
                return null;

            return (T)temp;
        }

        /// <summary>
        /// Gets the stored procedure command.
        /// </summary>
        /// <param name="storedProcedureName">Name of the stored procedure.</param>
        /// <returns><see cref="DbCommand"/></returns>
        public DbCommand GetStoredProcCommand(string storedProcedureName)
        {
            DbCommand storedProcCommand = this.database.GetStoredProcCommand(storedProcedureName);
            storedProcCommand.CommandTimeout = Settings.Default.CommandTimeout;
            return storedProcCommand;
        }
    }
}