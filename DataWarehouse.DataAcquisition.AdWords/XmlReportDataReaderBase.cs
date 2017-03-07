using System;
using System.Collections.Generic;
using System.Data;
using System.Xml;
using System.Xml.Linq;

namespace DataWarehouse.DataAcquisition.AdWords.Data
{
    public abstract class XmlReportSqlBulkCopyDataReader : IDataReader
    {
        private string _xmlReportFilePath;
        protected long _rptlogId;
        protected DateTime _rptdDate;
        protected long _clientId;
        protected bool _eof = false;
        protected object[] _values;
        protected IEnumerator<XElement> _records;
        private XmlReader _reader = null;
        private bool _disposed = false;

        public abstract string DestinationTable { get; }

        public abstract int FieldCount { get; }

        public abstract int BatchSize { get; }

        public abstract bool EnableStreaming { get; }

        public abstract bool Read();

        public void Initialize(string xmlReportFilePath, long rptlogId, DateTime rptdDate, long clientId)
        {
            this._xmlReportFilePath = xmlReportFilePath;
            this._rptlogId = rptlogId;
            this._rptdDate = rptdDate;
            this._clientId = clientId;
            InitializeRecords();
        }

        private void InitializeRecords()
        {
            if (_records != null)
            {
                _records.Dispose();
            }
            if (_reader != null)
            {
                _reader.Dispose();
            }
            this._values = null;
            this._eof = false;
            this._records = StreamReportData(this._xmlReportFilePath).GetEnumerator();
        }

        private IEnumerable<XElement> StreamReportData(string xmlReportFilePath)
        {
            // the report downloads from google have minimal non-data nodes
            // if performance of ignoring these "ignored" nodes is unacceptable
            // you could improve the navigation to the data nodes prior to the first yield
            // note: once the first data node (i.e. "row" node) is found the remianing nodes
            // should always be at the next node until the end of the data nodes are read
            // the data nodes currently are at the end of the doc
            using (_reader = XmlReader.Create(xmlReportFilePath))
            {
                _reader.MoveToContent();

                while (!_reader.EOF)
                {
                    if (_reader.NodeType == XmlNodeType.Element && _reader.Name == "row") // report data row
                    {
                        XElement el = XElement.ReadFrom(_reader) as XElement;
                        if (el != null)
                        {
                            yield return el;
                        }
                    }
                    else
                    {
                        _reader.Read(); // not a data row, advance to next node
                    }
                }
            }
        }

        protected void Reset()
        {
            InitializeRecords();
        }

        public object GetValue(int i)
        {
            return this._values[i];
        }

        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }
        
        protected virtual void Dispose(bool disposing)
        {
            if (_disposed)
            {
                return;
            }

            if (disposing)
            {
                if (_records != null)
                {
                    _records.Dispose();
                }
                if (_reader != null)
                {
                    _reader.Dispose();
                }
            }

            _disposed = true;
        }

        #region un-needed IDataReader methods for SqlBulkCopy

        public void Close()
        {
            throw new NotImplementedException();
        }

        public int Depth
        {
            get { throw new NotImplementedException(); }
        }

        public DataTable GetSchemaTable()
        {
            throw new NotImplementedException();
        }

        public bool IsClosed
        {
            get { throw new NotImplementedException(); }
        }

        public bool NextResult()
        {
            throw new NotImplementedException();
        }

        public int RecordsAffected
        {
            get { throw new NotImplementedException(); }
        }

        public bool GetBoolean(int i)
        {
            throw new NotImplementedException();
        }

        public byte GetByte(int i)
        {
            throw new NotImplementedException();
        }

        public long GetBytes(int i, long fieldOffset, byte[] buffer, int bufferoffset, int length)
        {
            throw new NotImplementedException();
        }

        public char GetChar(int i)
        {
            throw new NotImplementedException();
        }

        public long GetChars(int i, long fieldoffset, char[] buffer, int bufferoffset, int length)
        {
            throw new NotImplementedException();
        }

        public IDataReader GetData(int i)
        {
            throw new NotImplementedException();
        }

        public string GetDataTypeName(int i)
        {
            throw new NotImplementedException();
        }

        public DateTime GetDateTime(int i)
        {
            throw new NotImplementedException();
        }

        public decimal GetDecimal(int i)
        {
            throw new NotImplementedException();
        }

        public double GetDouble(int i)
        {
            throw new NotImplementedException();
        }

        public Type GetFieldType(int i)
        {
            throw new NotImplementedException();
        }

        public float GetFloat(int i)
        {
            throw new NotImplementedException();
        }

        public Guid GetGuid(int i)
        {
            throw new NotImplementedException();
        }

        public short GetInt16(int i)
        {
            throw new NotImplementedException();
        }

        public int GetInt32(int i)
        {
            throw new NotImplementedException();
        }

        public long GetInt64(int i)
        {
            throw new NotImplementedException();
        }

        public string GetName(int i)
        {
            throw new NotImplementedException();
        }

        public int GetOrdinal(string name)
        {
            throw new NotImplementedException();
        }

        public string GetString(int i)
        {
            throw new NotImplementedException();
        }

        public int GetValues(object[] values)
        {
            throw new NotImplementedException();
        }

        public bool IsDBNull(int i)
        {
            throw new NotImplementedException();
        }

        public object this[string name]
        {
            get { throw new NotImplementedException(); }
        }

        public object this[int i]
        {
            get { throw new NotImplementedException(); }
        }

        #endregion
    }
}