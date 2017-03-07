using DataWarehouse.DataAcquisition.AdWords.Processors;
using DataWarehouse.DataAcquisition.AdWords.Properties;
using System;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Text;
using System.Xml;
using System.Xml.Serialization;

namespace DataWarehouse.DataAcquisition.AdWords
{
    static public class Utilities
    {
        private static readonly string _reportDownloadTempFolder = Settings.Default.ReportDownloadTempFolder.TrimEnd(Path.DirectorySeparatorChar);

        public static string GetTempDir()
        {
            if (!Directory.Exists(_reportDownloadTempFolder))
            {
                Directory.CreateDirectory(_reportDownloadTempFolder);
            }

            return _reportDownloadTempFolder;
        }

        public static string ExtractGZipFile(FileInfo fileToUnzip, string newExtension, bool deleteOriginalFile)
        {
            string currentFileName = fileToUnzip.FullName;
            string newFileName = currentFileName.Remove(currentFileName.Length - fileToUnzip.Extension.Length) + "." + newExtension;
            using (FileStream originalFileStream = fileToUnzip.OpenRead())
            {
                using (var decompressedFileStream = File.Create(newFileName))
                {
                    using (var decompressionStream = new GZipStream(originalFileStream, CompressionMode.Decompress))
                    {
                        decompressionStream.CopyTo(decompressedFileStream);
                    }
                }
            }

            if (deleteOriginalFile) { fileToUnzip.Delete(); }

            return newFileName;
        }

        public static string ToXml<T>(this T[] target) where T : class, new()
        {
            var sb = new StringBuilder();
            for (int i = 0; i < target.Length; i++)
            {
                sb.Append(target[i].ToXml());
            }
            return sb.ToString();
        }

        public static string ToXml<T>(this T target) where T : class, new()
        {
            if (target == null) { return null; }
            XmlWriter xw = null;
            try
            {
                XmlSerializer s = new XmlSerializer(typeof(T));
                XmlWriterSettings xmlSettings = new XmlWriterSettings();
                xmlSettings.OmitXmlDeclaration = true;

                XmlSerializerNamespaces nameSpaces = new XmlSerializerNamespaces();
                nameSpaces.Add("", "");

                StringBuilder sb = new StringBuilder();
                xw = XmlWriter.Create(sb, xmlSettings);
                s.Serialize(xw, (T)target, nameSpaces);
                return sb.ToString();
            }
            catch (Exception ex)
            {
                throw new Exception("Error in Serializing Object into Xml", ex);
            }
            finally
            {
                if (xw != null) { xw.Close(); }
            }
        }

        public static bool Contains(this ReportTypes target, ReportTypes reportType)
        {
            return (target & reportType) == reportType;
        }

        public static ReportTypes Except(this ReportTypes target, ReportTypes reportType)
        {
            return target & ~reportType;
        }
    }
}