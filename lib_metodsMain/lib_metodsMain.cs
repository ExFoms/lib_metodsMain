using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.IO;
using System.Data.SqlClient;
using System.Data;
using System.Net.Mail;
using System.Net;
using Npgsql;
using System.Diagnostics;
using System.Xml;
using System.Xml.Serialization;
using System.Xml.Schema;
public static class clsLibrary
{
    public class Id_row
    {
        public int id;
        public string row;
        public Id_row(int id_, string row_)
        {
            id = id_;
            row = row_;
        }
    }
    public class VarResult
    {
        public bool result;
        public string comment;
        public int code;
        public VarResult()
        { }
        public VarResult(bool result_, string comment_, int code_)
        {
            result = result_;
            comment = comment_;
            code = code_;
        }
    }
    
    //----- Other metods
    #region other metods
    public static bool moveFile(string fileSource, string pathDestination, out string comments, bool rewrite = false)
    /* 
     * Перемещение файла в назначенную папку
     */
    {
        //Проверяем наличие файла
        if (!File.Exists(fileSource))
        {
            comments = "File not find";
            return false;
        }
        //Контроль наличия папки
        if (!Directory.Exists(pathDestination))
        {
            try
            {
                Directory.CreateDirectory(pathDestination);
            }
            catch
            {
                comments = "Can't create folder";
                return false;
            }
        }
        //Переносим файл
        string fileDestination = Path.Combine(pathDestination, Path.GetFileName(fileSource));
        if (!File.Exists(fileDestination))
        {
            try
            {
                File.Move(fileSource, fileDestination);
                comments = "ok";
                return true;
            }
            catch
            {
                comments = "Can't move file";
                return false;
            }
        }
        else
        {
            if (rewrite)
            {
                try
                {
                    File.Delete(fileDestination);
                    File.Move(fileSource, fileDestination);
                    comments = "ok";
                    return true;
                }
                catch
                {
                    comments = "Can't move file";
                    return false;
                }
            }
            else //В цикле формируем новое имя файла
            {
                int number = 0;
                string nameFile = Path.GetFileNameWithoutExtension(fileSource);
                string extantionFile = Path.GetExtension(fileSource);
                do
                {
                    number++;
                    fileDestination = Path.Combine(pathDestination, String.Format("{0}({1}){2}", nameFile, number.ToString(), extantionFile));
                } while (File.Exists(fileDestination));
                try
                {
                    File.Move(fileSource, fileDestination);
                    comments = "ok";
                    return true;
                }
                catch
                {
                    comments = "Can't create destination file";
                    return false;
                }
            }
        }
    }
    public static string trancateLongString(string value, int count, bool before = false, string postfix = "...")
    //сокращение с заполнением до или после / удлинение пробелами строки
    {
        if (count > 0 && value != null)
        {
            if (value.Length > count)
                value = (before) ?
                    postfix + value.Substring(value.Length - count, count) :
                    value = value.Substring(0, count) + postfix;
            else
                value = (value.Length < count) ? value + (new String((Char)32, count - value.Length)) : value;
        }
        return value;
    }
    public static int import_FileInDB(string file, int skip /*пропускаемые строки*/, int count /*столбцы*/, string query, string connection, string db)
    //импорт текстового файла в БД
    {
        int result = -1;
        List<string> list = new List<string>();
        string[] columns = null;
        try
        {
            string[] rows = File.ReadAllLines((string)file, Encoding.Default);
            int count_row = 0;
            for (int row = 0; row < rows.Length; row++)
            {
                if (!String.IsNullOrEmpty(rows[row]))
                {
                    columns = rows[row].Split(';');
                    string value = "";
                    for (int column = 0; column < count; column++)
                    {
                        value = value + clsLibrary.string_Apostrophe(columns[column]);
                        if (column < count - 1) value = value + ",";
                    }
                    if (value != ",,,,,,,") { count_row++; if (count_row > skip) list.Add(value); }
                }
            }
            if (execQuery_insertList_bool(connection + ";" + db, query, list))
                result = list.Count;
        }
        catch
        { }
        return result;
    }
    public static bool createFileTXT_FromList(List<string> list, string file, Encoding encoding = null, bool rewrite = true, bool append = false)
    //создание текстового файла из текстового списка 
    {
        if (File.Exists(file))
            if (rewrite) File.Delete(file);
            else return false;
        if (encoding == null) { encoding = Encoding.GetEncoding("Windows-1251"); }
        StreamWriter file_target = null;
        try
        {
            file_target = new StreamWriter(file, append, encoding); ;
            foreach (string row in list) file_target.WriteLine(row);
            file_target.Close();
            return true;
        }
        catch
        {
            file_target.Close();
            return false;
        }

    }
    public static bool createFileTXT_FromListAndId(List<Id_row> list, string file, Encoding encoding = null)
    //создание текстового файла из текстового списка 
    {
        if (encoding == null) { encoding = Encoding.GetEncoding("Windows-1251"); }
        StreamWriter file_target = new StreamWriter(file, false, encoding);
        bool result = false;
        try
        {
            foreach (Id_row row in list) file_target.WriteLine(row.id.ToString() + ";" + row.row);
            result = true;
        }
        catch
        {
            result = false;
        }
        file_target.Close();
        if (!result) File.Delete(file);
        return result;

    }
    public static string InsertNameFile(List<clsConnections> link_connections, string reglament_connections, string database, string filename, string tableName, string command, string value, int attempt = 0/*попытка записать файл*/, int i = 0)
    //Вносит имя файла в таблицу, должен быть уникальный
    {
        string id = "0";
        string connection_name = "";
        get_stringSplitPos(ref connection_name, reglament_connections, ';', i);
        SqlConnection connection = new SqlConnection(link_connections.Find(x => x.name == connection_name).connectionString + ";database=" + database);
        connection.Open();
        SqlCommand sqlCommand = connection.CreateCommand();
        sqlCommand.Connection = connection;
        sqlCommand.CommandText = String.Format("Select top 1 id from {0} where FILENAME = '{1}'", tableName, filename);

        SqlDataReader rdr = sqlCommand.ExecuteReader();
        if (rdr.HasRows)
        {
            rdr.Read();
            if (attempt != 0) id = rdr.GetString(0);
            else id = "-1";
        }
        connection.Close();
        if (id == "0")
        {
            if (execQuery_insert(ref link_connections, reglament_connections, database, command, value))
                id = InsertNameFile(link_connections, reglament_connections, filename, database, tableName, command, value, ++attempt);
            else id = "-1";
        }
        return id;
    }
    public static string string_ForDB(string value)
    //формирует символьную строку для запсиси в БД
    {
        value = value.ToString().Replace("\"", string.Empty);
        value = value.Trim();
        if (value == string.Empty || value.Trim() == "")
            return "null";
        else
            return value;
    }
    public static string string_Apostrophe(string value)
    //оборачивает строку апострофами
    {
        if (value == null || value == "null" || value == string.Empty || value.Trim() == "")
            return value;
        else
            return "'" + value + "'";
    }
    public static int InsertNameFile(string filename, int attempt = 0/*попытка записать файл*/)
    //Вносит имя файла в таблицу, должен быть уникальный
    {
        int id = 0;
        /*System.Data.SqlClient.SqlConnection sqlConnection1 = new System.Data.SqlClient.SqlConnection("uid=sa;pwd=Cvbqwe2!;server=server-r;database=SRZ3_00;");
        System.Data.SqlClient.SqlCommand cmd1 = new System.Data.SqlClient.SqlCommand();
        cmd1.CommandType = System.Data.CommandType.Text;
        cmd1.Connection = sqlConnection1;
        cmd1.CommandText = "Select top 1 id from MO_LOG where FNAME = '" + filename + "'";
        sqlConnection1.Open();
        SqlDataReader rdr = cmd1.ExecuteReader();
        if (rdr.HasRows)
        {
            rdr.Read();
            if (attempt != 0) id = rdr.GetInt32(0);
            else id = -1;
        }
        sqlConnection1.Close();*/
        if (id == 0)
        {
            execQuery_insert(
                "uid=sa;pwd=Cvbqwe2!;server=server-r;database=SRZ3_00;",
                "insert into MO_LOG (FNAME, DT, NREC, NERR) VALUES ",
                string_Apostrophe(filename) + "," + string_Apostrophe(DateTime.Now.ToString("yyyyMMdd")) + ",0,0");
            id = InsertNameFile(filename, ++attempt);
        }
        return id;
    }
    public static bool get_stringSplitPos(ref string result, string value, char splitSymbol, int position)
    /* получение значения из строки с разделителями
     */
    {
        try
        {
            result = value.Split(splitSymbol)[position];
            return true;
        }
        catch
        {
            return false;
        }
    }
    public static string GetConnectionString_XLS(string dir)
    {
        Dictionary<string, string> props = new Dictionary<string, string>();
        FileInfo _file = new FileInfo(dir);

        // XLSX - Excel 2007, 2010, 2012, 2013
        if (_file.Extension == ".xlsx")
        {
            props["Provider"] = "Microsoft.ACE.OLEDB.12.0;";
            props["Extended Properties"] = "Excel 12.0 XML";
            props["Data Source"] = _file.FullName;
        }
        else if (_file.Extension == ".xls")
        {
            props["Provider"] = "Microsoft.Jet.OLEDB.4.0";
            props["Extended Properties"] = "Excel 8.0";
            props["Data Source"] = dir;
        }
        else throw new Exception("Неизвестное расширение файла!");

        StringBuilder sb = new StringBuilder();

        foreach (KeyValuePair<string, string> prop in props)
        {
            sb.Append(prop.Key);
            sb.Append('=');
            sb.Append(prop.Value);
            sb.Append(';');
        }

        return sb.ToString();
    }
    public static bool SendMail(string smtpServer, string from, string password, string mailto, string caption, string message, string attachFile = null)
    {
        try
        {
            MailMessage mail = new MailMessage();
            mail.From = new MailAddress(from);
            mail.To.Add(new MailAddress(mailto));
            mail.Subject = caption;
            mail.Body = message;
            if (!string.IsNullOrEmpty(attachFile))
                mail.Attachments.Add(new Attachment(attachFile));
            SmtpClient client = new SmtpClient();
            client.Host = smtpServer;
            client.Port = 5025;
            client.EnableSsl = false;
            client.Credentials = new NetworkCredential(from.Split('@')[0], password);
            client.DeliveryMethod = SmtpDeliveryMethod.Network;
            client.Send(mail);
            mail.Dispose();
            return true;
        }
        catch
        {
            return false;
        }
    }
    public static void SaveXML_prt(sf_schema.FLK_P xml, string fn)
    {
        FileStream res = new System.IO.FileStream(fn, FileMode.Create);
        System.Xml.Serialization.XmlSerializer writer =
            new System.Xml.Serialization.XmlSerializer(typeof(sf_schema.FLK_P));
        #region Перекодировка из UTF-8 в windows 1251
        System.IO.StreamWriter file = new System.IO.StreamWriter(res, Encoding.GetEncoding(1251));
        writer.Serialize(file, xml);
        res.Close();
        #endregion
    }
    public static void SaveXML_flk(sp_schema.FLK_P xml, string fn)
    {
        FileStream res = new System.IO.FileStream(fn, FileMode.Create);
        System.Xml.Serialization.XmlSerializer writer =
            new System.Xml.Serialization.XmlSerializer(typeof(sp_schema.FLK_P));
        #region Перекодировка из UTF-8 в windows 1251
        System.IO.StreamWriter file = new System.IO.StreamWriter(res, Encoding.GetEncoding(1251));
        writer.Serialize(file, xml);
        res.Close();
        #endregion
    }
    public static void unpack(string mask, string source = @"u:\", string zipperPath = @"C:\Program Files\7-Zip\7z.exe")
    {
        bool find = false;
        do
        {
            string[] files = null;
            files = Directory.GetFiles(source, mask + ".ZIP");
            files = files.Concat(Directory.GetFiles(source, mask + ".RAR")).ToArray();
            files = files.Concat(Directory.GetFiles(source, mask + ".7z")).ToArray();
            find = files.Count() > 0;
            if (find)
                foreach (string file in files)
                {
                    ProcessStartInfo processInfo;
                    Process process;

                    processInfo = new ProcessStartInfo(zipperPath,
                                               @" e -y " + "\"" + file + "\"" + " -o" + "\"" + source + "\"");
                    processInfo.CreateNoWindow = true;
                    processInfo.UseShellExecute = false;
                    // *** Redirect the output ***
                    //processInfo.RedirectStandardError = false;
                    //processInfo.RedirectStandardOutput = false;

                    process = Process.Start(processInfo);
                    //process.Threads.Sleep(1000);
                    process.WaitForExit();

                    int exitCode = process.ExitCode;
                    process.Close();
                    //MessageBox.Show("Swich " + exitCode.ToString());
                    switch (exitCode)
                    {
                        case 0:
                        case 1:
                        default:
                            File.Delete(file);
                            break;
                    }
                }
        }
        while (find);
    }

    public static bool moveFile_byPrefix(string file, string prefix)
    //перемещение файла в папку по префикусу, определяется по месту файла
    {
        try
        {
            string destination = Path.Combine(Path.GetDirectoryName(file), prefix);
            System.IO.Directory.CreateDirectory(destination);
            File.Copy(file, Path.Combine(destination, Path.GetFileName(file)), true);
            File.Delete(file);
            return true;
        }
        catch
        {
            return false;
        }
    }
    public static bool getTeg_HEADER(string filename, ref XmlDocument HEADER, ref string version)
    {
        try
        {
            using (FileStream fstream = File.OpenRead(filename))
            {
                byte[] array = new byte[2000/*fstream.Length*/];
                fstream.Read(array, 0, array.Length);
                //string textFromFile = Regex.Replace(System.Text.Encoding.Default.GetString(array), @"[ ]", ""); 
                string textFromFile = System.Text.Encoding.Default.GetString(array);
                int header_teg = textFromFile.IndexOf("<HEADER"); int header_teg_close = textFromFile.IndexOf("</HEADER>");
                if (header_teg != -1 && header_teg_close != -1)
                    HEADER.LoadXml(textFromFile.Substring(header_teg, header_teg_close - header_teg + 9));
                version = (HEADER.GetElementsByTagName("VERSION")[0]).InnerText;
            }
            return true;
        }
        catch
        {
            return false;
        }
    }
    public static string getName_fromLibraies(ref List<clsConnections> link_connections, string nameField, string table, string fieldCode, string code)
    //получение имени МО по коду
    {
        string result = null;
        if (fieldCode != null)
            result = clsLibrary.execQuery_getString(
                 ref link_connections, null, "libraries",
                 String.Format("SELECT top 1 {0} FROM {1} where {2} = '{3}'", nameField, table, fieldCode, code)
                 );
        if (result == null)
            return string.Empty;
        else
            return result;
    }
    public static T DeserializeFrom<T>(this XmlElement xml) where T : new()
    {
        T xmlObject = new T();
        try
        {
            XmlSerializer xmlSerializer = new XmlSerializer(typeof(T));
            StringReader stringReader = new StringReader(xml.OuterXml);
            xmlObject = (T)xmlSerializer.Deserialize(stringReader);
        }
        catch (Exception e)
        {

        }
        return xmlObject;
    }
    public static string SerializeTo<T>(this T xmlObject, bool clear = false)
    {
        if (!clear)
        {
            XmlSerializer xmlSerializer = new XmlSerializer(typeof(T));
            MemoryStream memoryStream = new MemoryStream();
            XmlTextWriter xmlTextWriter = new XmlTextWriter(memoryStream, Encoding.UTF8);
            xmlTextWriter.Formatting = Formatting.Indented;
            xmlSerializer.Serialize(xmlTextWriter, xmlObject);
            string output = Encoding.UTF8.GetString(memoryStream.ToArray());
            string _byteOrderMarkUtf8 = Encoding.UTF8.GetString(Encoding.UTF8.GetPreamble());
            if (output.StartsWith(_byteOrderMarkUtf8))
            {
                output = output.Remove(0, _byteOrderMarkUtf8.Length);
            }
            return output;
        }
        else
        {
            var emptyNamespaces = new XmlSerializerNamespaces(new[] { XmlQualifiedName.Empty });
            var serializer = new XmlSerializer(xmlObject.GetType());
            var settings = new XmlWriterSettings();
            settings.Indent = true;
            settings.OmitXmlDeclaration = true;
            using (var stream = new StringWriter())
            using (var writer = XmlWriter.Create(stream, settings))
            {
                serializer.Serialize(writer, xmlObject, emptyNamespaces);
                return stream.ToString();
            }
        }
    }
    #endregion other metods

    //------ MSSQL region
    #region mssql queries
    internal static List<string> execQuery_getListString(List<clsConnections> link_connections, ref string reglament_connections, string p1, string p2)
    {
        throw new NotImplementedException();
    }
    public static string execQuery_NewRequest(string connection_string, string filename, string mnemonics, string schemaname, string header)
    /* выполнение запроса c возвратом первого значения
     * при ошибке возвращает -1
     */
    {
        string result = "-1"; //ошибка вставки
        try
        {
            //0 - не вставлена запись, guid - успешно
            SqlConnection connection = new SqlConnection(connection_string);
            SqlCommand command = new SqlCommand();
            command.CommandType = CommandType.Text;
            command.Connection = connection;
            command.CommandText = String.Format("EXEC eir.dbo.insert_newRequest '{0}','{1}','{2}','{3}'", filename, mnemonics, schemaname, header);
            connection.Open();
            SqlDataReader reader = command.ExecuteReader();
            reader.Read();
            result = reader[0].ToString();
            connection.Close();
        }
        catch
        {
        }
        return result;
    }
    public static List<string> execQuery_getListString(string connection_string, string query)
    //выполнение запроса c возвратом простого списка из текстовых строк
    {
        List<string> result = new List<string>();
        try
        {
            SqlConnection connection = new SqlConnection(connection_string);
            SqlCommand command = new SqlCommand();
            command.CommandType = CommandType.Text;
            command.Connection = connection;
            connection.Open();
            command.CommandText = query;
            SqlDataReader reader = command.ExecuteReader();
            while (reader.Read())
            {
                result.Add(reader[0].ToString());
            }
            connection.Close();
        }
        catch
        {
            result = null;
        }
        return result;
    }
    public static List<string> execQuery_getListString(ref List<clsConnections> link_connections, ref string reglament_connections, string database, string query, int i = 0)
    //выполнение запроса c возвратом простого списка из текстовых строк
    {
        List<string> result = new List<string>();
        try
        {
            string connection_name = "";
            get_stringSplitPos(ref connection_name, reglament_connections, ';', i);
            SqlConnection connection = new SqlConnection(link_connections.Find(x => x.name == connection_name).connectionString + ";database=" + database);
            SqlCommand command = new SqlCommand();
            command.CommandType = CommandType.Text;
            command.Connection = connection;
            connection.Open();
            command.CommandText = query;
            SqlDataReader reader = command.ExecuteReader();
            while (reader.Read())
            {
                result.Add(reader[0].ToString());
            }
            connection.Close();
        }
        catch (Exception e)
        {
            result = null;
        }
        return result;
    }
    public static bool execQuery_getListString(ref List<string[]> list, ref List<clsConnections> link_connections, string reglament_connections, string database, string query, int commandTimeout_ = 0, string symbol = "", int i = 0)
    //выполнение запроса c возвратом простого списка из массива текстовых строк
    {
        try
        {
            string connection_name = "";
            get_stringSplitPos(ref connection_name, reglament_connections, ';', i);
            SqlConnection connection = new SqlConnection(link_connections.Find(x => x.name == connection_name).connectionString + ";database=" + database);
            SqlCommand command = new SqlCommand();
            command.CommandType = CommandType.Text;
            command.Connection = connection;
            connection.Open();
            command.CommandText = query;
            SqlDataReader reader = command.ExecuteReader();
            if (reader.HasRows)
            {
                reader.Read();
                do
                {
                    string[] row = new string[reader.FieldCount];
                    for (int id = 0; id < row.Length; id++)
                    {
                        row[id] = symbol + reader[id].ToString() + symbol;
                    }
                    list.Add(row);
                }
                while (reader.Read());
            }
            connection.Close();
            return true;
        }
        catch (Exception e)
        {
            string mes = e.Message;
            return false;
        }
    }
    public static bool ExecQurey_GetListStrings(string connection_string, string query, ref List<string[]> list, int commandTimeout_ = 0)
    {
        try
        {
            SqlConnection connection = new SqlConnection(connection_string);
            SqlCommand command = new SqlCommand();
            command.CommandType = CommandType.Text;
            command.CommandTimeout = commandTimeout_;
            command.Connection = connection;
            connection.Open();
            command.CommandText = query;
            SqlDataReader reader = command.ExecuteReader();

            if (reader.Read())
                do
                {
                    string[] row = new string[reader.FieldCount];
                    for (int i = 0; i < row.Length; i++)
                    {
                        row[i] = reader[i].ToString();
                    }
                    list.Add(row);
                }
                while (reader.Read());
            connection.Close();
            return true;
        }
        catch (Exception s)
        {
            //MessageBox.Show(s.ToString());
            return false;
        }
    }
    public static bool ExecQurey_GetListStrings(List<clsConnections> link_connections, string reglament_connections, string database, string query, ref List<string[]> list, int commandTimeout_ = 0, int i = 0)
    {
        try
        {
            string connection_name = "";
            if (reglament_connections == null || reglament_connections == String.Empty)
                connection_name = database;
            else
                get_stringSplitPos(ref connection_name, reglament_connections, ';', i);
            SqlConnection connection = new SqlConnection(link_connections.Find(x => x.name == connection_name).connectionString + ";database=" + database);
            SqlCommand command = new SqlCommand();
            command.CommandType = CommandType.Text;
            command.CommandTimeout = commandTimeout_;
            command.Connection = connection;
            connection.Open();
            command.CommandText = query;
            SqlDataReader reader = command.ExecuteReader();

            if (reader.Read())
                do
                {
                    string[] row = new string[reader.FieldCount];
                    for (int col = 0; col < row.Length; col++)
                    {
                        row[col] = reader[col].ToString();
                    }
                    list.Add(row);
                }
                while (reader.Read());
            connection.Close();
            return true;
        }
        catch (Exception s)
        {
            //MessageBox.Show(s.ToString());
            return false;
        }
    }
    public static bool ExecQurey_GetTable(string connection_string, string query, ref DataTable table, int commandTimeout_ = 0)
    {
        try
        {
            SqlConnection connection = new SqlConnection(connection_string);
            SqlCommand command = new SqlCommand();
            command.CommandType = CommandType.Text;
            command.CommandTimeout = commandTimeout_;
            command.Connection = connection;
            connection.Open();
            command.CommandText = query;
            SqlDataReader reader = command.ExecuteReader();
            DataRow row = null;
            while (reader.Read())
            {
                row = table.NewRow();
                for (int i = 0; i < table.Columns.Count; i++)
                {
                    row[i] = reader[i].ToString();
                }
                table.Rows.Add(row);
            }
            connection.Close();
            return true;
        }
        catch
        {
            return false;
        }
    }
    public static bool execQuery_insert(string connection_string, string query, string value)
    //выполнение запроса на вставку одной записи
    {
        if (value == null) return false;
        try
        {
            SqlConnection connection = new SqlConnection(connection_string);
            SqlCommand cmd = new SqlCommand();
            cmd.CommandType = CommandType.Text;
            cmd.Connection = connection;
            connection.Open();
            cmd.CommandText = query + " (" + value + ")";
            cmd.ExecuteNonQuery();
            connection.Close();
            return true;
        }
        catch
        {
            return false;
        }
    }
    public static bool execQuery_insert(ref List<clsConnections> link_connections, string reglament_connections, string database, string query, string value, int i = 0)
    //выполнение запроса на вставку одной записи
    {
        if (value == null) return false;
        try
        {
            string connection_name = "";
            if (reglament_connections == null || reglament_connections == String.Empty)
                connection_name = database;
            else
                get_stringSplitPos(ref connection_name, reglament_connections, ';', i);
            SqlConnection connection = new SqlConnection(link_connections.Find(x => x.name == connection_name).connectionString + ";database=" + database);
            SqlCommand command = new SqlCommand();
            command.CommandType = CommandType.Text;
            command.Connection = connection;
            connection.Open();
            command.CommandText = query + " (" + value + ")";
            command.ExecuteNonQuery();
            connection.Close();
            return true;
        }
        catch
        {
            return false;
        }
    }
    public static List<int> execQuery_insertList_list(string connection_string, string query, List<string> list)
    //выполнение запроса на вставку данных из списка
    //возвращает null при ошибке подключения
    //возвращает список из порядковых номеров записей, возникших при записи в БД
    {
        List<int> list_error = new List<int>();
        if (list == null || list.Count() == 0) return list_error;
        try
        {
            SqlConnection connection = new SqlConnection(connection_string);
            SqlCommand cmd = new SqlCommand();
            cmd.CommandType = CommandType.Text;
            cmd.Connection = connection;
            connection.Open();
            for (int i = 0; i < list.Count(); i++)
            {
                cmd.CommandText = query + " (" + list[i] + ")";
                try
                {
                    cmd.ExecuteNonQuery();
                }
                catch
                {
                    list_error.Add(i);
                }
            }
            connection.Close();
        }
        catch
        {
            return null;
        }
        return list_error;
    }
    public static bool execQuery_insertList_bool(string connection_string, string query, List<string> list, int limit_transaction = 1)
    //выполнение запроса на вставку данных из списка
    //возвращает false при ошибке
    {
        bool result = false;
        if (list == null || list.Count() == 0) return result;
        try
        {
            SqlConnection connection = new SqlConnection(connection_string);
            SqlCommand cmd = new SqlCommand();
            cmd.CommandType = CommandType.Text;
            cmd.Connection = connection;
            connection.Open();
            int count = 0;
            string values = string.Empty;
            int count_list = list.Count();
            for (int i = 0; i < count_list; i++)
            {
                ++count;
                if (count != 1) values += ",";
                values += " (" + list[i] + ")";
                if (count == limit_transaction || i + 1 == count_list)
                {
                    cmd.CommandText = query + values;
                    cmd.ExecuteNonQuery();
                    values = string.Empty;
                    count = 0;
                }
            }
            connection.Close();
            result = true;
        }
        catch
        { }
        return result;
    }


    public static bool execQuery_insertList_bool(string connection_string, string query, List<string[]> list, int limit_transaction = 1)
    //выполнение запроса на вставку данных из списка
    //возвращает false при ошибке
    {
        bool result = false;

        int count = 0;
        string values = string.Empty;
        int count_row = list.Count();
        string row_string;
        

        if (list == null || list.Count() == 0) return result;
        try
        {
            int count_row_i = list[0].Count();
            SqlConnection connection = new SqlConnection(connection_string);
            connection.Open();
            SqlCommand sqlCommand = connection.CreateCommand();
            SqlTransaction sqlTransaction = connection.BeginTransaction("SampleTransaction");
            sqlCommand.Connection = connection;
            sqlCommand.Transaction = sqlTransaction;

            for (int row = 0; row < count_row; ++row)
            {
                ++count;
                if (count != 1) values += ",";
                row_string = "";
                for (int row_i = 0; row_i < count_row_i; ++row_i)
                {
                    list[row][row_i] = (string.IsNullOrEmpty(list[row][row_i])) ? "null" : string_Apostrophe(list[row][row_i]);
                }
                values += " (" + string.Join(",", list[row]) + ")";
                if (count == limit_transaction || row + 1 == count_row)
                {
                    sqlCommand.CommandText = query + values;
                    sqlCommand.ExecuteNonQuery();
                    values = string.Empty;
                    count = 0;
                }
            }
            sqlTransaction.Commit();
            connection.Close();
            result = true;
        }
        catch (Exception ex)
        {
            string str = ex.Message;
        }
        return result;
    }
   /* public static string execQuery_insertList(string connection_string, string query, List<string[]> list, int limit_transaction = 1)
    //выполнение запроса на вставку данных из списка
    //возвращает false при ошибке
    {
        //bool result = false;
        string result = "";

        int count = 0;
        string values = string.Empty;
        int count_row = list.Count();

        if (list == null || list.Count() == 0) return result;
        try
        {
            SqlConnection connection = new SqlConnection(connection_string);
            connection.Open();
            SqlCommand sqlCommand = connection.CreateCommand();
            SqlTransaction sqlTransaction = connection.BeginTransaction("SampleTransaction");
            sqlCommand.Connection = connection;
            sqlCommand.Transaction = sqlTransaction;

            for (int row = 0; row < count_row; ++row)
            {
                ++count;
                if (count != 1) values += ","; 
                 values += " (" + string.Join(",", list[row]) + ")";
                if (count == limit_transaction || row + 1 == count_row)
                {
                    sqlCommand.CommandText = query + values;
                    result = sqlCommand.CommandText;
                    sqlCommand.ExecuteNonQuery();
                    values = string.Empty;
                    count = 0;
                }
            }
            
            sqlTransaction.Commit();
            connection.Close();
            //result = true;
        }
        catch (Exception ex)
        {
            string str = ex.Message;
        }
        return result;
    }
    */
    public static bool execQuery(string connection_string, string query, int commandTimeout_ = 0)
    //выполнение запроса 
    {
        try
        {
            SqlConnection connection = new SqlConnection(connection_string);
            SqlCommand command = new SqlCommand();
            command.CommandType = CommandType.Text;
            command.Connection = connection;
            command.CommandTimeout = commandTimeout_;
            connection.Open();
            command.CommandText = query;
            command.ExecuteNonQuery();
            connection.Close();
            return true;
        }
        catch
        {
            return false;
        }
    }
    public static bool execQuery(ref List<clsConnections> link_connections, string reglament_connections, string database, string query, int commandTimeout_ = 0, int i = 0)
    //выполнение запроса 
    {
        try
        {
            string connection_name = "";
            if (reglament_connections == null || reglament_connections == String.Empty)
                connection_name = database;
            else
                get_stringSplitPos(ref connection_name, reglament_connections, ';', i);
            SqlConnection connection = new SqlConnection(link_connections.Find(x => x.name == connection_name).connectionString + ";database=" + database);
            SqlCommand command = new SqlCommand();
            command.CommandType = CommandType.Text;
            command.Connection = connection;
            command.CommandTimeout = commandTimeout_;
            connection.Open();
            command.CommandText = query;
            command.ExecuteNonQuery();
            connection.Close();
            return true;
        }
        catch
        {
            return false;
        }
    }
    public static VarResult execQuery_VarResult(ref List<clsConnections> link_connections, string reglament_connections, string database, string query, int commandTimeout_ = 0, int i = 0)
    //выполнение запроса 
    {
        VarResult result = new VarResult(false, "", 0);
        try
        {
            string connection_name = "";
            if (reglament_connections == null || reglament_connections == String.Empty)
                connection_name = database;
            else
                get_stringSplitPos(ref connection_name, reglament_connections, ';', i);
            SqlConnection connection = new SqlConnection(link_connections.Find(x => x.name == connection_name).connectionString + ";database=" + database);
            SqlCommand command = new SqlCommand();
            command.CommandType = CommandType.Text;
            command.Connection = connection;
            command.CommandTimeout = commandTimeout_;
            connection.Open();
            command.CommandText = query;
            command.ExecuteNonQuery();
            connection.Close();

            result.result = true;
            result.comment = "Ok";
        }
        catch (Exception ex)
        {
            result.comment = ex.Message;
        }
        return result;
    }
    public static bool execQuery_updateListString(ref List<string> list, ref List<clsConnections> link_connections, string reglament_connections, int number, string database, int limit_transaction = 1, int commandTimeout_ = 0)
    /* MSSQL
     * Обновление данных списком запросов
     * В списке получаем готовые запросы на обновление
     */
    {
        bool result = false;
        int count = 0;
        string values = string.Empty;

        if (list == null || list.Count() == 0) return result;
        try
        {
            string connection_name = "";
            get_stringSplitPos(ref connection_name, reglament_connections, ';', number);
            SqlConnection connection = new SqlConnection(link_connections.Find(x => x.name == connection_name).connectionString + ";database=" + database);
            connection.Open();
            SqlCommand sqlCommand = connection.CreateCommand();
            SqlTransaction sqlTransaction = connection.BeginTransaction();
            sqlCommand.Connection = connection;
            sqlCommand.CommandTimeout = commandTimeout_;
            sqlCommand.Transaction = sqlTransaction;
            int row_count = list.Count;
            for (int row = 0; row < row_count; row++)
            //foreach (string row in list)
            {
                ++count;
                values += list[row] + ";";
                if (count == limit_transaction || row + 1 == row_count)
                {
                    sqlCommand.CommandText = values;
                    sqlCommand.ExecuteNonQuery();
                    values = string.Empty;
                    count = 0;
                }
            }
            sqlTransaction.Commit();
            connection.Close();
            result = true;
        }
        catch (Exception ex)
        {
            string str = ex.Message;
        }
        return result;
    }
    public static int execQuery_getInt(string connection_string, string query, int commandTimeout_ = 0)
    /* выполнение запроса c возвратом первого значения
     * при ошибке возвращает -1
     */
    {
        int result = 1;
        try
        {
            SqlConnection connection = new SqlConnection(connection_string);
            SqlCommand command = new SqlCommand();
            command.CommandType = CommandType.Text;
            command.Connection = connection;
            command.CommandTimeout = commandTimeout_;
            command.CommandText = query;
            connection.Open();
            SqlDataReader reader = command.ExecuteReader();
            reader.Read();
            result = Convert.ToInt32(reader[0].ToString());
            connection.Close();
        }
        catch
        {
            return -1;
        }
        return result;
    }
    public static int execQuery_getInt(ref List<clsConnections> link_connections, string reglament_connections, string database, string query, int i = 0, int commandTimeout_ = 0)
    /* выполнение запроса c возвратом первого значения
     * при ошибке возвращает -1
     */
    {
        int result = 1;
        try
        {
            string connection_name = "";
            get_stringSplitPos(ref connection_name, reglament_connections, ';', i);
            SqlConnection connection = new SqlConnection(link_connections.Find(x => x.name == connection_name).connectionString + ";database=" + database);
            SqlCommand command = new SqlCommand();
            command.CommandType = CommandType.Text;
            command.Connection = connection;
            command.CommandTimeout = commandTimeout_;
            command.CommandText = query;
            connection.Open();
            SqlDataReader reader = command.ExecuteReader();
            reader.Read();
            result = Convert.ToInt32(reader[0].ToString());
            connection.Close();
        }
        catch
        {
            return -1;
        }
        return result;
    }
    public static string execQuery_getString(ref List<clsConnections> link_connections, string reglament_connections, string database, string query, int i = 0, int commandTimeout_ = 0)
    /* выполнение запроса c возвратом первого значения
     * при ошибке возвращает -1
     */
    {
        string result = null;
        try
        {
            string connection_name = "";
            if (reglament_connections == null || reglament_connections == String.Empty)
                connection_name = database;
            else
                get_stringSplitPos(ref connection_name, reglament_connections, ';', i);
            SqlConnection connection = new SqlConnection(link_connections.Find(x => x.name == connection_name).connectionString + ";database=" + database);
            SqlCommand command = new SqlCommand();
            command.CommandType = CommandType.Text;
            command.Connection = connection;
            command.CommandTimeout = commandTimeout_;
            command.CommandText = query;
            connection.Open();
            SqlDataReader reader = command.ExecuteReader();
            reader.Read();
            result = reader[0].ToString();
            connection.Close();
        }
        catch { }
        return result;
    }
    public static string execQuery_getString(string connection_string, string query, int commandTimeout_ = 0)
    /**/
    {
        string result = null;
        try
        {
            SqlConnection connection = new SqlConnection(connection_string);
            SqlCommand command = new SqlCommand();
            command.CommandType = CommandType.Text;
            command.Connection = connection;
            command.CommandTimeout = commandTimeout_;
            command.CommandText = query;
            connection.Open();
            SqlDataReader reader = command.ExecuteReader();
            reader.Read();
            result = reader[0].ToString();
            connection.Close();
        }
        catch { }
        return result;
    }
    #endregion mssql queries











    //----- Postgres region
    #region postgres queries

    public static bool execQuery_PGR(ref List<clsConnections> link_connections, string database, string query, int commandTimeout_ = 0, int i = 0)
    //выполнение запроса 
    {
        bool result = false;
        NpgsqlConnection connection = null;
        try
        {
            connection = new NpgsqlConnection(link_connections.Find(x => x.name == database).connectionString + ";database=" + database);
            NpgsqlCommand command = new NpgsqlCommand();
            command.CommandType = CommandType.Text;
            command.Connection = connection;
            command.CommandTimeout = commandTimeout_;
            connection.Open();
            command.CommandText = query;
            command.ExecuteNonQuery();
            return true;
        }
        catch { }
        if (connection.State == ConnectionState.Open) connection.Close();
        connection.Dispose();
        return result;
    }
    public static VarResult execQuery_PGR_varResult(ref List<clsConnections> link_connections, string database, string query, int commandTimeout_ = 0)
    //выполнение запроса 
    {
        VarResult varResult = new VarResult();
        NpgsqlConnection connection = null;
        try
        {
            connection = new NpgsqlConnection(link_connections.Find(x => x.name == database).connectionString + ";database=" + database);
            NpgsqlCommand command = new NpgsqlCommand(query, connection);
            command.CommandType = CommandType.Text;
            command.CommandTimeout = commandTimeout_;
            connection.Open();
            NpgsqlDataReader reader = command.ExecuteReader();
            reader.Read();
            if (reader.HasRows)
            {
                varResult.result = (bool)reader[0];
                varResult.comment = (string)reader[1];
                varResult.code = (int)reader[2];
            }
            else
            {
                varResult.result = false;
                varResult.comment = "Не получен ответ от БД на запрос";
                varResult.code = 0;
            }            
        }
        catch { }
        if(connection.State == ConnectionState.Open) connection.Close();
        connection.Dispose();
        return varResult;
    }

    public static int execQuery_PGR_function_bool(ref List<clsConnections> link_connections, string database, string query, int commandTimeout_ = 0)
    //выполнение запроса 
    {
        int result = -1;
        NpgsqlConnection connection = null;
        try
        {
            connection = new NpgsqlConnection(link_connections.Find(x => x.name == database).connectionString + ";database=" + database);
            NpgsqlCommand command = new NpgsqlCommand(query, connection);
            command.CommandType = CommandType.Text;
            command.CommandTimeout = commandTimeout_;
            connection.Open();
            NpgsqlDataReader reader = command.ExecuteReader();
            reader.Read();
            if ((bool) reader[0])
            {
                result = 1;
            }
            else
            {
                result = 0;
            }            
        }
        catch {}
        if (connection.State == ConnectionState.Open)
            connection.Close();
        connection.Dispose();
        return result;
    }
    public static bool execQuery_PGR_insertList(string connection_string, string query, List<string> list, int limit_transaction = 1)
    //выполнение запроса на вставку данных из списка
    //возвращает false при ошибке
    {
        bool result = false;
        if (list == null || list.Count() == 0) return result;
        NpgsqlConnection connection = null;
        try
        {
            connection = new NpgsqlConnection(connection_string);
            NpgsqlCommand cmd = new NpgsqlCommand();
            cmd.CommandType = CommandType.Text;
            cmd.Connection = connection;
            connection.Open();
            int count = 0;
            string values = string.Empty;
            int count_list = list.Count();
            for (int i = 0; i < count_list; i++)
            {
                ++count;
                if (count != 1) values += ",";
                values += " (" + list[i] + ")";
                if (count == limit_transaction || i + 1 == count_list)
                {
                    cmd.CommandText = query + values;
                    cmd.ExecuteNonQuery();
                    values = string.Empty;
                    count = 0;
                }
            }
            result = true;
        }
        catch
        { }
        if (connection.State == ConnectionState.Open) connection.Close();
        connection.Dispose();
        return result;
    }
    
    public static bool execQuery_PGR_insertList(ref List<clsConnections> link_connections, string database, string query, ref List<string[]> list, int limit_transaction = 1)
    //выполнение запроса на вставку данных из списка
    //возвращает false при ошибке
    {
        bool result = false;
        int count = 0;
        string values = string.Empty;
        int count_row = list.Count();
        string row_string;
        NpgsqlConnection connection = null;
        if (list == null || list.Count() == 0) return result;
        try
        {
            int count_row_i = list[0].Count();
            connection = new NpgsqlConnection(link_connections.Find(x => x.name == database).connectionString + ";database=" + database);
            NpgsqlCommand command = new NpgsqlCommand();
            command.CommandType = CommandType.Text;
            command.Connection = connection;
            connection.Open();
            int count_list = list.Count();
            for (int row = 0; row < count_row; ++row)
            {
                ++count;
                if (count != 1) values += ",";
                row_string = "";
                for (int row_i = 0; row_i < count_row_i; ++row_i)
                {
                    list[row][row_i] = (string.IsNullOrEmpty(list[row][row_i])) ? "null" : string_Apostrophe(list[row][row_i]);
                }
                values += " (" + string.Join(",", list[row]) + ")";
                if (count == limit_transaction || row + 1 == count_row)
                {
                    command.CommandText = query + values;
                    command.ExecuteNonQuery();
                    values = string.Empty;
                    count = 0;
                }
            }
            result = true;
        }            
        catch { }
        if (connection.State == ConnectionState.Open) connection.Close();
        connection.Dispose();
        return result;
    }



    public static string execQuery_PGR_getString(string connection_string, string query, int commandTimeout_ = 0)
    {
        string result = null;
        NpgsqlConnection connection = null;
        try
        {
            connection = new NpgsqlConnection(connection_string);
            NpgsqlCommand command = new NpgsqlCommand(query, connection);
            command.CommandType = CommandType.Text;
            command.Connection = connection;
            command.CommandTimeout = commandTimeout_;
            connection.Open();
            NpgsqlDataReader reader = command.ExecuteReader();
            reader.Read();
            result = reader[0].ToString();
        }
        catch { }
        finally
        {
            if (connection.State == ConnectionState.Open) connection.Close();
            connection.Dispose();
        }
  
        return result;
    }
    public static string execQuery_PGR_getString(ref List<clsConnections> link_connections, string database, string query, int commandTimeout_ = 0)
    {
        string result = null;
        NpgsqlConnection connection = null;
        try
        {
            connection = new NpgsqlConnection(link_connections.Find(x => x.name == database).connectionString + ";database=" + database);
            NpgsqlCommand command = new NpgsqlCommand(query, connection);
            command.CommandTimeout = commandTimeout_;
            connection.Open();
            NpgsqlDataReader reader = command.ExecuteReader();
            reader.Read();
            result = reader[0].ToString();
        }
        catch {/* Должно оставться result = null*/}
        finally
        {
            if (connection.State == ConnectionState.Open) connection.Close();
            connection.Dispose();
        }
        return result;
    }
    public static bool execQuery_PGR_Update(string connection_string, string query, int commandTimeout_ = 0)
    /* Postgres
     * выполнение запроса на обновление данных
     */
    {
        bool result = false;
        NpgsqlConnection connection = null;
        try
        {
            connection = new NpgsqlConnection(connection_string);
            NpgsqlCommand command = new NpgsqlCommand(query, connection);
            command.CommandTimeout = commandTimeout_;
            connection.Open();
            command.ExecuteNonQuery();
            result = true;
        }
        catch { }
        if (connection.State == ConnectionState.Open) connection.Close();
        connection.Dispose();
        return result;
    }
    public static bool execQuery_PGR_updateList(string connection_string, List<string> list, int limit_transaction = 1, int commandTimeout_ = 0)
    /* Postgres
     * Обновление данных списком запросов
     * В списке получаем готовые запросы на обновление
     */
    {
        bool result = false;

        int count = 0;
        string values = string.Empty;
        int count_row = list.Count();

        if (list == null || list.Count() == 0) return result;
        NpgsqlConnection connection = null;
        //NpgsqlTransaction sqlTransaction = null;
        try
        {
            connection = new NpgsqlConnection(connection_string);
            connection.Open();
            NpgsqlCommand sqlCommand = connection.CreateCommand();
            //sqlTransaction = connection.BeginTransaction(IsolationLevel.Chaos);
            sqlCommand.Connection = connection;
            sqlCommand.CommandTimeout = commandTimeout_;
            //sqlCommand.Transaction = sqlTransaction;

            for (int row = 0; row < count_row; ++row)
            {
                ++count;
                values += list[row] + ";";
                if (count == limit_transaction || row + 1 == count_row)
                {
                    sqlCommand.CommandText = values;
                    sqlCommand.ExecuteNonQuery();
                    values = string.Empty;
                    count = 0;
                }
            }
            //sqlTransaction.Commit();
            result = true;
        }
        catch { }
        if (connection.State == ConnectionState.Open) connection.Close();
        //sqlTransaction.Dispose();
        connection.Dispose();
        return result;
    }

    public static bool execQuery_PGR_updateList(ref List<clsConnections> link_connections, string reglament_connections, string database, ref List<string> list, int limit_transaction = 1, int commandTimeout_ = 0, int i = 0)
    /* Postgres
     * Обновление данных списком запросов
     * В списке получаем готовые запросы на обновление
     */
    {
        bool result = false;

        int count = 0;
        string values = string.Empty;
        int count_row = list.Count();

        if (list == null || list.Count() == 0) return result;
        NpgsqlConnection connection = null;
        //NpgsqlTransaction sqlTransaction = null;
        try
        {
            string connection_name = "";
            if (reglament_connections == null || reglament_connections == String.Empty)
                connection_name = database;
            else
                get_stringSplitPos(ref connection_name, reglament_connections, ';', i);
            connection = new NpgsqlConnection(link_connections.Find(x => x.name == connection_name).connectionString + ";database=" + database);
            connection.Open();
            //NpgsqlCommand command = new NpgsqlCommand(query, connection);
            NpgsqlCommand sqlCommand = connection.CreateCommand();
            //sqlTransaction = connection.BeginTransaction(IsolationLevel.Chaos);
            sqlCommand.Connection = connection;
            sqlCommand.CommandTimeout = commandTimeout_;
            //sqlCommand.Transaction = sqlTransaction;

            for (int row = 0; row < count_row; ++row)
            {
                ++count;
                values += list[row] + ";";
                if (count == limit_transaction || row + 1 == count_row)
                {
                    sqlCommand.CommandText = values;
                    sqlCommand.ExecuteNonQuery();
                    values = string.Empty;
                    count = 0;
                }
            }
            //sqlTransaction.Commit();
            result = true;
        }
        catch (Exception ex)
        {
            string str = ex.Message;
        }
        if (connection.State == ConnectionState.Open) connection.Close();
        //sqlTransaction.Dispose();
        connection.Dispose();
        return result;
    }

    public static VarResult execQuery_PGR_updateList_varResult(ref List<clsConnections> link_connections, string reglament_connections, string database, ref List<string> list, int limit_transaction = 1, int commandTimeout_ = 0, int i = 0)
    /* Postgres
     * Обновление данных списком запросов
     * В списке получаем готовые запросы на обновление
     */
    {
        VarResult result = new VarResult(false,"",0);

        int count = 0;
        string values = string.Empty;
        int count_row = list.Count();

        if (list == null || list.Count() == 0)
        {
            result.result = true;
            result.comment = "список пуст";
            return result;
        }
        NpgsqlConnection connection = null;
        //NpgsqlTransaction sqlTransaction = null;
        try
        {
            string connection_name = "";
            if (reglament_connections == null || reglament_connections == String.Empty)
                connection_name = database;
            else
                get_stringSplitPos(ref connection_name, reglament_connections, ';', i);
            connection = new NpgsqlConnection(link_connections.Find(x => x.name == connection_name).connectionString + ";database=" + database);
            connection.Open();
            //NpgsqlCommand command = new NpgsqlCommand(query, connection);
            NpgsqlCommand sqlCommand = connection.CreateCommand();
            //sqlTransaction = connection.BeginTransaction(IsolationLevel.Chaos);
            sqlCommand.Connection = connection;
            sqlCommand.CommandTimeout = commandTimeout_;
            //sqlCommand.Transaction = sqlTransaction;

            for (int row = 0; row < count_row; ++row)
            {
                ++count;
                values += list[row] + ";";
                if (count == limit_transaction || row + 1 == count_row)
                {
                    sqlCommand.CommandText = values;
                    sqlCommand.ExecuteNonQuery();
                    values = string.Empty;
                    count = 0;
                }
            }
            //sqlTransaction.Commit();
            result.result = true;
            result.comment = "Ok";
        }
        catch (Exception ex)
        {
            result.comment = ex.Message;
        }
        if (connection.State == ConnectionState.Open) connection.Close();
        //sqlTransaction.Dispose();
        connection.Dispose();
        return result;
    }
    public static int execQuery_PGR_getInt(ref List<clsConnections> link_connections, string reglament_connections, string database, string query, int commandTimeout_ = 0, char symbol = ';', int i = 0)
    /* POSTGRES
     * выполнение запроса c возвратом первого значения
     * при ошибке возвращает -1
     */
    {
        int result = 1;
        NpgsqlConnection connection = null;
        try
        {
            string connection_name = "";
            get_stringSplitPos(ref connection_name, reglament_connections, symbol, i);
            connection = new NpgsqlConnection(link_connections.Find(x => x.name == connection_name).connectionString + ";database=" + database);
            NpgsqlCommand command = new NpgsqlCommand(query, connection);
            command.CommandTimeout = commandTimeout_;
            connection.Open();
            NpgsqlDataReader reader = command.ExecuteReader();
            reader.Read();
            result = Convert.ToInt32(reader[0].ToString());
        }
        catch
        {
            result = -1;
        }
        if (connection.State == ConnectionState.Open) connection.Close();
        connection.Dispose();
        return result;
    }

    public static bool ExecQurey_PGR_GetListStrings(string connection_string, string query, ref List<string> list, int commandTimeout_ = 0, string symbol = "")
    {
        bool result = false;
        NpgsqlConnection connection = null;
        try
        {
           connection = new NpgsqlConnection(connection_string);
            NpgsqlCommand command = new NpgsqlCommand(query, connection);
            command.CommandTimeout = commandTimeout_;
            connection.Open();
            NpgsqlDataReader reader = command.ExecuteReader();
            if (reader.HasRows)
            {
                reader.Read();
                do
                {
                    string row = string.Empty;
                    for (int i = 0; i < reader.FieldCount; i++)
                    {
                        if (row != string.Empty) row = row + ",";
                        row += symbol + reader[i].ToString() + symbol;
                    }
                    list.Add(row);
                }
                while (reader.Read());
            }
            result = true;
        }
        catch {}
        if (connection.State == ConnectionState.Open) 
            connection.Close();
        connection.Dispose();
        return result;
    }

    public static bool ExecQurey_PGR_GetListStrings(string connection_string, string query, ref List<string[]> list, int commandTimeout_ = 0, string symbol = "")
    {
        bool result = false;
        NpgsqlConnection connection = null;
        try
        {
            connection = new NpgsqlConnection(connection_string);
            NpgsqlCommand command = new NpgsqlCommand(query, connection);
            command.CommandTimeout = commandTimeout_;
            connection.Open();
            NpgsqlDataReader reader = command.ExecuteReader();
            if (reader.HasRows)
            {
                reader.Read();
                do
                {
                    string[] row = new string[reader.FieldCount];
                    for (int i = 0; i < row.Length; i++)
                    {
                        row[i] = symbol + reader[i].ToString() + symbol;
                    }
                    list.Add(row);
                }
                while (reader.Read());
            }
            result = true;
        }
        catch { }
        if (connection.State == ConnectionState.Open) connection.Close();
        connection.Dispose();
        return result;
    }


    public static bool ExecQurey_PGR_GetListStrings(ref List<clsConnections> link_connections, string reglament_connections, string database, string query, ref List<string[]> list, int commandTimeout_ = 0, int i = 0)
    {
        bool result = false;
        NpgsqlConnection connection = null;
        try
        {
            string connection_name = "";
            if (reglament_connections == null || reglament_connections == String.Empty)
                connection_name = database;
            else
                get_stringSplitPos(ref connection_name, reglament_connections, ';', i);
            connection = new NpgsqlConnection(link_connections.Find(x => x.name == connection_name).connectionString + ";database=" + database);
            NpgsqlCommand command = new NpgsqlCommand(query, connection);
            command.CommandTimeout = commandTimeout_;
            connection.Open();
            NpgsqlDataReader reader = command.ExecuteReader();
            if (reader.HasRows)
            {
                reader.Read();
                do
                {
                    string[] row = new string[reader.FieldCount];
                    for (int id = 0; id < row.Length; id++)
                    {
                        row[id] = reader[id].ToString();
                    }
                    list.Add(row);
                }
                while (reader.Read());
            }
            result = true;
        }
        catch { }
        if (connection.State == ConnectionState.Open) connection.Close();
        connection.Dispose();
        return result;
    }


    public static bool execQuery_PGR_insertList(ref List<clsConnections> link_connections, string database, string query, List<string> list, int limit_transaction = 1)
    //выполнение запроса на вставку данных из списка
    //возвращает false при ошибке
    {
        bool result = false;
        NpgsqlConnection connection = null;
        if (list == null || list.Count() == 0) return result;
        try
        {
            connection = new NpgsqlConnection(link_connections.Find(x => x.name == database).connectionString + ";database=" + database);
            NpgsqlCommand command = new NpgsqlCommand();
            command.CommandType = CommandType.Text;
            command.Connection = connection;
            connection.Open();
            int count = 0;
            string values = string.Empty;
            int count_list = list.Count();
            for (int i = 0; i < count_list; i++)
            {
                ++count;
                if (count != 1) values += ",";
                values += " (" + list[i] + ")";
                if (count == limit_transaction || i + 1 == count_list)
                {
                    command.CommandText = query + values;
                    command.ExecuteNonQuery();
                    values = string.Empty;
                    count = 0;
                }
            }
            result = true;
        }
        catch{}
        if (connection.State == ConnectionState.Open) connection.Close();
        connection.Dispose();
        return result;
    }

    public static string execQuery_PGR_getString(ref List<clsConnections> link_connections, string reglament_connections, string database, string query, int commandTimeout_ = 0, int i = 0)
    {
        string result = null;
        NpgsqlConnection connection = null;
        try
        {
            string connection_name = "";
            if (reglament_connections == null || reglament_connections == String.Empty)
                connection_name = database;
            else
                get_stringSplitPos(ref connection_name, reglament_connections, ';', i);
            connection = new NpgsqlConnection(link_connections.Find(x => x.name == connection_name).connectionString + ";database=" + database);
            NpgsqlCommand command = new NpgsqlCommand(query, connection);
            command.CommandType = CommandType.Text;
            command.Connection = connection;
            command.CommandTimeout = commandTimeout_;
            connection.Open();
            NpgsqlDataReader reader = command.ExecuteReader();
            reader.Read();
            result = reader[0].ToString();
        }
        catch { }
        if (connection.State == ConnectionState.Open) connection.Close();
        connection.Dispose();
        return result;
    }



    #endregion
}
//------- XML
public static partial class XmlHelper
{
    public static readonly XmlSerializerNamespaces Namespaces = new XmlSerializerNamespaces();
    static XmlHelper()
    {
        //Namespaces.Add("ns1", "http://ffoms.ru/GetInsuredRenderedMedicalServices/1.0.0");
        //Namespaces.Add("q1", "urn://x-artefacts-smev-gov-ru/supplementary/commons/1.2");
    }

    public static string SerializeTo<T>(this T xmlObject, bool useNamespaces = false)
    {
        XmlSerializer xmlSerializer = new XmlSerializer(typeof(T));
        MemoryStream memoryStream = new MemoryStream();
        XmlTextWriter xmlTextWriter = new XmlTextWriter(memoryStream, Encoding.UTF8);
        xmlTextWriter.Formatting = Formatting.Indented;

        if (useNamespaces)
        {
            xmlSerializer.Serialize(xmlTextWriter, xmlObject, Namespaces);
        }
        else
            xmlSerializer.Serialize(xmlTextWriter, xmlObject, new System.Xml.Serialization.XmlSerializerNamespaces(new XmlQualifiedName[] { new XmlQualifiedName(string.Empty) }));

        string output = Encoding.UTF8.GetString(memoryStream.ToArray());
        string _byteOrderMarkUtf8 = Encoding.UTF8.GetString(Encoding.UTF8.GetPreamble());
        if (output.StartsWith(_byteOrderMarkUtf8))
        {
            output = output.Remove(0, _byteOrderMarkUtf8.Length);
        }

        return output;
    }
    public static T DeserializeFrom<T>(this XmlElement xml) where T : new()
    {
        T xmlObject = new T();
        try
        {
            XmlSerializer xmlSerializer = new XmlSerializer(typeof(T));
            StringReader stringReader = new StringReader(xml.OuterXml);
            xmlObject = (T)xmlSerializer.Deserialize(stringReader);
        }
        catch
        {
        }
        return xmlObject;
    }



    public static string SerializeClear<T>(this T xmlObject)
    //автор: Jon Nolen
    {
        if (xmlObject == null) return null;
        XmlSerializer xs = null;

        //These are the objects that will free us from extraneous markup.
        XmlWriterSettings settings = null;
        XmlSerializerNamespaces ns = null;

        //We use a XmlWriter instead of a StringWriter.
        XmlWriter xw = null;

        String outString = null;

        try
        {
            //To get rid of the xml declaration we create an 
            //XmlWriterSettings object and tell it to OmitXmlDeclaration.
            settings = new XmlWriterSettings();
            settings.OmitXmlDeclaration = true;

            //To get rid of the default namespaces we create a new
            //set of namespaces with one empty entry.
            ns = new XmlSerializerNamespaces();
            ns.Add("", "");

            StringBuilder sb = new StringBuilder();

            xs = new XmlSerializer(typeof(T));

            //We create a new XmlWriter with the previously created settings 
            //(to OmitXmlDeclaration).
            xw = XmlWriter.Create(sb, settings);

            //We call xs.Serialize and pass in our custom 
            //XmlSerializerNamespaces object.
            xs.Serialize(xw, xmlObject, ns);

            xw.Flush();

            outString = sb.ToString();
        }
        catch { }
        finally
        {
            if (xw != null)
            {
                xw.Close();
            }
        }
        return outString;
    }


}

public class sf_schema
// Протоколы прикладной обработки 
{
    /// <remarks/>
    [System.CodeDom.Compiler.GeneratedCodeAttribute("xsd", "4.0.30319.33440")]
    [System.SerializableAttribute()]
    [System.Diagnostics.DebuggerStepThroughAttribute()]
    [System.ComponentModel.DesignerCategoryAttribute("code")]
    [System.Xml.Serialization.XmlTypeAttribute(AnonymousType = true)]
    [System.Xml.Serialization.XmlRootAttribute(Namespace = "", IsNullable = false)]
    public partial class FLK_P
    {

        private HEADER hEADERField;

        private FLK_PRES[] rESField;

        /// <remarks/>
        public HEADER HEADER
        {
            get
            {
                return this.hEADERField;
            }
            set
            {
                this.hEADERField = value;
            }
        }

        /// <remarks/>
        [System.Xml.Serialization.XmlElementAttribute("RES")]
        public FLK_PRES[] RES
        {
            get
            {
                return this.rESField;
            }
            set
            {
                this.rESField = value;
            }
        }
    }

    /// <remarks/>
    [System.CodeDom.Compiler.GeneratedCodeAttribute("xsd", "4.0.30319.33440")]
    [System.SerializableAttribute()]
    [System.Diagnostics.DebuggerStepThroughAttribute()]
    [System.ComponentModel.DesignerCategoryAttribute("code")]
    [System.Xml.Serialization.XmlTypeAttribute(AnonymousType = true)]
    public partial class HEADER
    {

        private string vERSField;

        private string fNAMEField;

        private string fNAME_1Field;

        private string nRECORDSField;

        private string fAILSField;

        private string rESField;

        /// <remarks/>
        public string VERS
        {
            get
            {
                return this.vERSField;
            }
            set
            {
                this.vERSField = value;
            }
        }

        /// <remarks/>
        public string FNAME
        {
            get
            {
                return this.fNAMEField;
            }
            set
            {
                this.fNAMEField = value;
            }
        }

        /// <remarks/>
        public string FNAME_1
        {
            get
            {
                return this.fNAME_1Field;
            }
            set
            {
                this.fNAME_1Field = value;
            }
        }

        /// <remarks/>
        public string NRECORDS
        {
            get
            {
                return this.nRECORDSField;
            }
            set
            {
                this.nRECORDSField = value;
            }
        }

        /// <remarks/>
        public string FAILS
        {
            get
            {
                return this.fAILSField;
            }
            set
            {
                this.fAILSField = value;
            }
        }

        /// <remarks/>
        public string RES
        {
            get
            {
                return this.rESField;
            }
            set
            {
                this.rESField = value;
            }
        }
    }

    /// <remarks/>
    [System.CodeDom.Compiler.GeneratedCodeAttribute("xsd", "4.0.30319.33440")]
    [System.SerializableAttribute()]
    [System.Diagnostics.DebuggerStepThroughAttribute()]
    [System.ComponentModel.DesignerCategoryAttribute("code")]
    [System.Xml.Serialization.XmlTypeAttribute(AnonymousType = true)]
    public partial class FLK_PRES
    {

        private string rESULField;

        private string nRECField;

        private string cOMMENTField;

        /// <remarks/>
        public string RESUL
        {
            get
            {
                return this.rESULField;
            }
            set
            {
                this.rESULField = value;
            }
        }

        /// <remarks/>
        public string NREC
        {
            get
            {
                return this.nRECField;
            }
            set
            {
                this.nRECField = value;
            }
        }

        /// <remarks/>
        public string COMMENT
        {
            get
            {
                return this.cOMMENTField;
            }
            set
            {
                this.cOMMENTField = value;
            }
        }
    }
}
public class sp_schema
//Протоколы ФЛК
{
    [System.CodeDom.Compiler.GeneratedCodeAttribute("xsd", "4.0.30319.33440")]
    [System.SerializableAttribute()]
    [System.Diagnostics.DebuggerStepThroughAttribute()]
    [System.ComponentModel.DesignerCategoryAttribute("code")]
    [System.Xml.Serialization.XmlTypeAttribute(AnonymousType = true)]
    [System.Xml.Serialization.XmlRootAttribute(Namespace = "", IsNullable = false)]
    public partial class FLK_P
    {

        private HEADER hEADERField;

        private FLK_POSH[] oSHField;

        /// <remarks/>
        public HEADER HEADER
        {
            get
            {
                return this.hEADERField;
            }
            set
            {
                this.hEADERField = value;
            }
        }

        /// <remarks/>
        [System.Xml.Serialization.XmlElementAttribute("OSH")]
        public FLK_POSH[] OSH
        {
            get
            {
                return this.oSHField;
            }
            set
            {
                this.oSHField = value;
            }
        }
    }

    /// <remarks/>
    [System.CodeDom.Compiler.GeneratedCodeAttribute("xsd", "4.0.30319.33440")]
    [System.SerializableAttribute()]
    [System.Diagnostics.DebuggerStepThroughAttribute()]
    [System.ComponentModel.DesignerCategoryAttribute("code")]
    [System.Xml.Serialization.XmlTypeAttribute(AnonymousType = true)]
    public partial class HEADER
    {

        private string vERSField;

        private string fNAMEField;

        private string fNAME_1Field;

        private string nRECORDSField;

        /// <remarks/>
        public string VERS
        {
            get
            {
                return this.vERSField;
            }
            set
            {
                this.vERSField = value;
            }
        }

        /// <remarks/>
        public string FNAME
        {
            get
            {
                return this.fNAMEField;
            }
            set
            {
                this.fNAMEField = value;
            }
        }

        /// <remarks/>
        public string FNAME_1
        {
            get
            {
                return this.fNAME_1Field;
            }
            set
            {
                this.fNAME_1Field = value;
            }
        }

        /// <remarks/>
        public string NRECORDS
        {
            get
            {
                return this.nRECORDSField;
            }
            set
            {
                this.nRECORDSField = value;
            }
        }
    }

    /// <remarks/>
    [System.CodeDom.Compiler.GeneratedCodeAttribute("xsd", "4.0.30319.33440")]
    [System.SerializableAttribute()]
    [System.Diagnostics.DebuggerStepThroughAttribute()]
    [System.ComponentModel.DesignerCategoryAttribute("code")]
    [System.Xml.Serialization.XmlTypeAttribute(AnonymousType = true)]
    public partial class FLK_POSH
    {
        public FLK_POSH(string nRECField, string oSHIBField, string cOMMENTField)
        {
            NREC = nRECField;
            OSHIB = oSHIBField;
            COMMENT = cOMMENTField;
        }

        public FLK_POSH()
        {
            // TODO: Complete member initialization
        }

        private string nRECField;

        private string oSHIBField;

        private string cOMMENTField;

        /// <remarks/>
        public string NREC
        {
            get
            {
                return this.nRECField;
            }
            set
            {
                this.nRECField = value;
            }
        }

        /// <remarks/>
        public string OSHIB
        {
            get
            {
                return this.oSHIBField;
            }
            set
            {
                this.oSHIBField = value;
            }
        }

        /// <remarks/>
        public string COMMENT
        {
            get
            {
                return this.cOMMENTField;
            }
            set
            {
                this.cOMMENTField = value;
            }
        }
    }
}
public abstract class XML_abstract_file
{
    protected internal System.IO.FileInfo fileInfo;

    [System.Xml.Serialization.XmlIgnoreAttribute()]
    public List<object> _Errors = new List<object>();
    [System.Xml.Serialization.XmlIgnoreAttribute()]
    public string xmlNameSpaces = "";
    public string mnemonics = string.Empty;

    public int Count { get { return _Errors.Count; } }
    protected int MaxErr = 1000;
    protected uint n_Errors = 0;
    static bool _flk_check = false;

    public void SetFileInfo(System.IO.FileInfo _fileInfo)
    {
        fileInfo = _fileInfo;
    }

    public bool flk_check()
    {
        return _flk_check;
    }
    public bool SaveXML(string fileName)
    {
        bool result = true;
        FileStream fileStream = new System.IO.FileStream(fileName, FileMode.Create);
        try
        {
            System.Xml.Serialization.XmlSerializer writer =
                new System.Xml.Serialization.XmlSerializer(GetType());
            #region Перекодировка из UTF-8 в windows 1251
            System.IO.StreamWriter file = new System.IO.StreamWriter(fileStream, Encoding.GetEncoding(1251));
            if (xmlNameSpaces == "")
            {
                System.Xml.Serialization.XmlSerializerNamespaces nameSpaces = new System.Xml.Serialization.XmlSerializerNamespaces();
                nameSpaces.Add("", "");
                writer.Serialize(file, this, nameSpaces);
            }
            else writer.Serialize(file, this);

            #endregion
        }
        catch
        {
            result = false;
        }
        fileStream.Close();
        return result;

    }
    public MemoryStream XML_memoryStream
    {
        get
        {
            MemoryStream result = new System.IO.MemoryStream();
            System.Xml.Serialization.XmlSerializer writer =
                new System.Xml.Serialization.XmlSerializer(GetType());
            #region Перекодировка из UTF-8 в windows 1251
            System.IO.StreamWriter file = new System.IO.StreamWriter(result, Encoding.GetEncoding(1251));
            if (xmlNameSpaces == "")
            {
                System.Xml.Serialization.XmlSerializerNamespaces nameSpaces = new System.Xml.Serialization.XmlSerializerNamespaces();
                nameSpaces.Add("", "");

                writer.Serialize(file, this, nameSpaces);
            }
            else
            {
                writer.Serialize(file, this);
            }
            result.Position = 0;
            #endregion
            return result;
        }
    }
    private void ValidationEventHandler(object sender, ValidationEventArgs e)
    {
        _Errors.Add(new Protocol(
            n_Errors
            , "Строка " + e.Exception.LineNumber.ToString() + " Столбец " + e.Exception.LinePosition.ToString() + "\n" + e.Message
            ));
    }
    private void Serializer_UnknownNode(object sender, XmlNodeEventArgs e)
    {
        _Errors.Add(new Protocol(n_Errors, "Неизвестный узел:" + e.Name + "\t" + e.Text));
    }
    private void Serializer_UnknownAttribute(object sender, XmlAttributeEventArgs e)
    {
        _Errors.Add(new Protocol(n_Errors, "Неизвестный  атрибут " + e.Attr.Name + "='" + e.Attr.Value + "'"));
    }
    private void Serializer_UnknownElement(object sender, XmlElementEventArgs e)
    {
        _Errors.Add(new Protocol(n_Errors, "Неизвестный эллемент " + e.Element.Name + "='" + e.Element.InnerXml + "'"));
    }
    /*private void Serializer_UnreferencedObject(object sender, UnreferencedObjectEventArgs e)
    {
        _Errors.Add(new Protocol(n_Errors, "Неверный тип " + e.UnreferencedObject + "='" + e.UnreferencedId + "'"));
        Console.WriteLine("UnreferencedObject:");
        Console.WriteLine("ID: " + e.UnreferencedId);
        Console.WriteLine("UnreferencedObject: " + e.UnreferencedObject);
        //Console.WriteLine("License: " + myVehicle.licenseNumber);
    }*/
    public static object FromXml(Stream xmlMemory, Type ObjType, string schemaFile, string _mnemonics = null)
    {
        Boolean result = true;
        object obj = Activator.CreateInstance(ObjType);
        try
        {
            if (result)
            {
                if (xmlMemory != null && xmlMemory.Length > 0)
                {
                    XmlReaderSettings settings = new XmlReaderSettings { ValidationType = ValidationType.Schema };
                    settings.ValidationFlags |= XmlSchemaValidationFlags.None; // ReportValidationWarnings;
                    settings.ValidationEventHandler += new ValidationEventHandler((obj as XML_abstract_file).ValidationEventHandler);
                    XmlSchemaSet xmlSchemaSet;
                    xmlSchemaSet = new XmlSchemaSet();
                    xmlSchemaSet.Add((obj as XML_abstract_file).xmlNameSpaces, new XmlTextReader(schemaFile));// new MemoryStream(ObjXsdVal)));
                    settings.Schemas = xmlSchemaSet;
                    (obj as XML_abstract_file).mnemonics = _mnemonics;
                    (obj as XML_abstract_file).n_Errors = 901;

                    xmlMemory.Position = 0;
                    XmlReader reader;
                    reader = XmlReader.Create(xmlMemory, settings);
                    while (reader.Read() && (obj as XML_abstract_file)._Errors.Count <= (obj as XML_abstract_file).MaxErr) ;
                    result = (obj as XML_abstract_file)._Errors.Count == 0;
                }
                if (result)
                {
                    _flk_check = true;
                    xmlMemory.Position = 0;
                    System.Xml.Serialization.XmlSerializer reader = new System.Xml.Serialization.XmlSerializer(ObjType);
                    /*
                    reader.UnknownNode += new XmlNodeEventHandler((obj as XML_abstract_file).Serializer_UnknownNode);
                    reader.UnknownAttribute += new XmlAttributeEventHandler((obj as XML_abstract_file).Serializer_UnknownAttribute);
                    reader.UnknownElement += new XmlElementEventHandler((obj as XML_abstract_file).Serializer_UnknownElement);
                    */
                    //reader.UnreferencedObject += new UnreferencedObjectEventHandler((obj as XML_abstract_file).Serializer_UnreferencedObject);
                    if (result) obj = reader.Deserialize(xmlMemory);
                }
            }
            else
            {
                (obj as XML_abstract_file)._Errors.Add(new Protocol(901, "Размер XML файла равен 0"));
            }
        }
        catch (System.Xml.XmlException e)
        {
            result = false;
            (obj as XML_abstract_file)._Errors.Add(new Protocol((obj as XML_abstract_file).n_Errors, e.Message));
        }

        return obj;
    }

}
public partial class Protocol
{

    private uint oSHIBField;

    private string iM_POLField;

    private string bAS_POLField;

    private string n_RECField;

    private string cOMMENTField;

    /// <remarks/>
    [System.Xml.Serialization.XmlElementAttribute(Form = System.Xml.Schema.XmlSchemaForm.Qualified)]
    public uint OSHIB
    {
        get
        {
            return oSHIBField;
        }
        set
        {
            oSHIBField = value;
        }
    }

    /// <remarks/>
    [System.Xml.Serialization.XmlElementAttribute(Form = System.Xml.Schema.XmlSchemaForm.Qualified)]
    public string IM_POL
    {
        get
        {
            return iM_POLField;
        }
        set
        {
            iM_POLField = value;
        }
    }

    /// <remarks/>
    [System.Xml.Serialization.XmlElementAttribute(Form = System.Xml.Schema.XmlSchemaForm.Qualified)]
    public string BAS_POL
    {
        get
        {
            return bAS_POLField;
        }
        set
        {
            bAS_POLField = value;
        }
    }

    /// <remarks/>
    [System.Xml.Serialization.XmlElementAttribute(Form = System.Xml.Schema.XmlSchemaForm.Qualified)]
    public string N_REC
    {
        get
        {
            return n_RECField;
        }
        set
        {
            n_RECField = value;
        }
    }

    /// <remarks/>
    [System.Xml.Serialization.XmlElementAttribute(Form = System.Xml.Schema.XmlSchemaForm.Qualified)]
    public string COMMENT
    {
        get
        {
            return cOMMENTField;
        }
        set
        {
            cOMMENTField = value;
        }
    }

    public Protocol()
    { }
    public Protocol(uint PoSHIB, string PcOMMENT/*, string N_REC, string IM_POL*/)
    {
        oSHIBField = PoSHIB;
        cOMMENTField = PcOMMENT;
    }
    public string spToString()
    {
        string otv;
        otv =
            oSHIBField.ToString() + ":" +
            bAS_POLField != null ? BAS_POL : "" + ":" +
            iM_POLField != null ? IM_POL : "" + ":" +
            n_RECField != null ? N_REC : "" + ":" +
            cOMMENTField != null ? COMMENT : ""
            ;
        return otv;
    }

}
