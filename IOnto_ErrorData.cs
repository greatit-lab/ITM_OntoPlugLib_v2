// Onto_ErrorDataLib/IOnto_ErrorData.cs
using System;
using System.Collections.Generic;
using System.Collections.Concurrent;
using System.Data;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading;
using Npgsql;
using ConnectInfo;
using ITM_Agent.Services;

namespace Onto_ErrorDataLib
{
    /*──────────────────────── Logger ───────────────────────*/
    internal static class SimpleLogger
    {
        private static volatile bool _debugEnabled = false;
        public static void SetDebug(bool enabled) { _debugEnabled = enabled; }

        private static readonly object _sync = new object();
        private static readonly string _dir = System.IO.Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "Logs");
        private static string PathOf(string sfx) => System.IO.Path.Combine(_dir, $"{DateTime.Now:yyyyMMdd}_{sfx}.log");

        private static void Write(string s, string m)
        {
            try
            {
                lock (_sync)
                {
                    System.IO.Directory.CreateDirectory(_dir);
                    var line = $"{DateTime.Now:yyyy-MM-dd HH:mm:ss.fff} [ErrorData] {m}{Environment.NewLine}";
                    System.IO.File.AppendAllText(PathOf(s), line, System.Text.Encoding.UTF8);
                }
            }
            catch { /* 로깅 실패 무시 */ }
        }

        public static void Event(string m) { Write("event", m); }
        public static void Error(string m) { Write("error", m); }
        public static void Debug(string m)
        {
            if (_debugEnabled) Write("debug", m);
        }
    }

    public interface IOnto_ErrorData
    {
        string PluginName { get; }
        void ProcessAndUpload(string filePath, object arg1 = null, object arg2 = null);
    }

    public class Onto_ErrorData : IOnto_ErrorData
    {
        private static readonly Dictionary<string, long> _lastLen =
            new Dictionary<string, long>(StringComparer.OrdinalIgnoreCase);

        // 충돌 방지를 위한 지연 처리(Debounce) 큐 및 타이머
        private static readonly ConcurrentDictionary<string, string> _pendingEqpids = new ConcurrentDictionary<string, string>(StringComparer.OrdinalIgnoreCase);
        private static readonly ConcurrentDictionary<string, DateTime> _pendingProcessTime = new ConcurrentDictionary<string, DateTime>(StringComparer.OrdinalIgnoreCase);
        private static Timer _batchTimer;
        private static int _isProcessing = 0;

        private readonly string _pluginName = "Onto_ErrorData";
        public string PluginName { get { return _pluginName; } }

        public string DefaultTaskName => "Error";
        public string DefaultFileFilter => "*Error.dat";

        static Onto_ErrorData()
        {
            #if NETCOREAPP || NET5_0_OR_GREATER
                Encoding.RegisterProvider(CodePagesEncodingProvider.Instance);
            #endif
            // 10초 주기로 지연된 파일이 있는지 검사하는 백그라운드 타이머 작동
            _batchTimer = new Timer(OnBatchTimer, null, 10000, 10000);
        }

        #region === Public API ===

        public void ProcessAndUpload(string filePath, object arg1 = null, object arg2 = null)
        {
            string eqpid = GetEqpidFromSettings(arg1 as string ?? "Settings.ini");
            _pendingEqpids[filePath] = eqpid;

            // ⭐️ [변경 핵심] TryAdd를 인덱서(=)로 변경하여 Sliding Window(타이머 리셋) 구현
            // 장비가 계속 에러를 쓰면서 이벤트를 발생시키면, 대기 시간이 계속 30초 뒤로 밀려납니다.
            // 즉, 장비가 쓰기를 "완전히 멈추고 30초가 지났을 때만" 파일에 접근합니다.
            _pendingProcessTime[filePath] = DateTime.Now.AddSeconds(30);

            SimpleLogger.Debug($"Event received. Timer reset. File queued for processing (waiting for 30s idle): {Path.GetFileName(filePath)}");
        }

        #endregion

        #region === Batch Processing Core ===

        private static void OnBatchTimer(object state)
        {
            if (Interlocked.CompareExchange(ref _isProcessing, 1, 0) != 0) return;

            try
            {
                var now = DateTime.Now;
                var filesToProcess = new List<string>();

                foreach (var kvp in _pendingProcessTime)
                {
                    // 예약된 30초가 완전히 경과한(장비가 조용해진) 파일들만 추출
                    if (now >= kvp.Value)
                    {
                        filesToProcess.Add(kvp.Key);
                    }
                }

                foreach (var file in filesToProcess)
                {
                    _pendingProcessTime.TryRemove(file, out _);
                    if (_pendingEqpids.TryRemove(file, out string eqpid))
                    {
                        ProcessFileActual(file, eqpid);
                    }
                }
            }
            finally
            {
                Interlocked.Exchange(ref _isProcessing, 0);
            }
        }

        private static void ProcessFileActual(string filePath, string eqpid)
        {
            SimpleLogger.Event("Process (Batch/Incremental) ▶ " + filePath);

            long prevLen = 0;
            long currLen = 0;
            string[] addedLines = null;
            string[] allLinesForMeta = null;

            byte[] fileBuffer = null;
            long bytesToRead = 0;

            int maxRetries = 2;
            int delayMs = 100;
            bool fileReadSuccess = false;

            for (int i = 0; i < maxRetries; i++)
            {
                try
                {
                    lock (_lastLen)
                    {
                        _lastLen.TryGetValue(filePath, out prevLen);
                    }

                    // 파일 핸들(Lock)을 쥐기 전에 OS 파일 시스템(MFT) 정보만 읽어 크기 계산
                    var fi = new FileInfo(filePath);
                    if (!fi.Exists)
                    {
                        SimpleLogger.Debug("File disappeared: " + filePath);
                        lock (_lastLen) { _lastLen.Remove(filePath); }
                        return;
                    }
                    currLen = fi.Length;

                    if (currLen == prevLen && prevLen > 0)
                    {
                        SimpleLogger.Debug("File length unchanged, skipping incremental process: " + filePath);
                        return;
                    }

                    if (currLen < prevLen)
                    {
                        SimpleLogger.Event("File truncated (Size decreased). Resetting offset: " + filePath);
                        prevLen = 0;
                    }

                    bytesToRead = currLen - prevLen;
                    if (bytesToRead > 0)
                    {
                        fileBuffer = new byte[bytesToRead];

                        // 가장 빠른 SequentialScan 옵션 적용, 열자마자 메모리로 들이마시고 즉각 블록 탈출
                        using (var fs = new FileStream(filePath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite | FileShare.Delete, 4096, FileOptions.SequentialScan))
                        {
                            fs.Seek(prevLen, SeekOrigin.Begin);

                            int totalRead = 0;
                            while (totalRead < bytesToRead)
                            {
                                int read = fs.Read(fileBuffer, totalRead, (int)bytesToRead - totalRead);
                                if (read == 0) break;
                                totalRead += read;
                            }
                        }
                    }

                    fileReadSuccess = true;
                    break;
                }
                catch (IOException ioEx) when (i < maxRetries - 1)
                {
                    SimpleLogger.Debug($"File locked by equipment, retry {i + 1}: {ioEx.Message}");
                    Thread.Sleep(delayMs);
                }
                catch (Exception ex)
                {
                    SimpleLogger.Error($"Error reading file {filePath} on attempt {i + 1}: {ex.Message}");
                    if (i == maxRetries - 1) return;
                    Thread.Sleep(delayMs);
                }
            }

            if (!fileReadSuccess)
            {
                // 장비가 파일을 강하게 물고 있으면, 큐에 다시 밀어넣어 10초 뒤에 재시도
                SimpleLogger.Error($"File is heavily locked by equipment. Yielding and re-queuing for later: {filePath}");
                _pendingEqpids[filePath] = eqpid;
                _pendingProcessTime[filePath] = DateTime.Now.AddSeconds(10);
                return;
            }

            try
            {
                if (fileBuffer != null && bytesToRead > 0)
                {
                    string textData = Encoding.GetEncoding(949).GetString(fileBuffer);

                    if (prevLen == 0)
                    {
                        allLinesForMeta = textData.Split(new[] { "\r\n", "\n" }, StringSplitOptions.RemoveEmptyEntries);
                        addedLines = allLinesForMeta;
                    }
                    else
                    {
                        addedLines = textData.Split(new[] { "\r\n", "\n" }, StringSplitOptions.RemoveEmptyEntries);
                    }
                }

                if (prevLen == 0 && allLinesForMeta != null)
                {
                    var meta = ParseMeta(allLinesForMeta);
                    if (!meta.ContainsKey("EqpId")) meta["EqpId"] = eqpid;

                    var infoTable = BuildInfoDataTable(meta);
                    UploadItmInfoUpsert(infoTable);
                }

                if (addedLines == null || addedLines.Length == 0)
                {
                    SimpleLogger.Debug("No new lines detected.");
                }
                else
                {
                    var errorTable = BuildErrorDataTable(addedLines, eqpid);

                    HashSet<string> allowSet = LoadErrorFilterSet();
                    int matched, skipped;
                    DataTable filtered = ApplyErrorFilter(errorTable, allowSet, out matched, out skipped);

                    SimpleLogger.Event(string.Format("ErrorFilter (Batch) ▶ read_lines={0}, matched={1}, skipped={2}",
                                          (addedLines ?? new string[0]).Length, matched, skipped));

                    if (filtered != null && filtered.Rows.Count > 0)
                    {
                        UploadDataTable(filtered, "plg_error");
                    }
                }

                lock (_lastLen)
                {
                    _lastLen[filePath] = currLen;
                }
            }
            catch (Exception ex)
            {
                SimpleLogger.Error($"Unhandled EX in ProcessFileActual for {filePath} ▶ {ex.GetBaseException().Message}");
            }
        }

        #endregion

        #region === Core Data Processing & DB Helpers ===

        private static string NormalizeErrorId(object v)
        {
            if (v == null || v == DBNull.Value) return string.Empty;
            string s = v.ToString().Trim();
            return s.ToUpperInvariant();
        }

        private static HashSet<string> LoadErrorFilterSet()
        {
            var set = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            string cs = DatabaseInfo.CreateDefault().GetConnectionString();

            const string SQL = @"SELECT error_id FROM public.err_severity_map;";

            try
            {
                using (var conn = new NpgsqlConnection(cs))
                {
                    conn.Open();
                    using (var cmd = new NpgsqlCommand(SQL, conn))
                    using (var rd = cmd.ExecuteReader())
                    {
                        while (rd.Read())
                        {
                            var id = rd.IsDBNull(0) ? string.Empty : rd.GetString(0);
                            id = NormalizeErrorId(id);
                            if (!string.IsNullOrEmpty(id)) set.Add(id);
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                SimpleLogger.Error("Failed to load ErrorFilterSet from DB: " + ex.Message);
            }
            return set;
        }

        private static DataTable ApplyErrorFilter(DataTable src, HashSet<string> allowSet, out int matched, out int skipped)
        {
            matched = 0; skipped = 0;
            if (src == null || src.Rows.Count == 0)
            {
                return src != null ? src.Clone() : new DataTable();
            }

            if (allowSet == null || allowSet.Count == 0)
            {
                skipped = src.Rows.Count;
                return src.Clone();
            }

            var dst = src.Clone();
            foreach (DataRow r in src.Rows)
            {
                string id = NormalizeErrorId(r["error_id"]);
                if (allowSet.Contains(id))
                {
                    dst.ImportRow(r);
                    matched++;
                }
                else
                {
                    skipped++;
                }
            }
            return dst;
        }

        private static void UploadItmInfoUpsert(DataTable dt)
        {
            if (dt == null || dt.Rows.Count == 0) return;

            var r = dt.Rows[0];
            string cs = DatabaseInfo.CreateDefault().GetConnectionString();

            try
            {
                if (!IsInfoChanged(dt))
                {
                    SimpleLogger.Event("itm_info unchanged ▶ eqpid=" + (r["eqpid"] ?? ""));
                    return;
                }

                DateTime srcDate = DateTime.Now;
                var dv = r["date"];
                if (dv != null && dv != DBNull.Value)
                {
                    if (DateTime.TryParseExact(dv.ToString(), "yyyy-MM-dd HH:mm:ss",
                        CultureInfo.InvariantCulture, DateTimeStyles.None, out DateTime dtParsed))
                        srcDate = dtParsed;
                }
                var srv = ITM_Agent.Services.TimeSyncProvider
                              .Instance.ToSynchronizedKst(srcDate);
                srv = new DateTime(srv.Year, srv.Month, srv.Day, srv.Hour, srv.Minute, srv.Second);

                const string INSERT_SQL = @"
                    INSERT INTO public.itm_info
                        (eqpid, system_name, system_model, serial_num, application, version, db_version, ""date"", serv_ts)
                    VALUES
                        (@eqpid, @system_name, @system_model, @serial_num, @application, @version, @db_version, @date, @serv_ts);
                ";

                const string UPDATE_SQL = @"
                    UPDATE public.itm_info
                    SET
                        system_name = @system_name,
                        system_model = @system_model,
                        serial_num = @serial_num,
                        application = @application,
                        version = @version,
                        db_version = @db_version,
                        ""date"" = @date,
                        serv_ts = @serv_ts
                    WHERE eqpid = @eqpid;
                ";

                using (var conn = new NpgsqlConnection(cs))
                {
                    conn.Open();

                    void SetParams(NpgsqlCommand cmd)
                    {
                        cmd.Parameters.AddWithValue("@eqpid", r["eqpid"] ?? (object)DBNull.Value);
                        cmd.Parameters.AddWithValue("@system_name", r["system_name"] ?? (object)DBNull.Value);
                        cmd.Parameters.AddWithValue("@system_model", r["system_model"] ?? (object)DBNull.Value);
                        cmd.Parameters.AddWithValue("@serial_num", r["serial_num"] ?? (object)DBNull.Value);
                        cmd.Parameters.AddWithValue("@application", r["application"] ?? (object)DBNull.Value);
                        cmd.Parameters.AddWithValue("@version", r["version"] ?? (object)DBNull.Value);
                        cmd.Parameters.AddWithValue("@db_version", r["db_version"] ?? (object)DBNull.Value);

                        object dateParam = DBNull.Value;
                        if (dv != null && dv != DBNull.Value)
                        {
                            if (DateTime.TryParseExact(dv.ToString(), "yyyy-MM-dd HH:mm:ss",
                                CultureInfo.InvariantCulture, DateTimeStyles.None, out DateTime dtParsed))
                                dateParam = dtParsed;
                            else
                                dateParam = dv.ToString();
                        }
                        cmd.Parameters.AddWithValue("@date", dateParam);
                        cmd.Parameters.AddWithValue("@serv_ts", srv);
                    }

                    try
                    {
                        using (var cmd = new NpgsqlCommand(INSERT_SQL, conn))
                        {
                            SetParams(cmd);
                            cmd.ExecuteNonQuery();
                        }
                        SimpleLogger.Event("itm_info inserted ▶ eqpid=" + (r["eqpid"] ?? ""));
                    }
                    catch (PostgresException pgEx) when (pgEx.SqlState == "23505")
                    {
                        using (var cmd = new NpgsqlCommand(UPDATE_SQL, conn))
                        {
                            SetParams(cmd);
                            cmd.ExecuteNonQuery();
                        }
                        SimpleLogger.Event("itm_info updated (duplicate key) ▶ eqpid=" + (r["eqpid"] ?? ""));
                    }
                }
            }
            catch (Exception ex)
            {
                SimpleLogger.Error("UploadItmInfoUpsert failed: " + ex.Message);
            }
        }

        private static Dictionary<string, string> ParseMeta(string[] lines)
        {
            var d = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);

            for (int i = 0; i < lines.Length; i++)
            {
                var ln = lines[i];
                int idx = ln.IndexOf(":,");
                if (idx <= 0) continue;

                string key = ln.Substring(0, idx).Trim();
                string val = ln.Substring(idx + 2).Trim();
                if (key.Length == 0) continue;

                if (string.Equals(key, "EXPORT_TYPE", StringComparison.OrdinalIgnoreCase))
                    continue;

                d[key] = val;
            }

            string ds;
            if (d.TryGetValue("DATE", out ds))
            {
                if (DateTime.TryParseExact(ds, "M/d/yyyy H:m:s", CultureInfo.InvariantCulture, DateTimeStyles.None, out DateTime dt))
                    d["DATE"] = dt.ToString("yyyy-MM-dd HH:mm:ss");
            }

            return d;
        }

        private static DataTable BuildInfoDataTable(Dictionary<string, string> meta)
        {
            var map = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
            {
                ["DATE"] = "date",
                ["SYSTEM_NAME"] = "system_name",
                ["SYSTEM_MODEL"] = "system_model",
                ["SERIAL_NUM"] = "serial_num",
                ["APPLICATION"] = "application",
                ["VERSION"] = "version",
                ["DB_VERSION"] = "db_version",
                ["EqpId"] = "eqpid"
            };

            var dt = new DataTable();
            foreach (var c in map.Values) dt.Columns.Add(c, typeof(string));
            var dr = dt.NewRow();
            foreach (var kv in map)
                dr[kv.Value] = meta.TryGetValue(kv.Key, out string v) ? (object)v : DBNull.Value;
            dt.Rows.Add(dr);
            return dt;
        }

        private static DataTable BuildErrorDataTable(string[] lines, string eqpid)
        {
            var dt = new DataTable();
            dt.Columns.AddRange(new[]
            {
                new DataColumn("eqpid", typeof(string)),
                new DataColumn("error_id", typeof(string)),
                new DataColumn("time_stamp", typeof(DateTime)),
                new DataColumn("error_label", typeof(string)),
                new DataColumn("error_desc", typeof(string)),
                new DataColumn("millisecond", typeof(int)),
                new DataColumn("extra_message_1", typeof(string)),
                new DataColumn("extra_message_2", typeof(string)),
                new DataColumn("serv_ts", typeof(DateTime))
            });

            var rg = new Regex(
                @"^(?<id>\w+),\s*(?<ts>[^,]+),\s*(?<lbl>[^,]+),\s*(?<desc>[^,]+),\s*(?<ms>\d+)(?:,\s*(?<extra>.*))?",
                RegexOptions.Compiled);

            foreach (var ln in lines)
            {
                var m = rg.Match(ln);
                if (!m.Success) continue;

                if (!DateTime.TryParseExact(
                    m.Groups["ts"].Value.Trim(),
                    "dd-MMM-yy h:mm:ss tt",
                    CultureInfo.InvariantCulture,
                    DateTimeStyles.None,
                    out DateTime parsedTs))
                {
                    continue;
                }

                var dr = dt.NewRow();
                dr["eqpid"] = eqpid;
                dr["error_id"] = m.Groups["id"].Value.Trim();
                dr["time_stamp"] = parsedTs;
                dr["error_label"] = m.Groups["lbl"].Value.Trim();
                dr["error_desc"] = m.Groups["desc"].Value.Trim();

                if (int.TryParse(m.Groups["ms"].Value, out int ms)) dr["millisecond"] = ms;

                dr["extra_message_1"] = m.Groups["extra"].Value.Trim();
                dr["extra_message_2"] = "";

                var srv = ITM_Agent.Services.TimeSyncProvider
                                .Instance.ToSynchronizedKst(parsedTs);
                srv = new DateTime(srv.Year, srv.Month, srv.Day,
                                   srv.Hour, srv.Minute, srv.Second);
                dr["serv_ts"] = srv;

                dt.Rows.Add(dr);
            }
            return dt;
        }

        private static bool IsInfoChanged(DataTable dt)
        {
            if (dt == null || dt.Rows.Count == 0) return false;
            var r = dt.Rows[0];

            string cs = DatabaseInfo.CreateDefault().GetConnectionString();
            const string SQL = @"
                SELECT 1
                FROM public.itm_info
                WHERE eqpid = @eqp
                  AND system_name IS NOT DISTINCT FROM @sn
                  AND system_model IS NOT DISTINCT FROM @sm
                  AND serial_num IS NOT DISTINCT FROM @snm
                  AND application IS NOT DISTINCT FROM @app
                  AND version IS NOT DISTINCT FROM @ver
                  AND db_version IS NOT DISTINCT FROM @dbv
                LIMIT 1;";

            using (var conn = new NpgsqlConnection(cs))
            {
                conn.Open();
                using (var cmd = new NpgsqlCommand(SQL, conn))
                {
                    cmd.Parameters.AddWithValue("@eqp", r["eqpid"] ?? (object)DBNull.Value);
                    cmd.Parameters.AddWithValue("@sn", r["system_name"] ?? (object)DBNull.Value);
                    cmd.Parameters.AddWithValue("@sm", r["system_model"] ?? (object)DBNull.Value);
                    cmd.Parameters.AddWithValue("@snm", r["serial_num"] ?? (object)DBNull.Value);
                    cmd.Parameters.AddWithValue("@app", r["application"] ?? (object)DBNull.Value);
                    cmd.Parameters.AddWithValue("@ver", r["version"] ?? (object)DBNull.Value);
                    cmd.Parameters.AddWithValue("@dbv", r["db_version"] ?? (object)DBNull.Value);

                    object o = cmd.ExecuteScalar();
                    return o == null;
                }
            }
        }

        private static int UploadDataTable(DataTable dt, string tableName)
        {
            if (dt == null || dt.Rows.Count == 0) return 0;

            string cs = DatabaseInfo.CreateDefault().GetConnectionString();

            using (var conn = new NpgsqlConnection(cs))
            {
                conn.Open();
                using (var tx = conn.BeginTransaction())
                {
                    var cols = dt.Columns.Cast<DataColumn>().Select(c => c.ColumnName).ToArray();
                    string colList = string.Join(",", cols.Select(c => "\"" + c + "\""));
                    string paramList = string.Join(",", cols.Select(c => "@" + c));

                    string sql = string.Format(
                        "INSERT INTO public.{0} ({1}) VALUES ({2}) ON CONFLICT DO NOTHING;",
                        tableName, colList, paramList);

                    using (var cmd = new NpgsqlCommand(sql, conn, tx))
                    {
                        foreach (var c in cols)
                        {
                            var param = new NpgsqlParameter("@" + c, DBNull.Value);
                            cmd.Parameters.Add(param);
                        }

                        int inserted = 0;
                        try
                        {
                            foreach (DataRow r in dt.Rows)
                            {
                                foreach (var c in cols)
                                    cmd.Parameters["@" + c].Value = r[c] ?? DBNull.Value;

                                int affected = cmd.ExecuteNonQuery();
                                if (affected == 1) inserted++;
                            }
                            tx.Commit();

                            int skipped = dt.Rows.Count - inserted;
                            SimpleLogger.Debug(
                                string.Format("DB OK ▶ {0}, inserted={1}, total={2}", tableName, inserted, dt.Rows.Count));
                            if (skipped > 0)
                                SimpleLogger.Event("Duplicate entry skipped ▶ " + tableName + " (skipped=" + skipped + ")");

                            return inserted;
                        }
                        catch (Exception ex)
                        {
                            tx.Rollback();
                            SimpleLogger.Error($"DB FAIL ({tableName}) ▶ " + ex.Message);
                            return 0;
                        }
                    }
                }
            }
        }

        #endregion

        #region === Utility ===
        
        private static string GetEqpidFromSettings(string iniPath)
        {
            try
            {
                string path = Path.IsPathRooted(iniPath)
                    ? iniPath
                    : Path.Combine(AppDomain.CurrentDomain.BaseDirectory, iniPath);

                if (!File.Exists(path)) return string.Empty;

                foreach (var line in File.ReadLines(path))
                {
                    var trimmed = line.Trim();
                    if (trimmed.StartsWith("Eqpid", StringComparison.OrdinalIgnoreCase))
                    {
                        int idx = trimmed.IndexOf('=');
                        if (idx > 0) return trimmed.Substring(idx + 1).Trim();
                    }
                }
            }
            catch (Exception ex)
            {
                SimpleLogger.Error("GetEqpidFromSettings EX ▶ " + ex.Message);
            }
            return string.Empty;
        }

        #endregion
    }
}
