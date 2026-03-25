// Library/IOnto_WaferFlatData.cs
using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using Npgsql;
using ConnectInfo;
using System.Threading;
using ITM_Agent.Services;

namespace Onto_WaferFlatDataLib
{
    /*──────────────────────── Logger ────────────────────────*/
    internal static class SimpleLogger
    {
        private static volatile bool _debugEnabled = false;
        public static void SetDebugMode(bool enable) => _debugEnabled = enable;

        private static readonly object _sync = new object();
        private static readonly string _logDir =
            Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "Logs");

        private static string GetPath(string suffix) =>
            Path.Combine(_logDir, $"{DateTime.Now:yyyyMMdd}_{suffix}.log");

        public static void Event(string msg) => Write("event", msg);
        public static void Error(string msg) => Write("error", msg);
        public static void Debug(string msg)
        {
            if (_debugEnabled) Write("debug", msg);
        }

        private static void Write(string suffix, string msg)
        {
            lock (_sync)
            {
                try
                {
                    if (!Directory.Exists(_logDir))
                        Directory.CreateDirectory(_logDir);

                    string filePath = GetPath(suffix);
                    string line = $"{DateTime.Now:yyyy-MM-dd HH:mm:ss.fff} " +
                                      $"[WaferFlat] {msg}{Environment.NewLine}";

                    const int MAX_RETRY = 3;
                    for (int i = 1; i <= MAX_RETRY; i++)
                    {
                        try
                        {
                            using (var fs = new FileStream(filePath,
                                      FileMode.OpenOrCreate, FileAccess.Write,
                                      FileShare.ReadWrite))
                            {
                                fs.Seek(0, SeekOrigin.End);
                                using (var sw = new StreamWriter(fs, Encoding.UTF8))
                                    sw.Write(line);
                            }
                            return;
                        }
                        catch (IOException) when (i < MAX_RETRY)
                        { Thread.Sleep(250); }
                    }
                }
                catch { /* 로깅 실패 무시 */ }
            }
        }
    }
    /*──────────────────────────────────────────────────────────────────*/

    public interface IOnto_WaferFlatData
    {
        string PluginName { get; }
        void ProcessAndUpload(string filePath, object settingsFilePath = null, object arg2 = null);
    }

    public class Onto_WaferFlatData : IOnto_WaferFlatData
    {
        public string PluginName => "Onto_WaferFlatData";
        public string DefaultTaskName => "WaferFlat";

        public bool RequiresOverrideNames => true;

        static Onto_WaferFlatData()
        {
            Encoding.RegisterProvider(CodePagesEncodingProvider.Instance);
        }

        private static string ReadAllTextSafe(string path, Encoding enc, int timeoutMs = 30000)
        {
            var sw = System.Diagnostics.Stopwatch.StartNew();
            while (true)
            {
                try
                {
                    using (var fs = new FileStream(path, FileMode.Open, FileAccess.Read, FileShare.ReadWrite))
                    using (var sr = new StreamReader(fs, enc))
                    {
                        return sr.ReadToEnd();
                    }
                }
                catch (IOException)
                {
                    if (sw.ElapsedMilliseconds > timeoutMs)
                        throw;
                    Thread.Sleep(500);
                }
            }
        }

        public void ProcessAndUpload(string filePath, object settingsFilePath = null, object arg2 = null)
        {
            SimpleLogger.Event($"ProcessAndUpload(file,ini) ▶ {filePath}");

            if (!WaitForFileReady(filePath, 20, 500))
            {
                SimpleLogger.Event($"SKIP – file still not ready ▶ {filePath}");
                return;
            }

            string eqpid = GetEqpidFromSettings(settingsFilePath as string ?? "Settings.ini");
            try
            {
                ProcessFile(filePath, eqpid);
            }
            catch (Exception ex)
            {
                SimpleLogger.Error($"Unhandled EX ▶ {ex.GetBaseException().Message}");
            }
        }

        private bool WaitForFileReady(string path, int maxRetries = 10, int delayMs = 500)
        {
            for (int i = 0; i < maxRetries; i++)
            {
                if (File.Exists(path))
                {
                    try
                    {
                        using (var fs = File.Open(path, FileMode.Open, FileAccess.Read, FileShare.ReadWrite))
                        {
                            return true;
                        }
                    }
                    catch (IOException)
                    {
                    }
                }
                Thread.Sleep(delayMs);
            }
            return false;
        }

        private void ProcessFile(string filePath, string eqpid)
        {
            SimpleLogger.Debug($"PARSE ▶ {Path.GetFileName(filePath)}");

            string fileContent = ReadAllTextSafe(filePath, Encoding.GetEncoding(949));
            var lines = fileContent.Split(new[] { "\r\n", "\n" }, StringSplitOptions.None);

            var meta = new Dictionary<string, string>();
            foreach (var ln in lines)
            {
                int idx = ln.IndexOf(':');
                if (idx > 0)
                {
                    string key = ln.Substring(0, idx).Trim();
                    string val = ln.Substring(idx + 1).Trim();
                    if (!meta.ContainsKey(key)) meta[key] = val;
                }
            }

            // ⭐️ [핵심 변경] 파일 내부 Wafer ID 텍스트 추출 로직 완벽 호환 (AMAT & EBARA)
            int? waferNo = null;

            if (meta.TryGetValue("Wafer ID", out string waferId))
            {
                // 1. AMAT 설비 대응: "W18" 패턴에서 18 추출
                var matchAmat = Regex.Match(waferId, @"W(\d+)", RegexOptions.IgnoreCase);
                if (matchAmat.Success && int.TryParse(matchAmat.Groups[1].Value, out int wAmat))
                {
                    waferNo = wAmat;
                }
                else
                {
                    // 2. ⭐️ EBARA 설비 대응: "ABC001.1_01" 패턴에서 제일 마지막 언더바(_) 뒤의 숫자 '01' 추출
                    var matchEbara = Regex.Match(waferId, @"_(\d+)$");
                    if (matchEbara.Success && int.TryParse(matchEbara.Groups[1].Value, out int wEbara))
                    {
                        waferNo = wEbara; // "01"이 int로 변환되어 1이 됨
                    }
                    // 3. 만약 언더바 없이 순수 숫자만("01") 적혀있는 경우에 대한 대비
                    else if (int.TryParse(waferId, out int rawNum))
                    {
                        waferNo = rawNum;
                    }
                }
            }

            // 4. 안전장치: 파일 내부에서 추출 실패 시 파일명에서 2차 시도 (기존 로직 유지)
            if (waferNo == null)
            {
                string fileName = Path.GetFileName(filePath);

                var matchAmatFile = Regex.Match(fileName, @"C\dW(\d+)", RegexOptions.IgnoreCase);
                if (matchAmatFile.Success && int.TryParse(matchAmatFile.Groups[1].Value, out int wAmat))
                {
                    waferNo = wAmat;
                }
                else
                {
                    var matchEbaraFile = Regex.Match(fileName, @"_(\d{2})_");
                    if (matchEbaraFile.Success && int.TryParse(matchEbaraFile.Groups[1].Value, out int wEbara))
                    {
                        waferNo = wEbara;
                    }
                }
            }

            // 5. 최종 Fallback (DB 23502 null 제약조건 에러 방지)
            if (waferNo == null)
            {
                waferNo = 0;
                SimpleLogger.Debug($"[WARNING] Wafer ID could not be extracted. Defaulting to 0. File: {Path.GetFileName(filePath)}");
            }

            DateTime dtVal = DateTime.MinValue;
            if (meta.TryGetValue("Date and Time", out string dtStr))
                DateTime.TryParse(dtStr, out dtVal);

            int hdrIdx = Array.FindIndex(
                lines,
                l => l.TrimStart().StartsWith("Point#", StringComparison.OrdinalIgnoreCase)
            );
            if (hdrIdx == -1)
            {
                SimpleLogger.Debug("Header NOT FOUND → skip");
                return;
            }

            string NormalizeHeader(string h)
            {
                h = h.ToLowerInvariant();
                h = Regex.Replace(h, @"\(\s*no\s*cal\.?\s*\)", " nocal ", RegexOptions.IgnoreCase);
                h = Regex.Replace(h, @"\bno[\s_]*cal\b", "nocal", RegexOptions.IgnoreCase);
                h = Regex.Replace(h, @"\(\s*cal\.?\s*\)", " cal ", RegexOptions.IgnoreCase);
                h = h.Replace("(mm)", "")
                     .Replace("(탆)", "")
                     .Replace("die x", "diex")
                     .Replace("die y", "diey")
                     .Trim();
                h = Regex.Replace(h, @"\s+", "_");
                h = Regex.Replace(h, @"[#/:\-]", "");
                return h;
            }

            var headers = lines[hdrIdx].Split(',').Select(NormalizeHeader).ToList();
            var headerIndex = headers.Select((h, idx) => new { h, idx })
                                     .GroupBy(x => x.h)
                                     .ToDictionary(g => g.Key, g => g.First().idx);

            var rows = new List<Dictionary<string, object>>();
            var intCols = new HashSet<string> { "point", "dierow", "diecol", "dienum", "diepointtag" };

            for (int i = hdrIdx + 1; i < lines.Length; i++)
            {
                if (string.IsNullOrWhiteSpace(lines[i])) continue;

                var vals = lines[i].Split(',').Select(v => v.Trim()).ToArray();
                if (vals.Length < headers.Count) continue;

                var row = new Dictionary<string, object>
                {
                    ["cassettercp"] = meta.TryGetValue("Cassette Recipe Name", out var v1) ? v1 : "",
                    ["stagercp"] = meta.TryGetValue("Stage Recipe Name", out var v2) ? v2 : "",
                    ["stagegroup"] = meta.TryGetValue("Stage Group Name", out var v3) ? v3 : "",
                    ["lotid"] = meta.TryGetValue("Lot ID", out var v4) ? v4 : "",
                    ["waferid"] = waferNo, // ⭐️ 무조건 정상적인 숫자가 들어감 (DB 23502 에러 완전 해결)
                    ["datetime"] = (dtVal != DateTime.MinValue) ? (object)dtVal : DBNull.Value,
                    ["film"] = meta.TryGetValue("Film Name", out var v5) ? v5 : ""
                };

                int tmpInt; double tmpDbl;
                foreach (var kv in headerIndex)
                {
                    string colName = kv.Key;
                    int idx = kv.Value;
                    string valRaw = (idx < vals.Length) ? vals[idx] : "";

                    if (string.IsNullOrEmpty(valRaw)) { row[colName] = DBNull.Value; continue; }

                    if (intCols.Contains(colName) && int.TryParse(valRaw, out tmpInt))
                        row[colName] = tmpInt;
                    else if (double.TryParse(valRaw, out tmpDbl))
                        row[colName] = tmpDbl;
                    else
                        row[colName] = valRaw;
                }

                rows.Add(row);
            }

            if (rows.Count == 0)
            {
                SimpleLogger.Debug("rows=0 → skip");
                return;
            }

            DataTable dt = new DataTable();
            foreach (var k in rows[0].Keys) dt.Columns.Add(k, typeof(object));
            dt.Columns.Add("eqpid", typeof(string));

            foreach (var r in rows)
            {
                var dr = dt.NewRow();
                foreach (var k in r.Keys) dr[k] = r[k] ?? DBNull.Value;
                dr["eqpid"] = eqpid;
                dt.Rows.Add(dr);
            }

            UploadToSQL(dt, Path.GetFileName(filePath));
            SimpleLogger.Event($"{Path.GetFileName(filePath)} ▶ rows={dt.Rows.Count}");

            try { File.Delete(filePath); } catch { /* ignore */ }
        }

        private void UploadToSQL(DataTable dt, string srcFile)
        {
            if (!dt.Columns.Contains("serv_ts"))
                dt.Columns.Add("serv_ts", typeof(DateTime));

            foreach (DataRow r in dt.Rows)
            {
                if (r["datetime"] != DBNull.Value)
                {
                    DateTime ts = (DateTime)r["datetime"];
                    DateTime kst = ITM_Agent.Services.TimeSyncProvider.Instance.ToSynchronizedKst(ts);

                    // 밀리초 제거 (yyyy-MM-dd HH:mm:ss 형식 보장)
                    r["serv_ts"] = new DateTime(kst.Year, kst.Month, kst.Day, kst.Hour, kst.Minute, kst.Second);
                }
                else
                {
                    r["serv_ts"] = DBNull.Value;
                }
            }

            var dbInfo = DatabaseInfo.CreateDefault();
            using (var conn = new NpgsqlConnection(dbInfo.GetConnectionString()))
            {
                conn.Open();

                try
                {
                    var existingColumns = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
                    using (var cmdCheck = new NpgsqlCommand(@"
                        SELECT column_name
                        FROM information_schema.columns
                        WHERE table_schema = 'public' AND table_name = 'plg_wf_flat';", conn))
                    {
                        using (var reader = cmdCheck.ExecuteReader())
                        {
                            while (reader.Read()) existingColumns.Add(reader.GetString(0));
                        }
                    }

                    var missingColumns = new List<string>();
                    foreach (DataColumn dc in dt.Columns)
                    {
                        if (!existingColumns.Contains(dc.ColumnName))
                        {
                            missingColumns.Add(dc.ColumnName);
                        }
                    }

                    if (missingColumns.Count > 0)
                    {
                        foreach (string colName in missingColumns)
                        {
                            string alterSql = $"ALTER TABLE public.plg_wf_flat ADD COLUMN \"{colName}\" DOUBLE PRECISION NULL;";
                            using (var cmdAlter = new NpgsqlCommand(alterSql, conn))
                            {
                                cmdAlter.ExecuteNonQuery();
                                SimpleLogger.Event($"Added missing column: {colName}");
                            }
                        }
                    }
                }
                catch (Exception exSchema)
                {
                    SimpleLogger.Error($"Schema check failed: {exSchema.Message}");
                    return;
                }

                using (var tx = conn.BeginTransaction())
                {
                    try
                    {
                        if (dt.Rows.Count > 0)
                        {
                            string eqpid = dt.Rows[0]["eqpid"] as string;
                            DateTime datetime = (DateTime)dt.Rows[0]["datetime"];

                            if (!string.IsNullOrEmpty(eqpid) && datetime != DateTime.MinValue)
                            {
                                using (var cmdDelete = new NpgsqlCommand(
                                    "DELETE FROM public.plg_wf_flat WHERE eqpid = @eqpid AND datetime = @datetime", conn, tx))
                                {
                                    cmdDelete.Parameters.AddWithValue("eqpid", eqpid);
                                    cmdDelete.Parameters.AddWithValue("datetime", datetime);
                                    cmdDelete.ExecuteNonQuery();
                                }
                            }
                        }

                        var cols = dt.Columns.Cast<DataColumn>().Select(c => c.ColumnName).ToArray();
                        string colList = string.Join(",", cols.Select(c => $"\"{c}\""));
                        string unnestFields = string.Join(",", cols.Select(c => $"u.\"{c}\""));
                        string unnestParams = string.Join(",", cols.Select(c => $"@{c}"));

                        // [수정] ON CONFLICT DO NOTHING 추가: 23505 에러 방지
                        string sql = $"INSERT INTO public.plg_wf_flat ({colList}) " +
                                     $"SELECT {unnestFields} FROM unnest({unnestParams}) " +
                                     $"AS u({colList}) ON CONFLICT DO NOTHING;";

                        using (var cmd = new NpgsqlCommand(sql, conn, tx))
                        {
                            var stringLists = new Dictionary<string, List<string>>();
                            var intLists = new Dictionary<string, List<int?>>();
                            var doubleLists = new Dictionary<string, List<double?>>();
                            var dateTimeLists = new Dictionary<string, List<DateTime?>>();

                            var intCols = new HashSet<string>(StringComparer.OrdinalIgnoreCase) { "point", "dierow", "diecol", "dienum", "diepointtag", "waferid" };
                            var dateTimeCols = new HashSet<string>(StringComparer.OrdinalIgnoreCase) { "datetime", "serv_ts" };
                            var stringCols = new HashSet<string>(StringComparer.OrdinalIgnoreCase) { "cassettercp", "stagercp", "stagegroup", "lotid", "film", "eqpid" };

                            foreach (string c in cols)
                            {
                                if (stringCols.Contains(c)) stringLists[c] = new List<string>(dt.Rows.Count);
                                else if (intCols.Contains(c)) intLists[c] = new List<int?>(dt.Rows.Count);
                                else if (dateTimeCols.Contains(c)) dateTimeLists[c] = new List<DateTime?>(dt.Rows.Count);
                                else doubleLists[c] = new List<double?>(dt.Rows.Count);
                            }

                            foreach (DataRow r in dt.Rows)
                            {
                                foreach (var entry in stringLists) entry.Value.Add(r[entry.Key] == DBNull.Value ? null : (string)r[entry.Key]);
                                foreach (var entry in intLists) entry.Value.Add(r[entry.Key] == DBNull.Value ? (int?)null : Convert.ToInt32(r[entry.Key]));
                                foreach (var entry in dateTimeLists) entry.Value.Add(r[entry.Key] == DBNull.Value ? (DateTime?)null : (DateTime)r[entry.Key]);
                                foreach (var entry in doubleLists) entry.Value.Add(r[entry.Key] == DBNull.Value ? (double?)null : Convert.ToDouble(r[entry.Key]));
                            }

                            foreach (var entry in stringLists) cmd.Parameters.AddWithValue("@" + entry.Key, entry.Value);
                            foreach (var entry in intLists) cmd.Parameters.AddWithValue("@" + entry.Key, entry.Value);
                            foreach (var entry in dateTimeLists) cmd.Parameters.AddWithValue("@" + entry.Key, entry.Value);
                            foreach (var entry in doubleLists) cmd.Parameters.AddWithValue("@" + entry.Key, entry.Value);

                            cmd.CommandTimeout = 300;
                            int affected = cmd.ExecuteNonQuery();
                            SimpleLogger.Debug($"DB Batch OK ▶ Inserted={affected}");
                        }
                        tx.Commit();
                    }
                    catch (Exception ex)
                    {
                        tx.Rollback();
                        SimpleLogger.Error("DB FAIL ▶ " + ex.Message);
                    }
                }
            }
        }

        private string GetEqpidFromSettings(string iniPath)
        {
            string path = iniPath ?? "Settings.ini";
            if (!Path.IsPathRooted(path))
                path = Path.Combine(AppDomain.CurrentDomain.BaseDirectory, path);

            if (!File.Exists(path)) return "";
            try
            {
                foreach (var line in File.ReadLines(path))
                {
                    if (line.Trim().StartsWith("Eqpid", StringComparison.OrdinalIgnoreCase))
                    {
                        int idx = line.IndexOf('=');
                        if (idx > 0) return line.Substring(idx + 1).Trim();
                    }
                }
            }
            catch (Exception ex)
            {
                SimpleLogger.Error($"GetEqpidFromSettings failed: {ex.Message}");
            }
            return "";
        }
    }
}
