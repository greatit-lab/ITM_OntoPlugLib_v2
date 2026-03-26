// Library/IOnto_PrealignData.cs
using System;
using System.Collections.Generic;
using System.Data;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading;
using ConnectInfo;
using Npgsql;
using ITM_Agent.Services;

namespace Onto_PrealignDataLib
{
    /*──────────────────────── Logger ────────────────────────*/
    internal static class SimpleLogger
    {
        private static volatile bool _debugEnabled = false;
        public static void SetDebugMode(bool enable) { _debugEnabled = enable; }

        private static readonly object _sync = new object();
        private static readonly string _dir = System.IO.Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "Logs");
        private static string PathOf(string sfx) => System.IO.Path.Combine(_dir, $"{DateTime.Now:yyyyMMdd}_{sfx}.log");

        private static void Write(string s, string m)
        {
            lock (_sync)
            {
                System.IO.Directory.CreateDirectory(_dir);
                string line = $"{DateTime.Now:yyyy-MM-dd HH:mm:ss.fff} [Prealign] {m}{Environment.NewLine}";
                try { System.IO.File.AppendAllText(PathOf(s), line, System.Text.Encoding.UTF8); }
                catch { /* 로깅 실패 무시 */ }
            }
        }

        public static void Event(string m) { Write("event", m); }
        public static void Error(string m) { Write("error", m); }
        public static void Debug(string m)
        {
            if (_debugEnabled) Write("debug", m);
        }
    }

    /*──────────────────────── Interface ─────────────────────*/
    public interface IOnto_PrealignData
    {
        string PluginName { get; }
        void ProcessAndUpload(string filePath, object arg1 = null, object arg2 = null);
    }

    /*──────────────────────── Implementation ────────────────*/
    public class Onto_PrealignData : IOnto_PrealignData
    {
        private static readonly Dictionary<string, long> _lastLen =
            new Dictionary<string, long>(StringComparer.OrdinalIgnoreCase);

        private readonly string _pluginName;
        public string PluginName => _pluginName;

        public string DefaultTaskName => "PreAlign";
        public string DefaultFileFilter => "PreAlignLog.dat";

        static Onto_PrealignData()
        {
            #if NETCOREAPP || NET5_0_OR_GREATER
                Encoding.RegisterProvider(CodePagesEncodingProvider.Instance);
            #endif
        }

        public Onto_PrealignData()
        {
            _pluginName = "Onto_PrealignData";
        }

        public void ProcessAndUpload(string filePath, object arg1 = null, object arg2 = null)
        {
            SimpleLogger.Event("Process (Incremental) ▶ " + filePath);
            string eqpid = GetEqpid(arg1 as string ?? "Settings.ini");

            long prevLen = 0;
            long currLen = 0;
            string addedText = "";

            byte[] fileBuffer = null;
            long bytesToRead = 0;

            int maxRetries = 5;
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

                    using (var fs = new FileStream(filePath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite | FileShare.Delete))
                    {
                        currLen = fs.Length;

                        if (currLen == prevLen && prevLen > 0)
                        {
                            SimpleLogger.Debug("File length unchanged, skipping: " + filePath);
                            return;
                        }

                        if (currLen < prevLen)
                        {
                            SimpleLogger.Event("File truncated. Resetting offset: " + filePath);
                            prevLen = 0;
                        }

                        bytesToRead = currLen - prevLen;
                        if (bytesToRead > 0)
                        {
                            // [핵심 개선 1: OOM 방지] 한 번의 사이클에 최대 10MB까지만 읽도록 제한
                            long maxChunkSize = 10 * 1024 * 1024; // 10MB
                            bool isChunked = bytesToRead > maxChunkSize;
                            if (isChunked)
                            {
                                bytesToRead = maxChunkSize;
                            }

                            fileBuffer = new byte[bytesToRead];
                            fs.Seek(prevLen, SeekOrigin.Begin);

                            int totalRead = 0;
                            while (totalRead < bytesToRead)
                            {
                                int read = fs.Read(fileBuffer, totalRead, (int)bytesToRead - totalRead);
                                if (read == 0) break;
                                totalRead += read;
                            }

                            // [핵심 개선 2: 데이터 무결성 보장] 청크로 잘렸을 경우 줄바꿈(\n)까지만 유효 데이터로 자름
                            if (isChunked)
                            {
                                int validLen = totalRead;
                                for (int j = totalRead - 1; j >= 0; j--)
                                {
                                    if (fileBuffer[j] == (byte)'\n')
                                    {
                                        validLen = j + 1;
                                        break;
                                    }
                                }

                                if (validLen < totalRead && validLen > 0)
                                {
                                    bytesToRead = validLen;
                                    Array.Resize(ref fileBuffer, validLen);
                                }
                                currLen = prevLen + bytesToRead; // 남은 데이터는 다음 턴에 읽도록 offset 세팅
                            }
                        }
                    }
                    fileReadSuccess = true;
                    break;
                }
                catch (IOException ioEx) when (i < maxRetries - 1)
                {
                    SimpleLogger.Debug($"[Prealign] IO Exception attempt {i + 1} (retrying): {ioEx.Message}");
                    Thread.Sleep(delayMs);
                }
                catch (FileNotFoundException)
                {
                    SimpleLogger.Debug("File not found (maybe deleted): " + filePath);
                    lock (_lastLen) { _lastLen.Remove(filePath); }
                    return;
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
                SimpleLogger.Error($"IO Exception during processing {filePath} (retries failed). Skipping this turn.");
                return;
            }

            try
            {
                if (fileBuffer != null && bytesToRead > 0)
                {
                    addedText = Encoding.GetEncoding(949).GetString(fileBuffer);
                }

                var rows = new List<Tuple<decimal, decimal, decimal, DateTime>>();
                
                // Regex 성능 향상을 위해 Compiled 옵션 추가
                var rex = new Regex(
                    @"Xmm\s*([-\d.]+)\s*Ymm\s*([-\d.]+)\s*Notch\s*([-\d.]+)\s*Time\s*([\d\-:\s]+)",
                    RegexOptions.IgnoreCase | RegexOptions.Compiled);

                foreach (Match m in rex.Matches(addedText))
                {
                    DateTime ts;
                    bool ok = DateTime.TryParseExact(
                                 m.Groups[4].Value.Trim(),
                                 new[] { "MM-dd-yy HH:mm:ss", "M-d-yy HH:mm:ss" },
                                 CultureInfo.InvariantCulture,
                                 DateTimeStyles.None,
                                 out ts) ||
                              DateTime.TryParse(m.Groups[4].Value.Trim(), out ts);
                    if (!ok) continue;

                    decimal x, y, n;
                    if (decimal.TryParse(m.Groups[1].Value, out x) &&
                        decimal.TryParse(m.Groups[2].Value, out y) &&
                        decimal.TryParse(m.Groups[3].Value, out n))
                    {
                        rows.Add(Tuple.Create(x, y, n, ts));
                    }
                }

                if (rows.Count > 0)
                {
                    InsertRows(rows, eqpid); // DB 업로드
                }
                else
                {
                    SimpleLogger.Debug("No valid new rows found in incremental text.");
                }

                lock (_lastLen)
                {
                    _lastLen[filePath] = currLen;
                }
            }
            catch (Exception ex)
            {
                SimpleLogger.Error($"Unhandled EX in ProcessAndUpload for {filePath} ▶ {ex.GetBaseException().Message}");
            }
        }

        /*──────────────── 행 → DB 삽입 공통 ────────────────*/
        private void InsertRows(List<Tuple<decimal, decimal, decimal, DateTime>> rows, string eqpid)
        {
            rows.Sort((a, b) => a.Item4.CompareTo(b.Item4));

            // [핵심 개선 3: DB Timeout 방지] 데이터를 3000건씩 분할하여 DB로 배치 전송
            int batchSize = 3000;
            
            for (int i = 0; i < rows.Count; i += batchSize)
            {
                var batchRows = rows.GetRange(i, Math.Min(batchSize, rows.Count - i));
                
                var dt = new DataTable();
                dt.Columns.Add("eqpid", typeof(string));
                dt.Columns.Add("datetime", typeof(DateTime));
                dt.Columns.Add("xmm", typeof(decimal));
                dt.Columns.Add("ymm", typeof(decimal));
                dt.Columns.Add("notch", typeof(decimal));
                dt.Columns.Add("serv_ts", typeof(DateTime));

                foreach (var r in batchRows)
                {
                    DateTime serv_kst = TimeSyncProvider.Instance.ToSynchronizedKst(r.Item4);
                    serv_kst = serv_kst.AddTicks(-(serv_kst.Ticks % TimeSpan.TicksPerSecond));

                    dt.Rows.Add(eqpid, r.Item4, r.Item1, r.Item2, r.Item3, serv_kst);
                }

                Upload(dt);
                SimpleLogger.Event($"Batch uploaded: {dt.Rows.Count} rows.");
            }
        }

        /*──────────────── DB Upload ───────────────────────*/
        private void Upload(DataTable dt)
        {
            string cs = DatabaseInfo.CreateDefault().GetConnectionString();
            try
            {
                using (var conn = new NpgsqlConnection(cs))
                {
                    conn.Open();

                    var eqpids = new List<string>(dt.Rows.Count);
                    var datetimes = new List<DateTime>(dt.Rows.Count);
                    var xmms = new List<decimal>(dt.Rows.Count);
                    var ymms = new List<decimal>(dt.Rows.Count);
                    var notches = new List<decimal>(dt.Rows.Count);
                    var serv_tss = new List<DateTime>(dt.Rows.Count);

                    foreach (DataRow row in dt.Rows)
                    {
                        eqpids.Add(row["eqpid"] as string);
                        datetimes.Add((DateTime)row["datetime"]);
                        xmms.Add((decimal)row["xmm"]);
                        ymms.Add((decimal)row["ymm"]);
                        notches.Add((decimal)row["notch"]);
                        serv_tss.Add((DateTime)row["serv_ts"]);
                    }

                    const string sql = @"
                        INSERT INTO public.plg_prealign 
                            (eqpid, datetime, xmm, ymm, notch, serv_ts)
                        SELECT
                            u.eqpid, u.datetime, u.xmm, u.ymm, u.notch, u.serv_ts
                        FROM
                            unnest(
                                @eqpids, 
                                @datetimes, 
                                @xmms, 
                                @ymms, 
                                @notches, 
                                @serv_tss
                            ) AS u(eqpid, datetime, xmm, ymm, notch, serv_ts)
                        ON CONFLICT (eqpid, datetime) DO NOTHING;";

                    using (var cmd = new NpgsqlCommand(sql, conn))
                    {
                        cmd.Parameters.AddWithValue("eqpids", eqpids);
                        cmd.Parameters.AddWithValue("datetimes", datetimes);
                        cmd.Parameters.AddWithValue("xmms", xmms);
                        cmd.Parameters.AddWithValue("ymms", ymms);
                        cmd.Parameters.AddWithValue("notches", notches);
                        cmd.Parameters.AddWithValue("serv_tss", serv_tss);

                        int affected = cmd.ExecuteNonQuery();
                        SimpleLogger.Debug($"DB Batch OK ▶ Total processed={dt.Rows.Count}, Inserted={affected}");
                    }
                }
            }
            catch (Exception ex)
            {
                if (ex.InnerException != null)
                {
                    SimpleLogger.Error($"DB Upload failed: {ex.Message} (Inner: {ex.InnerException.Message})");
                }
                else
                {
                    SimpleLogger.Error("DB Upload failed: " + ex.Message);
                }
            }
        }

        /*──────────────── Utilities ───────────────────────*/
        private string GetEqpid(string ini)
        {
            string iniPath = Path.IsPathRooted(ini)
                ? ini
                : Path.Combine(AppDomain.CurrentDomain.BaseDirectory, ini);

            if (!File.Exists(iniPath)) return string.Empty;
            try
            {
                foreach (string ln in File.ReadLines(iniPath))
                {
                    if (ln.Trim().StartsWith("Eqpid", StringComparison.OrdinalIgnoreCase))
                    {
                        int idx = ln.IndexOf('=');
                        if (idx > 0) return ln.Substring(idx + 1).Trim();
                    }
                }
            }
            catch (Exception ex)
            {
                SimpleLogger.Error("GetEqpid failed: " + ex.Message);
            }
            return string.Empty;
        }
    }
}
