// ConnectInfo/DatabaseInfo.cs
using System;
using Npgsql;
using System.IO;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading;
using System.Security.Cryptography; // (AES, SHA256 사용을 위해 필수)
using Newtonsoft.Json; // (FTP 정보 JSON 역직렬화)

namespace ConnectInfo
{
    /// <summary>
    /// 솔루션 전체에서 공유되는 암호화 키를 중앙에서 관리합니다.
    /// </summary>
    public static class AgentCryptoConfig
    {
        public const string AES_COMMON_KEY = "itm-agent-v1-secret";
    }

    internal class FtpConfig
    {
        public string Host { get; set; }
        public int Port { get; set; }
        public string Username { get; set; }
        public string Password { get; set; }
    }

    public sealed class DatabaseInfo
    {
        private static readonly string _configPath;
        private static readonly string _settingsPath; // Settings.ini 경로 추가
        private static readonly object _dbLock = new object();
        private static string _cachedDbConnectionString = null;
        private static DateTime _dbCacheLastRead = DateTime.MinValue;

        static DatabaseInfo()
        {
            _configPath = Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "Connection.ini");
            _settingsPath = Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "Settings.ini");
        }

        private DatabaseInfo() { }
        public static DatabaseInfo CreateDefault() => new DatabaseInfo();

        /// <summary>
        /// PostgreSQL 전용 연결 문자열 생성 및 동적 치환
        /// </summary>
        public string GetConnectionString()
        {
            lock (_dbLock)
            {
                if (_cachedDbConnectionString != null && (DateTime.Now - _dbCacheLastRead).TotalSeconds < 10)
                {
                    return _cachedDbConnectionString;
                }

                try
                {
                    string content = ReadAllTextSafe(_configPath);
                    if (string.IsNullOrEmpty(content))
                    {
                        throw new InvalidOperationException("Connection.ini is empty or not found.");
                    }

                    string encryptedDbConfig = ParseIniValue(content, "Database", "Config");
                    if (string.IsNullOrEmpty(encryptedDbConfig))
                        throw new InvalidOperationException("[Database] Config not found in Connection.ini.");

                    // 원본 평문 연결 문자열
                    string plainConnectionString = DecryptAES(encryptedDbConfig, AgentCryptoConfig.AES_COMMON_KEY);

                    // Settings.ini의 Proxy 설정 읽기 및 동적 스와핑 (DB 포트는 15432 고정)
                    if (GetSettingsIniValue("Network", "UseProxy") == "1")
                    {
                        string proxyIp = GetSettingsIniValue("Network", "ProxyIP");
                        if (!string.IsNullOrEmpty(proxyIp))
                        {
                            var builder = new NpgsqlConnectionStringBuilder(plainConnectionString);
                            builder.Host = proxyIp;
                            builder.Port = 15432; // Proxy DB 포트로 강제 치환

                            // [핵심 추가] 프록시 환경에서 연결이 유휴 상태일 때 끊기는 현상을 방지하기 위해 풀링 강제 해제
                            builder.Pooling = false;

                            plainConnectionString = builder.ConnectionString;
                        }
                    }

                    _cachedDbConnectionString = plainConnectionString;
                    _dbCacheLastRead = DateTime.Now;

                    return _cachedDbConnectionString;
                }
                catch (Exception ex)
                {
                    if (_cachedDbConnectionString != null) return _cachedDbConnectionString;
                    throw new InvalidOperationException("Failed to read/parse/decrypt Connection.ini: " + ex.Message, ex);
                }
            }
        }

        // --- 공용 헬퍼 메서드 ---

        public static string GetIniValue(string section, string key)
        {
            try
            {
                string content = ReadAllTextSafe(_configPath);
                return ParseIniValue(content, section, key);
            }
            catch { return null; }
        }

        /// <summary>
        /// Settings.ini에서 값을 읽어오기 위한 헬퍼 메서드
        /// </summary>
        public static string GetSettingsIniValue(string section, string key)
        {
            try
            {
                string content = ReadAllTextSafe(_settingsPath);
                return ParseIniValue(content, section, key);
            }
            catch { return null; }
        }

        public static string ParseIniValue(string fileContent, string section, string key)
        {
            if (string.IsNullOrEmpty(fileContent)) return null;

            var match = new Regex(
                @"\[" + Regex.Escape(section) + @"\](?:[^\[]*)?" +
                Regex.Escape(key) + @"\s*=\s*(.*)",
                RegexOptions.IgnoreCase | RegexOptions.Singleline
            ).Match(fileContent);

            if (match.Success)
            {
                return match.Groups[1].Value.Split(new[] { '\r', '\n' }, StringSplitOptions.RemoveEmptyEntries)[0].Trim();
            }
            return null;
        }

        public static string ReadAllTextSafe(string path, int timeoutMs = 5000)
        {
            var sw = System.Diagnostics.Stopwatch.StartNew();
            while (true)
            {
                try
                {
                    using (var fs = new FileStream(path, FileMode.Open, FileAccess.Read, FileShare.ReadWrite))
                    using (var sr = new StreamReader(fs, Encoding.UTF8))
                    {
                        return sr.ReadToEnd();
                    }
                }
                catch (FileNotFoundException) { return null; }
                catch (IOException)
                {
                    if (sw.ElapsedMilliseconds > timeoutMs)
                        throw new TimeoutException($"Failed to read {path} within {timeoutMs}ms.");
                    Thread.Sleep(300);
                }
            }
        }

        public static void WriteAllTextSafe(string path, string content, int timeoutMs = 5000)
        {
            var sw = System.Diagnostics.Stopwatch.StartNew();
            while (true)
            {
                try
                {
                    using (var fs = new FileStream(path, FileMode.Create, FileAccess.Write, FileShare.Read))
                    using (var writer = new StreamWriter(fs, Encoding.UTF8))
                    {
                        writer.Write(content);
                        return;
                    }
                }
                catch (IOException)
                {
                    if (sw.ElapsedMilliseconds > timeoutMs)
                        throw new TimeoutException($"Failed to write {path} within {timeoutMs}ms.");
                    Thread.Sleep(300);
                }
            }
        }

        // --- AES 헬퍼 ---
        private static string DecryptAES(string cipherText, string keyString)
        {
            byte[] fullCipher = Convert.FromBase64String(cipherText);
            byte[] keyBytes;
            using (var sha = SHA256.Create())
            {
                keyBytes = sha.ComputeHash(Encoding.UTF8.GetBytes(keyString));
            }

            byte[] iv = new byte[16];
            byte[] cipher = new byte[fullCipher.Length - 16];
            Buffer.BlockCopy(fullCipher, 0, iv, 0, 16);
            Buffer.BlockCopy(fullCipher, 16, cipher, 0, fullCipher.Length - 16);

            using (var aes = new AesManaged())
            {
                aes.Key = keyBytes;
                aes.IV = iv;
                aes.Mode = CipherMode.CBC;
                aes.Padding = PaddingMode.PKCS7;

                using (var decryptor = aes.CreateDecryptor(aes.Key, aes.IV))
                using (var ms = new MemoryStream(cipher))
                using (var cs = new CryptoStream(ms, decryptor, CryptoStreamMode.Read))
                using (var sr = new StreamReader(cs))
                {
                    return sr.ReadToEnd();
                }
            }
        }

        public void TestConnection()
        {
            Console.WriteLine($"[DB] Connection Check Start...");
            string cs = GetConnectionString();
            Console.WriteLine($"[DB] ConnectionString (Masked) ▶ {Regex.Replace(cs, "Password=.*", "Password=***")}");

            using (var conn = new NpgsqlConnection(cs))
            {
                conn.Open();
                Console.WriteLine($"[DB] 연결 성공 ▶ {conn.PostgreSqlVersion}");
            }
        }
    }

    /// <summary>
    /// FileZilla Server (FTPS) / API 서버 접속 정보 관리
    /// </summary>
    public sealed class FtpsInfo
    {
        private static readonly object _ftpLock = new object();
        private static FtpConfig _cachedFtpConfig = null;
        private static DateTime _ftpCacheLastRead = DateTime.MinValue;

        private FtpConfig GetFtpConfig()
        {
            lock (_ftpLock)
            {
                if (_cachedFtpConfig != null && (DateTime.Now - _ftpCacheLastRead).TotalSeconds < 10)
                {
                    return _cachedFtpConfig;
                }

                try
                {
                    string encryptedFtpConfig = DatabaseInfo.GetIniValue("Ftps", "Config");

                    if (string.IsNullOrEmpty(encryptedFtpConfig))
                        throw new InvalidOperationException("[Ftps] Config not found in Connection.ini.");

                    string plainJson = DecryptAES_Internal(encryptedFtpConfig, AgentCryptoConfig.AES_COMMON_KEY);
                    var config = JsonConvert.DeserializeObject<FtpConfig>(plainJson);

                    // Settings.ini의 Proxy 설정 읽기 및 동적 스와핑
                    if (DatabaseInfo.GetSettingsIniValue("Network", "UseProxy") == "1")
                    {
                        string proxyIp = DatabaseInfo.GetSettingsIniValue("Network", "ProxyIP");
                        if (!string.IsNullOrEmpty(proxyIp))
                        {
                            config.Host = proxyIp;
                            config.Port = 18082; // Proxy API 포트로 18082 강제 치환
                        }
                    }

                    _cachedFtpConfig = config;
                    _ftpCacheLastRead = DateTime.Now;

                    return _cachedFtpConfig;
                }
                catch (Exception ex)
                {
                    if (_cachedFtpConfig != null) return _cachedFtpConfig;
                    throw new InvalidOperationException("Failed to read/parse/decrypt [Ftps] Config: " + ex.Message, ex);
                }
            }
        }

        public string Host => GetFtpConfig()?.Host;
        public int Port => GetFtpConfig()?.Port ?? 21;
        public string Username => GetFtpConfig()?.Username;
        public string Password => GetFtpConfig()?.Password;
        public string UploadPath => "/";

        private FtpsInfo() { }
        public static FtpsInfo CreateDefault() => new FtpsInfo();

        private static string DecryptAES_Internal(string cipherText, string keyString)
        {
            byte[] fullCipher = Convert.FromBase64String(cipherText);
            byte[] keyBytes;
            using (var sha = SHA256.Create())
            {
                keyBytes = sha.ComputeHash(Encoding.UTF8.GetBytes(keyString));
            }
            byte[] iv = new byte[16];
            byte[] cipher = new byte[fullCipher.Length - 16];
            Buffer.BlockCopy(fullCipher, 0, iv, 0, 16);
            Buffer.BlockCopy(fullCipher, 16, cipher, 0, fullCipher.Length - 16);
            using (var aes = new AesManaged())
            {
                aes.Key = keyBytes;
                aes.IV = iv;
                aes.Mode = CipherMode.CBC;
                aes.Padding = PaddingMode.PKCS7;
                using (var decryptor = aes.CreateDecryptor(aes.Key, aes.IV))
                using (var ms = new MemoryStream(cipher))
                using (var cs = new CryptoStream(ms, decryptor, CryptoStreamMode.Read))
                using (var sr = new StreamReader(cs))
                {
                    return sr.ReadToEnd();
                }
            }
        }
    }
}
