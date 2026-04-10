using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net.Http;
using System.Reflection;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;
using System.Threading.Tasks;
using CounterStrikeSharp.API;
using CounterStrikeSharp.API.Core;
using CounterStrikeSharp.API.Core.Attributes;
using CounterStrikeSharp.API.Modules.Commands;
using CounterStrikeSharp.API.Modules.Entities;
using CounterStrikeSharp.API.Modules.Events;
using CounterStrikeSharp.API.Modules.Timers;
using CounterStrikeSharp.API.Modules.Utils;

using CsTimer = CounterStrikeSharp.API.Modules.Timers.Timer;

namespace CompStats
{
    [MinimumApiVersion(80)]
    public class PlayerStatsEventTracker : BasePlugin
    {
        public class DebugLogEntry
        {
            public string Timestamp { get; set; } = "";
            public string Reason { get; set; } = "";
            public string PreviousMatchId { get; set; } = "";
            public string NewMatchId { get; set; } = "";
            public string CurrentMap { get; set; } = "";
        }
        public class MatchDatabase
        {
            public string MatchID { get; set; } = "";
            public string MapName { get; set; } = "";
            public string WorkshopID { get; set; } = "";
            public string CollectionID { get; set; } = "";
            public DateTime StartTime { get; set; }
            public DateTime LastUpdated { get; set; }
            public bool MatchComplete { get; set; }

            public int CTWins { get; set; }
            public int TWins { get; set; }
            public int TotalRounds { get; set; }

            [JsonConverter(typeof(InlineListConverter<int>))]
            public List<int> CTScoreHistory { get; set; } = new();

            [JsonConverter(typeof(InlineListConverter<int>))]
            public List<int> TScoreHistory { get; set; } = new();

            [JsonIgnore]
            public bool IsWarmup { get; set; }

            public List<PlayerMatchData> Players { get; set; } = new();

            public List<CombatLog> KillFeed { get; set; } = new();
            public List<ObjectiveLog> EventFeed { get; set; } = new();
            public List<ChatLog> ChatFeed { get; set; } = new();
        }

        public class PlayerMatchData
        {
            public ulong SteamID { get; set; }
            public string Name { get; set; } = "Unknown";
            public bool IsBot { get; set; }

            [JsonPropertyName("Team")]
            [JsonConverter(typeof(InlineListConverter<int>))]
            public List<int> TeamHistory { get; set; } = new();

            [JsonPropertyName("Kills")]
            [JsonConverter(typeof(InlineListConverter<int>))]
            public List<int> KillsHistory { get; set; } = new();

            [JsonPropertyName("Deaths")]
            [JsonConverter(typeof(InlineListConverter<int>))]
            public List<int> DeathsHistory { get; set; } = new();

            [JsonPropertyName("Assists")]
            [JsonConverter(typeof(InlineListConverter<int>))]
            public List<int> AssistsHistory { get; set; } = new();

            [JsonPropertyName("ZeusKills")]
            [JsonConverter(typeof(InlineListConverter<int>))]
            public List<int> ZeusKillsHistory { get; set; } = new();

            [JsonPropertyName("MVPs")]
            [JsonConverter(typeof(InlineListConverter<int>))]
            public List<int> MVPsHistory { get; set; } = new();

            [JsonPropertyName("Score")]
            [JsonConverter(typeof(InlineListConverter<int>))]
            public List<int> ScoreHistory { get; set; } = new();

            [JsonPropertyName("Alive")]
            [JsonConverter(typeof(InlineListConverter<bool>))]
            public List<bool> AliveHistory { get; set; } = new();

            [JsonPropertyName("Inventory")]
            [JsonConverter(typeof(InlineListConverter<string>))]
            public List<string> InventoryHistory { get; set; } = new();


            [JsonIgnore] public int CurrentTeam { get; set; }
            [JsonIgnore] public int CurrentKills { get; set; }
            [JsonIgnore] public int CurrentRoundKills { get; set; }
            [JsonIgnore] public int CurrentDeaths { get; set; }
            [JsonIgnore] public int CurrentAssists { get; set; }
            [JsonIgnore] public int CurrentZeusKills { get; set; }
            [JsonIgnore] public int CurrentMVPs { get; set; }
            [JsonIgnore] public int CurrentScore { get; set; }
            [JsonIgnore] public int CurrentHeadshotKills { get; set; }
            [JsonIgnore] public int CurrentDamage { get; set; }
            [JsonIgnore] public int CurrentUtilityDamage { get; set; }
            [JsonIgnore] public int CurrentEnemiesFlashed { get; set; }
        }

        public class CombatLog
        {
            public int Round { get; set; }
            public string Type { get; set; } = "Unknown";
            public string PlayerTeam { get; set; } = "";
            public string PlayerName { get; set; } = "Unknown";
            public ulong PlayerSteamID { get; set; }
            public string OpponentName { get; set; } = "None";
            public ulong OpponentSteamID { get; set; }
            public string Weapon { get; set; } = "";
            public int Damage { get; set; }
            public bool IsHeadshot { get; set; }
            public string Timestamp { get; set; } = "";
        }

        public class ObjectiveLog
        {
            public int Round { get; set; }
            public string PlayerName { get; set; } = "Unknown";
            public ulong PlayerSteamID { get; set; }
            public string Event { get; set; } = "";
            public string Timestamp { get; set; } = "";
        }

        public class ChatLog
        {
            public int Round { get; set; }
            public string PlayerName { get; set; } = "Unknown";
            public ulong PlayerSteamID { get; set; }
            public string Message { get; set; } = "";
            public bool TeamChat { get; set; }
            public string Timestamp { get; set; } = "";
        }

        public class InlineListConverter<T> : JsonConverter<List<T>>
        {
            public override List<T>? Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
            {
                return JsonSerializer.Deserialize<List<T>>(ref reader, options);
            }

            public override void Write(Utf8JsonWriter writer, List<T> value, JsonSerializerOptions options)
            {
                var compactOptions = new JsonSerializerOptions { WriteIndented = false };
                string jsonString = JsonSerializer.Serialize(value, compactOptions);
                writer.WriteRawValue(jsonString);
            }
        }

        // Live Data Container - Optimized to be a single persistent object
        private MatchDatabase _matchData = new();
        // Fast lookup to find player objects inside _matchData.Players
        private readonly ConcurrentDictionary<ulong, PlayerMatchData> _playerLookup = new();

        private int _currentRound = 1;

        // Caching scores locally to check for 0-0 reset
        private int _ctWins = 0;
        private int _tWins = 0;

        private bool _roundStatsSnapshotTaken = false;
        private bool _matchEndedNormally = false;

        private const int TEAM_CT_MANAGER_ID = 3;
        private const int TEAM_T_MANAGER_ID = 2;

        private readonly Dictionary<string, string> _workshopMapIds = new();
        private string _loadedCollectionId = "N/A";
        private readonly List<string> _loadLog = new();

        private bool _usesMatchLibrarian = true;
        private FileSystemWatcher? _fileWatcher;
        private DateTime _lastReloadTime = DateTime.MinValue;

        private bool _announceZeusLeader = false;
        private bool _announceAces = false;
        private int _highestZeusKills = 0;

        private readonly Dictionary<int, int> _lastDeathTick = new();

        private CsTimer? _spectatorKickTimer = null;
        private CsTimer? _noHumansRestartTimer = null;
        private CancellationTokenSource _workshopCts = new();
        private CommandInfo.CommandListenerCallback? _chatCommandDelegate;

        private string _steamApiKey = "";
        private const string WorkshopContentRelPath = "../bin/linuxsteamrt64/steamapps/workshop/content/730";
        private const string WorkshopGrabLogRelPath = "addons/counterstrikesharp/configs/plugins/CompStats/workshopgrab.log";

        public override string ModuleName => "CompStats";
        public override string ModuleVersion => "2.2.0";
        public override string ModuleAuthor => "|ZAPS| BONE";

        public override void Load(bool hotReload)
        {
            RegisterEventHandler<EventPlayerDeath>(OnPlayerDeath, HookMode.Post);
            RegisterEventHandler<EventRoundOfficiallyEnded>(OnRoundEnded, HookMode.Post);
            RegisterEventHandler<EventRoundPrestart>(OnRoundPrestart, HookMode.Post);
            RegisterEventHandler<EventMapShutdown>(OnMapShutdown, HookMode.Post);
            RegisterEventHandler<EventCsWinPanelMatch>(OnMatchEnd, HookMode.Post);

            // Removed OnMatchRestart, relying solely on Score 0:0 check in RoundPrestart
            // RegisterEventHandler<EventRoundAnnounceMatchStart>(OnMatchRestart, HookMode.Post);

            RegisterEventHandler<EventPlayerDisconnect>(OnPlayerDisconnect, HookMode.Post);
            RegisterEventHandler<EventPlayerTeam>(OnPlayerTeam, HookMode.Post);

            RegisterEventHandler<EventBombPlanted>(OnBombPlanted, HookMode.Post);
            RegisterEventHandler<EventBombDefused>(OnBombDefused, HookMode.Post);
            RegisterEventHandler<EventBombExploded>(OnBombExploded, HookMode.Post);
            RegisterEventHandler<EventHostageFollows>(OnHostagePickup, HookMode.Post);
            RegisterEventHandler<EventHostageRescued>(OnHostageRescued, HookMode.Post);
            RegisterEventHandler<EventPlayerHurt>(OnPlayerHurt, HookMode.Post);
            RegisterEventHandler<EventPlayerBlind>(OnPlayerBlind, HookMode.Post);

            _chatCommandDelegate = OnPlayerChatCommand;
            AddCommandListener("say", _chatCommandDelegate);
            AddCommandListener("say_team", _chatCommandDelegate);

            LoadConfigIni();
            LoadWorkshopIni();
            InitializeFileWatcher();

            // REMOVED: Initializing match ID on load.
            // We now strictly wait for OnRoundPrestart to detect a 0-0 score before creating a Match ID.
            // This prevents duplicate match creation on map load and ensures restarts on the same map are handled correctly.

            AddCommand("css_players", "Print tracked player stats (humans and bots)", (caller, cmdInfo) =>
            {
                if (caller != null)
                {
                    cmdInfo.ReplyToCommand("Command disabled for players.");
                }
                else
                {
                    PrintPlayerStats(caller, cmdInfo);
                }
            });

            AddCommand("css_workshoplog", "Show the log of loading workshop.ini", (caller, cmdInfo) =>
            {
                CmdLog(caller, cmdInfo);
            });

            AddCommand("css_collectionid", "Output the server's loaded collection ID", (caller, cmdInfo) =>
            {
                cmdInfo.ReplyToCommand($"Server Collection ID: {_loadedCollectionId}");
            });

            AddCommand("css_databaseon", "Check if database recording is enabled", (caller, cmdInfo) =>
            {
                cmdInfo.ReplyToCommand($"[CompStats] Database Recording (UsesMatchLibrarian): {(_usesMatchLibrarian ? "ENABLED" : "DISABLED")}");
            });

            string baseGameDir = Server.GameDirectory;
            if (Path.GetFileName(baseGameDir) == "game")
            {
                baseGameDir = Path.Combine(baseGameDir, "csgo");
            }

            Task.Run(async () =>
            {
                try
                {
                    await ProcessWorkshopCollection(baseGameDir, _workshopCts.Token);
                }
                catch (OperationCanceledException) { }
                catch (Exception ex)
                {
                    LogWorkshopGrabber(baseGameDir, $"CRITICAL ERROR: {ex.Message}");
                }
            });
        }

        public override void Unload(bool hotReload)
        {
            _workshopCts.Cancel();
            _workshopCts.Dispose();
            _workshopCts = new CancellationTokenSource();

            if (_noHumansRestartTimer != null)
            {
                _noHumansRestartTimer.Kill();
                _noHumansRestartTimer = null;
            }

            if (_spectatorKickTimer != null)
            {
                _spectatorKickTimer.Kill();
                _spectatorKickTimer = null;
            }

            if (_fileWatcher != null)
            {
                _fileWatcher.EnableRaisingEvents = false;
                _fileWatcher.Changed -= OnConfigFileChanged;
                _fileWatcher.Dispose();
                _fileWatcher = null;
            }

            if (_chatCommandDelegate != null)
            {
                RemoveCommandListener("say", _chatCommandDelegate, HookMode.Pre);
                RemoveCommandListener("say_team", _chatCommandDelegate, HookMode.Pre);
                _chatCommandDelegate = null;
            }

            // Deregister all event handlers to prevent GC delegate crashes after hot reload
            DeregisterEventHandler<EventPlayerDeath>(OnPlayerDeath, HookMode.Post);
            DeregisterEventHandler<EventRoundOfficiallyEnded>(OnRoundEnded, HookMode.Post);
            DeregisterEventHandler<EventRoundPrestart>(OnRoundPrestart, HookMode.Post);
            DeregisterEventHandler<EventMapShutdown>(OnMapShutdown, HookMode.Post);
            DeregisterEventHandler<EventCsWinPanelMatch>(OnMatchEnd, HookMode.Post);
            DeregisterEventHandler<EventPlayerDisconnect>(OnPlayerDisconnect, HookMode.Post);
            DeregisterEventHandler<EventPlayerTeam>(OnPlayerTeam, HookMode.Post);
            DeregisterEventHandler<EventBombPlanted>(OnBombPlanted, HookMode.Post);
            DeregisterEventHandler<EventBombDefused>(OnBombDefused, HookMode.Post);
            DeregisterEventHandler<EventBombExploded>(OnBombExploded, HookMode.Post);
            DeregisterEventHandler<EventHostageFollows>(OnHostagePickup, HookMode.Post);
            DeregisterEventHandler<EventHostageRescued>(OnHostageRescued, HookMode.Post);
            DeregisterEventHandler<EventPlayerHurt>(OnPlayerHurt, HookMode.Post);
            DeregisterEventHandler<EventPlayerBlind>(OnPlayerBlind, HookMode.Post);
        }

        private string CompStatsConfigDir => Path.Combine(Server.GameDirectory, "csgo", "addons", "counterstrikesharp", "configs", "plugins", "CompStats");
        private string WorkshopIniPath => Path.Combine(CompStatsConfigDir, "workshop.ini");
        private string GeneralConfigPath => Path.Combine(CompStatsConfigDir, "config.ini");

        private string MatchLibrarianDir => Path.Combine(Server.GameDirectory, "csgo", "addons", "counterstrikesharp", "configs", "plugins", "MatchLibrarian");
        private string MatchesDirPath => Path.Combine(MatchLibrarianDir, "matches");

        private async Task ProcessWorkshopCollection(string csgoDir, CancellationToken token)
        {
            string configIniPath = Path.GetFullPath(Path.Combine(csgoDir, "addons/counterstrikesharp/configs/plugins/CompStats/config.ini"));
            string workshopIniPath = Path.GetFullPath(Path.Combine(csgoDir, "addons/counterstrikesharp/configs/plugins/CompStats/workshop.ini"));
            string workshopContentPath = Path.GetFullPath(Path.Combine(csgoDir, WorkshopContentRelPath));

            LogWorkshopGrabber(csgoDir, $"--- Starting Workshop Map Loader Session: {DateTime.UtcNow} ---");

            string collectionId = "";

            if (File.Exists(configIniPath))
            {
                foreach (var line in File.ReadAllLines(configIniPath))
                {
                    var trimmed = line.Trim();
                    if (string.IsNullOrEmpty(trimmed) || trimmed.StartsWith("#") || trimmed.StartsWith("//")) continue;

                    if (trimmed.StartsWith("api_key=", StringComparison.OrdinalIgnoreCase))
                    {
                        var parts = trimmed.Split('=', 2);
                        if (parts.Length > 1) _steamApiKey = parts[1].Trim();
                    }
                    else if (trimmed.StartsWith("collection_id=", StringComparison.OrdinalIgnoreCase))
                    {
                        var parts = trimmed.Split('=', 2);
                        if (parts.Length > 1) collectionId = parts[1].Trim();
                    }
                }
            }

            if (string.IsNullOrEmpty(_steamApiKey))
            {
                LogWorkshopGrabber(csgoDir, "Error: 'api_key=' not found or empty in config.ini");
                return;
            }

            if (string.IsNullOrEmpty(collectionId))
            {
                LogWorkshopGrabber(csgoDir, "Error: 'collection_id=' not found in config.ini");
                return;
            }

            token.ThrowIfCancellationRequested();

            LogWorkshopGrabber(csgoDir, $"Processing Collection ID from Config: {collectionId}");

            List<string> mapIds;
            try
            {
                mapIds = await FetchCollectionItems(collectionId, token);
                LogWorkshopGrabber(csgoDir, $"API success. Found {mapIds.Count} items in collection.");
            }
            catch (OperationCanceledException) { throw; }
            catch (Exception ex)
            {
                LogWorkshopGrabber(csgoDir, $"API Request Failed: {ex.Message}");
                return;
            }

            token.ThrowIfCancellationRequested();

            Dictionary<string, string> validMaps = new Dictionary<string, string>();

            if (!Directory.Exists(workshopContentPath))
            {
                LogWorkshopGrabber(csgoDir, $"Error: Workshop content path missing: {workshopContentPath}");
                return;
            }

            foreach (var mapId in mapIds)
            {
                string mapFolderPath = Path.Combine(workshopContentPath, mapId);

                if (!Directory.Exists(mapFolderPath)) continue;

                var vpkFiles = Directory.GetFiles(mapFolderPath, "*.vpk");
                if (vpkFiles.Length == 0) continue;

                string mainVpkPath;
                var dirVpk = vpkFiles.FirstOrDefault(f => f.EndsWith("_dir.vpk", StringComparison.OrdinalIgnoreCase));

                if (dirVpk != null)
                {
                    mainVpkPath = dirVpk;
                }
                else
                {
                    Array.Sort(vpkFiles);
                    mainVpkPath = vpkFiles[0];
                }

                string? internalMapName = ExtractMapNameFromVpk(mainVpkPath, mapId, csgoDir);

                if (!string.IsNullOrEmpty(internalMapName))
                {
                    validMaps[internalMapName] = mapId;
                    LogWorkshopGrabber(csgoDir, $"Identified: {internalMapName} -> {mapId}");
                }
                else
                {
                    LogWorkshopGrabber(csgoDir, $"Warning: Could not parse map name from VPK for ID {mapId}");
                }
            }

            List<string> newOutput = new List<string>();
            newOutput.Add($"// Generated by CompStats from Collection: {collectionId}");

            foreach (var kvp in validMaps.OrderBy(x => x.Key))
            {
                newOutput.Add($"{kvp.Key}={kvp.Value}");
            }

            try
            {
                File.WriteAllLines(workshopIniPath, newOutput);
                LogWorkshopGrabber(csgoDir, $"Success! Updated workshop.ini with {validMaps.Count} maps.");
                Server.NextFrame(LoadWorkshopIni);
            }
            catch (Exception ex)
            {
                LogWorkshopGrabber(csgoDir, $"Error writing workshop.ini: {ex.Message}");
            }
        }

        private async Task<List<string>> FetchCollectionItems(string collectionId, CancellationToken token)
        {
            using var client = new HttpClient();
            var content = new FormUrlEncodedContent(new[]
            {
                new KeyValuePair<string, string>("collectioncount", "1"),
                new KeyValuePair<string, string>("publishedfileids[0]", collectionId)
            });

            string url = $"https://api.steampowered.com/ISteamRemoteStorage/GetCollectionDetails/v1/?key={_steamApiKey}";
            var response = await client.PostAsync(url, content, token);
            response.EnsureSuccessStatusCode();

            string json = await response.Content.ReadAsStringAsync(token);
            var data = JsonSerializer.Deserialize<SteamCollectionResponse>(json);

            List<string> ids = new List<string>();
            if (data?.response?.collectiondetails != null && data.response.collectiondetails.Count > 0)
            {
                var children = data.response.collectiondetails[0].children;
                if (children != null)
                {
                    foreach (var child in children)
                    {
                        if (child.publishedfileid != null)
                            ids.Add(child.publishedfileid);
                    }
                }
            }
            return ids;
        }

        private void LogMatchCreationDebug(string reason, string newId)
        {
            try
            {
                string debugFilePath = Path.Combine(MatchLibrarianDir, "debug.json");
                List<DebugLogEntry> logEntries;

                if (File.Exists(debugFilePath))
                {
                    string existingJson = File.ReadAllText(debugFilePath);
                    try
                    {
                        logEntries = JsonSerializer.Deserialize<List<DebugLogEntry>>(existingJson) ?? new List<DebugLogEntry>();
                    }
                    catch
                    {
                        logEntries = new List<DebugLogEntry>();
                    }
                }
                else
                {
                    logEntries = new List<DebugLogEntry>();
                }

                var entry = new DebugLogEntry
                {
                    Timestamp = DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss"),
                    Reason = reason,
                    PreviousMatchId = _matchData.MatchID,
                    NewMatchId = newId,
                    CurrentMap = Server.MapName
                };

                logEntries.Add(entry);

                if (logEntries.Count > 50)
                {
                    logEntries = logEntries.Skip(logEntries.Count - 50).ToList();
                }

                var jsonOptions = new JsonSerializerOptions { WriteIndented = true };
                File.WriteAllText(debugFilePath, JsonSerializer.Serialize(logEntries, jsonOptions));
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[CompStats] Failed to write debug.json: {ex.Message}");
            }
        }

        private string? ExtractMapNameFromVpk(string vpkPath, string mapId, string logDir)
        {
            try
            {
                using var fs = new FileStream(vpkPath, FileMode.Open, FileAccess.Read);
                using var reader = new BinaryReader(fs);

                uint signature = reader.ReadUInt32();
                if (signature != 0x55aa1234) return null;

                uint version = reader.ReadUInt32();
                uint treeSize = reader.ReadUInt32();
                if (version == 2) reader.ReadBytes(16);

                long treeStart = fs.Position;
                long treeEnd = treeStart + treeSize;

                List<string> foundMaps = new List<string>();

                while (fs.Position < treeEnd)
                {
                    string extension = ReadNullTerminatedString(reader);
                    if (extension == "") break;

                    while (fs.Position < treeEnd)
                    {
                        string path = ReadNullTerminatedString(reader);
                        if (path == "") break;

                        string normPath = path.Replace("\\", "/");
                        bool isMapLocation = (normPath == "maps" || string.IsNullOrWhiteSpace(normPath));

                        while (fs.Position < treeEnd)
                        {
                            string filename = ReadNullTerminatedString(reader);
                            if (filename == "") break;

                            uint crc = reader.ReadUInt32();
                            ushort preloadBytes = reader.ReadUInt16();
                            reader.ReadUInt16();
                            reader.ReadUInt32();
                            reader.ReadUInt32();
                            ushort terminator = reader.ReadUInt16();

                            if (terminator != 0xFFFF) break;
                            if (preloadBytes > 0) reader.ReadBytes(preloadBytes);

                            if (extension == "vpk" && isMapLocation)
                            {
                                foundMaps.Add(filename);
                            }
                        }
                    }
                }

                if (foundMaps.Count > 0)
                {
                    foundMaps.Sort();
                    return foundMaps[0];
                }
            }
            catch
            {
            }
            return null;
        }

        private string ReadNullTerminatedString(BinaryReader reader)
        {
            List<byte> charBytes = new List<byte>();
            while (true)
            {
                if (reader.BaseStream.Position >= reader.BaseStream.Length) break;
                byte b = reader.ReadByte();
                if (b == 0x00) break;
                charBytes.Add(b);
            }
            return Encoding.UTF8.GetString(charBytes.ToArray());
        }

        private void LogWorkshopGrabber(string baseDir, string message)
        {
            try
            {
                string logFullPath = Path.Combine(baseDir, WorkshopGrabLogRelPath);
                string timestamp = DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss");
                string logLine = $"[{timestamp}] {message}{Environment.NewLine}";

                string? directory = Path.GetDirectoryName(logFullPath);
                if (directory != null && !Directory.Exists(directory))
                {
                    Directory.CreateDirectory(directory);
                }
                File.AppendAllText(logFullPath, logLine);
            }
            catch { }
        }

        private void StartNewMatchId(string reason)
        {
            // Reset main variables
            string newId = DateTime.UtcNow.ToString("yyyy-MM-dd-HH-mm-ss");
            LogMatchCreationDebug(reason, newId);

            // Create a fresh MatchDatabase object.
            _matchData = new MatchDatabase();
            _matchData.MatchID = newId;
            _matchData.StartTime = DateTime.UtcNow;

            _highestZeusKills = 0;

            // Clear lookups as the old objects are gone
            _playerLookup.Clear();
            _lastDeathTick.Clear();

            _currentRound = 1;
            _matchEndedNormally = false;
            _roundStatsSnapshotTaken = false;

            Console.WriteLine($"[CompStats] Started new Match ID: {_matchData.MatchID} ({reason})");
        }

        private void InitializeFileWatcher()
        {
            try
            {
                if (!Directory.Exists(CompStatsConfigDir)) Directory.CreateDirectory(CompStatsConfigDir);
                if (!Directory.Exists(MatchLibrarianDir)) Directory.CreateDirectory(MatchLibrarianDir);
                if (!Directory.Exists(MatchesDirPath)) Directory.CreateDirectory(MatchesDirPath);

                _fileWatcher = new FileSystemWatcher(CompStatsConfigDir);
                _fileWatcher.NotifyFilter = NotifyFilters.LastWrite | NotifyFilters.FileName;
                _fileWatcher.Filter = "*.ini";
                _fileWatcher.Changed += OnConfigFileChanged;
                _fileWatcher.EnableRaisingEvents = true;
                Console.WriteLine($"[CompStats] Watching for config changes in: {CompStatsConfigDir}");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[CompStats] Failed to initialize file watcher: {ex.Message}");
            }
        }

        private void OnConfigFileChanged(object sender, FileSystemEventArgs e)
        {
            if ((DateTime.UtcNow - _lastReloadTime).TotalSeconds < 1) return;
            _lastReloadTime = DateTime.UtcNow;

            if (e.Name != null && e.Name.Contains("workshop.ini"))
            {
                Server.NextFrame(LoadWorkshopIni);
            }
            else if (e.Name != null && e.Name.Contains("config.ini"))
            {
                Server.NextFrame(LoadConfigIni);
            }
        }

        private void LoadConfigIni()
        {
            try
            {
                if (!Directory.Exists(CompStatsConfigDir)) Directory.CreateDirectory(CompStatsConfigDir);

                if (!File.Exists(GeneralConfigPath))
                {
                    string defaultConfig = @"// CompStats General Configuration
UsesMatchLibrarian=true
// Announce when a player takes the lead in Zeus kills (true/false)
announce_zeus_leader=true
// Announce when a player gets an Ace (5 kills in a round) (true/false)
announce_aces=true
// Insert your Steam Web API Key below
api_key=
// Insert your Workshop Collection ID below
collection_id=";
                    File.WriteAllText(GeneralConfigPath, defaultConfig);
                    _usesMatchLibrarian = true;
                    _loadedCollectionId = "N/A";
                    _announceZeusLeader = true;
                    Console.WriteLine("[CompStats] Created default config.ini.");
                    return;
                }

                foreach (var line in File.ReadAllLines(GeneralConfigPath))
                {
                    var trimmed = line.Trim();
                    if (string.IsNullOrEmpty(trimmed) || trimmed.StartsWith("//") || trimmed.StartsWith("#")) continue;

                    var parts = trimmed.Split('=', 2);
                    if (parts.Length != 2) continue;

                    var key = parts[0].Trim();
                    var value = parts[1].Trim();

                    if (key.Equals("UsesMatchLibrarian", StringComparison.OrdinalIgnoreCase))
                    {
                        if (bool.TryParse(value, out bool result)) _usesMatchLibrarian = result;
                    }
                    else if (key.Equals("api_key", StringComparison.OrdinalIgnoreCase))
                    {
                        _steamApiKey = value;
                    }
                    else if (key.Equals("announce_zeus_leader", StringComparison.OrdinalIgnoreCase))
                    {
                        if (bool.TryParse(value, out bool result)) _announceZeusLeader = result;
                    }
                    else if (key.Equals("announce_aces", StringComparison.OrdinalIgnoreCase))
                    {
                        if (bool.TryParse(value, out bool result)) _announceAces = result;
                    }
                    else if (key.Equals("collection_id", StringComparison.OrdinalIgnoreCase))
                    {
                        _loadedCollectionId = value;
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[CompStats] Error loading config.ini: {ex.Message}");
            }
        }

        private void LoadWorkshopIni()
        {
            _workshopMapIds.Clear();
            _loadLog.Clear();
            _loadLog.Add($"Reading workshop.ini from: {WorkshopIniPath}");

            try
            {
                if (!Directory.Exists(CompStatsConfigDir)) Directory.CreateDirectory(CompStatsConfigDir);

                if (!File.Exists(WorkshopIniPath))
                {
                    string defaultWorkshop = @"// This file is automatically generated by CompStats if api_key and collection_id are set in config.ini
// You can also manually add map=id pairs here.";

                    File.WriteAllText(WorkshopIniPath, defaultWorkshop);
                    _loadLog.Add("Created default workshop.ini.");
                    Console.WriteLine("[CompStats] Created default workshop.ini.");
                }

                string[] lines = File.ReadAllLines(WorkshopIniPath);
                foreach (string line in lines)
                {
                    string trimmed = line.Trim();
                    if (string.IsNullOrEmpty(trimmed) || trimmed.StartsWith("//") || trimmed.StartsWith("#")) continue;

                    string[] parts = trimmed.Split('=');
                    if (parts.Length < 2) continue;

                    string key = parts[0].Trim();
                    string value = parts[1].Trim();

                    if (key.Equals("collection_id", StringComparison.OrdinalIgnoreCase))
                    {
                    }
                    else
                    {
                        if (!_workshopMapIds.ContainsKey(key))
                        {
                            _workshopMapIds.Add(key, value);
                        }
                    }
                }
                _loadLog.Add($"DONE: Loaded CollectionID: {_loadedCollectionId} | Mapped Maps: {_workshopMapIds.Count}");
                Console.WriteLine($"[CompStats] Loaded CollectionID: {_loadedCollectionId} and {_workshopMapIds.Count} map IDs.");
            }
            catch (Exception ex)
            {
                _loadLog.Add($"EXCEPTION: {ex.Message}");
                Console.WriteLine($"[CompStats] Exception loading workshop.ini: {ex.Message}");
            }
        }

        private void CmdLog(CCSPlayerController? caller, CommandInfo info)
        {
            info.ReplyToCommand("--- workshop.ini Load Log ---");
            foreach (var msg in _loadLog) info.ReplyToCommand(msg);
            info.ReplyToCommand("--- End Log ---");
        }

        private bool IsWarmup()
        {
            try
            {
                var gameRulesProxy = Utilities.FindAllEntitiesByDesignerName<CCSGameRulesProxy>("cs_gamerules").FirstOrDefault();
                if (gameRulesProxy == null || !gameRulesProxy.IsValid || gameRulesProxy.GameRules == null) return false;

                return gameRulesProxy.GameRules.WarmupPeriod;
            }
            catch { return false; }
        }

        private HookResult OnRoundPrestart(EventRoundPrestart @event, GameEventInfo info)
        {
            bool isWarmup = IsWarmup();
            _matchData.IsWarmup = isWarmup;
            _roundStatsSnapshotTaken = false;

            foreach (var kvp in _playerLookup)
            {
                kvp.Value.CurrentRoundKills = 0;
            }

            UpdateTeamScores();

            // ONLY start a new match ID if the score is 0-0 and it is NOT warmup.
            // This covers map changes (starts at 0-0), restartgame (resets to 0-0), etc.
            if (!isWarmup && _ctWins == 0 && _tWins == 0)
            {
                // Logic to prevent re-triggering if we just started
                // If total rounds recorded is > 0, we definitely need a reset.
                // If total rounds is 0, we might have just reset. 
                // However, "StartNewMatchId" is cheap if the data is already empty.
                // A simple way to verify if we *just* reset is checking if the list is empty.
                // But we want to guarantee a new ID on 0-0.
                // To avoid looping in the same round, we rely on the fact RoundPrestart fires once per round.

                // If we have data from a previous match in memory, or if this is a fresh start event.
                // We simply check if we have any round history. If we do, this 0-0 is definitely a new match.
                // If we don't have round history, we can still reset to be safe and ensure the timestamp is fresh.
                StartNewMatchId("Score Reset 0-0");
            }

            return HookResult.Continue;
        }

        private HookResult OnMapShutdown(EventMapShutdown @event, GameEventInfo info)
        {
            // Optional: Save on shutdown to prevent total data loss if server crashes,
            // even though prompt said "only... on each round end".
            // Generally safer to keep this, but respecting the prompt's focus on logic.
            // I will leave it out to strictly follow "Only save updates... on each round end".
            return HookResult.Continue;
        }

        private HookResult OnMatchEnd(EventCsWinPanelMatch @event, GameEventInfo info)
        {
            _matchEndedNormally = true;
            _matchData.MatchComplete = true;

            // Kill any outstanding timers that could fire during map transition
            if (_noHumansRestartTimer != null) { _noHumansRestartTimer.Kill(); _noHumansRestartTimer = null; }
            if (_spectatorKickTimer != null) { _spectatorKickTimer.Kill(); _spectatorKickTimer = null; }

            try
            {
                if (!_roundStatsSnapshotTaken)
                {
                    SnapshotRoundStats();
                }

                SaveMatchData();
                Console.WriteLine($"[CompStats] Match Finished. Final data saved for ID: {_matchData.MatchID}");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[CompStats] OnMatchEnd error: {ex.Message}");
            }
            return HookResult.Continue;
        }

        private HookResult OnPlayerDisconnect(EventPlayerDisconnect @event, GameEventInfo info)
        {
            var player = @event.Userid;

            if (player != null && !player.IsBot && !player.IsHLTV)
            {
                var remainingActiveHumans = Utilities.GetPlayers().Count(p =>
                    !p.IsBot &&
                    !p.IsHLTV &&
                    p.Slot != player.Slot &&
                    (p.TeamNum == 2 || p.TeamNum == 3));

                if (remainingActiveHumans == 0 && _noHumansRestartTimer == null && !_matchEndedNormally)
                {
                    Console.WriteLine("[CompStats] No active humans detected. Scheduling restart in 90 seconds.");
                    _noHumansRestartTimer = AddTimer(90.0f, OnNoHumansRestartTimer, TimerFlags.STOP_ON_MAPCHANGE);
                }

                CheckAndHandlePlayerCounts(player.Slot);
            }

            return HookResult.Continue;
        }

        private HookResult OnPlayerTeam(EventPlayerTeam @event, GameEventInfo info)
        {
            Server.NextFrame(() => CheckAndHandlePlayerCounts());
            return HookResult.Continue;
        }

        private void CheckAndHandlePlayerCounts(int? ignoreSlot = null)
        {
            var allPlayers = Utilities.GetPlayers();
            int activeHumans = 0;
            int specHumans = 0;

            foreach (var p in allPlayers)
            {
                if (p == null || !p.IsValid || p.IsBot || p.IsHLTV) continue;
                if (ignoreSlot.HasValue && p.Slot == ignoreSlot.Value) continue;

                if (p.TeamNum == 2 || p.TeamNum == 3)
                {
                    activeHumans++;
                }
                else if (p.TeamNum == 1)
                {
                    specHumans++;
                }
            }

            if (activeHumans == 0 && specHumans > 0)
            {
                if (_spectatorKickTimer == null)
                {
                    Server.PrintToChatAll($" {ChatColors.Red}[CompStats] WARNING: NO ACTIVE PLAYERS. SPECTATORS WILL BE KICKED IN 30 SECONDS.");
                    _spectatorKickTimer = AddTimer(30.0f, KickSpectatorsAndRestart, TimerFlags.STOP_ON_MAPCHANGE);
                }
            }
            else if (activeHumans > 0)
            {
                if (_spectatorKickTimer != null)
                {
                    _spectatorKickTimer.Kill();
                    _spectatorKickTimer = null;
                    Server.PrintToChatAll(" [CompStats] Active player joined. Spectator kick timer cancelled.");
                }
                CancelNoHumansRestartTimer();
            }
            else if (activeHumans == 0 && specHumans == 0 && _spectatorKickTimer != null)
            {
                _spectatorKickTimer.Kill();
                _spectatorKickTimer = null;
            }
        }

        private void CancelNoHumansRestartTimer()
        {
            if (_noHumansRestartTimer != null)
            {
                _noHumansRestartTimer.Kill();
                _noHumansRestartTimer = null;
                Console.WriteLine("[CompStats] No-humans restart timer cancelled. Active players present.");
            }
        }

        private void OnNoHumansRestartTimer()
        {
            _noHumansRestartTimer = null;

            try
            {
                var activeHumans = Utilities.GetPlayers().Count(p =>
                    p != null &&
                    p.IsValid &&
                    !p.IsBot &&
                    !p.IsHLTV &&
                    (p.TeamNum == 2 || p.TeamNum == 3));

                if (activeHumans > 0)
                {
                    Console.WriteLine("[CompStats] No-humans restart aborted. Active players found.");
                    return;
                }

                Console.WriteLine("[CompStats] Last active human left. Restarting game to reset match.");
                Server.ExecuteCommand("mp_restartgame 1");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[CompStats] No-humans restart timer error (possibly mid-map-transition): {ex.Message}");
            }
        }

        private void KickSpectatorsAndRestart()
        {
            _spectatorKickTimer = null;

            try
            {
                var allPlayers = Utilities.GetPlayers();
                bool kicked = false;

                foreach (var p in allPlayers)
                {
                    if (p != null && p.IsValid && !p.IsBot && !p.IsHLTV && p.TeamNum == 1)
                    {
                        Server.ExecuteCommand($"kickid {p.UserId} \"AFK Spectator\"");
                        kicked = true;
                    }
                }

                if (kicked)
                {
                    Console.WriteLine("[CompStats] Kicked spectators due to inactivity.");
                }

                Server.ExecuteCommand("mp_restartgame 1");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[CompStats] KickSpectatorsAndRestart error (possibly mid-map-transition): {ex.Message}");
            }
        }

        private PlayerMatchData GetOrAddPlayer(CCSPlayerController player)
        {
            if (player == null || !player.IsValid) return new PlayerMatchData();

            ulong steamId = player.SteamID;
            if (steamId == 0) steamId = (ulong)player.Handle.ToInt64();

            return _playerLookup.GetOrAdd(steamId, _ => {
                var newData = new PlayerMatchData
                {
                    SteamID = steamId,
                    Name = player.PlayerName ?? "Unknown",
                    IsBot = player.IsBot,
                    CurrentTeam = player.TeamNum
                };

                // Backfill history if player joins late
                for (int i = 0; i < _currentRound - 1; i++)
                {
                    newData.TeamHistory.Add(0);
                    newData.KillsHistory.Add(0);
                    newData.DeathsHistory.Add(0);
                    newData.AssistsHistory.Add(0);
                    newData.ZeusKillsHistory.Add(0);
                    newData.MVPsHistory.Add(0);
                    newData.ScoreHistory.Add(0);
                    newData.AliveHistory.Add(false);
                    newData.InventoryHistory.Add("");
                }

                // CRITICAL: Add to the main list directly
                lock (_matchData.Players)
                {
                    _matchData.Players.Add(newData);
                }

                return newData;
            });
        }

        private string GetTeamName(int teamNum)
        {
            return teamNum switch
            {
                2 => "T",
                3 => "CT",
                1 => "SPEC",
                _ => "None"
            };
        }

        private HookResult OnPlayerDeath(EventPlayerDeath @event, GameEventInfo info)
        {
            if (IsWarmup()) return HookResult.Continue;

            try
            {
                var victim = @event.Userid;

                // Prevent processing duplicate events for the same death on the same tick
                if (victim != null && victim.IsValid)
                {
                    int tick = Server.TickCount;
                    if (_lastDeathTick.TryGetValue(victim.Slot, out int lastTick) && lastTick == tick)
                        return HookResult.Continue;
                    _lastDeathTick[victim.Slot] = tick;
                }

                var attacker = @event.Attacker;
                var assister = @event.Assister;
                string weaponName = @event.Weapon ?? "unknown";
                int damageDone = @event.DmgHealth;
                bool isHeadshot = @event.Headshot;

                if (victim != null && victim.IsValid)
                {
                    var data = GetOrAddPlayer(victim);
                    data.CurrentDeaths++;

                    string attackerName = (attacker != null && attacker.IsValid) ? (attacker.PlayerName ?? "Unknown") : "World/Self";
                    ulong attackerSteamID = (attacker != null && attacker.IsValid) ? attacker.SteamID : 0;

                    _matchData.KillFeed.Add(new CombatLog
                    {
                        Round = _currentRound,
                        Type = "Death",
                        PlayerTeam = GetTeamName(victim.TeamNum),
                        PlayerName = data.Name,
                        PlayerSteamID = data.SteamID,
                        OpponentName = attackerName,
                        OpponentSteamID = attackerSteamID,
                        Weapon = weaponName,
                        Damage = damageDone,
                        IsHeadshot = isHeadshot,
                        Timestamp = DateTime.UtcNow.ToString("HH:mm:ss")
                    });
                }

                if (attacker != null && attacker.IsValid && attacker != victim)
                {
                    var data = GetOrAddPlayer(attacker);
                    data.CurrentKills++;
                    if (isHeadshot) data.CurrentHeadshotKills++;

                    data.CurrentRoundKills++;
                    if (_announceAces && data.CurrentRoundKills == 5)
                    {
                        Server.PrintToChatAll($" {ChatColors.Yellow}{data.Name} just got an Ace!");
                    }

                    bool victimIsBot = victim != null && victim.IsValid && victim.IsBot;
                    if (weaponName.Contains("taser", StringComparison.OrdinalIgnoreCase) && !IsWarmup() && !victimIsBot)
                    {
                        data.CurrentZeusKills++;
                        if (data.CurrentZeusKills > _highestZeusKills)
                        {
                            _highestZeusKills = data.CurrentZeusKills;

                            if (_announceZeusLeader)
                            {
                                Server.PrintToChatAll($" {ChatColors.Blue}New Zeus Leader: {data.Name}");
                            }
                        }
                    }

                    string victimName = (victim != null && victim.IsValid) ? (victim.PlayerName ?? "Unknown") : "Unknown";
                    ulong victimSteamID = (victim != null && victim.IsValid) ? victim.SteamID : 0;

                    _matchData.KillFeed.Add(new CombatLog
                    {
                        Round = _currentRound,
                        Type = "Kill",
                        PlayerTeam = GetTeamName(attacker.TeamNum),
                        PlayerName = data.Name,
                        PlayerSteamID = data.SteamID,
                        OpponentName = victimName,
                        OpponentSteamID = victimSteamID,
                        Weapon = weaponName,
                        Damage = damageDone,
                        IsHeadshot = isHeadshot,
                        Timestamp = DateTime.UtcNow.ToString("HH:mm:ss")
                    });
                }

                if (assister != null && assister.IsValid && assister != attacker && assister != victim)
                {
                    var data = GetOrAddPlayer(assister);
                    data.CurrentAssists++;
                }
            }
            catch { }

            return HookResult.Continue;
        }

        private void LogObjective(CCSPlayerController? player, string eventDescription)
        {
            if (player == null || !player.IsValid || IsWarmup()) return;

            var data = GetOrAddPlayer(player);
            _matchData.EventFeed.Add(new ObjectiveLog
            {
                Round = _currentRound,
                PlayerName = data.Name,
                PlayerSteamID = data.SteamID,
                Event = eventDescription,
                Timestamp = DateTime.UtcNow.ToString("HH:mm:ss")
            });
        }

        private HookResult OnBombPlanted(EventBombPlanted @event, GameEventInfo info)
        {
            LogObjective(@event.Userid, "Bomb Planted");
            return HookResult.Continue;
        }

        private HookResult OnBombDefused(EventBombDefused @event, GameEventInfo info)
        {
            LogObjective(@event.Userid, "Bomb Defused");
            return HookResult.Continue;
        }

        private HookResult OnBombExploded(EventBombExploded @event, GameEventInfo info)
        {
            LogObjective(@event.Userid, "Bomb Exploded");
            return HookResult.Continue;
        }

        private HookResult OnHostagePickup(EventHostageFollows @event, GameEventInfo info)
        {
            LogObjective(@event.Userid, "Hostage Picked Up");
            return HookResult.Continue;
        }

        private HookResult OnHostageRescued(EventHostageRescued @event, GameEventInfo info)
        {
            LogObjective(@event.Userid, "Hostage Rescued");
            return HookResult.Continue;
        }

        private static readonly HashSet<string> UtilityWeapons = new(StringComparer.OrdinalIgnoreCase)
        {
            "hegrenade", "molotov", "incgrenade", "inferno", "decoy"
        };

        private HookResult OnPlayerHurt(EventPlayerHurt @event, GameEventInfo info)
        {
            if (IsWarmup()) return HookResult.Continue;

            try
            {
                var attacker = @event.Attacker;
                var victim = @event.Userid;

                if (attacker != null && attacker.IsValid && victim != null && victim.IsValid && attacker != victim)
                {
                    // Only count damage against enemies (different teams, both on T/CT)
                    if ((attacker.TeamNum == 2 || attacker.TeamNum == 3) &&
                        (victim.TeamNum == 2 || victim.TeamNum == 3) &&
                        attacker.TeamNum != victim.TeamNum)
                    {
                        var data = GetOrAddPlayer(attacker);
                        int damage = @event.DmgHealth;
                        data.CurrentDamage += damage;

                        string weapon = @event.Weapon ?? "";
                        if (UtilityWeapons.Contains(weapon))
                        {
                            data.CurrentUtilityDamage += damage;
                        }
                    }
                }
            }
            catch { }

            return HookResult.Continue;
        }

        private HookResult OnPlayerBlind(EventPlayerBlind @event, GameEventInfo info)
        {
            if (IsWarmup()) return HookResult.Continue;

            try
            {
                var attacker = @event.Attacker;
                var victim = @event.Userid;

                if (attacker != null && attacker.IsValid && victim != null && victim.IsValid && attacker != victim)
                {
                    // Only count flashing enemies (different teams, both on T/CT)
                    if ((attacker.TeamNum == 2 || attacker.TeamNum == 3) &&
                        (victim.TeamNum == 2 || victim.TeamNum == 3) &&
                        attacker.TeamNum != victim.TeamNum)
                    {
                        var data = GetOrAddPlayer(attacker);
                        data.CurrentEnemiesFlashed++;
                    }
                }
            }
            catch { }

            return HookResult.Continue;
        }

        private HookResult OnPlayerChatCommand(CCSPlayerController? player, CommandInfo info)
        {
            if (player == null || !player.IsValid) return HookResult.Continue;

            string message = info.GetArg(1);
            if (string.IsNullOrEmpty(message)) return HookResult.Continue;

            bool isTeamChat = info.GetArg(0).Equals("say_team", StringComparison.OrdinalIgnoreCase);

            _matchData.ChatFeed.Add(new ChatLog
            {
                Round = _currentRound,
                PlayerName = player.PlayerName ?? "Unknown",
                PlayerSteamID = player.SteamID,
                Message = message,
                TeamChat = isTeamChat,
                Timestamp = DateTime.UtcNow.ToString("HH:mm:ss")
            });

            return HookResult.Continue;
        }

        private HookResult OnRoundEnded(EventRoundOfficiallyEnded @event, GameEventInfo info)
        {
            if (IsWarmup()) return HookResult.Continue;

            SnapshotRoundStats();
            _currentRound++;

            SaveMatchData();
            return HookResult.Continue;
        }

        private void SnapshotRoundStats()
        {
            if (_roundStatsSnapshotTaken) return;
            if (IsWarmup()) return;

            // Mark taken immediately to prevent re-entry
            _roundStatsSnapshotTaken = true;

            try
            {
                UpdateTeamScores();

                // Direct modification of _matchData properties
                _matchData.CTWins = _ctWins;
                _matchData.TWins = _tWins;
                _matchData.TotalRounds = _ctWins + _tWins;

                _matchData.CTScoreHistory.Add(_ctWins);
                _matchData.TScoreHistory.Add(_tWins);

                // Get players ONCE and cache the list to avoid repeated entity iteration
                List<CCSPlayerController> currentPlayers;
                try
                {
                    currentPlayers = Utilities.GetPlayers()
                        .Where(p => p != null && p.IsValid)
                        .ToList();
                }
                catch
                {
                    // Entity system may be unavailable during map transition
                    currentPlayers = new List<CCSPlayerController>();
                }

                // Ensure all connected players are tracked
                foreach (var p in currentPlayers)
                {
                    try
                    {
                        if (p.IsValid && p.Connected == PlayerConnectedState.PlayerConnected)
                            GetOrAddPlayer(p);
                    }
                    catch { /* Player handle freed mid-iteration */ }
                }

                // Update stats for all tracked players (snapshot to avoid collection-modified errors)
                foreach (var data in _matchData.Players.ToList())
                {
                    try
                    {
                        CCSPlayerController? playerEntity = null;
                        foreach (var p in currentPlayers)
                        {
                            try
                            {
                                if (!p.IsValid) continue;
                                ulong pid = p.SteamID;
                                if (pid == 0) pid = (ulong)p.Handle.ToInt64();
                                if (pid == data.SteamID) { playerEntity = p; break; }
                            }
                            catch { /* Handle freed */ }
                        }

                        if (playerEntity != null)
                        {
                            try
                            {
                                if (playerEntity.IsValid)
                                    UpdateLivePlayerFields(playerEntity, data);
                                else
                                {
                                    data.AliveHistory.Add(false);
                                    data.InventoryHistory.Add("");
                                }
                            }
                            catch
                            {
                                data.AliveHistory.Add(false);
                                data.InventoryHistory.Add("");
                            }
                        }
                        else
                        {
                            data.AliveHistory.Add(false);
                            data.InventoryHistory.Add("");
                        }
                    }
                    catch
                    {
                        data.AliveHistory.Add(false);
                        data.InventoryHistory.Add("");
                    }

                    data.TeamHistory.Add(data.CurrentTeam);
                    data.KillsHistory.Add(data.CurrentKills);
                    data.DeathsHistory.Add(data.CurrentDeaths);
                    data.AssistsHistory.Add(data.CurrentAssists);
                    data.ZeusKillsHistory.Add(data.CurrentZeusKills);
                    data.MVPsHistory.Add(data.CurrentMVPs);
                    data.ScoreHistory.Add(data.CurrentScore);
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[CompStats] SnapshotRoundStats error: {ex.Message}");
            }
        }

        private void UpdateLivePlayerFields(CCSPlayerController p, PlayerMatchData data)
        {
            try
            {
                if (!p.IsValid) { data.AliveHistory.Add(false); data.InventoryHistory.Add(""); return; }
                data.Name = p.PlayerName ?? data.Name;
                data.CurrentTeam = p.TeamNum;
                data.CurrentScore = GetPlayerScore(p);
                data.CurrentMVPs = GetPlayerMVP(p);

                bool isAlive = false;
                try
                {
                    var pawnHandle = p.PlayerPawn;
                    if (pawnHandle != null && pawnHandle.IsValid)
                    {
                        var pawn = pawnHandle.Value;
                        if (pawn != null && pawn.IsValid && pawn.LifeState == (byte)LifeState_t.LIFE_ALIVE)
                            isAlive = true;
                    }
                }
                catch { /* Pawn handle freed during map transition */ }
                data.AliveHistory.Add(isAlive);

                data.InventoryHistory.Add(GetPlayerInventory(p));
            }
            catch
            {
                data.AliveHistory.Add(false);
                data.InventoryHistory.Add("");
            }
        }

        private void SaveMatchData()
        {
            if (!_usesMatchLibrarian) return;
            if (IsWarmup()) return;
            if (string.IsNullOrEmpty(_matchData.MatchID)) return;

            // Only save if we actually have round history or if match just ended
            if (_matchData.CTScoreHistory.Count == 0 && _matchData.TScoreHistory.Count == 0 && !_matchEndedNormally) return;

            try
            {
                var mapName = Server.MapName;
                if (string.IsNullOrEmpty(mapName)) mapName = "UnknownMap";

                string workshopId = _workshopMapIds.TryGetValue(mapName, out var id) ? id : "N/A";

                // Update header info fields
                _matchData.MapName = mapName;
                _matchData.WorkshopID = workshopId;
                _matchData.CollectionID = _loadedCollectionId;
                _matchData.LastUpdated = DateTime.UtcNow;

                var now = DateTime.UtcNow;
                var yearFolder = now.ToString("yyyy");
                var monthFolder = now.ToString("MM");
                var dayFolder = now.ToString("dd");

                var dailyDirectory = Path.Combine(MatchesDirPath, yearFolder, monthFolder, dayFolder);

                if (!Directory.Exists(dailyDirectory)) Directory.CreateDirectory(dailyDirectory);

                var matchFileName = $"{_matchData.MatchID}.json";
                var fullFilePath = Path.Combine(dailyDirectory, matchFileName);

                var jsonOptions = new JsonSerializerOptions { WriteIndented = true };

                // Optimization: Directly serialize the persistent object
                File.WriteAllText(fullFilePath, JsonSerializer.Serialize(_matchData, jsonOptions));
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[CompStats] Error saving match data: {ex.Message}");
            }
        }

        private void PrintPlayerStats(CCSPlayerController? caller, CommandInfo cmd)
        {
            UpdateTeamScores();

            var allPlayers = Utilities.GetPlayers()
                .Where(p => p != null && p.IsValid && p.Connected == PlayerConnectedState.PlayerConnected)
                .ToList();

            var humans = allPlayers.Where(p => !p.IsBot && !p.IsHLTV).ToList();
            var bots = allPlayers.Where(p => p.IsBot && !p.IsHLTV).ToList();

            string zeusLeaderName = "None";
            int maxZeusKills = 0;

            foreach (var player in allPlayers)
            {
                var data = GetOrAddPlayer(player);
                if (data.CurrentZeusKills > maxZeusKills)
                {
                    maxZeusKills = data.CurrentZeusKills;
                    zeusLeaderName = player.PlayerName ?? "Unknown";
                }
            }

            bool isWarmup = IsWarmup();
            string mapName = Server.MapName;
            string workshopId = _workshopMapIds.TryGetValue(mapName, out var id) ? id : "N/A";
            string recordingStatus = _usesMatchLibrarian ? "ON" : "OFF";

            cmd.ReplyToCommand($"--- Status: Map: {mapName} | ID: {_matchData.MatchID} | CollectionID: {_loadedCollectionId} | WorkshopID: {workshopId} | Warmup: {(isWarmup ? "Yes" : "No")} | DB: {recordingStatus} ---");
            cmd.ReplyToCommand($"--- Humans: {humans.Count}, Bots: {bots.Count} | T Wins: {_tWins}, CT Wins: {_ctWins} ---");

            if (humans.Any())
            {
                cmd.ReplyToCommand("--- Humans ---");
                foreach (var p in humans) PrintSinglePlayerStat(p, cmd);
            }

            if (bots.Any())
            {
                if (humans.Any()) cmd.ReplyToCommand(" ");
                cmd.ReplyToCommand("--- Bots ---");
                foreach (var p in bots) PrintSinglePlayerStat(p, cmd);
            }

            if (maxZeusKills > 0)
            {
                cmd.ReplyToCommand($"--- Zeus Leader: {zeusLeaderName} ({maxZeusKills} Kills) ---");
            }

            cmd.ReplyToCommand("--- End ---");
        }

        private void PrintSinglePlayerStat(CCSPlayerController p, CommandInfo cmd)
        {
            try
            {
                if (!p.IsValid) return;
                var data = GetOrAddPlayer(p);
                int score = GetPlayerScore(p);
                int money = GetPlayerMoney(p);

                var pingStr = p.IsBot ? "BOT" : p.Ping.ToString();
                string teamStr = GetTeamName(p.TeamNum);

                bool isAlive = false;
                try
                {
                    var pawnHandle = p.PlayerPawn;
                    if (pawnHandle != null && pawnHandle.IsValid)
                    {
                        var pawn = pawnHandle.Value;
                        if (pawn != null && pawn.IsValid && pawn.LifeState == (byte)LifeState_t.LIFE_ALIVE)
                            isAlive = true;
                    }
                }
                catch { /* Pawn handle freed */ }

                string hsPercent = data.CurrentKills > 0
                    ? $"{(data.CurrentHeadshotKills * 100.0 / data.CurrentKills):F1}%"
                    : "0.0%";
                string kdr = data.CurrentDeaths > 0
                    ? $"{(data.CurrentKills / (double)data.CurrentDeaths):F2}"
                    : data.CurrentKills.ToString("F2");
                int roundsPlayed = Math.Max(_currentRound, 1);
                string adr = $"{(data.CurrentDamage / (double)roundsPlayed):F1}";

                cmd.ReplyToCommand(
                    $"[{p.Slot}] {data.Name} | Team:{teamStr} K:{data.CurrentKills} D:{data.CurrentDeaths} A:{data.CurrentAssists} " +
                    $"HS:{hsPercent} DMG:{data.CurrentDamage} UD:{data.CurrentUtilityDamage} EF:{data.CurrentEnemiesFlashed} " +
                    $"KDR:{kdr} ADR:{adr} Z:{data.CurrentZeusKills} MVP:{data.CurrentMVPs} Score:{score} Money:${money} " +
                    $"Alive:{(isAlive ? "Yes" : "No")} Ping:{pingStr}"
                );
            }
            catch { /* Player freed during stat print */ }
        }

        private string GetPlayerInventory(CCSPlayerController player)
        {
            try
            {
                if (player == null || !player.IsValid)
                    return "N/A";

                var pawnHandle = player.PlayerPawn;
                if (pawnHandle == null || !pawnHandle.IsValid)
                    return "N/A";

                var pawn = pawnHandle.Value;
                if (pawn == null || !pawn.IsValid)
                    return "None";

                var weaponServices = pawn.WeaponServices;
                if (weaponServices == null)
                    return "None";

                var myWeapons = weaponServices.MyWeapons;
                if (myWeapons == null)
                    return "None";

                List<string> weaponNames = new List<string>();
                foreach (var weaponHandle in myWeapons)
                {
                    try
                    {
                        if (weaponHandle == null || !weaponHandle.IsValid) continue;
                        var weapon = weaponHandle.Value;
                        if (weapon == null || !weapon.IsValid) continue;

                        string name = weapon.DesignerName;
                        if (name != null && name.StartsWith("weapon_")) name = name.Substring(7);
                        if (!string.IsNullOrEmpty(name)) weaponNames.Add(name);
                    }
                    catch { /* Weapon handle freed during map transition */ }
                }
                return weaponNames.Count == 0 ? "None" : string.Join(", ", weaponNames);
            }
            catch
            {
                return "N/A";
            }
        }

        private int GetPlayerMoney(CCSPlayerController player)
        {
            if (player == null || !player.IsValid) return 0;
            var moneyServices = player.InGameMoneyServices;
            return moneyServices == null ? 0 : moneyServices.Account;
        }

        private int GetPlayerScore(CCSPlayerController player)
        {
            if (player == null || !player.IsValid) return 0;
            try
            {
                var pi = player.GetType().GetProperty("Score", BindingFlags.Instance | BindingFlags.Public | BindingFlags.IgnoreCase);
                if (pi != null && pi.GetValue(player) is int vi) return vi;
            }
            catch { }
            return 0;
        }

        private int GetPlayerMVP(CCSPlayerController player)
        {
            if (player == null || !player.IsValid) return 0;
            try
            {
                var pi = player.GetType().GetProperty("MVPs", BindingFlags.Instance | BindingFlags.Public | BindingFlags.IgnoreCase);
                if (pi != null && pi.GetValue(player) is int vi) return vi;
            }
            catch { }
            return 0;
        }

        private void UpdateTeamScores()
        {
            _ctWins = 0;
            _tWins = 0;
            try
            {
                var teams = Utilities.FindAllEntitiesByDesignerName<CCSTeam>("cs_team_manager");
                foreach (var team in teams)
                {
                    if (team == null || !team.IsValid) continue;
                    if (team.TeamNum == TEAM_T_MANAGER_ID) _tWins = team.Score;
                    else if (team.TeamNum == TEAM_CT_MANAGER_ID) _ctWins = team.Score;
                }
            }
            catch { }
        }

        public class SteamCollectionResponse
        {
            public SteamCollectionResponseData? response { get; set; }
        }
        public class SteamCollectionResponseData
        {
            public List<CollectionDetails>? collectiondetails { get; set; }
        }
        public class CollectionDetails
        {
            public List<CollectionChild>? children { get; set; }
        }
        public class CollectionChild
        {
            public string? publishedfileid { get; set; }
        }
    }
}
