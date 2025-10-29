// client/AgenaForecastClient.cs
// .NET 4.5-kompatibler Mini-Client für AnalysePro-Forecasts.
// Features: Local/URL, einfacher Cache, 3x Retry, JSON-Parser ohne externe NuGet (JavaScriptSerializer).
// JSON-Format erwartet: { "symbol": "...", "per_h": { "5": {...}, "10": {...} }, "meta": {...} }

using System;
using System.Collections.Generic;
using System.IO;
using System.Net;
using System.Text;
using System.Web.Script.Serialization;   // <- .NET 4.x: System.Web.Extensions Referenz

namespace AgenaBridge
{
    public class ForecastPerH
    {
        public double? p_up { get; set; }
        public double? p_dn { get; set; }
        public double? up_med { get; set; }
        public double? dn_med { get; set; }
        public double? dd_p50 { get; set; }
        public double? dd_p90 { get; set; }
        public double? dir_score { get; set; }
        public double? n_eff { get; set; }
        public double? iv_up { get; set; }
        public double? iv_dn { get; set; }
    }

    public class ForecastRoot
    {
        public string symbol { get; set; }
        public Dictionary<string, ForecastPerH> per_h { get; set; }
        public Dictionary<string, object> meta { get; set; }
    }

    public static class AgenaForecastClient
    {
        private static readonly Dictionary<string, Tuple<DateTime, string>> _cache = new Dictionary<string, Tuple<DateTime, string>>();
        private static readonly TimeSpan _cacheTtl = TimeSpan.FromSeconds(10);

        // ---- Public API -----------------------------------------------------

        // Liest docs/… oder data/processed/forecast_{SYMBOL}.json unter einem BasePath (Repo-Root).
        public static ForecastRoot ReadLocal(string basePath, string symbol)
        {
            // erst data/processed, dann docs/ (falls du später aggregierst)
            var p1 = Path.Combine(basePath, "data", "processed", "forecast_" + symbol + ".json");
            var p2 = Path.Combine(basePath, "docs", "forecast_" + symbol + ".json");
            var path = File.Exists(p1) ? p1 : (File.Exists(p2) ? p2 : null);
            if (path == null) throw new FileNotFoundException("Forecast JSON nicht gefunden", p1);

            var js = File.ReadAllText(path, Encoding.UTF8);
            return Parse(js);
        }

        // Holt JSON von URL (z. B. später FastAPI/Raw GitHub).
        public static ForecastRoot ReadFromUrl(string url)
        {
            var js = GetStringWithRetry(url, 3, 1500);
            return Parse(js);
        }

        // Praktische Helper
        public static double? GetDirScore(ForecastRoot f, int horizon)
        {
            var key = horizon.ToString();
            if (f == null || f.per_h == null || !f.per_h.ContainsKey(key)) return null;
            return f.per_h[key].dir_score;
        }

        public static Tuple<double?, double?> GetTargetMoves(ForecastRoot f, int horizon)
        {
            var key = horizon.ToString();
            if (f == null || f.per_h == null || !f.per_h.ContainsKey(key)) return new Tuple<double?, double?>(null, null);
            var ph = f.per_h[key];
            // up_med positiv (z. B. +0.012 = +1.2%), dn_med positiv als Betrag (wir geben negativ zurück)
            double? up = ph.up_med;
            double? dn = ph.dn_med.HasValue ? -Math.Abs(ph.dn_med.Value) : (double?)null;
            return new Tuple<double?, double?>(up, dn);
        }

        public static double? GetCrashFlagDDp90(ForecastRoot f, int horizon)
        {
            var key = horizon.ToString();
            if (f == null || f.per_h == null || !f.per_h.ContainsKey(key)) return null;
            return f.per_h[key].dd_p90; // typischerweise negativ (z. B. -0.06 = -6%)
        }

        // ---- Internals ------------------------------------------------------

        private static string GetStringWithRetry(string url, int maxTry, int sleepMs)
        {
            // Cache (kurz) – reduziert UI-Lags, wenn Indikator mehrfach pro Bar aufruft.
            if (_cache.ContainsKey(url))
            {
                var entry = _cache[url];
                if (DateTime.UtcNow - entry.Item1 < _cacheTtl)
                    return entry.Item2;
            }

            Exception last = null;
            for (int i = 0; i < maxTry; i++)
            {
                try
                {
                    using (var wc = new WebClient())
                    {
                        wc.Encoding = Encoding.UTF8;
                        var s = wc.DownloadString(url);
                        _cache[url] = Tuple.Create(DateTime.UtcNow, s);
                        return s;
                    }
                }
                catch (Exception ex)
                {
                    last = ex;
                    System.Threading.Thread.Sleep(sleepMs);
                }
            }
            throw new WebException("Download fehlgeschlagen: " + url, last);
        }

        private static ForecastRoot Parse(string json)
        {
            var ser = new JavaScriptSerializer();
            // Locker parsen (kein harter POCO-Zwang über alle Felder):
            var root = ser.Deserialize<Dictionary<string, object>>(json);
            var fr = new ForecastRoot();
            fr.symbol = root.ContainsKey("symbol") ? Convert.ToString(root["symbol"]) : "";

            // per_h
            fr.per_h = new Dictionary<string, ForecastPerH>();
            if (root.ContainsKey("per_h"))
            {
                var ph = root["per_h"] as Dictionary<string, object>;
                if (ph != null)
                {
                    foreach (var kv in ph)
                    {
                        var hKey = kv.Key; // "5","10",…
                        var entry = kv.Value as Dictionary<string, object>;
                        if (entry == null) continue;
                        var per = new ForecastPerH
                        {
                            p_up = ToN(entry, "p_up"),
                            p_dn = ToN(entry, "p_dn"),
                            up_med = ToN(entry, "up_med"),
                            dn_med = ToN(entry, "dn_med"),
                            dd_p50 = ToN(entry, "dd_p50"),
                            dd_p90 = ToN(entry, "dd_p90"),
                            dir_score = ToN(entry, "dir_score"),
                            n_eff = ToN(entry, "n_eff"),
                            iv_up = ToN(entry, "iv_up"),
                            iv_dn = ToN(entry, "iv_dn")
                        };
                        fr.per_h[hKey] = per;
                    }
                }
            }

            // meta (optional, nicht strikt typisiert)
            fr.meta = new Dictionary<string, object>();
            if (root.ContainsKey("meta"))
            {
                var meta = root["meta"] as Dictionary<string, object>;
                if (meta != null)
                {
                    foreach (var kv in meta) fr.meta[kv.Key] = kv.Value;
                }
            }
            return fr;
        }

        private static double? ToN(Dictionary<string, object> d, string key)
        {
            if (!d.ContainsKey(key) || d[key] == null) return null;
            try
            {
                // JavaScriptSerializer liefert Numbers als double
                return Convert.ToDouble(d[key]);
            }
            catch { return null; }
        }
    }
}
