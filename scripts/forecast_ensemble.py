# scripts/forecast_ensemble.py
# Kernlogik: Reparierter Forecast (keine “ständigen gleichen Zahlen”)
# - Realisierte Moves: robust (winsorize, n_eff, stride, min_hits)
# - IV-Band: nähert +/-1σ Tages-Drift und skaliert auf H Tage
# - Ensemble: Richtungsscore + Zielbereiche (p50 / p90 DD) + Qualitätsmaße

from dataclasses import dataclass
import math, statistics as stats
from typing import List, Dict, Any, Optional

@dataclass
class RealizedCfg:
    horizons: List[int]
    stride: int = 2
    winsor_pct: float = 1.0
    min_raw: int = 80
    max_hist: int = 3200

@dataclass
class IVBandCfg:
    iv_annual: Optional[float] = None   # z.B. 0.22
    drift_bps_per_day: float = 0.0      # optional (z.B. +1.5bp/Tag => 0.00015)

@dataclass
class EnsembleOut:
    per_h: Dict[int, Dict[str, Any]]
    meta: Dict[str, Any]

def _winsorize(arr: List[float], pct: float):
    if not arr or pct <= 0: return arr
    n = len(arr)
    k = max(0, int(round((pct/100.0)*n)))
    arr_sorted = sorted(arr)
    lo = arr_sorted[k]
    hi = arr_sorted[-k-1] if k < n else arr_sorted[-1]
    return [min(max(x, lo), hi) for x in arr]

def _safe_pct(x: float) -> float:
    return max(0.0, min(1.0, x))

def realized_block(prices: List[float], cfg: RealizedCfg):
    """Berechnet je Horizont:
       p_up, p_dn, up_med, dn_med, dd_p50, dd_p90, n_raw, n_eff
       dd = minimaler Drawdown innerhalb H (negativ, als Anteil)"""
    out = {}
    n = len(prices)
    if n < max(cfg.horizons)+5:
        return out

    for H in cfg.horizons:
        last = n - 1 - H
        max_i = max(0, min(cfg.max_hist-1, last))
        ups, dns, dds, weights = [], [], [], []
        w_up = 0.0; w_dn = 0.0; raw = 0

        # simple Regime-Nähe via 50/200 SMA Differenz (verhindert “immer gleich”)
        def regime_weight(i):
            # robust proxy: |(SMA50-SMA200)/price|
            if i+200 >= n: return 1.0
            p = prices[i]
            s50  = sum(prices[i:i+50])/50.0
            s200 = sum(prices[i:i+200])/200.0
            d = abs((s50 - s200) / max(1e-9, p))
            # kleiner d => ähnlich wie jetzt -> höheres Gewicht
            # clamp in [0,1], invertiert
            d = min(d, 0.05)
            return 1.0 - d/0.05

        for i in range(0, max_i+1, cfg.stride):
            p0 = prices[i]
            # Up/Down Ertrag zum Horizontende
            r  = prices[i+H]/p0 - 1.0
            # Min-Drowdown im Pfad
            minp = min(prices[i:i+H+1])
            dd   = minp/p0 - 1.0  # <= 0
            raw += 1
            w = regime_weight(i)
            weights.append(w)
            dds.append(dd)
            if r >= 0:
                w_up += w; ups.append(r)
            else:
                w_dn += w; dns.append(abs(r))

        if raw < cfg.min_raw:
            continue

        # Winsorize
        if len(ups) > 8:
            ups = _winsorize(ups, cfg.winsor_pct)
        if len(dns) > 8:
            dns = _winsorize(dns, cfg.winsor_pct)
        if len(dds) > 8:
            dds = _winsorize(dds, cfg.winsor_pct)

        # p_up/p_dn mit Jeffreys-Smoothing
        w_tot = w_up + w_dn
        if w_tot <= 0:
            p_up=p_dn=up_med=dn_med=dd_p50=dd_p90=None
        else:
            p_up = (w_up/w_tot); p_dn = (w_dn/w_tot)
            p_up = (p_up*w_tot + 0.5)/(w_tot + 1.0)
            p_dn = (p_dn*w_tot + 0.5)/(w_tot + 1.0)
            up_med = stats.median(ups) if ups else None
            dn_med = stats.median(dns) if dns else None
            dds_sorted = sorted(dds)
            def q(p):
                if not dds_sorted: return None
                idx = (len(dds_sorted)-1)*p
                lo, hi = int(math.floor(idx)), int(math.ceil(idx))
                vlo, vhi = dds_sorted[lo], dds_sorted[hi]
                return vlo + (idx-lo)*(vhi-vlo)
            dd_p50 = q(0.50)
            dd_p90 = q(0.90)

        # Effektive Stichprobe (n_eff) ≈ (Σw)^2 / Σw^2
        s1 = sum(weights); s2 = sum(w*w for w in weights)
        n_eff = (s1*s1/s2) if s2>0 else None

        out[H] = dict(
            p_up=p_up, p_dn=p_dn, up_med=up_med, dn_med=dn_med,
            dd_p50=dd_p50, dd_p90=dd_p90, n_raw=raw, n_eff=n_eff
        )
    return out

def iv_band_block(iv_cfg: IVBandCfg, horizons: List[int]):
    """IV-Range je Horizont (ohne Richtung). Näherung: Daily σ ≈ iv/sqrt(252)."""
    out = {}
    if iv_cfg.iv_annual is None or iv_cfg.iv_annual <= 0:
        for H in horizons:
            out[H] = dict(up=None, dn=None)
        return out
    sigma_d = iv_cfg.iv_annual / math.sqrt(252.0)
    for H in horizons:
        # √H Skalierung + linearer Drift (optional)
        mu = iv_cfg.drift_bps_per_day * H
        rng = sigma_d * math.sqrt(H)
        out[H] = dict(up=mu + rng, dn=mu - rng)
    return out

def ensemble(prices: List[float], horizons: List[int], iv_cfg: IVBandCfg) -> EnsembleOut:
    rz = realized_block(prices, RealizedCfg(horizons=horizons))
    iv = iv_band_block(iv_cfg, horizons)

    per_h = {}
    for H in horizons:
        rzH = rz.get(H, {})
        ivH = iv.get(H, {})
        # Richtung: p_up vs p_dn; Move: mediane aus realized, aber durch IV gedeckelt
        if rzH:
            pU, pD = rzH.get("p_up"), rzH.get("p_dn")
            up_med, dn_med = rzH.get("up_med"), rzH.get("dn_med")
        else:
            pU=pD=up_med=dn_med=None

        # Deckelung (Falls Realized unrealistische Werte liefert)
        cap_up = ivH.get("up")
        cap_dn = ivH.get("up")  # symmetrisch zum Betrag
        if up_med is not None and cap_up is not None:
            up_med = min(up_med, cap_up)
        if dn_med is not None and cap_dn is not None:
            dn_med = min(dn_med, cap_dn)

        # Richtungsscore (0..100)
        dir_score = None
        if pU is not None and pD is not None:
            dir_score = _safe_pct(pU - pD) * 100.0

        per_h[H] = dict(
            p_up=pU, p_dn=pD, up_med=up_med, dn_med=dn_med,
            dd_p50=rzH.get("dd_p50"), dd_p90=rzH.get("dd_p90"),
            dir_score=dir_score, n_raw=rzH.get("n_raw"), n_eff=rzH.get("n_eff"),
            iv_up=ivH.get("up"), iv_dn=ivH.get("dn")
        )

    meta = dict(
        engine="AnalysePro-Forecast-Ensemble",
        ver="1.0.0",
        notes="Realized (regime-weighted) + IV-Band; Jeffreys-smoothing; winsorize 1%"
    )
    return EnsembleOut(per_h=per_h, meta=meta)
