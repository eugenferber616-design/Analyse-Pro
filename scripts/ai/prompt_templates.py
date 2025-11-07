SYSTEM = (
  "Du bist ein präziser Aktien-Analyst. Antworte kompakt, strukturiert, "
  "mit stichpunktartigen Begründungen und klaren Ampel-Scores (0-100)."
)

def make_user_prompt(sym, snippets):
    # snippets: dict mit kurzen Text-Zusammenfassungen aus Fundamentals/Earnings/Options/Credit/HV/Riskindex
    return f"""Analysiere {sym}.
Fundamentals: {snippets['funds']}
Earnings: {snippets['earn']}
Options: {snippets['opt']}
Credit: {snippets['crd']}
Volatilität: {snippets['hv']}
Makro/Risk: {snippets['risk']}

Gib JSON mit Feldern:
symbol, score (0-100), stance (Long/Neutral/Short), rationale (3-6 bullets), risks (2-3 bullets), horizon_days (7/30/90).
Nur JSON."""
