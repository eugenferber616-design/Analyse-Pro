import os, google.generativeai as genai

def get_gemini(model="gemini-1.5-flash"):
    api_key = os.environ.get("GOOGLE_API_KEY")
    if not api_key:
        raise RuntimeError("GOOGLE_API_KEY fehlt")
    genai.configure(api_key=api_key)
    return genai.GenerativeModel(model)
