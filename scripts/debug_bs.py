
import numpy as np
from scipy.stats import norm
from scipy.optimize import brentq

RISK_FREE_RATE = 0.045

def bs_d1(S, K, T, r, sigma):
    return (np.log(S / K) + (r + 0.5 * sigma**2) * T) / (sigma * np.sqrt(T))

def bs_price(S, K, T, r, sigma, kind="call"):
    d1 = bs_d1(S, K, T, r, sigma)
    d2 = d1 - sigma * np.sqrt(T)
    if kind == "call":
        return S * norm.cdf(d1) - K * np.exp(-r * T) * norm.cdf(d2)
    else:
        return K * np.exp(-r * T) * norm.cdf(-d2) - S * norm.cdf(-d1)

def calculate_imp_vol(price, S, K, T, r, kind="call"):
    if price <= 0 or T <= 0: return 0.0
    
    def obj(sigma):
        val = bs_price(S, K, T, r, sigma, kind)
        return val - price
    
    try:
        low = 0.01
        high = 5.0
        
        v_low = obj(low)
        v_high = obj(high)
        print(f"Obj(0.01)={v_low}, Obj(5.0)={v_high}")
        
        if v_low * v_high > 0:
            print("No solution in range 0.01 - 5.0")
            return 0.0
            
        return brentq(obj, low, high, xtol=1e-4)
    except Exception as e:
        print(f"Error: {e}")
        return 0.0

# Case AAPL
S = 277.89
K = 277.5
T = 3.0/365.0
r = 0.045
price = 2.91
kind = "call"

print(f"S={S}, K={K}, T={T}, r={r}, Price={price}")
iv = calculate_imp_vol(price, S, K, T, r, kind)
print(f"Calculated IV: {iv}")
