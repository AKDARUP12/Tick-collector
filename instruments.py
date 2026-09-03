"""Resolve NIFTY spot + N strikes (nearest weekly expiry) - frozen at 09:14 for backtest stability."""
import re
from typing import List, Dict, Tuple
from pyarrow_client import ArrowClient, Exchange

STRIKE_STEP = 100  # matches nbt/normalise.py:27


def get_nifty_spot_token(client: ArrowClient) -> Tuple[str, int]:
    """Find NIFTY spot token via index list. Falls back to known token 26000."""
    try:
        indices = client.get_index_list()
        # exact names only - "NIFTY 50" as substring also matches NIFTY NEXT 50 / NIFTY 500
        wanted = {"NIFTY", "NIFTY 50", "NIFTY50", "NIFTY-50"}
        for idx in indices:
            name = (idx.get("name") or idx.get("indexName") or "").upper().strip()
            if name in wanted:
                token = int(idx["token"])
                return (name, token)
        for idx in indices:  # second pass: names ending in the standalone word
            name = (idx.get("name") or idx.get("indexName") or "").upper().strip()
            if re.search(r"(?<![A-Z])NIFTY$", name):
                token = int(idx["token"])
                return (name, token)
    except Exception as e:
        print(f"get_index_list failed: {e}")
    return ("NIFTY", 26000)


def _choose_strikes(chain_strikes: List[float], atm: int, n: int) -> List[float]:
    """Pick n strike prices around atm from sorted chain_strikes."""
    if len(chain_strikes) <= n:
        return sorted(chain_strikes)
    # Centered window; for even n bias up (gap_bias upstream)
    # e.g. atm=25000, n=4 => [24900,25000,25100,25200]; n=5 => [-200..+200]
    # Compute closest strikes to atm
    ranked = sorted(chain_strikes, key=lambda s: (abs(s - atm), s))
    # Take closest n, then sort for storage
    chosen = sorted(ranked[:n])
    return chosen


def get_nearest_nifty_options(client: ArrowClient, num_strikes: int = 4, atm: int | None = None, gap_bias: str = "auto") -> List[Dict]:
    """
    Returns ~ num_strikes strike levels (both CE and PE) around ATM for nearest weekly expiry.
    - num_strikes: 4 or 5 (configurable; 5 = ATM±200 symmetric, 4 = ATM±100+one far side)
    - atm: if None, inferred as median chain strike (proxy for ATM when spot not yet open)
           else pass spot ATM = round(open/100)*100
    - gap_bias: "auto" | "up" | "down" | "center" - for even n, which far side to keep

    Uses get_option_chain_symbols -> nearest expiry -> get_option_chain.
    Falls back to synthetic chain around atm if API fails.
    """
    # Synthetic fallback helper
    def _synthetic(atm_val: int, n: int) -> List[Dict]:
        # n=4 => atm-100, atm, atm+100, atm+200 (up bias) ; n=5 => atm-200..+200
        if n == 5:
            strikes = [atm_val - 200, atm_val - 100, atm_val, atm_val + 100, atm_val + 200]
        elif gap_bias == "down":
            strikes = [atm_val - 200, atm_val - 100, atm_val, atm_val + 100]
        elif gap_bias == "center":
            # closest 4 without far bias - will be re-chosen anyway
            strikes = [atm_val - 100, atm_val, atm_val + 100, atm_val + 200]
        else:  # up / auto
            strikes = [atm_val - 100, atm_val, atm_val + 100, atm_val + 200]
        fb = []
        for strike in strikes[:n]:
            for opt in ("CE", "PE"):
                fb.append({"symbol": f"NIFTY{strike}{opt}", "token": 0, "strikePrice": str(strike), "optionType": opt, "segment": "NFO"})
        return fb

    try:
        chains = client.get_option_chain_symbols()
        indices = chains.get("indices", {})
        # Prefer true NIFTY: "INDEX:BANKNIFTY"/"FINNIFTY" also contain "NIFTY" -
        # only a key ending in the standalone word NIFTY qualifies.
        cands = [k for k in indices if "NIFTY" in k.upper()]
        nifty_key = next((k for k in cands if re.search(r"(?<![A-Z])NIFTY$", k.upper())), None)
        if not nifty_key:
            nifty_key = cands[0] if cands else None
        if not nifty_key:
            raise ValueError(f"No NIFTY key in {list(indices.keys())[:5]}")
        expiries = indices[nifty_key]
        if not expiries:
            raise ValueError("No expiries for NIFTY")
        nearest_expiry = expiries[0]
        print(f"Nearest NIFTY expiry: {nearest_expiry} (key={nifty_key})")

        # Ask for a wider window than needed, then narrow to atm-centered
        fetch_n = max(num_strikes + 6, 12)
        chain = client.get_option_chain("NIFTY", Exchange.INDEX, count=fetch_n, expiry=nearest_expiry)

        seen: Dict[float, List[Dict]] = {}
        for leg in chain:
            try:
                strike = float(leg["strikePrice"])
            except:
                continue
            seen.setdefault(strike, []).append(leg)

        chain_strikes = sorted(seen.keys())
        if not chain_strikes:
            raise ValueError("empty chain")

        if atm is None:
            # median chain strike as proxy
            atm = int(sorted(chain_strikes)[len(chain_strikes)//2] // STRIKE_STEP * STRIKE_STEP)
            print(f"ATM inferred from chain median: {atm}")

        # For gap_bias=auto, look at spot open vs prev close if we have it; otherwise up
        if gap_bias == "auto":
            try:
                # try to fetch spot to decide bias
                from pyarrow_client import QuoteMode
                q = client.get_quote(QuoteMode.OHLCV, "NIFTY", Exchange.INDEX)
                open_px = q.get("open", q.get("ltp", atm*100)) / 100
                close_px = q.get("close", open_px*100) / 100
                gap_bias = "up" if open_px >= close_px else "down"
                print(f"Gap bias from spot open {open_px:.1f} vs close {close_px:.1f} => {gap_bias}")
            except:
                gap_bias = "up"

        # If even and bias matters, our _choose_strikes already bias-agnostic (closest). For true bias, shift window:
        chosen = _choose_strikes(chain_strikes, atm, num_strikes)
        # If even and we want to honor bias, nudge: for up bias ensure top strike is +200 not -200
        if num_strikes == 4 and len(chosen) == 4:
            # _choose_strikes gives closest 4; if atm=25000, it gives [24900,25000,25100,25200] (up) - ok
            # ensure not giving [-200,-100,0,+100] when gap is up - if gap up and min(chosen) < atm-100, shift
            if gap_bias == "up" and min(chosen) < atm - 100:
                chosen = sorted([atm - 100, atm, atm + 100, atm + 200])
                chosen = [s for s in chosen if s in chain_strikes] or chosen
            if gap_bias == "down" and max(chosen) > atm + 100:
                chosen = sorted([atm - 200, atm - 100, atm, atm + 100])
                chosen = [s for s in chosen if s in chain_strikes] or chosen

        result = []
        for s in chosen:
            result.extend(seen[s])
        print(f"Selected {len(result)} legs across {len(chosen)} strikes: {chosen} (atm={atm}, req={num_strikes})")
        # Attach expiry to each leg for downstream
        for leg in result:
            leg.setdefault("expiry", nearest_expiry)
        return result

    except Exception as e:
        print(f"get_option_chain failed ({e}) - synthetic fallback")
        atm_fallback = atm if atm is not None else 24500
        return _synthetic(atm_fallback, num_strikes)
