# src/ml/predict.py
import pickle
from datetime import date
import numpy as np
from src.db.supabase_client import insert_scores

def load_local_model(path="tmp/model.pkl"):
    with open(path, "rb") as f:
        return pickle.load(f)

def finance_score(features: dict) -> float:
    """
    Output #1 정량 진단 중 '재무/현금흐름 신호' MVP 점수
    """
    rev = features.get("finance:revenue") or 0
    op  = features.get("finance:op_income")
    d2a = features.get("ratio:debt_to_assets")
    score = 50.0
    if op is not None:
        score += 20.0 if op > 0 else -20.0
    if d2a is not None:
        score += 10.0 if d2a < 0.5 else -10.0
    # revenue는 스케일이 너무 커서 여기서는 반영 생략(MVP)
    return max(0.0, min(100.0, score))

def gtm_score_stub(features: dict) -> float:
    """
    GTM 적합도는 지금 데이터로는 약함.
    (나중에 pricing/고객/운영국가/투자조건 등을 facts로 채우면 강화)
    """
    return 50.0

def ops_legal_risk_stub(features: dict) -> float:
    return 50.0

def run_predict(sb, model_path="tmp/model.pkl", as_of: date | None = None, model_version="v0.1"):
    pack = load_local_model(model_path)
    model = pack["model"]
    keys = pack["keys"]

    rows = (sb.table("ml_features")
              .select("company_key, features")
              .order("created_at", desc=True)
              .limit(500)
              .execute()).data or []

    for r in rows:
        ck = r["company_key"]
        f = r["features"] or {}
        x = np.array([[float(f.get(k) or 0.0) for k in keys]])
        # risk probability(1)
        prob = float(model.predict_proba(x)[0][1])

        finance = finance_score(f)
        gtm = gtm_score_stub(f)
        ops = ops_legal_risk_stub(f)

        total = 0.5 * finance + 0.3 * gtm + 0.2 * (100 - 100*prob)  # 예시 조합
        scores = {
            "finance": round(finance, 2),
            "gtm": round(gtm, 2),
            "ops_legal": round(ops, 2),
            "model_risk_prob": round(prob, 4),
            "total": round(total, 2)
        }
        signals = {
            "cashflow_signal": "watch" if finance < 45 else "ok",
            "risk_bucket": "high" if prob > 0.66 else ("mid" if prob > 0.33 else "low")
        }
        insert_scores(sb, ck, scores, signals, model_version=model_version, as_of=as_of)

