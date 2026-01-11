# src/ml/train.py
import os, json, pickle
from datetime import date
import numpy as np
from sklearn.linear_model import LogisticRegression

MODEL_VERSION = "v0.1"

def weak_label(features: dict) -> int:
    """
    임시 라벨(0/1) 생성 예시:
    - 영업이익이 음수면 리스크(1)
    - 부채비율이 너무 높으면 리스크(1)
    """
    op = features.get("finance:op_income")
    d2a = features.get("ratio:debt_to_assets")
    risk = 0
    if op is not None and op < 0:
        risk = 1
    if d2a is not None and d2a > 0.8:
        risk = 1
    return risk

def run_train(sb, as_of: date | None = None) -> str:
    rows = (sb.table("ml_features")
              .select("company_key, as_of_date, features")
              .order("created_at", desc=True)
              .limit(2000)
              .execute()).data or []

    if not rows:
        print("No features to train.")
        return MODEL_VERSION

    # feature space 고정
    keys = set()
    for r in rows:
        keys.update((r["features"] or {}).keys())
    keys = sorted(list(keys))

    X, y = [], []
    for r in rows:
        f = r["features"] or {}
        vec = [float(f.get(k) or 0.0) for k in keys]
        X.append(vec)
        y.append(weak_label(f))
    X = np.array(X)
    y = np.array(y)

    model = LogisticRegression(max_iter=200)
    model.fit(X, y)

    blob = pickle.dumps({"model": model, "keys": keys, "version": MODEL_VERSION})
    # Supabase Storage 대신, MVP는 reports 테이블에 저장하거나 별도 table 권장.
    # 여기서는 간단히 reports에 모델 메타만 남김(실제 모델 저장은 다음 단계에서 확장)
    sb.table("reports").insert({
        "report_type": "model_registry",
        "as_of_date": (as_of or date.today()).isoformat(),
        "scope": {"model_version": MODEL_VERSION},
        "content_md": f"Trained {MODEL_VERSION} with {len(rows)} samples and {len(keys)} features."
    }).execute()

    # 실제 운영: Storage bucket(models)에 업로드 권장
    # -> 다음 단계에서 붙이자.

    # 임시로 로컬 파일 저장(깃액션 런타임에만 존재)
    os.makedirs("tmp", exist_ok=True)
    with open("tmp/model.pkl", "wb") as f:
        f.write(blob)

    print("Saved tmp/model.pkl")
    return MODEL_VERSION
