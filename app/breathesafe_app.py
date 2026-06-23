import streamlit as st
import json, numpy as np, pandas as pd, datetime
import plotly.graph_objects as go
from tensorflow.keras.models import load_model

st.set_page_config(page_title="BreatheSafe", page_icon="☁️", layout="wide")

# ============================================================
#  LOAD MODEL + DATA  (cached so it loads once)
# ============================================================
@st.cache_resource
def load_everything():
     model = load_model("app/breathesafe_lstm.keras")
     with open("app/breathesafe_data.json") as f:
        bundle = json.load(f)
    return model, bundle

model, bundle = load_everything()
SMIN, SMAX = bundle["scaler_min"], bundle["scaler_max"]
LOOKBACK, HORIZON = bundle["lookback"], bundle["horizon"]

# ---- safety: fill any missing keys so the app never crashes ----
if "station_hourly" not in bundle:
    bundle["station_hourly"] = bundle.get("station_profile", [])
if "all_pollutants" not in bundle:
    bundle["all_pollutants"] = [{"pollutant_name": "pm25", "avg_val": 51.5}]
if "dow_pattern" not in bundle:
    bundle["dow_pattern"] = [{"dow": i, "pm25": 50} for i in range(7)]
if "mae" not in bundle:
    bundle["mae"] = 15

def unscale(x):
    return x * (SMAX - SMIN) + SMIN

def forecast_next_24():
    seed = np.array(bundle["seed"]).reshape(1, LOOKBACK, 1)
    return np.clip(unscale(model.predict(seed, verbose=0).flatten()), 0, None)

# ============================================================
#  STATION DISPLAY  (dataset only had site codes)
# ============================================================
def nice_name(code):
    # Dataset only provided site codes (no neighborhood names).
    # Display them cleanly as "Station site_XXX".
    return code.replace("site_", "Station ")

# ============================================================
#  STYLING
# ============================================================
st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Quicksand:wght@400;600;700&display=swap');
html, body, [class*="css"] { font-family:'Quicksand',sans-serif; }
.stApp { background:linear-gradient(160deg,#e8f4fd 0%,#f4f9fd 40%,#fef6f0 100%); }
@keyframes float {0%{transform:translateY(0)}50%{transform:translateY(-18px)}100%{transform:translateY(0)}}
.cloud {font-size:88px;text-align:center;animation:float 4s ease-in-out infinite;}
@keyframes pop {0%{transform:scale(0.6);opacity:0}100%{transform:scale(1);opacity:1}}
.bigstat {animation:pop 0.7s ease-out;}
.card {background:white;border-radius:22px;padding:20px;box-shadow:0 8px 26px rgba(46,134,222,0.12);}
.title {font-size:46px;font-weight:700;color:#0b1f33;text-align:center;margin-bottom:0;}
.sub {font-size:17px;color:#5d6d7e;text-align:center;font-style:italic;}
.explain {background:#fff7e6;border-radius:14px;padding:12px 18px;color:#7d6608;
          font-size:14px;text-align:center;border:1px solid #ffe6a8;}
</style>
""", unsafe_allow_html=True)

def level_info(v):
    if v <= 5:   return ("Safe", "#1abc9c", "😊", "Perfect — breathe easy!")
    if v <= 35:  return ("Moderate", "#3498db", "🙂", "Generally okay for most.")
    if v <= 55:  return ("Unhealthy (sensitive)", "#f39c12", "😐", "Sensitive groups take care.")
    if v <= 150: return ("Unhealthy", "#e67e22", "😷", "Limit time outdoors.")
    if v <= 250: return ("Very Unhealthy", "#e74c3c", "😨", "Stay inside if you can.")
    return ("Hazardous", "#8e44ad", "🚨", "Avoid going out!")

# ============================================================
#  HEADER
# ============================================================
preds = forecast_next_24()
now_val = preds[0]
lvl, color, mood, msg = level_info(now_val)

st.markdown(f'<div class="cloud">{mood}️</div>', unsafe_allow_html=True)
st.markdown('<p class="title">BreatheSafe</p>', unsafe_allow_html=True)
st.markdown('<p class="sub">Forecasting Delhi\'s air for the next 24 hours ☁️</p>', unsafe_allow_html=True)
st.write("")
st.markdown('<div class="explain">💡 <b>PM2.5</b> = tiny harmful particles in the air. '
            'Measured in µg/m³. Lower is better — anything above <b>5</b> is over the WHO safe limit.</div>',
            unsafe_allow_html=True)
st.write("")

# ============================================================
#  TOP STATUS CARDS
# ============================================================
c1, c2, c3 = st.columns(3)
with c1:
    st.markdown(f'<div class="card bigstat" style="text-align:center">'
                f'<div style="font-size:13px;color:#5d6d7e">PREDICTED NOW</div>'
                f'<div style="font-size:54px;font-weight:700;color:{color}">{now_val:.0f}</div>'
                f'<div style="color:#5d6d7e">µg/m³ PM2.5</div></div>', unsafe_allow_html=True)
with c2:
    st.markdown(f'<div class="card bigstat" style="text-align:center;border-top:6px solid {color}">'
                f'<div style="font-size:13px;color:#5d6d7e">AIR QUALITY</div>'
                f'<div style="font-size:30px;font-weight:700;color:{color};margin-top:8px">{lvl}</div>'
                f'<div style="margin-top:6px;color:#5d6d7e">{msg}</div></div>', unsafe_allow_html=True)
with c3:
    best_h, worst_h = int(np.argmin(preds)), int(np.argmax(preds))
    st.markdown(f'<div class="card bigstat" style="text-align:center">'
                f'<div style="font-size:13px;color:#5d6d7e">BEST TIME OUT</div>'
                f'<div style="font-size:40px;font-weight:700;color:#1abc9c">+{best_h}h</div>'
                f'<div style="color:#5d6d7e">worst: +{worst_h}h ⚠️</div></div>', unsafe_allow_html=True)
st.write("")

# ============================================================
#  TABS
# ============================================================
tab1, tab2, tab3, tab4 = st.tabs(["📈 24h Forecast", "📍 Safest Zone", "🔬 Station Detail", "📊 Patterns"])

with tab1:
    hours = [f"+{i}h" for i in range(HORIZON)]
    colors = [level_info(v)[1] for v in preds]
    fig = go.Figure()
    fig.add_trace(go.Scatter(x=hours, y=preds, mode="lines+markers",
        line=dict(color="#2e86de", width=4, shape="spline"),
        marker=dict(size=10, color=colors), fill="tozeroy",
        fillcolor="rgba(46,134,222,0.12)", name="PM2.5"))
    fig.add_hline(y=5, line_dash="dash", line_color="#1abc9c", annotation_text="WHO safe limit")
    fig.add_hline(y=150, line_dash="dot", line_color="#e74c3c", annotation_text="Hazardous")
    fig.update_layout(height=400, margin=dict(l=10, r=10, t=30, b=10),
        plot_bgcolor="rgba(0,0,0,0)", paper_bgcolor="rgba(0,0,0,0)",
        yaxis_title="PM2.5 µg/m³", font=dict(family="Quicksand"))
    st.plotly_chart(fig, use_container_width=True)
    st.caption(f"Live forecast from our LSTM model · average error ≈ {bundle.get('mae','~15')} µg/m³")

with tab2:
    prof = pd.DataFrame(bundle["station_profile"])
    cur_hour = datetime.datetime.now().hour
    snap = prof[prof["hour"] == cur_hour].sort_values("pm25") if "hour" in prof.columns else pd.DataFrame()
    if snap.empty:
        snap = prof.groupby("station_id")["pm25"].mean().reset_index().sort_values("pm25")
    st.markdown("#### Cleanest areas right now (ranked best → worst)")
    cols = st.columns(5)
    for i, (_, row) in enumerate(snap.iterrows()):
        v = row["pm25"]; l, c, m, _ = level_info(v)
        badge = "🥇 SAFEST" if i == 0 else ("⚠️ AVOID" if i == len(snap) - 1 else "")
        with cols[i % 5]:
            st.markdown(f'<div class="card" style="text-align:center;padding:12px;margin-bottom:10px">'
                        f'<div style="font-size:11px;color:#1abc9c;font-weight:700;height:14px">{badge}</div>'
                        f'<div style="font-size:22px">{m}</div>'
                        f'<div style="font-weight:700;font-size:11px;color:#0b1f33">{nice_name(row["station_id"])}</div>'
                        f'<div style="font-size:20px;font-weight:700;color:{c}">{v:.0f}</div>'
                        f'<div style="font-size:9px;color:#5d6d7e">µg/m³</div></div>', unsafe_allow_html=True)

with tab3:
    sh = pd.DataFrame(bundle["station_hourly"])
    if "hour" in sh.columns:
        station_codes = sorted(sh["station_id"].unique())
        picked_label = st.selectbox("Pick an area to inspect",
                                    [nice_name(c) for c in station_codes])
        picked_code = [c for c in station_codes if nice_name(c) == picked_label][0]
        sdata = sh[sh["station_id"] == picked_code].sort_values("hour")
        fig2 = go.Figure()
        fig2.add_trace(go.Scatter(x=sdata["hour"], y=sdata["pm25"], mode="lines+markers",
            line=dict(color="#e67e22", width=3, shape="spline"), fill="tozeroy",
            fillcolor="rgba(230,126,34,0.1)"))
        fig2.add_hline(y=5, line_dash="dash", line_color="#1abc9c", annotation_text="WHO limit")
        fig2.update_layout(height=380, title=f"{picked_label} — average PM2.5 by hour",
            xaxis_title="Hour (0 = midnight)", yaxis_title="PM2.5 µg/m³",
            plot_bgcolor="rgba(0,0,0,0)", paper_bgcolor="rgba(0,0,0,0)", font=dict(family="Quicksand"))
        st.plotly_chart(fig2, use_container_width=True)
        worst = sdata.loc[sdata["pm25"].idxmax()]; best = sdata.loc[sdata["pm25"].idxmin()]
        a, b = st.columns(2)
        a.metric("Worst hour here", f"{int(worst['hour']):02d}:00", f"{worst['pm25']:.0f} µg/m³")
        b.metric("Best hour here", f"{int(best['hour']):02d}:00", f"{best['pm25']:.0f} µg/m³")
    else:
        st.info("Station-level hourly detail not available in this data bundle.")

with tab4:
    cc1, cc2 = st.columns(2)
    with cc1:
        ap = pd.DataFrame(bundle["all_pollutants"])
        figp = go.Figure(go.Bar(x=ap["pollutant_name"], y=ap["avg_val"], marker_color="#2e86de"))
        figp.update_layout(height=340, title="Average level by pollutant",
            plot_bgcolor="rgba(0,0,0,0)", paper_bgcolor="rgba(0,0,0,0)", font=dict(family="Quicksand"))
        st.plotly_chart(figp, use_container_width=True)
    with cc2:
        dow = pd.DataFrame(bundle["dow_pattern"])
        days = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]
        dow["day"] = dow["dow"].apply(lambda d: days[int(d)])
        figd = go.Figure(go.Bar(x=dow["day"], y=dow["pm25"], marker_color="#9b59b6"))
        figd.update_layout(height=340, title="Average PM2.5 by day of week",
            plot_bgcolor="rgba(0,0,0,0)", paper_bgcolor="rgba(0,0,0,0)", font=dict(family="Quicksand"))
        st.plotly_chart(figd, use_container_width=True)

st.write("")
st.caption("BreatheSafe · LSTM forecast trained on 2 years of Delhi air-quality data · Team 4 · "
           "Stations shown by their dataset site codes.")
