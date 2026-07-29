import asyncio, json, math, os, time, logging
from datetime import datetime, timezone
from typing import Any

import httpx
import redis.asyncio as aioredis
from fastapi import BackgroundTasks, FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse
from nacl.exceptions import BadSignatureError
from nacl.signing import VerifyKey
from pydantic import BaseModel, Field

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("league-bot")

DISCORD_API       = "https://discord.com/api/v10"
ELO_K, DEFAULT_MMR, MAX_TEAM_SIZE = 32, 1000, 4
SEASON_WEEKS, LEAGUE_ADMIN_ROLE = 8, "League Admin"
LEAGUE_CHANNELS = ["📋-overview","📊-leaderboard","📅-schedule","🤝-free-agents","📢-announcements","💬-league-chat"]

app = FastAPI(title="Elements Divided League Bot", version="1.0.0")

# ── Redis ─────────────────────────────────────────────────────────────────
import redis
REDIS_URL = os.environ["REDIS_URL"]
_redis = redis.from_url(REDIS_URL, ssl_cert_reqs=None, decode_responses=True)

async def _gs(k):
    return json.loads(_redis.get(f"edl:{k}")) if _redis.get(f"edl:{k}") else None

async def _ss(k, v):
    _redis.set(f"edl:{k}", json.dumps(v))

async def _ds(k):
    _redis.delete(f"edl:{k}")

async def get_redis():
    return _redis

# ── Ed25519 ───────────────────────────────────────────────────────────────
def verify_sig(pk, sig, ts, body):
    try:
        VerifyKey(bytes.fromhex(pk)).verify(ts.encode() + body, bytes.fromhex(sig)); return True
    except BadSignatureError: return False

# ── Discord Helpers ───────────────────────────────────────────────────────
def _hdrs(): return {"Authorization": f"Bot {os.environ['DISCORD_BOT_TOKEN']}", "Content-Type": "application/json"}

async def _dc(method, path, data=None):
    url = f"{DISCORD_API}{path}"
    async with httpx.AsyncClient(timeout=30) as c:
        for _ in range(4):
            if method == "GET": r = await c.get(url, headers=_hdrs())
            elif method == "POST": r = await c.post(url, headers=_hdrs(), json=data)
            elif method == "PATCH": r = await c.patch(url, headers=_hdrs(), json=data)
            elif method == "PUT": r = await c.put(url, headers=_hdrs(), json=data)
            if r.status_code == 429: await asyncio.sleep(float(r.json().get("retry_after", 1.5)) + 0.1); continue
            if r.status_code == 204: return {}
            try: return r.json()
            except: return {}
    return {}

async def fu(aid, tok, msg):
    if len(msg) > 2000: msg = msg[:1950] + "\n_[truncated]_"
    auth = {"Authorization": f"Bot {os.environ['DISCORD_BOT_TOKEN']}"}
    async with httpx.AsyncClient(timeout=30) as c:
        for _ in range(4):
            r = await c.post(f"{DISCORD_API}/webhooks/{aid}/{tok}", headers=auth, json={"content": msg})
            if r.status_code == 429: await asyncio.sleep(float(r.json().get("retry_after", 1.5)) + 0.1); continue
            return

async def has_role(gid, uid, role):
    try:
        m = await _dc("GET", f"/guilds/{gid}/members/{uid}")
        rs = await _dc("GET", f"/guilds/{gid}/roles")
        if isinstance(rs, list):
            rid = next((r["id"] for r in rs if r.get("name","").lower() == role.lower()), None)
            return rid in m.get("roles", []) if rid else False
    except: return False

async def uname(gid, uid):
    try:
        m = await _dc("GET", f"/guilds/{gid}/members/{uid}")
        return m.get("nick") or m.get("user", {}).get("username", "unknown")
    except: return "unknown"

def elo(w, l):
    ew = 1 / (1 + math.pow(10, (l - w) / 400))
    return round(ELO_K * (1 - ew)), round(ELO_K * (-ew))

# ── State ─────────────────────────────────────────────────────────────────
async def gp(gid, uid): return await _gs(f"p:{gid}:{uid}")
async def sp(gid, uid, d): await _ss(f"p:{gid}:{uid}", d)
async def dp(gid, uid): await _ds(f"p:{gid}:{uid}")
async def gcfg(gid): return await _gs(f"cfg:{gid}")
async def scfg(gid, d): await _ss(f"cfg:{gid}", d)

async def gt(gid, sid, name):
    r = await get_redis()
    tid = await r.hget(f"edl:ti:{gid}:{sid}", name.lower())
    if tid:
        v = await r.get(f"edl:t:{gid}:{sid}:{tid}"); return json.loads(v) if v else None
async def gtid(gid, sid, tid): return await _gs(f"t:{gid}:{sid}:{tid}")
async def st(gid, sid, t):
    r = await get_redis()
    await r.set(f"edl:t:{gid}:{sid}:{t['id']}", json.dumps(t))
    await r.hset(f"edl:ti:{gid}:{sid}", t["name"].lower(), t["id"])
    await r.sadd(f"edl:tl:{gid}:{sid}", t["id"])
async def dt(gid, sid, t):
    r = await get_redis()
    await r.delete(f"edl:t:{gid}:{sid}:{t['id']}")
    await r.hdel(f"edl:ti:{gid}:{sid}", t["name"].lower())
    await r.srem(f"edl:tl:{gid}:{sid}", t["id"])
async def lt(gid, sid):
    r = await get_redis(); ids = await r.smembers(f"edl:tl:{gid}:{sid}")
    out = []
    for tid in ids:
        v = await r.get(f"edl:t:{gid}:{sid}:{tid}")
        if v: out.append(json.loads(v))
    return out

async def gfa(gid): v = await _gs(f"fa:{gid}"); return v or []
async def sfa(gid, fas): await _ss(f"fa:{gid}", fas)
async def gse(gid): return await _gs(f"s:{gid}")
async def sse(gid, s): await _ss(f"s:{gid}", s)
async def gm(gid, sid): v = await _gs(f"m:{gid}:{sid}"); return v or []
async def sm(gid, sid, m): await _ss(f"m:{gid}:{sid}", m)

def _o(opts, name, default=""):
    for o in opts:
        if o.get("name") == name: return o.get("value", default)
    return default

# ── Commands ──────────────────────────────────────────────────────────────
async def c_setup(gid, uid, aid, tok):
    if not await has_role(gid, uid, LEAGUE_ADMIN_ROLE): await fu(aid, tok, f"❌ Need **{LEAGUE_ADMIN_ROLE}** role."); return
    if await gcfg(gid): await fu(aid, tok, "⚠️ Already set up."); return
    cat = await _dc("POST", f"/guilds/{gid}/channels", {"name": "⚔️ Elements Divided League", "type": 4})
    chs = {}
    for ch in LEAGUE_CHANNELS:
        c = await _dc("POST", f"/guilds/{gid}/channels", {"name": ch, "type": 0, "parent_id": cat["id"]}); chs[ch] = c["id"]
    await scfg(gid, {"cat": cat["id"], "chs": chs})
    await fu(aid, tok, f"✅ **League ready!** {len(LEAGUE_CHANNELS)} channels. Use `/start-season`!")

async def c_start(gid, uid, aid, tok):
    if not await has_role(gid, uid, LEAGUE_ADMIN_ROLE): await fu(aid, tok, f"❌ Need **{LEAGUE_ADMIN_ROLE}**."); return
    if not await gcfg(gid): await fu(aid, tok, "❌ `/setup-league` first."); return
    ex = await gse(gid)
    if ex and ex.get("status") in ("active", "finals"): await fu(aid, tok, "❌ Season exists."); return
    sid = str(int(time.time())); await sse(gid, {"id": sid, "status": "active", "week": 1})
    await fu(aid, tok, f"🏆 **Season started!** {SEASON_WEEKS}w | Use `/create-team`!")

async def c_end(gid, uid, aid, tok):
    if not await has_role(gid, uid, LEAGUE_ADMIN_ROLE): await fu(aid, tok, f"❌ Need **{LEAGUE_ADMIN_ROLE}**."); return
    s = await gse(gid)
    if not s or s["status"] != "active": await fu(aid, tok, "❌ No active season."); return
    ts = await lt(gid, s["id"])
    if len(ts) < 4: await fu(aid, tok, f"❌ Need 4+ teams (have {len(ts)})."); return
    ts.sort(key=lambda t: t.get("mmr", DEFAULT_MMR), reverse=True); t4 = ts[:4]; finals = []
    for i in range(len(t4)):
        for j in range(i + 1, len(t4)): finals.append({"t1": t4[i]["id"], "t1n": t4[i]["name"], "t2": t4[j]["id"], "t2n": t4[j]["name"]})
    s["status"] = "finals"; s["finals"] = finals; s["ft"] = [{"id": t["id"], "name": t["name"], "mmr": t["mmr"]} for t in t4]
    await sse(gid, s)
    msg = "🏆 **FINALS!**\n\n"
    for i, t in enumerate(t4, 1): msg += f"{i}. **{t['name']}** — {t['mmr']} MMR\n"
    await fu(aid, tok, msg)

async def c_cteam(gid, uid, aid, tok, name):
    s = await gse(gid)
    if not s or s["status"] != "active": await fu(aid, tok, "❌ No season."); return
    p = await gp(gid, uid)
    if p:
        if p.get("tid"): await fu(aid, tok, "❌ In team. `/leave-team`."); return
        if p.get("fa"): await fu(aid, tok, "❌ FA. `/unregister-fa`."); return
    if await gt(gid, s["id"], name): await fu(aid, tok, f"❌ {name} exists."); return
    un = await uname(gid, uid); tid = f"t_{int(time.time())}"
    t = {"id": tid, "name": name, "captain": uid, "players": [{"id": uid, "name": un}], "mmr": DEFAULT_MMR, "wins": 0, "losses": 0}
    await st(gid, s["id"], t); await sp(gid, uid, {"tid": tid, "captain": True, "fa": False})
    await fu(aid, tok, f"✅ **{name}** created! 👑 {un}\n👥 1/{MAX_TEAM_SIZE}")

async def c_invite(gid, uid, aid, tok, tid2):
    p = await gp(gid, uid)
    if not p or not p.get("captain"): await fu(aid, tok, "❌ Captains only."); return
    s = await gse(gid)
    if not s or s["status"] != "active": await fu(aid, tok, "❌ No season."); return
    t = await gtid(gid, s["id"], p["tid"])
    if not t: await fu(aid, tok, "❌ Team not found."); return
    if len(t["players"]) >= MAX_TEAM_SIZE: await fu(aid, tok, f"❌ Full ({MAX_TEAM_SIZE})."); return
    tp = await gp(gid, tid2)
    if tp and tp.get("tid"): await fu(aid, tok, "❌ In team."); return
    if tp and tp.get("fa"): await fu(aid, tok, "❌ Is FA."); return
    if any(pl["id"] == tid2 for pl in t["players"]): await fu(aid, tok, "❌ Already in team."); return
    tn = await uname(gid, tid2); t["players"].append({"id": tid2, "name": tn})
    await st(gid, s["id"], t); await sp(gid, tid2, {"tid": t["id"], "captain": False, "fa": False})
    await fu(aid, tok, f"✅ **{tn}** joined **{t['name']}**!\n👥 {len(t['players'])}/{MAX_TEAM_SIZE}")

async def c_kick(gid, uid, aid, tok, tid2):
    p = await gp(gid, uid)
    if not p or not p.get("captain"): await fu(aid, tok, "❌ Captains only."); return
    if tid2 == uid: await fu(aid, tok, "❌ Use `/leave-team`."); return
    s = await gse(gid)
    if not s: return
    t = await gtid(gid, s["id"], p["tid"])
    if not t or not any(pl["id"] == tid2 for pl in t["players"]): await fu(aid, tok, "❌ Not in team."); return
    tn = await uname(gid, tid2); t["players"] = [pl for pl in t["players"] if pl["id"] != tid2]
    await st(gid, s["id"], t); await dp(gid, tid2)
    await fu(aid, tok, f"👢 **{tn}** kicked. {len(t['players'])}/{MAX_TEAM_SIZE}")

async def c_leave(gid, uid, aid, tok):
    p = await gp(gid, uid)
    if not p or not p.get("tid"): await fu(aid, tok, "❌ Not in team."); return
    s = await gse(gid)
    if not s: return
    t = await gtid(gid, s["id"], p["tid"])
    if t:
        wc = p.get("captain"); t["players"] = [pl for pl in t["players"] if pl["id"] != uid]
        if not t["players"]:
            await dt(gid, s["id"], t); await fu(aid, tok, f"💥 **{t['name']}** disbanded.")
        else:
            if wc:
                t["captain"] = t["players"][0]["id"]
                await sp(gid, t["players"][0]["id"], {"tid": t["id"], "captain": True, "fa": False})
            await st(gid, s["id"], t)
            cm = f"\n👑 New captain: <@{t['captain']}>" if wc else ""
            await fu(aid, tok, f"👋 Left **{t['name']}**.{cm}\n👥 {len(t['players'])}/{MAX_TEAM_SIZE}")
    await dp(gid, uid)

async def c_regfa(gid, uid, aid, tok):
    p = await gp(gid, uid)
    if p and p.get("tid"): await fu(aid, tok, "❌ In team."); return
    if p and p.get("fa"): await fu(aid, tok, "❌ Already FA."); return
    un = await uname(gid, uid); await sp(gid, uid, {"tid": None, "captain": False, "fa": True})
    fas = await gfa(gid); fas.append({"id": uid, "name": un}); await sfa(gid, fas)
    await fu(aid, tok, f"🤝 **{un}** is now a free agent!")

async def c_unregfa(gid, uid, aid, tok):
    p = await gp(gid, uid)
    if not p or not p.get("fa"): await fu(aid, tok, "❌ Not FA."); return
    await dp(gid, uid); fas = [fa for fa in await gfa(gid) if fa["id"] != uid]; await sfa(gid, fas)
    await fu(aid, tok, "👋 No longer FA.")

async def c_reqfa(gid, uid, aid, tok):
    p = await gp(gid, uid)
    if not p or not p.get("captain"): await fu(aid, tok, "❌ Captains only."); return
    s = await gse(gid)
    if not s: return
    t = await gtid(gid, s["id"], p["tid"])
    if not t: return
    fas = await gfa(gid)
    if not fas: await fu(aid, tok, "❌ No FAs."); return
    pings = " ".join(f"<@{fa['id']}>" for fa in fas)
    await fu(aid, tok, f"📢 **{t['name']}** needs sub! {pings}\nDM <@{uid}>!")

async def c_match(gid, uid, aid, tok, opp, os_, ts_):
    try: os_ = int(os_); ts_ = int(ts_)
    except: await fu(aid, tok, "❌ Invalid scores."); return
    s = await gse(gid)
    if not s or s["status"] not in ("active", "finals"): await fu(aid, tok, "❌ No season."); return
    p = await gp(gid, uid)
    if not p or not p.get("tid"): await fu(aid, tok, "❌ Not in team."); return
    rt = await gtid(gid, s["id"], p["tid"]); ot = await gt(gid, s["id"], opp)
    if not rt or not ot: await fu(aid, tok, "❌ Team not found."); return
    if rt["id"] == ot["id"]: await fu(aid, tok, "❌ Same team."); return
    if os_ == ts_: rslt = f"🤝 Draw! {os_}-{ts_}"
    elif os_ > ts_:
        wc, lc = elo(rt["mmr"], ot["mmr"]); rt["mmr"] += wc; rt["wins"] += 1; ot["mmr"] += lc; ot["losses"] += 1
        rslt = f"🏆 **{rt['name']}** wins! ({os_}-{ts_})"
    else:
        wc, lc = elo(ot["mmr"], rt["mmr"]); rt["mmr"] += lc; rt["losses"] += 1; ot["mmr"] += wc; ot["wins"] += 1
        rslt = f"🏆 **{ot['name']}** wins! ({ts_}-{os_})"
    await st(gid, s["id"], rt); await st(gid, s["id"], ot)
    ms = await gm(gid, s["id"]); ms.append({"t1": rt["name"], "t2": ot["name"], "s1": os_, "s2": ts_, "by": uid}); await sm(gid, s["id"], ms)
    await fu(aid, tok, f"📊 {rslt}\n\n**{rt['name']}**: {rt['mmr']} MMR\n**{ot['name']}**: {ot['mmr']} MMR")

async def c_lb(gid, aid, tok):
    s = await gse(gid)
    if not s: await fu(aid, tok, "❌ No season."); return
    ts = await lt(gid, s["id"])
    if not ts: await fu(aid, tok, "📊 No teams."); return
    ts.sort(key=lambda t: t.get("mmr", DEFAULT_MMR), reverse=True)
    st2 = "🏆 FINALS" if s["status"] == "finals" else f"📅 Wk {s.get('week','?')}/{SEASON_WEEKS}"
    msg = f"**📊 Leaderboard** — {st2}\n\n"
    for i, t in enumerate(ts, 1):
        m = ["🥇","🥈","🥉"][i-1] if i <= 3 else f"{i}."
        msg += f"{m} **{t['name']}** — {t.get('mmr', DEFAULT_MMR)} MMR | {t.get('wins',0)}W-{t.get('losses',0)}L\n"
    await fu(aid, tok, msg)

async def c_sched(gid, aid, tok):
    s = await gse(gid)
    if not s: await fu(aid, tok, "❌ No season."); return
    if s["status"] == "finals":
        fs = s.get("finals", []); msg = "**📅 Finals**\n\n"
        for m in fs: msg += f"⚔️ {m['t1n']} vs {m['t2n']}\n"
    else: msg = f"📅 **Wk {s.get('week',1)}** — Self-schedule + `/match-result`!"
    await fu(aid, tok, msg)

async def c_team(gid, uid, aid, tok, name=None):
    s = await gse(gid)
    if not s: await fu(aid, tok, "❌ No season."); return
    t = await gt(gid, s["id"], name) if name else None
    if not t:
        p = await gp(gid, uid)
        if not p or not p.get("tid"): await fu(aid, tok, "❌ Not in team."); return
        t = await gtid(gid, s["id"], p["tid"])
    if not t: await fu(aid, tok, "❌ Not found."); return
    msg = f"**👥 {t['name']}**\n📊 {t.get('mmr', DEFAULT_MMR)} MMR\n👑 <@{t['captain']}>\n👥 {len(t['players'])}/4:\n"
    for pl in t["players"]: msg += f"  • <@{pl['id']}>{' 👑' if pl['id'] == t['captain'] else ''}\n"
    await fu(aid, tok, msg)

async def c_ov(gid, aid, tok):
    s = await gse(gid)
    if not s: await fu(aid, tok, "❌ No season."); return
    ts = await lt(gid, s["id"]); fas = await gfa(gid)
    e = "🏆" if s["status"] == "finals" else "⚔️"
    msg = f"**{e} League Overview**\n\n"
    if ts:
        ts.sort(key=lambda t: t.get("mmr", DEFAULT_MMR), reverse=True); msg += "**Teams:**\n"
        for i, t in enumerate(ts, 1): msg += f"{i}. **{t['name']}** — {t['mmr']} MMR\n"
    else: msg += "**Teams:** None\n"
    if fas: msg += f"\n**FAs ({len(fas)}):**\n" + "\n".join(f"🤝 <@{fa['id']}>" for fa in fas)
    await fu(aid, tok, msg)

# ── Dispatch ──────────────────────────────────────────────────────────────
VALID_CMDS = {"setup-league","start-season","end-season","create-team","invite","kick","leave-team","register-fa","unregister-fa","request-fa","match-result","leaderboard","schedule","team","overview"}

async def dispatch(cmd, g, u, a, t, opts):
    if cmd == "setup-league":    await c_setup(g, u, a, t)
    elif cmd == "start-season":  await c_start(g, u, a, t)
    elif cmd == "end-season":    await c_end(g, u, a, t)
    elif cmd == "create-team":   await c_cteam(g, u, a, t, _o(opts, "name"))
    elif cmd == "invite":        await c_invite(g, u, a, t, _o(opts, "player"))
    elif cmd == "kick":          await c_kick(g, u, a, t, _o(opts, "player"))
    elif cmd == "leave-team":    await c_leave(g, u, a, t)
    elif cmd == "register-fa":   await c_regfa(g, u, a, t)
    elif cmd == "unregister-fa": await c_unregfa(g, u, a, t)
    elif cmd == "request-fa":    await c_reqfa(g, u, a, t)
    elif cmd == "match-result":  await c_match(g, u, a, t, _o(opts, "opponent"), _o(opts, "our-score", "0"), _o(opts, "their-score", "0"))
    elif cmd == "leaderboard":   await c_lb(g, a, t)
    elif cmd == "schedule":      await c_sched(g, a, t)
    elif cmd == "team":          await c_team(g, u, a, t, _o(opts, "name", None))
    elif cmd == "overview":      await c_ov(g, a, t)

# ── Endpoints ─────────────────────────────────────────────────────────────
class H(BaseModel): status: str = Field(default="ok")

@app.get("/health", response_model=H)
async def health(): return H()

@app.post("/")
async def interactions(request: Request, bg: BackgroundTasks):
    body = await request.body()
    if not verify_sig(os.environ.get("DISCORD_PUBLIC_KEY",""), request.headers.get("X-Signature-Ed25519",""), request.headers.get("X-Signature-Timestamp",""), body):
        raise HTTPException(401, "Bad signature")
    data = json.loads(body)
    if data.get("type") == 1: return JSONResponse({"type": 1})
    if data.get("type") == 2:
        cd = data.get("data", {}); cmd = cd.get("name", ""); tok = data.get("token", ""); gid = data.get("guild_id", "")
        mem = data.get("member") or data.get("user") or {}; uid = mem.get("user", {}).get("id", "") or mem.get("id", "")
        if cmd in VALID_CMDS:
            bg.add_task(dispatch, cmd, gid, uid, os.environ["DISCORD_APP_ID"], tok, cd.get("options", []))
            return JSONResponse({"type": 5})
        return JSONResponse({"type": 4, "data": {"content": f"❓ Unknown: `{cmd}`"}})
    return JSONResponse({"type": 1})

@app.post("/register-commands")
async def reg():
    tok = os.environ["DISCORD_BOT_TOKEN"]; aid = os.environ["DISCORD_APP_ID"]
    cmds = [
        {"name":"setup-league","description":"Create league channels (Admin)"},
        {"name":"start-season","description":"Start season (Admin)"},
        {"name":"end-season","description":"End season (Admin)"},
        {"name":"create-team","description":"Create team","options":[{"name":"name","description":"Team name","type":3,"required":True}]},
        {"name":"invite","description":"Invite player (Captain)","options":[{"name":"player","description":"Player","type":6,"required":True}]},
        {"name":"kick","description":"Kick player (Captain)","options":[{"name":"player","description":"Player","type":6,"required":True}]},
        {"name":"leave-team","description":"Leave team"},
        {"name":"register-fa","description":"Become free agent"},
        {"name":"unregister-fa","description":"Stop being free agent"},
        {"name":"request-fa","description":"Ping free agents (Captain)"},
        {"name":"match-result","description":"Report match","options":[
            {"name":"opponent","description":"Opponent","type":3,"required":True},
            {"name":"our-score","description":"Your score","type":4,"required":True},
            {"name":"their-score","description":"Their score","type":4,"required":True}]},
        {"name":"leaderboard","description":"MMR rankings"},
        {"name":"schedule","description":"Schedule"},
        {"name":"team","description":"Team info","options":[{"name":"name","description":"Team name","type":3,"required":False}]},
        {"name":"overview","description":"League overview"},
    ]
    async with httpx.AsyncClient(timeout=30) as c:
        r = await c.put(f"{DISCORD_API}/applications/{aid}/commands", headers={"Authorization":f"Bot {tok}","Content-Type":"application/json"}, json=cmds)
        r.raise_for_status(); return {"success": True, "commands": [x["name"] for x in r.json()]}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))
