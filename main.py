#!/usr/bin/env python3.11
"""Elements Divided League — Discord Slash Command Bot (Standalone).

Deploy on Railway, Render, or any FastAPI host. Set the URL as your
Discord Interactions Endpoint. Requires: DISCORD_APP_ID, DISCORD_BOT_TOKEN,
DISCORD_PUBLIC_KEY, REDIS_URL (from Upstash).
"""

import asyncio, json, math, os, time, logging
from datetime import datetime, timezone
from typing import Any

import httpx, redis.asyncio as aioredis
from fastapi import BackgroundTasks, FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse
from nacl.exceptions import BadSignatureError
from nacl.signing import VerifyKey
from pydantic import BaseModel, Field

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("league-bot")

# ── Constants ─────────────────────────────────────────────────────────────
DISCORD_API       = "https://discord.com/api/v10"
ELO_K             = 32
DEFAULT_MMR       = 1000
MAX_TEAM_SIZE     = 4
MATCHES_PER_WEEK  = 2
SEASON_WEEKS      = 8
LEAGUE_ADMIN_ROLE = "League Admin"

LEAGUE_CHANNELS = [
    "📋-overview", "📊-leaderboard", "📅-schedule",
    "🤝-free-agents", "📢-announcements", "💬-league-chat",
]

app = FastAPI(title="Elements Divided League Bot", version="1.0.0")

# ── Redis ─────────────────────────────────────────────────────────────────
REDIS_URL = os.environ["REDIS_URL"]
_redis: aioredis.Redis | None = None

async def get_redis() -> aioredis.Redis:
    global _redis
    if _redis is None:
        _redis = aioredis.from_url(REDIS_URL, decode_responses=True)
    return _redis

async def _gs(key: str) -> Any:
    r = await get_redis(); v = await r.get(f"league:{key}")
    return json.loads(v) if v else None

async def _ss(key: str, value: Any) -> None:
    r = await get_redis(); await r.set(f"league:{key}", json.dumps(value))

async def _ds(key: str) -> None:
    r = await get_redis(); await r.delete(f"league:{key}")

# ── Ed25519 ───────────────────────────────────────────────────────────────
def verify_signature(pk: str, sig: str, ts: str, body: bytes) -> bool:
    try:
        VerifyKey(bytes.fromhex(pk)).verify(ts.encode() + body, bytes.fromhex(sig))
        return True
    except BadSignatureError:
        return False

# ── Discord Helpers ───────────────────────────────────────────────────────
def _headers() -> dict:
    return {"Authorization": f"Bot {os.environ['DISCORD_BOT_TOKEN']}", "Content-Type": "application/json"}

async def _discord(method: str, path: str, data: dict | None = None) -> dict:
    url = f"{DISCORD_API}{path}"
    async with httpx.AsyncClient(timeout=30) as c:
        for _ in range(4):
            if method == "GET":    r = await c.get(url, headers=_headers())
            elif method == "POST": r = await c.post(url, headers=_headers(), json=data)
            elif method == "PATCH": r = await c.patch(url, headers=_headers(), json=data)
            elif method == "PUT":  r = await c.put(url, headers=_headers(), json=data)
            if r.status_code == 429:
                await asyncio.sleep(float(r.json().get("retry_after", 1.5)) + 0.1); continue
            if r.status_code == 204: return {}
            try: return r.json()
            except: return {"status": r.status_code}
        return {}

async def followup(app_id: str, token: str, content: str) -> None:
    if len(content) > 2000: content = content[:1950] + "\n\n_[truncated]_"
    auth = {"Authorization": f"Bot {os.environ['DISCORD_BOT_TOKEN']}"}
    url = f"{DISCORD_API}/webhooks/{app_id}/{token}"
    async with httpx.AsyncClient(timeout=30) as c:
        for _ in range(4):
            r = await c.post(url, headers=auth, json={"content": content})
            if r.status_code == 429:
                await asyncio.sleep(float(r.json().get("retry_after", 1.5)) + 0.1); continue
            return

async def has_role(gid: str, uid: str, role: str) -> bool:
    try:
        m = await _discord("GET", f"/guilds/{gid}/members/{uid}")
        roles = await _discord("GET", f"/guilds/{gid}/roles")
        if isinstance(roles, list):
            rid = next((r["id"] for r in roles if r.get("name","").lower() == role.lower()), None)
            return rid in m.get("roles", []) if rid else False
    except: return False

async def username(gid: str, uid: str) -> str:
    try:
        m = await _discord("GET", f"/guilds/{gid}/members/{uid}")
        return m.get("nick") or m.get("user",{}).get("username","unknown")
    except: return "unknown"

# ── Elo ───────────────────────────────────────────────────────────────────
def calc_elo(w_mmr: int, l_mmr: int) -> tuple[int,int]:
    ew = 1.0/(1.0+math.pow(10,(l_mmr-w_mmr)/400.0))
    return round(ELO_K*(1.0-ew)), round(ELO_K*(0.0-(1.0-ew)))

# ── State ─────────────────────────────────────────────────────────────────
async def get_player(gid,uid): return await _gs(f"p:{gid}:{uid}")
async def set_player(gid,uid,d): await _ss(f"p:{gid}:{uid}",d)
async def del_player(gid,uid): await _ds(f"p:{gid}:{uid}")

async def get_config(gid): return await _gs(f"cfg:{gid}")
async def set_config(gid,d): await _ss(f"cfg:{gid}",d)

async def get_team(gid,sid,name):
    r = await get_redis()
    tid = await r.hget(f"league:tidx:{gid}:{sid}", name.lower())
    if tid:
        v = await r.get(f"league:t:{gid}:{sid}:{tid}")
        return json.loads(v) if v else None
    return None

async def get_team_by_id(gid,sid,tid): return await _gs(f"t:{gid}:{sid}:{tid}")

async def save_team(gid,sid,t):
    r = await get_redis()
    await r.set(f"league:t:{gid}:{sid}:{t['id']}", json.dumps(t))
    await r.hset(f"league:tidx:{gid}:{sid}", t["name"].lower(), t["id"])
    await r.sadd(f"league:tlist:{gid}:{sid}", t["id"])

async def delete_team(gid,sid,t):
    r = await get_redis()
    await r.delete(f"league:t:{gid}:{sid}:{t['id']}")
    await r.hdel(f"league:tidx:{gid}:{sid}", t["name"].lower())
    await r.srem(f"league:tlist:{gid}:{sid}", t["id"])

async def list_teams(gid,sid):
    r = await get_redis()
    ids = await r.smembers(f"league:tlist:{gid}:{sid}")
    teams = []
    for tid in ids:
        v = await r.get(f"league:t:{gid}:{sid}:{tid}")
        if v: teams.append(json.loads(v))
    return teams

async def get_fas(gid): v = await _gs(f"fa:{gid}"); return v or []
async def save_fas(gid,fas): await _ss(f"fa:{gid}",fas)

async def get_season(gid): return await _gs(f"s:{gid}")
async def save_season(gid,s): await _ss(f"s:{gid}",s)

async def get_matches(gid,sid): v = await _gs(f"m:{gid}:{sid}"); return v or []
async def save_matches(gid,sid,m): await _ss(f"m:{gid}:{sid}",m)

# ── Commands ──────────────────────────────────────────────────────────────

async def cmd_setup(gid,uid,aid,tok):
    if not await has_role(gid,uid,LEAGUE_ADMIN_ROLE):
        await followup(aid,tok,f"❌ Need **{LEAGUE_ADMIN_ROLE}** role."); return
    if await get_config(gid):
        await followup(aid,tok,"⚠️ Already set up. `/start-season`."); return
    cat = await _discord("POST",f"/guilds/{gid}/channels",{"name":"⚔️ Elements Divided League","type":4})
    chs = {}
    for ch in LEAGUE_CHANNELS:
        c = await _discord("POST",f"/guilds/{gid}/channels",{"name":ch,"type":0,"parent_id":cat["id"]})
        chs[ch] = c["id"]
    await set_config(gid,{"guild_id":gid,"cat_id":cat["id"],"channels":chs})
    await followup(aid,tok,f"✅ **League ready!** {len(LEAGUE_CHANNELS)} channels.\nUse `/start-season`!")

async def cmd_start(gid,uid,aid,tok):
    if not await has_role(gid,uid,LEAGUE_ADMIN_ROLE):
        await followup(aid,tok,f"❌ Need **{LEAGUE_ADMIN_ROLE}**."); return
    if not await get_config(gid):
        await followup(aid,tok,"❌ `/setup-league` first."); return
    ex = await get_season(gid)
    if ex and ex.get("status") in ("active","finals"):
        await followup(aid,tok,"❌ Season exists."); return
    sid = str(int(time.time()))
    await save_season(gid,{"id":sid,"status":"active","week":1})
    await followup(aid,tok,f"🏆 **Season started!** {SEASON_WEEKS}w, {MATCHES_PER_WEEK} matches/wk\nUse `/create-team`!")

async def cmd_end(gid,uid,aid,tok):
    if not await has_role(gid,uid,LEAGUE_ADMIN_ROLE):
        await followup(aid,tok,f"❌ Need **{LEAGUE_ADMIN_ROLE}**."); return
    s = await get_season(gid)
    if not s or s["status"]!="active":
        await followup(aid,tok,"❌ No active season."); return
    ts = await list_teams(gid,s["id"])
    if len(ts)<4: await followup(aid,tok,f"❌ Need 4+ teams (have {len(ts)})."); return
    ts.sort(key=lambda t:t.get("mmr",DEFAULT_MMR), reverse=True)
    t4 = ts[:4]; finals = []
    for i in range(len(t4)):
        for j in range(i+1,len(t4)):
            finals.append({"t1":t4[i]["id"],"t1n":t4[i]["name"],"t2":t4[j]["id"],"t2n":t4[j]["name"]})
    s["status"]="finals"; s["finals"]=finals
    s["f_teams"]=[{"id":t["id"],"name":t["name"],"mmr":t["mmr"]} for t in t4]
    await save_season(gid,s)
    msg = "🏆 **FINALS — Top 4!**\n\n"
    for i,t in enumerate(t4,1): msg+=f"{i}. **{t['name']}** — {t['mmr']} MMR\n"
    msg+="\nUse `/match-result`!"
    await followup(aid,tok,msg)

async def cmd_cteam(gid,uid,aid,tok,name):
    s = await get_season(gid)
    if not s or s["status"]!="active": await followup(aid,tok,"❌ No season."); return
    p = await get_player(gid,uid)
    if p:
        if p.get("tid"): await followup(aid,tok,"❌ In team. `/leave-team`."); return
        if p.get("fa"): await followup(aid,tok,"❌ Free agent. `/unregister-fa`."); return
    if await get_team(gid,s["id"],name): await followup(aid,tok,f"❌ {name} exists."); return
    un = await username(gid,uid)
    t = {"id":f"t_{int(time.time())}","name":name,"captain":uid,
         "players":[{"id":uid,"name":un}],"mmr":DEFAULT_MMR,"wins":0,"losses":0}
    await save_team(gid,s["id"],t)
    await set_player(gid,uid,{"tid":t["id"],"captain":True,"fa":False})
    await followup(aid,tok,f"✅ **{name}** created! 👑 {un}\n👥 1/{MAX_TEAM_SIZE}")

async def cmd_invite(gid,uid,aid,tok,tid):
    p = await get_player(gid,uid)
    if not p or not p.get("captain"): await followup(aid,tok,"❌ Captains only."); return
    s = await get_season(gid)
    if not s or s["status"]!="active": await followup(aid,tok,"❌ No season."); return
    t = await get_team_by_id(gid,s["id"],p["tid"])
    if not t: await followup(aid,tok,"❌ Team not found."); return
    if len(t["players"])>=MAX_TEAM_SIZE: await followup(aid,tok,f"❌ Full ({MAX_TEAM_SIZE})."); return
    tp = await get_player(gid,tid)
    if tp:
        if tp.get("tid"): await followup(aid,tok,"❌ In team."); return
        if tp.get("fa"): await followup(aid,tok,"❌ Is FA."); return
    if any(pl["id"]==tid for pl in t["players"]): await followup(aid,tok,"❌ Already in team."); return
    tn = await username(gid,tid)
    t["players"].append({"id":tid,"name":tn})
    await save_team(gid,s["id"],t)
    await set_player(gid,tid,{"tid":t["id"],"captain":False,"fa":False})
    await followup(aid,tok,f"✅ **{tn}** joined **{t['name']}**!\n👥 {len(t['players'])}/{MAX_TEAM_SIZE}")

async def cmd_kick(gid,uid,aid,tok,tid):
    p = await get_player(gid,uid)
    if not p or not p.get("captain"): await followup(aid,tok,"❌ Captains only."); return
    if tid==uid: await followup(aid,tok,"❌ Use `/leave-team`."); return
    s = await get_season(gid)
    if not s: return
    t = await get_team_by_id(gid,s["id"],p["tid"])
    if not t or not any(pl["id"]==tid for pl in t["players"]):
        await followup(aid,tok,"❌ Not in team."); return
    tn = await username(gid,tid)
    t["players"] = [pl for pl in t["players"] if pl["id"]!=tid]
    await save_team(gid,s["id"],t); await del_player(gid,tid)
    await followup(aid,tok,f"👢 **{tn}** kicked. {len(t['players'])}/{MAX_TEAM_SIZE}")

async def cmd_leave(gid,uid,aid,tok):
    p = await get_player(gid,uid)
    if not p or not p.get("tid"): await followup(aid,tok,"❌ Not in team."); return
    s = await get_season(gid)
    if not s: return
    t = await get_team_by_id(gid,s["id"],p["tid"])
    if t:
        was_cap = p.get("captain")
        t["players"] = [pl for pl in t["players"] if pl["id"]!=uid]
        if not t["players"]:
            await delete_team(gid,s["id"],t)
            await followup(aid,tok,f"💥 **{t['name']}** disbanded.")
        else:
            if was_cap:
                t["captain"]=t["players"][0]["id"]
                await set_player(gid,t["players"][0]["id"],{"tid":t["id"],"captain":True,"fa":False})
            await save_team(gid,s["id"],t)
            cm = f"\n👑 New captain: <@{t['captain']}>" if was_cap else ""
            await followup(aid,tok,f"👋 Left **{t['name']}**.{cm}\n👥 {len(t['players'])}/{MAX_TEAM_SIZE}")
    await del_player(gid,uid)

async def cmd_reg_fa(gid,uid,aid,tok):
    p = await get_player(gid,uid)
    if p:
        if p.get("tid"): await followup(aid,tok,"❌ In team."); return
        if p.get("fa"): await followup(aid,tok,"❌ Already FA."); return
    un = await username(gid,uid)
    await set_player(gid,uid,{"tid":None,"captain":False,"fa":True})
    fas = await get_fas(gid)
    fas.append({"id":uid,"name":un})
    await save_fas(gid,fas)
    await followup(aid,tok,f"🤝 **{un}** is now a free agent!")

async def cmd_unreg_fa(gid,uid,aid,tok):
    p = await get_player(gid,uid)
    if not p or not p.get("fa"): await followup(aid,tok,"❌ Not FA."); return
    await del_player(gid,uid)
    fas = [fa for fa in await get_fas(gid) if fa["id"]!=uid]
    await save_fas(gid,fas)
    await followup(aid,tok,"👋 No longer FA.")

async def cmd_req_fa(gid,uid,aid,tok):
    p = await get_player(gid,uid)
    if not p or not p.get("captain"): await followup(aid,tok,"❌ Captains only."); return
    s = await get_season(gid)
    if not s: return
    t = await get_team_by_id(gid,s["id"],p["tid"])
    if not t: return
    fas = await get_fas(gid)
    if not fas: await followup(aid,tok,"❌ No FAs."); return
    pings = " ".join(f"<@{fa['id']}>" for fa in fas)
    await followup(aid,tok,f"📢 **{t['name']}** needs a sub! {pings}\nDM <@{uid}>!")

async def cmd_match(gid,uid,aid,tok,opp,os,ts_):
    try: os=int(os); ts_=int(ts_)
    except: await followup(aid,tok,"❌ Invalid scores."); return
    s = await get_season(gid)
    if not s or s["status"] not in ("active","finals"):
        await followup(aid,tok,"❌ No season."); return
    p = await get_player(gid,uid)
    if not p or not p.get("tid"): await followup(aid,tok,"❌ Not in team."); return
    rt = await get_team_by_id(gid,s["id"],p["tid"])
    ot = await get_team(gid,s["id"],opp)
    if not rt or not ot: await followup(aid,tok,"❌ Team not found."); return
    if rt["id"]==ot["id"]: await followup(aid,tok,"❌ Same team."); return
    if os==ts_:
        rslt=f"🤝 Draw! {os}-{ts_}"
    elif os>ts_:
        wc,lc=calc_elo(rt["mmr"],ot["mmr"]); rt["mmr"]+=wc; rt["wins"]+=1; ot["mmr"]+=lc; ot["losses"]+=1
        rslt=f"🏆 **{rt['name']}** wins! ({os}-{ts_})"
    else:
        wc,lc=calc_elo(ot["mmr"],rt["mmr"]); rt["mmr"]+=lc; rt["losses"]+=1; ot["mmr"]+=wc; ot["wins"]+=1
        rslt=f"🏆 **{ot['name']}** wins! ({ts_}-{os})"
    await save_team(gid,s["id"],rt); await save_team(gid,s["id"],ot)
    ms = await get_matches(gid,s["id"])
    ms.append({"t1":rt["name"],"t2":ot["name"],"s1":os,"s2":ts_,"by":uid})
    await save_matches(gid,s["id"],ms)
    await followup(aid,tok,f"📊 {rslt}\n\n**{rt['name']}**: {rt['mmr']} MMR\n**{ot['name']}**: {ot['mmr']} MMR")

async def cmd_lb(gid,aid,tok):
    s = await get_season(gid)
    if not s: await followup(aid,tok,"❌ No season."); return
    ts = await list_teams(gid,s["id"])
    if not ts: await followup(aid,tok,"📊 No teams."); return
    ts.sort(key=lambda t:t.get("mmr",DEFAULT_MMR), reverse=True)
    st = "🏆 FINALS" if s["status"]=="finals" else f"📅 Wk {s.get('week','?')}/{SEASON_WEEKS}"
    msg = f"**📊 Leaderboard** — {st}\n\n"
    for i,t in enumerate(ts,1):
        m = ["🥇","🥈","🥉"][i-1] if i<=3 else f"{i}."
        msg+=f"{m} **{t['name']}** — {t.get('mmr',DEFAULT_MMR)} MMR | {t.get('wins',0)}W-{t.get('losses',0)}L\n"
    await followup(aid,tok,msg)

async def cmd_sched(gid,aid,tok):
    s = await get_season(gid)
    if not s: await followup(aid,tok,"❌ No season."); return
    if s["status"]=="finals":
        fs = s.get("finals",[])
        msg = "**📅 Finals**\n\n"
        for m in fs: msg+=f"⚔️ {m['t1n']} vs {m['t2n']}\n"
    else:
        msg = f"📅 **Wk {s.get('week',1)}** — Self-schedule, use `/match-result`!"
    await followup(aid,tok,msg)

async def cmd_team_info(gid,uid,aid,tok,name=None):
    s = await get_season(gid)
    if not s: await followup(aid,tok,"❌ No season."); return
    t = await get_team(gid,s["id"],name) if name else None
    if not t:
        p = await get_player(gid,uid)
        if not p or not p.get("tid"): await followup(aid,tok,"❌ Not in team."); return
        t = await get_team_by_id(gid,s["id"],p["tid"])
    if not t: await followup(aid,tok,"❌ Not found."); return
    msg = f"**👥 {t['name']}**\n📊 {t.get('mmr',DEFAULT_MMR)} MMR\n👑 <@{t['captain']}>\n👥 {len(t['players'])}/4:\n"
    for pl in t["players"]:
        msg+=f"  • <@{pl['id']}>{' 👑' if pl['id']==t['captain'] else ''}\n"
    await followup(aid,tok,msg)

async def cmd_overview(gid,aid,tok):
    s = await get_season(gid)
    if not s: await followup(aid,tok,"❌ No season."); return
    ts = await list_teams(gid,s["id"])
    fas = await get_fas(gid)
    e = "🏆" if s["status"]=="finals" else "⚔️"
    msg = f"**{e} League Overview**\n\n"
    if ts:
        ts.sort(key=lambda t:t.get("mmr",DEFAULT_MMR), reverse=True)
        msg+="**Teams:**\n"
        for i,t in enumerate(ts,1): msg+=f"{i}. **{t['name']}** — {t['mmr']} MMR\n"
    else: msg+="**Teams:** None\n"
    if fas: msg+=f"\n**FAs ({len(fas)}):**\n"+"\n".join(f"🤝 <@{fa['id']}>" for fa in fas)
    await followup(aid,tok,msg)

# ── Router ────────────────────────────────────────────────────────────────
def _opt(opts, name, default=""):
    for o in opts:
        if o.get("name")==name: return o.get("value",default)
    return default

VALID_CMDS = {"setup-league","start-season","end-season","create-team","invite","kick","leave-team","register-fa","unregister-fa","request-fa","match-result","leaderboard","schedule","team","overview"}

async def dispatch(cmd,g,u,a,t,opts):
    if cmd=="setup-league":    await cmd_setup(g,u,a,t)
    elif cmd=="start-season":  await cmd_start(g,u,a,t)
    elif cmd=="end-season":    await cmd_end(g,u,a,t)
    elif cmd=="create-team":   await cmd_cteam(g,u,a,t,_opt(opts,"name"))
    elif cmd=="invite":        await cmd_invite(g,u,a,t,_opt(opts,"player"))
    elif cmd=="kick":          await cmd_kick(g,u,a,t,_opt(opts,"player"))
    elif cmd=="leave-team":    await cmd_leave(g,u,a,t)
    elif cmd=="register-fa":   await cmd_reg_fa(g,u,a,t)
    elif cmd=="unregister-fa": await cmd_unreg_fa(g,u,a,t)
    elif cmd=="request-fa":    await cmd_req_fa(g,u,a,t)
    elif cmd=="match-result":  await cmd_match(g,u,a,t,_opt(opts,"opponent"),_opt(opts,"our-score","0"),_opt(opts,"their-score","0"))
    elif cmd=="leaderboard":   await cmd_lb(g,a,t)
    elif cmd=="schedule":      await cmd_sched(g,a,t)
    elif cmd=="team":          await cmd_team_info(g,u,a,t,_opt(opts,"name",None))
    elif cmd=="overview":      await cmd_overview(g,a,t)

# ── Endpoints ─────────────────────────────────────────────────────────────

class HealthOut(BaseModel):
    status: str = Field(default="ok")

@app.get("/health", response_model=HealthOut)
async def health(): return HealthOut()

@app.post("/")
async def interactions(request: Request, bg: BackgroundTasks):
    body = await request.body()
    pk = os.environ.get("DISCORD_PUBLIC_KEY","")
    sig = request.headers.get("X-Signature-Ed25519","")
    ts = request.headers.get("X-Signature-Timestamp","")
    if not verify_signature(pk, sig, ts, body):
        raise HTTPException(401, "Bad signature")
    data = json.loads(body)
    if data.get("type") == 1:
        return JSONResponse({"type":1})
    if data.get("type") == 2:
        cd = data.get("data",{})
        cmd = cd.get("name","")
        tok = data.get("token","")
        gid = data.get("guild_id","")
        mem = data.get("member") or data.get("user") or {}
        uid = mem.get("user",{}).get("id","") or mem.get("id","")
       if cmd in VALID_CMDS:
    bg.add_task(dispatch, cmd, gid, uid, os.environ["DISCORD_APP_ID"], tok, cd.get("options",[]))
            return JSONResponse({"type":5})
        return JSONResponse({"type":4,"data":{"content":f"❓ Unknown: `{cmd}`"}})
    return JSONResponse({"type":1})

@app.post("/register-commands")
async def register():
    tok = os.environ["DISCORD_BOT_TOKEN"]
    aid = os.environ["DISCORD_APP_ID"]
    cmds = [
        {"name":"setup-league","description":"Create league channels (Admin)"},
        {"name":"start-season","description":"Start new season (Admin)"},
        {"name":"end-season","description":"End season → finals (Admin)"},
        {"name":"create-team","description":"Create a team","options":[{"name":"name","description":"Team name","type":3,"required":True}]},
        {"name":"invite","description":"Invite player (Captain)","options":[{"name":"player","description":"Player","type":6,"required":True}]},
        {"name":"kick","description":"Kick player (Captain)","options":[{"name":"player","description":"Player","type":6,"required":True}]},
        {"name":"leave-team","description":"Leave your team"},
        {"name":"register-fa","description":"Become free agent"},
        {"name":"unregister-fa","description":"Stop being free agent"},
        {"name":"request-fa","description":"Ping free agents (Captain)"},
        {"name":"match-result","description":"Report match result","options":[
            {"name":"opponent","description":"Opponent team","type":3,"required":True},
            {"name":"our-score","description":"Your score","type":4,"required":True},
            {"name":"their-score","description":"Their score","type":4,"required":True}]},
        {"name":"leaderboard","description":"MMR rankings"},
        {"name":"schedule","description":"Match schedule"},
        {"name":"team","description":"Team info","options":[{"name":"name","description":"Team name","type":3,"required":False}]},
        {"name":"overview","description":"League overview"},
    ]
    async with httpx.AsyncClient(timeout=30) as c:
        r = await c.put(f"{DISCORD_API}/applications/{aid}/commands",
            headers={"Authorization":f"Bot {tok}","Content-Type":"application/json"}, json=cmds)
        r.raise_for_status()
        return {"success":True,"commands":[x["name"] for x in r.json()]}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=int(os.environ.get("PORT",8000)))
