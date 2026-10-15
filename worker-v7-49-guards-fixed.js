import { connect } from "cloudflare:sockets";

// ================================================================
//  EPIC PANEL v7 — Cloudflare Worker + KV (Multi-Admin / Multi-Panel)
// ================================================================

const SUPERADMIN_USER  = "Epic";
const SUPERADMIN_PASS  = "Epic900";
const PANEL_NAME       = "Epic Panel";
// ─── نرخ تبدیل «ریکوئست» به گیگابایت — برای وقتی ادمین موقع ساخت اشتراک
//     محدودیت رو به‌جای گیگ، به تعداد ریکوئست وارد می‌کنه (رایج توی پنل‌های
//     مشابه؛ صرفاً یه واحد جایگزین برای راحتی ورودیه، نه یه معیار دقیق فنی)
const REQ_PER_GB = 6000;

// ─── کش حافظه‌ای (persist در همون isolate، صفر KV اضافه) ─────────────────

// ═══ ذخیره‌سازی: D1 (اگه بایند شده باشه) وگرنه KV — با همون API ═══
// D1 رایگان: ۱۰۰ هزار نوشتن و ۵ میلیون خوندن در روز (KV فقط ۱۰۰۰ نوشتن)
let _d1Ready=false;
// ── محافظ بار (ایده از بررسی worker_5): جلوگیری از سیل اتصال/ریکانکت ──
const WS_MAX_OPEN=400;            // سقف اتصال هم‌زمان WebSocket در هر isolate
const WS_PER_IP_MIN=300;          // سقف اتصال جدید در دقیقه برای هر IP
const WS_UP_MAX_BYTES=4*1024*1024;// سقف صف دیتای در انتظار نوشتن به مقصد
const WS_UP_MAX_ITEMS=256;
const RELAY_FAIL_STREAK=3, RELAY_QUARANTINE_MS=5*60*1000;
const _wsRate=new Map(); let _openWs=0;
const _relayQ=new Map();          // "host|port" → {fail,until}
function wsAllow(ip){
  const now=Date.now();let r=_wsRate.get(ip);
  if(!r||now-r.t>60000){r={n:0,t:now};_wsRate.set(ip,r);}
  r.n++;
  if(_wsRate.size>5000)_wsRate.clear();
  return r.n<=WS_PER_IP_MIN&&_openWs<WS_MAX_OPEN;
}
const _rk=(h,p)=>String(h).toLowerCase()+"|"+(p||0);
function relayBad(h,p){const e=_relayQ.get(_rk(h,p));return !!(e&&e.until>Date.now());}
function relayFail(h,p){
  const k=_rk(h,p);let e=_relayQ.get(k);if(!e){e={fail:0,until:0};_relayQ.set(k,e);}
  if(++e.fail>=RELAY_FAIL_STREAK)e.until=Date.now()+RELAY_QUARANTINE_MS;
  if(_relayQ.size>1000)_relayQ.clear();
}
function relayOk(h,p){_relayQ.delete(_rk(h,p));}
let _scanAt=0;                // آخرین اسکن کامل برای پیدا کردن ایندکس گمشده
const _loginTries=new Map();  // ip → {n,t} برای محدودیت تلاش ورود
let _lastErr=null;            // آخرین خطای واقعی، برای /__health
const _CACHE_MAX=1500;
function _trim(m){ if(m.size>_CACHE_MAX){ let n=m.size-_CACHE_MAX/2; for(const k of m.keys()){ m.delete(k); if(--n<=0)break; } } }
const _rc=new Map(); // کش خوندن کوتاه‌مدت داخل isolate
const _RC_TTL=4000;
function makeStore(env){
  const db=env.DB;
  if(!db||typeof db.prepare!=="function")return env.KV;
  const ensure=async()=>{
    if(_d1Ready)return;
    await db.prepare("CREATE TABLE IF NOT EXISTS kv (k TEXT PRIMARY KEY, v TEXT NOT NULL)").run();
    _d1Ready=true;
  };
  let _migDone=false;
  const migDone=async()=>{
    if(_migDone||!env.KV)return true;
    const r=await db.prepare("SELECT v FROM kv WHERE k='__mig_done__'").first();
    if(r){_migDone=true;return true;}
    return false;
  };
  // مهاجرت مرحله‌ای از KV (هر بار ≤۳۰ کلید، چون ورکر رایگان ۵۰ ساب‌ریکوئست داره)
  const migrateStep=async()=>{
    await ensure();
    if(await migDone())return true;
    const cr=await db.prepare("SELECT v FROM kv WHERE k='__mig_cursor__'").first();
    const cursor=cr&&cr.v?cr.v:undefined;
    const l=await env.KV.list({cursor,limit:30});
    const st=[];
    for(const k of l.keys){
      const v=await env.KV.get(k.name);
      if(v!==null)st.push(db.prepare("INSERT OR IGNORE INTO kv (k,v) VALUES (?,?)").bind(k.name,v));
    }
    if(l.list_complete)st.push(db.prepare("INSERT OR REPLACE INTO kv (k,v) VALUES ('__mig_done__','1')"));
    else st.push(db.prepare("INSERT OR REPLACE INTO kv (k,v) VALUES ('__mig_cursor__',?)").bind(l.cursor));
    if(st.length)await db.batch(st);
    // هر بار که اینجا رسیدیم یعنی حداقل یه‌بار migrateStep واقعی اجرا شده؛
    // _migDone فقط وقتی true میشه که list_complete باشه (خط بالا)
    if(l.list_complete){_migDone=true;return true;}
    return false;
  };
  const parse=(v,t)=>{if(v===null||v===undefined)return null;if(t==="json"||(t&&t.type==="json")){try{return JSON.parse(v);}catch(e){return null;}}return v;};
  return {
    async get(k,t){
      const c=_rc.get(k);
      if(c&&Date.now()-c.ts<_RC_TTL)return parse(c.v,t);
      await ensure();
      const r=await db.prepare("SELECT v FROM kv WHERE k=?").bind(k).first();
      let v=r?r.v:null;
      if(v===null&&env.KV&&!(await migDone())){try{v=await env.KV.get(k);}catch(e){}}
      _rc.set(k,{v,ts:Date.now()});_trim(_rc);
      return parse(v,t);
    },
    async put(k,v){
      await ensure();
      v=String(v);
      const c=_rc.get(k);
      if(c&&c.v===v&&Date.now()-c.ts<60000)return; // بدون تغییر: ننویس
      await db.prepare("INSERT INTO kv (k,v) VALUES (?,?) ON CONFLICT(k) DO UPDATE SET v=excluded.v").bind(k,v).run();
      _rc.set(k,{v,ts:Date.now()});
    },
    async delete(k){
      await ensure();
      await db.prepare("DELETE FROM kv WHERE k=?").bind(k).run();
      _rc.set(k,{v:null,ts:Date.now()});
    },
    migrateStep,
    async list({prefix=""}={}){
      await ensure();
      // یه کوئری برای همه‌ی مقادیر؛ get های بعدی از کش می‌خونن (بجای N کوئری)
      const r=await db.prepare("SELECT k,v FROM kv WHERE substr(k,1,?)=?").bind(prefix.length,prefix).all();
      const now=Date.now(),keys=[];
      for(const row of (r.results||[])){_rc.set(row.k,{v:row.v,ts:now});keys.push({name:row.k});}
      if(env.KV&&!(await migDone())){
        try{const kl=await env.KV.list({prefix});const have=new Set(keys.map(x=>x.name));
          for(const k of kl.keys)if(!have.has(k.name))keys.push({name:k.name});}catch(e){}
      }
      return {keys,list_complete:true};
    },
  };
}

let _superAdminOk=false;          // ensureSuperAdmin رو یه‌بار اجرا کن
let _migFinished=false;           // migrateStep رو بعد از تکمیل، دیگه صدا نزن (صرفه‌جویی ساب‌ریکوئست/CPU در پلن رایگان)
const _subCache=new Map();        // کش اشتراک‌ها: subKey → {val,ts,dirty,bytes}
const _SUB_TTL=30000;             // اعتبار کش: ۳۰ ثانیه
const _FLUSH_MS=300000;            // هر ۵ دقیقه flush (پلن رایگان D1 توی هر invocation فقط ۵۰ کوئری میده)

// ─── KV KEY LAYOUT ───────────────────────────────────────────────────────
// admin:<username>          → { username, password, created_at, is_super }
// sub:<adminUsername>:<name>→ subscription row, scoped per-admin
const AP = "admin:";
const SP = "sub:";

const kvGetAdmin  = (kv,u)   => kv.get(`${AP}${u}`,"json");
const kvPutAdmin  = (kv,u,d) => kv.put(`${AP}${u}`,JSON.stringify(d));
const kvDelAdmin  = (kv,u)   => kv.delete(`${AP}${u}`);
async function kvListAdmins(kv){
  const {keys}=await kv.list({prefix:AP});
  const all=await Promise.all(keys.map(k=>kv.get(k.name,"json")));
  return all.filter(Boolean).sort((a,b)=>a.created_at-b.created_at);
}

const subKey = (owner,name)=>`${SP}${owner}:::${name}`;
const kvGet  = (kv,owner,n)   => kv.get(subKey(owner,n),"json");
const kvPut  = (kv,owner,n,d) => kv.put(subKey(owner,n),JSON.stringify(d));
const kvDel  = (kv,owner,n)   => kv.delete(subKey(owner,n));
async function kvList(kv,owner){
  const {keys}=await kv.list({prefix:`${SP}${owner}:::`});
  const all=await Promise.all(keys.map(k=>kv.get(k.name,"json")));
  return all.filter(Boolean).sort((a,b)=>b.created_at-a.created_at);
}
// قبل از رندر داشبورد، برای هر اشتراک VLESS/Trojan که هنوز توکن ساب‌لینک
// کوتاه نداره یکی بساز — تا دکمه‌ی «لینک کانفیگ‌ها» توی جدول همیشه آماده باشه
async function ensureAllSubToks(kv,owner,subs){
  for(const row of subs){
    if(row.v2uuid&&!row.subTok)await v2EnsureSubTok(kv,row,owner,row.name);
  }
  return subs;
}

// ─── کش‌پوش KV برای اشتراک‌های فعال ──────────────────────────────────────
// به‌جای خوندن KV هر ثانیه، نتیجه رو ۳۰ ثانیه در حافظه نگه می‌داریم
function _subCacheKey(o,n){return o+" "+n;}
async function kvGetSub(kv,owner,name){
  const k=_subCacheKey(owner,name);
  const c=_subCache.get(k);
  if(c&&Date.now()-c.ts<_SUB_TTL)return c.val;
  const val=await kvGet(kv,owner,name);
  _subCache.set(k,{val,ts:Date.now(),dirty:false,bytes:0});
  if(_subCache.size>_CACHE_MAX){let n=_subCache.size-_CACHE_MAX/2;for(const [kk,vv] of _subCache){if(!vv.dirty){_subCache.delete(kk);if(--n<=0)break;}}}
  return val;
}
function kvSubAddBytes(owner,name,n){
  const k=_subCacheKey(owner,name);
  const c=_subCache.get(k);
  if(c){c.bytes=(c.bytes||0)+n;c.dirty=true;}
}
async function kvFlushSub(kv,owner,name){
  const k=_subCacheKey(owner,name);
  const c=_subCache.get(k);
  if(!c||!c.dirty||!c.val)return;
  const bytes=c.bytes||0;
  c.bytes=0;c.dirty=false;
  if(bytes>0){
    c.val.used_bytes=(c.val.used_bytes||0)+bytes;
    // ─ مصرف روزانه (سقف ۲۴ساعته): هر ۲۴ ساعت خودکار صفر می‌شه ─
    if(!c.val.dailyResetAt||Date.now()>c.val.dailyResetAt){
      c.val.dailyUsed=0;
      c.val.dailyResetAt=Date.now()+86400000;
    }
    c.val.dailyUsed=(c.val.dailyUsed||0)+bytes;
    // ─ تاریخچه‌ی مصرف روزانه (تقویمی، بر اساس UTC) — برای نمایش «چقدر
    //   کدوم روز مصرف کردیم». حداکثر ۳۰ روز آخر نگه داشته می‌شه ─
    const today=new Date().toISOString().slice(0,10);
    const hist=Array.isArray(c.val.dailyHistory)?c.val.dailyHistory:[];
    if(hist.length&&hist[hist.length-1].d===today){
      hist[hist.length-1].b+=bytes;
    }else{
      hist.push({d:today,b:bytes});
    }
    while(hist.length>30)hist.shift();
    c.val.dailyHistory=hist;
    c.ts=Date.now(); // تازه‌سازی TTL بعد از نوشتن
    await kvPut(kv,owner,name,c.val);
  }
}
// آیا این اشتراک از سقف مصرف روزانه‌ش رد شده؟ (فقط وقتی dailyLimitGb ست شده)
function isDailyCapped(row){
  if(!row.dailyLimitGb||row.dailyLimitGb<=0)return false;
  if(!row.dailyResetAt||Date.now()>row.dailyResetAt)return false; // روز جدید شروع شده، هنوز مصرفی ثبت نشده
  return (row.dailyUsed||0)>=row.dailyLimitGb*1073741824;
}

// ─── Bootstrap: اطمینان از وجود سوپرادمین Epic ──────────────────────────
async function ensureSuperAdmin(kv){
  if(_superAdminOk)return; // قبلاً چک شده، نیازی به KV نیست
  try{
    const existing=await kvGetAdmin(kv,SUPERADMIN_USER);
    if(!existing)await kvPutAdmin(kv,SUPERADMIN_USER,{
      username:SUPERADMIN_USER,password:SUPERADMIN_PASS,
      created_at:Date.now(),is_super:true,
    });
    _superAdminOk=true;
  }catch(e){}
}

// ─── Auth (per-admin session) ──────────────────────────────────────────
// کوکی به صورت "username:password" ذخیره میشه و در هر درخواست با KV چک میشه
const getCookie=(req,n)=>{const m=(req.headers.get("Cookie")||"").match(new RegExp(`(?:^|;\\s*)${n}=([^;]*)`));return m?decodeURIComponent(m[1]):null};
async function currentAdmin(req,kv){
  const raw=getCookie(req,"ep_auth");
  if(!raw)return null;
  const idx=raw.indexOf(":");
  if(idx<0)return null;
  const u=raw.slice(0,idx),p=raw.slice(idx+1);
  const admin=await kvGetAdmin(kv,u);
  if(!admin||admin.password!==p)return null;
  return admin;
}
// آیا پنل این ادمین منقضی شده؟ سوپرادمین هیچوقت منقضی نمیشه.
function isAdminExpired(admin){
  if(admin.is_super)return false;
  if(!admin.expire_days)return false; // بدون محدودیت روز
  return Date.now() > admin.created_at + admin.expire_days*86400000;
}
function adminDaysLeft(admin){
  if(admin.is_super||!admin.expire_days)return null;
  return Math.max(0,Math.floor((admin.created_at+admin.expire_days*86400000-Date.now())/86400000));
}
const mkAuthCookie=(u,p,d=7)=>`ep_auth=${encodeURIComponent(u+":"+p)}; Path=/; HttpOnly; SameSite=Strict; Max-Age=${d*86400}`;
const clrAuthCookie=()=>`ep_auth=; Path=/; HttpOnly; SameSite=Strict; Max-Age=-1`;
const redir=(loc,h={})=>new Response(null,{status:302,headers:{Location:loc,...h}});
const page=(b,s=200)=>new Response(b,{status:s,headers:{"Content-Type":"text/html;charset=utf-8"}});

// ─── fetch با تایم‌اوت اجباری ───────────────────────────────────────────
// یه fetch معمولی اگه سرور مقابل (WARP API، تلگرام و…) گیر کنه/جواب نده،
// کل درخواست رو معلق نگه می‌داره و Cloudflare با خطای 1101
// ("never generate a response") کل Worker رو می‌ندازه. این wrapper با
// AbortController یه سقف زمانی می‌ذاره تا این اتفاق هیچ‌وقت نیفته.
async function fetchT(url,init={},timeoutMs=10000){
  const controller=new AbortController();
  const timer=setTimeout(()=>controller.abort(),Math.max(1,timeoutMs));
  try{
    return await fetch(url,{...init,signal:controller.signal});
  }finally{
    clearTimeout(timer);
  }
}


// ═══════════════════════════════════════════════════════════════════════════
//  V2RAY ENGINE — VLESS + Trojan روی WebSocket (وی‌توری) ────────────────────
//  هر اشتراک یه protocol داره: "vless" | "trojan" | "all" (هر دو)
// ═══════════════════════════════════════════════════════════════════════════

const V2P = "v2settings"; // KV key تنظیمات سراسری وی‌توری (فقط سوپرادمین)
const HTTPS_PORTS = [443,8443,2053,2083,2087,2096];
const HTTP_PORTS  = [80,8080,2052,2082,2086,2095];

const LOGO_SVG=`<svg xmlns="http://www.w3.org/2000/svg" viewBox="60 60 1115 1115" width="512" height="512" role="img" aria-label="Epic"><defs><linearGradient id="p0" x1="0" y1="0" x2="0" y2="1"><stop offset="0" stop-color="#c443f1"/><stop offset="1" stop-color="#6b11f6"/></linearGradient><linearGradient id="p2" x1="0" y1="0" x2="0" y2="1"><stop offset="0" stop-color="#ab2af7"/><stop offset="1" stop-color="#5f0bee"/></linearGradient><linearGradient id="p3" x1="0" y1="0" x2="0" y2="1"><stop offset="0" stop-color="#8016e3"/><stop offset="1" stop-color="#7a13e7"/></linearGradient><linearGradient id="p4" x1="0" y1="0" x2="0" y2="1"><stop offset="0" stop-color="#9459f6"/><stop offset="1" stop-color="#813be1"/></linearGradient><linearGradient id="w" x1="0" y1="0" x2="0" y2="1"><stop offset="0" stop-color="#ffffff"/><stop offset="1" stop-color="#dcd8ee"/></linearGradient><radialGradient id="bg" cx=".5" cy=".5" r=".5"><stop offset="0" stop-color="#0c0916"/><stop offset="1" stop-color="#050309"/></radialGradient><filter id="gl" x="-10%" y="-10%" width="120%" height="120%"><feGaussianBlur stdDeviation="9"/></filter></defs><circle cx="617.5" cy="614.5" r="544.0" fill="url(#bg)"/><circle cx="617.5" cy="614.5" r="536.0" fill="none" stroke="#7b2cff" stroke-width="16" opacity=".55" filter="url(#gl)"/><circle cx="617.5" cy="614.5" r="536.0" fill="none" stroke="#7b2cff" stroke-width="13"/><path fill="url(#w)" fill-rule="evenodd" d="M760,907 767,920 776,925 787,927 885,927 942,892 820,892 816,890 814,880 831,835 835,830 846,826 926,826 962,795 837,795 826,797 806,805 792,816 782,832 761,889ZM472,926 521,927 540,879 543,876 645,876 666,868 679,856 689,835 694,819 691,805 682,797 674,795 530,795 501,825 502,826 634,826 634,835 627,846 502,846ZM501,795 360,795 353,798 313,828 275,927 328,898 332,898 353,921 352,926 343,928 431,927 465,896 336,896 335,889 341,877 424,877 464,845 354,846 353,841 358,829 361,826 470,826ZM774,794 721,795 672,927 726,926ZM917,368 714,366 502,367 367,470 411,460 418,461 418,467 291,743 523,744 666,652 455,651 454,646 482,590 682,590 766,538 791,521 802,511 801,508 524,507 523,501 547,453 810,452 823,446 910,376ZM626,156 612,184 621,168 627,166 637,176 647,194 641,180Z"/><path fill="url(#p0)" fill-rule="evenodd" d="M814,210 728,254 727,256 735,268 735,277 726,290 713,295 678,291 670,288 660,279 658,272 660,262 676,244 649,193 646,180 638,170 636,163 629,157 648,194 645,193 636,176 625,166 618,171 624,157 577,243 592,259 594,274 588,283 569,292 539,295 529,291 521,283 518,274 519,266 526,254 439,211 507,334 512,338 554,325 613,317 678,319 715,325 743,333ZM618,171 618,177 612,185 611,182Z"/><path fill="url(#p2)" fill-rule="evenodd" d="M899,581 789,581 596,713 546,750 666,731 880,595Z"/><path fill="url(#p3)" fill-rule="evenodd" d="M950,896 901,927 933,927Z"/><path fill="url(#p4)" fill-rule="evenodd" d="M355,924 330,898 280,925 278,928 340,928Z"/></svg>`;
const PI={
 vl:`<svg width="16" height="16" viewBox="0 0 32 32"><defs><linearGradient id="gv" x1="0" y1="0" x2="1" y2="1"><stop offset="0" stop-color="#7c5cff"/><stop offset="1" stop-color="#22e0ff"/></linearGradient></defs><rect x="2" y="2" width="28" height="28" rx="8" fill="url(#gv)"/><path d="M8 9h5l3 9 3-9h5l-6 15h-4z" fill="#fff"/></svg>`,
 tj:`<svg width="16" height="16" viewBox="0 0 32 32"><defs><linearGradient id="gt" x1="0" y1="0" x2="1" y2="1"><stop offset="0" stop-color="#ff4d8d"/><stop offset="1" stop-color="#c026d3"/></linearGradient></defs><rect x="2" y="2" width="28" height="28" rx="8" fill="url(#gt)"/><path d="M7 12c3-3 15-3 18 0v5c0 4-4 7-9 7s-9-3-9-7z" fill="#fff"/><ellipse cx="12" cy="16" rx="2.4" ry="1.6" fill="#c026d3"/><ellipse cx="20" cy="16" rx="2.4" ry="1.6" fill="#c026d3"/></svg>`
};
const NATURALCSS=`
:root:not([data-theme=light]){--bg:#0d0d12;--s1:#14141b;--s2:#1a1a23;--s3:#22222d;--bd:rgba(255,255,255,.08);--bdh:rgba(255,255,255,.15);--in:#8b5cf6;--vi:#a78bfa;--pk:#a78bfa;--cy:#a78bfa;--gi:linear-gradient(135deg,#8b5cf6,#8b5cf6);--gi2:linear-gradient(135deg,#8b5cf6,#8b5cf6);--sh:none;--sh-glow:none;--r:12px}
body::before,body::after{display:none!important}
.nav{background:var(--s1);-webkit-backdrop-filter:none;backdrop-filter:none;padding:0 18px}
.nav::after{display:none}
.nav-logo-img{width:36px;height:36px;border-radius:50%;flex-shrink:0;display:block}
.nav-name{background:none;-webkit-background-clip:border-box;background-clip:border-box;color:var(--t1);-webkit-text-fill-color:currentColor;font-weight:800}
.status-pill{background:transparent}
.card,.sc,.refresh-bar{box-shadow:none;border-radius:14px}
[data-theme=light] .card{box-shadow:0 1px 3px rgba(20,20,40,.08)}
.card::before,.sc::before{display:none}
.sc:hover{transform:none;box-shadow:none}
.card-t{color:var(--t2)}
.ii,.ig,.ic,.ir,.ia,.nav-logo{box-shadow:none;animation:none}
.btn.bp,.pg-sm .btn.bp{background:var(--in);box-shadow:none;color:#fff}
.btn.bp:hover{transform:none;box-shadow:none;filter:brightness(1.1)}
.glow-card{box-shadow:none!important}
.login-logo{display:block;margin:0 auto 6px;border-radius:50%}
.l-ic-wrap{display:flex;justify-content:center}
.sub-logo{display:block;margin:0 auto 14px;border-radius:50%}
.sub-hero{padding-top:26px}
.proto-hero{background:var(--s1);border:1px solid var(--bd)}
.proto-ic{box-shadow:none;background:var(--s2);color:var(--pc)}
.proto-ic svg{width:30px;height:30px}
.st-c{border-radius:14px}.st-c::after{display:none}
.st-p{border:1px solid var(--bd)}
.st-b i{background:var(--in)}
.grp-chip:has(input:checked){background:color-mix(in srgb,var(--in) 16%,transparent)}
.dock{background:var(--s1);-webkit-backdrop-filter:none;backdrop-filter:none;border-radius:18px;box-shadow:0 4px 18px rgba(0,0,0,.45)}
[data-theme=light] .dock{background:#fff}
.dk.on::before{background:var(--in);box-shadow:none;opacity:1;border-radius:14px}
`;
const LAYOUTCSS=`
.proto-pick{display:flex;align-items:center;gap:8px;min-height:42px;padding:8px 14px;border-radius:12px;background:var(--s2);border:1px solid var(--bd);flex-wrap:wrap}

.card{border-radius:22px;box-shadow:0 12px 34px rgba(0,0,0,.28)}
[data-theme=light] .card{box-shadow:0 10px 28px rgba(40,50,110,.10)}
.sc{border-radius:22px}
.pg{padding:20px 14px 44px}
.card-hd{padding:14px 18px}
.card-t{font-size:12px;letter-spacing:0;text-transform:none;color:var(--t2);font-weight:800}
.refresh-bar{border-radius:16px;background:transparent;border-style:dashed}
input,select{border-radius:12px}
.btn{border-radius:12px}
.dtab tbody tr:hover td{background:color-mix(in srgb,var(--in) 5%,transparent)}
@media(max-width:820px){
 .nav{padding:0 14px}
 .pg{padding:14px 12px 24px}
 .stats{gap:10px}
 .sc{padding:14px}
 .sc-v{font-size:22px}
 .tbw{overflow:visible;padding:10px}
 .dtab,.dtab tbody{display:block}
 .dtab thead{display:none}
 .dtab tbody tr{display:grid;grid-template-columns:1fr auto;grid-template-areas:"n s" "u u" "e a";gap:12px;margin:0 0 12px;padding:14px;border:1px solid var(--bd);border-radius:20px;background:var(--s2)}
 .dtab tbody tr:last-child{margin-bottom:0}
 .dtab tbody tr td{display:block;padding:0;border:0;min-width:0!important;background:none!important}
 .dtab td:nth-child(1){grid-area:n}
 .dtab td:nth-child(2){grid-area:s;justify-self:end}
 .dtab td:nth-child(3){grid-area:u}
 .dtab td:nth-child(4){grid-area:e;align-self:center}
 .dtab td:nth-child(5){grid-area:a;justify-self:end}
}
`;
const DOCKCSS=`
.dock{display:none}
@media (max-width:820px){
 body{padding-bottom:84px}
 .ham-hide{display:none!important}
 .dock{display:flex;position:fixed;z-index:550;left:12px;right:12px;bottom:max(12px,env(safe-area-inset-bottom));height:66px;padding:0 8px;justify-content:space-around;align-items:center;border-radius:24px;background:rgba(19,22,43,.82);-webkit-backdrop-filter:blur(18px);backdrop-filter:blur(18px);border:1px solid rgba(255,255,255,.12);box-shadow:0 14px 40px rgba(0,0,0,.55),0 0 0 1px color-mix(in srgb,var(--in) 15%,transparent)}
 [data-theme=light] .dock{background:rgba(255,255,255,.9)}
 .dk{flex:1;min-width:0;display:flex;flex-direction:column;align-items:center;gap:3px;color:var(--t3);font-size:10px;font-weight:700;text-decoration:none;position:relative;padding:6px 0;border:0;background:none;font-family:inherit;cursor:pointer;transition:transform .2s}
 .dk svg{width:22px;height:22px}
 .dk:active{transform:scale(.88)}
 .dk.on{color:#fff}
 .dk.on::before{content:'';position:absolute;top:-2px;width:44px;height:44px;border-radius:16px;background:linear-gradient(135deg,var(--in),var(--vi));opacity:.9;z-index:-1;box-shadow:0 8px 20px color-mix(in srgb,var(--vi) 45%,transparent);transform:translateY(-4px)}
 .dk.on svg{transform:translateY(-4px)}
 .dk.on span{opacity:0}
}
`;
let _V2GROUPS=[];
const _he=s=>String(s).replace(/&/g,"&amp;").replace(/"/g,"&quot;").replace(/</g,"&lt;");
function v2Groups(v2s){return [...new Set(((v2s&&v2s.ips)||[]).map(e=>((e.group||"")+"").trim()||"عمومی"))];}
function v2PickIps(ips,row){const g=row&&row.v2groups;if(!Array.isArray(g)||!g.length)return ips;const f=ips.filter(e=>g.includes(((e.group||"")+"").trim()||"عمومی"));return f.length?f:ips;}
const CSSX=`
.hero-fx{position:relative;text-align:center;padding:34px 12px 22px;margin-bottom:6px;overflow:hidden;border-radius:22px}
.hero-orb{position:absolute;width:220px;height:220px;border-radius:50%;filter:blur(60px);opacity:.45;pointer-events:none;animation:orb 9s ease-in-out infinite alternate}
.hero-orb.o1{background:var(--in);top:-80px;right:-60px}.hero-orb.o2{background:var(--vi);bottom:-100px;left:-60px;animation-delay:-4s}
@keyframes orb{to{transform:translate(30px,24px) scale(1.2)}}
.hero-emo{font-size:58px;line-height:1;position:relative;animation:bob 3s ease-in-out infinite;filter:drop-shadow(0 8px 24px color-mix(in srgb,var(--in) 70%,transparent))}
@keyframes bob{50%{transform:translateY(-8px) rotate(-4deg)}}
.hero-t{position:relative;margin:10px 0 4px;font-size:25px;font-weight:900;background:linear-gradient(90deg,var(--in),var(--vi),var(--cy),var(--in));background-size:300% 100%;-webkit-background-clip:text;background-clip:text;color:transparent;animation:shine 6s linear infinite}
@keyframes shine{to{background-position:300% 0}}
.hero-s{position:relative;margin:0;font-size:13px;color:var(--t2)}
.float-emo span{position:absolute;bottom:-20px;font-size:20px;opacity:0;animation:rise 7s linear infinite}
.float-emo span:nth-child(1){left:8%;animation-delay:0s}.float-emo span:nth-child(2){left:24%;animation-delay:1.3s}.float-emo span:nth-child(3){left:42%;animation-delay:2.6s}
.float-emo span:nth-child(4){left:60%;animation-delay:.8s}.float-emo span:nth-child(5){left:76%;animation-delay:3.4s}.float-emo span:nth-child(6){left:90%;animation-delay:2s}
@keyframes rise{0%{transform:translateY(0) scale(.6);opacity:0}15%{opacity:.9}100%{transform:translateY(-200px) scale(1.1);opacity:0}}
.pg-sm .card{background:linear-gradient(160deg,color-mix(in srgb,var(--in) 10%,transparent),color-mix(in srgb,var(--vi) 5%,transparent) 60%,color-mix(in srgb,var(--cy) 6%,transparent));border:1px solid rgba(255,255,255,.1);box-shadow:0 12px 40px rgba(0,0,0,.45),inset 0 1px 0 rgba(255,255,255,.07)}
.pg-sm .btn.bp,.btn.bp{background:linear-gradient(120deg,var(--in),var(--vi));box-shadow:0 8px 24px color-mix(in srgb,var(--vi) 35%,transparent)}
.grp-box{margin:0 0 12px;padding:12px;border:1px dashed var(--bdh);border-radius:12px}
.grp-hd{display:flex;align-items:center;justify-content:space-between;gap:8px;font-size:13px;font-weight:700;margin-bottom:10px}
.grp-all{display:flex;align-items:center;gap:6px;font-size:12px;font-weight:600;color:var(--cy);cursor:pointer}
.grp-list{display:flex;flex-wrap:wrap;gap:8px}
.grp-chip{display:flex;align-items:center;gap:6px;padding:7px 12px;border-radius:999px;background:var(--s2);border:1px solid var(--bd);font-size:12px;cursor:pointer}
.grp-chip:has(input:checked){border-color:var(--in);background:color-mix(in srgb,var(--in) 18%,transparent)}
.grp-chip input,.grp-all input{accent-color:var(--in);width:15px;height:15px}
@media (prefers-reduced-motion:reduce){.hero-orb,.hero-emo,.hero-t,.float-emo span{animation:none}}
`;
async function v2GetSettings(kv){
  const s=(await kv.get(V2P,"json"))||{};
  const allP=[...HTTPS_PORTS,...HTTP_PORTS];
  let ips=Array.isArray(s.ips)?s.ips.filter(e=>e&&typeof e==="object"):[];
  if(ips.length===0){
    // سازگاری با فرمت قدیم: cleanIp + ports → یه ردیف به‌ازای هر پورت
    const oldPorts=Array.isArray(s.ports)?s.ports.filter(p=>allP.includes(p)):[443];
    ips=oldPorts.map(p=>({cleanIp:s.cleanIp||"",proxyIp:"",name:"",port:p}));
    if(ips.length===0)ips=[{cleanIp:"",proxyIp:"",name:"",port:443}];
  }
  ips=ips.map(e=>({...e,port:allP.includes(Number(e.port))?Number(e.port):443}));
  return {ips};
}
async function v2PutSettings(kv,s){return kv.put(V2P,JSON.stringify(s));}
// ─── ایندکس معکوس UUID/پسورد → owner+name — چون کلاینت‌های VLESS/Trojan
//     اتصال WebSocket رو با یه مسیر ثابت/عمومی میزنن (نه مسیر اختصاصیِ هر
//     یوزر)، باید بشه فقط از روی UUID یا هش پسورد فهمید این اتصال مال کیه
const v2IdxUuidKey=(uuid)=>`v2u:${uuid}`;
const v2IdxPassKey=(hash)=>`v2t:${hash}`;
async function v2IndexPut(kv,row,owner,name){
  if(row.v2uuid)await kv.put(v2IdxUuidKey(row.v2uuid),JSON.stringify({owner,name}));
  if(row.v2passHash)await kv.put(v2IdxPassKey(row.v2passHash),JSON.stringify({owner,name}));
}
async function v2IndexDel(kv,row){
  if(row?.v2uuid)await kv.delete(v2IdxUuidKey(row.v2uuid));
  if(row?.v2passHash)await kv.delete(v2IdxPassKey(row.v2passHash));
}
// ─── توکن کوتاه ساب‌اسکریپشن — به‌جای /v2sub/owner/name (که با یوزرنیم/اسم
//     فارسی و اینکود شدنشون توی بعضی کلاینت‌ها/اپ‌های پیام‌رسان درست پیست
//     نمیشه)، یه لینک کوتاه و کاملاً ASCII میسازیم که فقط یه توکن تصادفیه
const v2IdxSubTokKey=(tok)=>`v2s:${tok}`;
function v2RandTok(){return v2RandStr(10,10);}
async function v2EnsureSubTok(kv,row,owner,name){
  if(row.subTok)return row.subTok;
  let tok;
  for(let tries=0;tries<5;tries++){
    tok=v2RandTok();
    const exists=await kv.get(v2IdxSubTokKey(tok));
    if(!exists)break;
  }
  row.subTok=tok;
  await kv.put(v2IdxSubTokKey(tok),JSON.stringify({owner,name}));
  await kvPut(kv,owner,name,row);
  return tok;
}
function v2PortCategory(port){return HTTPS_PORTS.includes(port)?"https":"http";}

// ─── SHA-224 (خالص جاوااسکریپت — Web Crypto از SHA-224 پشتیبانی نمیکنه؛
//     پروتکل Trojan برای هش پسورد دقیقاً به همین الگوریتم نیاز داره) ────────
function sha224Hex(str){
  const K=[0x428a2f98,0x71374491,0xb5c0fbcf,0xe9b5dba5,0x3956c25b,0x59f111f1,0x923f82a4,0xab1c5ed5,
    0xd807aa98,0x12835b01,0x243185be,0x550c7dc3,0x72be5d74,0x80deb1fe,0x9bdc06a7,0xc19bf174,
    0xe49b69c1,0xefbe4786,0x0fc19dc6,0x240ca1cc,0x2de92c6f,0x4a7484aa,0x5cb0a9dc,0x76f988da,
    0x983e5152,0xa831c66d,0xb00327c8,0xbf597fc7,0xc6e00bf3,0xd5a79147,0x06ca6351,0x14292967,
    0x27b70a85,0x2e1b2138,0x4d2c6dfc,0x53380d13,0x650a7354,0x766a0abb,0x81c2c92e,0x92722c85,
    0xa2bfe8a1,0xa81a664b,0xc24b8b70,0xc76c51a3,0xd192e819,0xd6990624,0xf40e3585,0x106aa070,
    0x19a4c116,0x1e376c08,0x2748774c,0x34b0bcb5,0x391c0cb3,0x4ed8aa4a,0x5b9cca4f,0x682e6ff3,
    0x748f82ee,0x78a5636f,0x84c87814,0x8cc70208,0x90befffa,0xa4506ceb,0xbef9a3f7,0xc67178f2];
  let h=[0xc1059ed8,0x367cd507,0x3070dd17,0xf70e5939,0xffc00b31,0x68581511,0x64f98fa7,0xbefa4fa4];
  const bytes=new TextEncoder().encode(str);
  const bitLen=bytes.length*8;
  const padLen=((bytes.length+9+63)>>6)<<6;
  const buf=new Uint8Array(padLen);
  buf.set(bytes);buf[bytes.length]=0x80;
  const dv=new DataView(buf.buffer);
  dv.setUint32(padLen-4,bitLen>>>0,false);
  dv.setUint32(padLen-8,Math.floor(bitLen/4294967296),false);
  const rotr=(x,n)=>(x>>>n)|(x<<(32-n));
  for(let off=0;off<padLen;off+=64){
    const w=new Uint32Array(64);
    for(let i=0;i<16;i++)w[i]=dv.getUint32(off+i*4,false);
    for(let i=16;i<64;i++){
      const s0=rotr(w[i-15],7)^rotr(w[i-15],18)^(w[i-15]>>>3);
      const s1=rotr(w[i-2],17)^rotr(w[i-2],19)^(w[i-2]>>>10);
      w[i]=(w[i-16]+s0+w[i-7]+s1)|0;
    }
    let [a,b,c,d,e,f,g,hh]=h;
    for(let i=0;i<64;i++){
      const S1=rotr(e,6)^rotr(e,11)^rotr(e,25);
      const ch=(e&f)^((~e)&g);
      const t1=(hh+S1+ch+K[i]+w[i])|0;
      const S0=rotr(a,2)^rotr(a,13)^rotr(a,22);
      const maj=(a&b)^(a&c)^(b&c);
      const t2=(S0+maj)|0;
      hh=g;g=f;f=e;e=(d+t1)|0;d=c;c=b;b=a;a=(t1+t2)|0;
    }
    h[0]=(h[0]+a)|0;h[1]=(h[1]+b)|0;h[2]=(h[2]+c)|0;h[3]=(h[3]+d)|0;
    h[4]=(h[4]+e)|0;h[5]=(h[5]+f)|0;h[6]=(h[6]+g)|0;h[7]=(h[7]+hh)|0;
  }
  return h.slice(0,7).map(x=>(x>>>0).toString(16).padStart(8,'0')).join('');
}

function v2NewCreds(){
  return {v2uuid:crypto.randomUUID(),v2pass:crypto.randomUUID().replace(/-/g,"")};
}

function base64ToBytes(b64){
  b64=(b64||"").replace(/-/g,"+").replace(/_/g,"/");
  try{const bin=atob(b64);const arr=new Uint8Array(bin.length);for(let i=0;i<bin.length;i++)arr[i]=bin.charCodeAt(i);return arr;}
  catch(e){return new Uint8Array(0);}
}
function safeCloseWS(ws){try{if(ws.readyState===1||ws.readyState===2)ws.close();}catch(e){}}


// ─── پارس هدر VLESS ──────────────────────────────────────────────────────
function parseVlessHeader(buf,expectedUUID){
  if(buf.byteLength<24)return{hasError:true,message:"داده ناقص"};
  const view=new DataView(buf);
  const version=new Uint8Array(buf.slice(0,1));
  const uuidBytes=new Uint8Array(buf.slice(1,17));
  const uuid=[...uuidBytes].map(b=>b.toString(16).padStart(2,"0")).join("");
  const uuidFmt=`${uuid.slice(0,8)}-${uuid.slice(8,12)}-${uuid.slice(12,16)}-${uuid.slice(16,20)}-${uuid.slice(20)}`;
  if(uuidFmt!==expectedUUID)return{hasError:true,message:"UUID نامعتبر"};
  const optLen=new Uint8Array(buf.slice(17,18))[0];
  let pos=18+optLen;
  const cmd=new Uint8Array(buf.slice(pos,pos+1))[0]; pos+=1; // 1=tcp 2=udp
  const port=view.getUint16(pos,false); pos+=2;
  const atype=new Uint8Array(buf.slice(pos,pos+1))[0]; pos+=1;
  let addr="";
  if(atype===1){ // IPv4
    addr=new Uint8Array(buf.slice(pos,pos+4)).join("."); pos+=4;
  }else if(atype===2){ // domain
    const len=new Uint8Array(buf.slice(pos,pos+1))[0]; pos+=1;
    addr=new TextDecoder().decode(buf.slice(pos,pos+len)); pos+=len;
  }else if(atype===3){ // IPv6
    const ipBytes=new Uint8Array(buf.slice(pos,pos+16)); pos+=16;
    const parts=[];for(let i=0;i<16;i+=2)parts.push(((ipBytes[i]<<8)|ipBytes[i+1]).toString(16));
    addr=parts.join(":");
  }else{
    return{hasError:true,message:"نوع آدرس نامعتبر"};
  }
  return{hasError:false,addressRemote:addr,portRemote:port,rawDataIndex:pos,isUDP:cmd===2,respHeader:new Uint8Array([version[0],0])};
}

// ─── پارس هدر Trojan ─────────────────────────────────────────────────────
function parseTrojanHeader(buf,expectedPassHashHex){
  if(buf.byteLength<58)return{hasError:true,message:"داده ناقص"};
  const head=new TextDecoder().decode(buf.slice(0,56));
  if(head.toLowerCase()!==expectedPassHashHex.toLowerCase())return{hasError:true,message:"پسورد نامعتبر"};
  let pos=58; // 56 hex + CRLF
  const view=new DataView(buf);
  const cmd=new Uint8Array(buf.slice(pos,pos+1))[0]; pos+=1;
  const atype=new Uint8Array(buf.slice(pos,pos+1))[0]; pos+=1;
  let addr="";
  if(atype===1){addr=new Uint8Array(buf.slice(pos,pos+4)).join("."); pos+=4;}
  else if(atype===3){
    const len=new Uint8Array(buf.slice(pos,pos+1))[0]; pos+=1;
    addr=new TextDecoder().decode(buf.slice(pos,pos+len)); pos+=len;
  }else if(atype===4){
    const ipBytes=new Uint8Array(buf.slice(pos,pos+16)); pos+=16;
    const parts=[];for(let i=0;i<16;i+=2)parts.push(((ipBytes[i]<<8)|ipBytes[i+1]).toString(16));
    addr=parts.join(":");
  }else{
    return{hasError:true,message:"نوع آدرس نامعتبر"};
  }
  const port=view.getUint16(pos,false); pos+=2;
  pos+=2; // CRLF بعد از پورت
  return{hasError:false,addressRemote:addr,portRemote:port,rawDataIndex:pos,isUDP:cmd===3,respHeader:null};
}


// ─── هندلر اصلی اتصال WebSocket برای اشتراک‌های VLESS/Trojan ────────────────
// نکته‌ی مهم معماری: کلاینت‌های VLESS/Trojan اتصال WebSocket رو با یه مسیر
// عمومی/ثابت میزنن (نه یه مسیر اختصاصی به‌ازای هر یوزر) — دقیقاً مثل نسخه‌ی
// قبلاً تست‌شده و کارکرده. پس نمیشه از رو مسیر URL فهمید این اتصال مال کدوم
// اشتراکه؛ باید از رو محتوای خودِ پروتکل (UUID برای VLESS، هش پسورد برای
// Trojan) با یه ایندکس معکوس توی KV پیدا بشه کدوم اشتراکه.
// خروجی: {response, done, kill} — done وقتی resolve میشه که اتصال بسته بشه
// kill() برای قطع فوری اتصال (وقتی ادمین دیسیبل/حذفش کرد وسط اتصال)

// ─── مهلت زمانی + failover اتصال (برگرفته از موتور پنل Nahan که خوب جواب می‌داد) ───
const V2_OPEN_TIMEOUT=5000, V2_WRITE_TIMEOUT=8000;
// رله‌ی پیش‌فرض نداریم؛ فقط proxyIP خود کانفیگ استفاده میشه
const V2_DEFAULT_RELAYS="";
function v2Deadline(p,ms,onTimeout){
  let t;
  const tp=new Promise((_,rej)=>{t=setTimeout(()=>{try{onTimeout&&onTimeout();}catch(e){}rej(new Error("timeout"));},ms);});
  return Promise.race([p,tp]).finally(()=>clearTimeout(t));
}
function v2ParseRelays(str){
  return String(str||"").split(/[\r\n,;|]+/).map(x=>x.trim()).filter(Boolean).map(x=>{
    let host=x,port=0;
    const m=x.match(/^\[([^\]]+)\](?::(\d+))?$/);
    if(m){host=m[1];port=parseInt(m[2])||0;}
    else{const ci=x.lastIndexOf(":");if(ci>0&&x.indexOf(":")===ci){host=x.slice(0,ci);port=parseInt(x.slice(ci+1))||0;}}
    return {host,port};
  });
}
function handleV2Socket(request,kv,onUsage,onMatched){
  const pair=new WebSocketPair();
  const client=pair[0],ws=pair[1];
  ws.accept();
  ws.binaryType="arraybuffer";

  // proxyip از query string مسیر WebSocket (e.g. ?ed=2560&proxyip=1.2.3.4:8080)
  let _proxyRaw="";
  try{
    const rUrl=new URL(request.url);
    _proxyRaw=(rUrl.searchParams.get("proxyip")||"").trim();
  }catch(e){}

  let remoteSocket=null,dataWriter=null,isInit=true,queue=Promise.resolve();
  let resolveDone;
  const done=new Promise(r=>{resolveDone=r;});
  const finish=()=>{
    if(remoteSocket)try{remoteSocket.close();}catch(e){}
    resolveDone();
  };
  ws.addEventListener("close",finish);
  ws.addEventListener("error",finish);

  // از رو بایت اول تشخیص میدیم VLESS‌ه یا Trojan: بایت نسخه‌ی VLESS همیشه
  // 0x00 هست؛ Trojan با ۵۶ کاراکتر hex متنی (SHA224 پسورد) شروع میشه که
  // بایت اولش هیچوقت 0x00 نیست (کاراکتر ASCII یه رقم/حرف hex‌ه)

  async function v2ScanFind(field,val){
    // جلوگیری از اسکن کامل دیتابیس به ازای هر UUID/پسورد الکی: حداکثر هر ۶۰ ثانیه یه بار
    if(Date.now()-_scanAt<60000)return null;
    _scanAt=Date.now();
    const {keys}=await kv.list({prefix:SP});
    for(const k of keys){const r=await kv.get(k.name,"json");
      if(r&&r[field]===val){const i=k.name.slice(SP.length).split(":::");return {owner:i[0],name:i.slice(1).join(":::")};}}
    return null;
  }
  async function resolveRowAndHeader(bufferData){
    const view=new Uint8Array(bufferData);
    if(view.byteLength<1)return null;
    if(view[0]===0x00){
      if(view.byteLength<17)return null;
      const uuidBytes=view.slice(1,17);
      const hex=[...uuidBytes].map(b=>b.toString(16).padStart(2,"0")).join("");
      const uuidFmt=`${hex.slice(0,8)}-${hex.slice(8,12)}-${hex.slice(12,16)}-${hex.slice(16,20)}-${hex.slice(20)}`;
      let idx=await kv.get(v2IdxUuidKey(uuidFmt),"json");
      if(!idx){idx=await v2ScanFind("v2uuid",uuidFmt);if(idx)try{await kv.put(v2IdxUuidKey(uuidFmt),JSON.stringify(idx));}catch(e){}}
      if(!idx)return null;
      const row=await kvGet(kv,idx.owner,idx.name);
      if(!row||!row.v2uuid||row.v2uuid!==uuidFmt)return null;
      const header=parseVlessHeader(bufferData,uuidFmt);
      if(header.hasError)return null;
      return{row,owner:idx.owner,name:idx.name,header};
    }else{
      if(view.byteLength<58)return null;
      const hashHex=new TextDecoder().decode(bufferData.slice(0,56)).toLowerCase();
      let idx=await kv.get(v2IdxPassKey(hashHex),"json");
      if(!idx){idx=await v2ScanFind("v2passHash",hashHex);if(idx)try{await kv.put(v2IdxPassKey(hashHex),JSON.stringify(idx));}catch(e){}}
      if(!idx)return null;
      const row=await kvGet(kv,idx.owner,idx.name);
      if(!row||!row.v2passHash||row.v2passHash!==hashHex)return null;
      const header=parseTrojanHeader(bufferData,hashHex);
      if(header.hasError)return null;
      return{row,owner:idx.owner,name:idx.name,header};
    }
  }

  async function parseAndConnect(bufferData){
    const match=await resolveRowAndHeader(bufferData);
    if(!match||match.row.disabled||isExp(match.row)||isDailyCapped(match.row)){safeCloseWS(ws);return;}
    if(onMatched)onMatched(match.owner,match.name);
    const{addressRemote,portRemote,rawDataIndex,respHeader}=match.header;
    // ─ مسیر سریع: اول همیشه سعی کن مستقیم به مقصد واقعی وصل شی (بدون هیچ
    //   هاپ واسطه) — این همون چیزیه که پنل‌های سریع انجام میدن. proxyIP رو
    //   فقط وقتی امتحان کن که اتصال مستقیم fail بشه (مثلاً چون مقصد خودش
    //   روی رنج IP کلادفلره و Worker اجازه‌ی connect مستقیم بهش رو نداره).
    //   مجبورکردن همیشگی ترافیک از یه IP واسط (که معمولاً مشترک و شلوغه)
    //   دقیقاً همون چیزیه که سرعت رو میاره پایین.
    let connected=false;
    try{
      const sk=connect({hostname:addressRemote,port:portRemote});
      remoteSocket=sk;
      await v2Deadline(sk.opened,V2_OPEN_TIMEOUT,()=>{try{sk.close();}catch(e){}});
      connected=true;
    }catch(e1){}
    if(!connected){
      let relays=v2ParseRelays(_proxyRaw);
      if(!relays.length)relays=v2ParseRelays(V2_DEFAULT_RELAYS);
      // شروع از یه نقطه‌ی ثابت بر اساس اشتراک تا ترافیک یه کاربر بین چند IP پخش نشه
      let h=0;const hs=String(match.owner+match.name);
      for(let i=0;i<hs.length;i++)h=(hs.charCodeAt(i)+((h<<5)-h))|0;
      const start=relays.length>1?Math.abs(h)%relays.length:0;
      // رله‌ای که پشت‌سر‌هم خراب بوده چند دقیقه کنار گذاشته میشه (مگه همه خراب باشن)
      const good=relays.filter(r=>!relayBad(r.host,r.port||portRemote));
      const pool=good.length?good:relays;
      for(let k=0;k<Math.min(pool.length,3)&&!connected;k++){
        const r=pool[(start+k)%pool.length];
        try{
          const sk=connect({hostname:r.host,port:r.port||portRemote});
          remoteSocket=sk;
          await v2Deadline(sk.opened,V2_OPEN_TIMEOUT,()=>{try{sk.close();}catch(e){}});
          connected=true;relayOk(r.host,r.port||portRemote);
        }catch(e2){relayFail(r.host,r.port||portRemote);}
      }
    }
    if(!connected){safeCloseWS(ws);return;}
    dataWriter=remoteSocket.writable.getWriter();
    const rawClientData=bufferData.slice(rawDataIndex);
    if(rawClientData.byteLength>0){
      await v2Deadline(dataWriter.write(new Uint8Array(rawClientData)),V2_WRITE_TIMEOUT,()=>{try{remoteSocket.close();}catch(e){}});
      if(onUsage)onUsage(rawClientData.byteLength||0);
    }
    // پایپ ریموت→کلاینت — عمداً await نمیشه، باید موازی و پس‌زمینه اجرا بشه
    let first=true;
    remoteSocket.readable.pipeTo(new WritableStream({
      write(chunk){
        if(ws.readyState!==1)throw new Error("ws closed");
        if(first&&respHeader){
          const merged=new Uint8Array(respHeader.byteLength+chunk.byteLength);
          merged.set(respHeader,0);merged.set(new Uint8Array(chunk),respHeader.byteLength);
          ws.send(merged);
        }else{
          ws.send(chunk);
        }
        first=false;
        if(onUsage)onUsage(chunk.byteLength||chunk.length||0);
      },
    })).catch(()=>{}).finally(()=>{safeCloseWS(ws);});
  }

  let pendBytes=0,pendItems=0;
  ws.addEventListener("message",(event)=>{
    const sz=(event.data&&event.data.byteLength)||0;
    pendBytes+=sz;pendItems++;
    // کلاینتی که سریع‌تر از مقصد دیتا می‌ریزه نباید حافظه‌ی ورکر رو پر کنه
    if(pendBytes>WS_UP_MAX_BYTES||pendItems>WS_UP_MAX_ITEMS){safeCloseWS(ws);return;}
    queue=queue.then(async()=>{
      pendBytes=Math.max(0,pendBytes-sz);pendItems=Math.max(0,pendItems-1);
      try{
        if(isInit){
          isInit=false;
          await parseAndConnect(event.data);
        }else if(dataWriter){
          await v2Deadline(dataWriter.write(new Uint8Array(event.data)),15000,()=>{try{remoteSocket.close();}catch(e){}});
          if(onUsage)onUsage(event.data.byteLength||0);
        }
      }catch(err){safeCloseWS(ws);}
    });
  });

  // دیتای اولیه (early-data) که ممکنه توی هدر sec-websocket-protocol اومده باشه
  const earlyDataHeader=request.headers.get("sec-websocket-protocol")||"";
  const early=base64ToBytes(earlyDataHeader);
  if(early.length>0){
    queue=queue.then(async()=>{
      try{isInit=false;await parseAndConnect(early.buffer);}
      catch(err){safeCloseWS(ws);}
    });
  }

  const kill=()=>{
    try{safeCloseWS(ws);}catch(e){}
    if(remoteSocket)try{remoteSocket.close();}catch(e){}
  };

  return {response:new Response(null,{status:101,webSocket:client}),done,kill};
}

// ─── ساخت لینک اشتراک‌گذاری VLESS / Trojan ──────────────────────────────

function v2Host(base){return base.replace(/^https?:\/\//,"");}

function v2RandStr(min,max){
  const chars="ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
  const len=Math.floor(Math.random()*(max-min+1))+min;
  let s="";for(let i=0;i<len;i++)s+=chars[Math.floor(Math.random()*62)];
  return s;
}

function v2RandomPath(proto){
  return`/${proto==="vless"?"vl":"tr"}/${v2RandStr(16,32)}`;
}


function buildVlessLinkP(base,owner,name,row,ipEntry,port,index){
  const host=v2Host(base);
  const cat=v2PortCategory(port);
  const wsPath=v2RandomPath("vless");
  const ip=typeof ipEntry==="string"?ipEntry:(ipEntry&&ipEntry.cleanIp)||"";
  const pIp=typeof ipEntry==="object"&&ipEntry&&ipEntry.proxyIp?ipEntry.proxyIp:"";
  const ipName=typeof ipEntry==="object"&&ipEntry&&ipEntry.name?ipEntry.name:"";
  const frag=typeof ipEntry==="object"&&ipEntry&&ipEntry.fragment?"1":"";
  const insecure=typeof ipEntry==="object"&&ipEntry&&ipEntry.allowInsecure?"1":"";
  // بدون ?ed=2560 — کلاینت‌های Xray هدر Sec-WebSocket-Protocol می‌فرستن که CF echo نمی‌کنه
  const pathSuffix=pIp?`?proxyip=${encodeURIComponent(pIp)}`:"";
  const path=encodeURIComponent(`${wsPath}${pathSuffix}`);
  const sec=cat==="https"?"tls":"none";
  const remarkTag=ipName||`${name}-${port}`;
  let url=`vless://${row.v2uuid}@${ip||host}:${port}?encryption=none&security=${sec}&type=ws&host=${host}&path=${path}`;
  if(sec==="tls")url+=`&sni=${host}&fp=chrome&alpn=http%2F1.1`;
  if(insecure)url+=`&allowInsecure=1`;
  // Fragment (ضد فیلتر/DPI) — پارامتر استاندارد کلاینت‌های Xray-core
  // (v2rayNG/NekoBox/Streisand): پکت هندشیک TLS رو تکه‌تکه می‌فرسته
  if(frag)url+=`&fragment=1,40-60,tlshello`;
  return url+`#${encodeURIComponent(remarkTag)}`;
}
function buildTrojanLinkP(base,owner,name,row,ipEntry,port,index){
  const host=v2Host(base);
  const cat=v2PortCategory(port);
  const wsPath=v2RandomPath("trojan");
  const ip=typeof ipEntry==="string"?ipEntry:(ipEntry&&ipEntry.cleanIp)||"";
  const pIp=typeof ipEntry==="object"&&ipEntry&&ipEntry.proxyIp?ipEntry.proxyIp:"";
  const ipName=typeof ipEntry==="object"&&ipEntry&&ipEntry.name?ipEntry.name:"";
  const frag=typeof ipEntry==="object"&&ipEntry&&ipEntry.fragment?"1":"";
  const insecure=typeof ipEntry==="object"&&ipEntry&&ipEntry.allowInsecure?"1":"";
  const pathSuffix=pIp?`?proxyip=${encodeURIComponent(pIp)}`:"";
  const path=encodeURIComponent(`${wsPath}${pathSuffix}`);
  const sec=cat==="https"?"tls":"none";
  const remarkTag=ipName||`${name}-${port}`;
  let url=`trojan://${row.v2pass}@${ip||host}:${port}?type=ws&host=${host}&path=${path}&security=${sec}`;
  if(sec==="tls")url+=`&sni=${host}&fp=chrome&alpn=http%2F1.1`;
  if(insecure)url+=`&allowInsecure=1`;
  if(frag)url+=`&fragment=1,40-60,tlshello`;
  return url+`#${encodeURIComponent(remarkTag)}`;
}
function buildV2LinkP(base,owner,name,row,ipEntry,port,index){
  if(row.protocol==="trojan")return buildTrojanLinkP(base,owner,name,row,ipEntry,port,index);
  return buildVlessLinkP(base,owner,name,row,ipEntry,port,index);
}
// اگه این اشتراک کشور/پروکسی IP اختصاصی داشته باشه، رو ردیف‌های عمومی اعمالش کن
function applyRowOverride(entry,row){
  // نکته: entry ممکنه یه رشته‌ی ساده (IP خام) باشه، نه آبجکت — نباید spread بشه
  const base=typeof entry==="string"?{cleanIp:entry,proxyIp:"",name:"",port:443}:entry;
  if(!row||(!row.proxyIp&&!row.country))return base;
  const e={...base};
  if(row.proxyIp)e.proxyIp=row.proxyIp;
  if(row.country){
    const c=countryOf(row.country);
    if(c.code)e.name=`${c.flag} ${c.name}${base.name?` — ${base.name}`:""}`;
  }
  return e;
}
// همه‌ی لینک‌ها: یه کانفیگ به‌ازای هر ردیف — برای ساب‌اسکریپشن
function buildV2AllLinks(base,owner,name,row,v2s){
  const ips=v2s.ips&&v2s.ips.length?v2s.ips:[{cleanIp:v2s.cleanIp||"",proxyIp:"",name:"",port:443}];
  return v2PickIps(ips,row).map((e,i)=>buildV2LinkP(base,owner,name,row,applyRowOverride(e,row),e.port||443,i+1));
}

// ─── ZIP ───────────────────────────────────────────────────────────────────

// ─── QR CODE (bundled inline — no external CDN, works even if jsdelivr/unpkg
//     are filtered on the client's network) ────────────────────────────────
const QR_LIB_JS = `
//---------------------------------------------------------------------
//
// QR Code Generator for JavaScript
//
// Copyright (c) 2009 Kazuhiko Arase
//
// URL: http://www.d-project.com/
//
// Licensed under the MIT license:
//  http://www.opensource.org/licenses/mit-license.php
//
// The word 'QR Code' is registered trademark of
// DENSO WAVE INCORPORATED
//  http://www.denso-wave.com/qrcode/faqpatent-e.html
//
//---------------------------------------------------------------------

var qrcode = function() {

  //---------------------------------------------------------------------
  // qrcode
  //---------------------------------------------------------------------

  /**
   * qrcode
   * @param typeNumber 1 to 40
   * @param errorCorrectionLevel 'L','M','Q','H'
   */
  var qrcode = function(typeNumber, errorCorrectionLevel) {

    var PAD0 = 0xEC;
    var PAD1 = 0x11;

    var _typeNumber = typeNumber;
    var _errorCorrectionLevel = QRErrorCorrectionLevel[errorCorrectionLevel];
    var _modules = null;
    var _moduleCount = 0;
    var _dataCache = null;
    var _dataList = [];

    var _this = {};

    var makeImpl = function(test, maskPattern) {

      _moduleCount = _typeNumber * 4 + 17;
      _modules = function(moduleCount) {
        var modules = new Array(moduleCount);
        for (var row = 0; row < moduleCount; row += 1) {
          modules[row] = new Array(moduleCount);
          for (var col = 0; col < moduleCount; col += 1) {
            modules[row][col] = null;
          }
        }
        return modules;
      }(_moduleCount);

      setupPositionProbePattern(0, 0);
      setupPositionProbePattern(_moduleCount - 7, 0);
      setupPositionProbePattern(0, _moduleCount - 7);
      setupPositionAdjustPattern();
      setupTimingPattern();
      setupTypeInfo(test, maskPattern);

      if (_typeNumber >= 7) {
        setupTypeNumber(test);
      }

      if (_dataCache == null) {
        _dataCache = createData(_typeNumber, _errorCorrectionLevel, _dataList);
      }

      mapData(_dataCache, maskPattern);
    };

    var setupPositionProbePattern = function(row, col) {

      for (var r = -1; r <= 7; r += 1) {

        if (row + r <= -1 || _moduleCount <= row + r) continue;

        for (var c = -1; c <= 7; c += 1) {

          if (col + c <= -1 || _moduleCount <= col + c) continue;

          if ( (0 <= r && r <= 6 && (c == 0 || c == 6) )
              || (0 <= c && c <= 6 && (r == 0 || r == 6) )
              || (2 <= r && r <= 4 && 2 <= c && c <= 4) ) {
            _modules[row + r][col + c] = true;
          } else {
            _modules[row + r][col + c] = false;
          }
        }
      }
    };

    var getBestMaskPattern = function() {

      var minLostPoint = 0;
      var pattern = 0;

      for (var i = 0; i < 8; i += 1) {

        makeImpl(true, i);

        var lostPoint = QRUtil.getLostPoint(_this);

        if (i == 0 || minLostPoint > lostPoint) {
          minLostPoint = lostPoint;
          pattern = i;
        }
      }

      return pattern;
    };

    var setupTimingPattern = function() {

      for (var r = 8; r < _moduleCount - 8; r += 1) {
        if (_modules[r][6] != null) {
          continue;
        }
        _modules[r][6] = (r % 2 == 0);
      }

      for (var c = 8; c < _moduleCount - 8; c += 1) {
        if (_modules[6][c] != null) {
          continue;
        }
        _modules[6][c] = (c % 2 == 0);
      }
    };

    var setupPositionAdjustPattern = function() {

      var pos = QRUtil.getPatternPosition(_typeNumber);

      for (var i = 0; i < pos.length; i += 1) {

        for (var j = 0; j < pos.length; j += 1) {

          var row = pos[i];
          var col = pos[j];

          if (_modules[row][col] != null) {
            continue;
          }

          for (var r = -2; r <= 2; r += 1) {

            for (var c = -2; c <= 2; c += 1) {

              if (r == -2 || r == 2 || c == -2 || c == 2
                  || (r == 0 && c == 0) ) {
                _modules[row + r][col + c] = true;
              } else {
                _modules[row + r][col + c] = false;
              }
            }
          }
        }
      }
    };

    var setupTypeNumber = function(test) {

      var bits = QRUtil.getBCHTypeNumber(_typeNumber);

      for (var i = 0; i < 18; i += 1) {
        var mod = (!test && ( (bits >> i) & 1) == 1);
        _modules[Math.floor(i / 3)][i % 3 + _moduleCount - 8 - 3] = mod;
      }

      for (var i = 0; i < 18; i += 1) {
        var mod = (!test && ( (bits >> i) & 1) == 1);
        _modules[i % 3 + _moduleCount - 8 - 3][Math.floor(i / 3)] = mod;
      }
    };

    var setupTypeInfo = function(test, maskPattern) {

      var data = (_errorCorrectionLevel << 3) | maskPattern;
      var bits = QRUtil.getBCHTypeInfo(data);

      // vertical
      for (var i = 0; i < 15; i += 1) {

        var mod = (!test && ( (bits >> i) & 1) == 1);

        if (i < 6) {
          _modules[i][8] = mod;
        } else if (i < 8) {
          _modules[i + 1][8] = mod;
        } else {
          _modules[_moduleCount - 15 + i][8] = mod;
        }
      }

      // horizontal
      for (var i = 0; i < 15; i += 1) {

        var mod = (!test && ( (bits >> i) & 1) == 1);

        if (i < 8) {
          _modules[8][_moduleCount - i - 1] = mod;
        } else if (i < 9) {
          _modules[8][15 - i - 1 + 1] = mod;
        } else {
          _modules[8][15 - i - 1] = mod;
        }
      }

      // fixed module
      _modules[_moduleCount - 8][8] = (!test);
    };

    var mapData = function(data, maskPattern) {

      var inc = -1;
      var row = _moduleCount - 1;
      var bitIndex = 7;
      var byteIndex = 0;
      var maskFunc = QRUtil.getMaskFunction(maskPattern);

      for (var col = _moduleCount - 1; col > 0; col -= 2) {

        if (col == 6) col -= 1;

        while (true) {

          for (var c = 0; c < 2; c += 1) {

            if (_modules[row][col - c] == null) {

              var dark = false;

              if (byteIndex < data.length) {
                dark = ( ( (data[byteIndex] >>> bitIndex) & 1) == 1);
              }

              var mask = maskFunc(row, col - c);

              if (mask) {
                dark = !dark;
              }

              _modules[row][col - c] = dark;
              bitIndex -= 1;

              if (bitIndex == -1) {
                byteIndex += 1;
                bitIndex = 7;
              }
            }
          }

          row += inc;

          if (row < 0 || _moduleCount <= row) {
            row -= inc;
            inc = -inc;
            break;
          }
        }
      }
    };

    var createBytes = function(buffer, rsBlocks) {

      var offset = 0;

      var maxDcCount = 0;
      var maxEcCount = 0;

      var dcdata = new Array(rsBlocks.length);
      var ecdata = new Array(rsBlocks.length);

      for (var r = 0; r < rsBlocks.length; r += 1) {

        var dcCount = rsBlocks[r].dataCount;
        var ecCount = rsBlocks[r].totalCount - dcCount;

        maxDcCount = Math.max(maxDcCount, dcCount);
        maxEcCount = Math.max(maxEcCount, ecCount);

        dcdata[r] = new Array(dcCount);

        for (var i = 0; i < dcdata[r].length; i += 1) {
          dcdata[r][i] = 0xff & buffer.getBuffer()[i + offset];
        }
        offset += dcCount;

        var rsPoly = QRUtil.getErrorCorrectPolynomial(ecCount);
        var rawPoly = qrPolynomial(dcdata[r], rsPoly.getLength() - 1);

        var modPoly = rawPoly.mod(rsPoly);
        ecdata[r] = new Array(rsPoly.getLength() - 1);
        for (var i = 0; i < ecdata[r].length; i += 1) {
          var modIndex = i + modPoly.getLength() - ecdata[r].length;
          ecdata[r][i] = (modIndex >= 0)? modPoly.getAt(modIndex) : 0;
        }
      }

      var totalCodeCount = 0;
      for (var i = 0; i < rsBlocks.length; i += 1) {
        totalCodeCount += rsBlocks[i].totalCount;
      }

      var data = new Array(totalCodeCount);
      var index = 0;

      for (var i = 0; i < maxDcCount; i += 1) {
        for (var r = 0; r < rsBlocks.length; r += 1) {
          if (i < dcdata[r].length) {
            data[index] = dcdata[r][i];
            index += 1;
          }
        }
      }

      for (var i = 0; i < maxEcCount; i += 1) {
        for (var r = 0; r < rsBlocks.length; r += 1) {
          if (i < ecdata[r].length) {
            data[index] = ecdata[r][i];
            index += 1;
          }
        }
      }

      return data;
    };

    var createData = function(typeNumber, errorCorrectionLevel, dataList) {

      var rsBlocks = QRRSBlock.getRSBlocks(typeNumber, errorCorrectionLevel);

      var buffer = qrBitBuffer();

      for (var i = 0; i < dataList.length; i += 1) {
        var data = dataList[i];
        buffer.put(data.getMode(), 4);
        buffer.put(data.getLength(), QRUtil.getLengthInBits(data.getMode(), typeNumber) );
        data.write(buffer);
      }

      // calc num max data.
      var totalDataCount = 0;
      for (var i = 0; i < rsBlocks.length; i += 1) {
        totalDataCount += rsBlocks[i].dataCount;
      }

      if (buffer.getLengthInBits() > totalDataCount * 8) {
        throw 'code length overflow. ('
          + buffer.getLengthInBits()
          + '>'
          + totalDataCount * 8
          + ')';
      }

      // end code
      if (buffer.getLengthInBits() + 4 <= totalDataCount * 8) {
        buffer.put(0, 4);
      }

      // padding
      while (buffer.getLengthInBits() % 8 != 0) {
        buffer.putBit(false);
      }

      // padding
      while (true) {

        if (buffer.getLengthInBits() >= totalDataCount * 8) {
          break;
        }
        buffer.put(PAD0, 8);

        if (buffer.getLengthInBits() >= totalDataCount * 8) {
          break;
        }
        buffer.put(PAD1, 8);
      }

      return createBytes(buffer, rsBlocks);
    };

    _this.addData = function(data, mode) {

      mode = mode || 'Byte';

      var newData = null;

      switch(mode) {
      case 'Numeric' :
        newData = qrNumber(data);
        break;
      case 'Alphanumeric' :
        newData = qrAlphaNum(data);
        break;
      case 'Byte' :
        newData = qr8BitByte(data);
        break;
      case 'Kanji' :
        newData = qrKanji(data);
        break;
      default :
        throw 'mode:' + mode;
      }

      _dataList.push(newData);
      _dataCache = null;
    };

    _this.isDark = function(row, col) {
      if (row < 0 || _moduleCount <= row || col < 0 || _moduleCount <= col) {
        throw row + ',' + col;
      }
      return _modules[row][col];
    };

    _this.getModuleCount = function() {
      return _moduleCount;
    };

    _this.make = function() {
      if (_typeNumber < 1) {
        var typeNumber = 1;

        for (; typeNumber < 40; typeNumber++) {
          var rsBlocks = QRRSBlock.getRSBlocks(typeNumber, _errorCorrectionLevel);
          var buffer = qrBitBuffer();

          for (var i = 0; i < _dataList.length; i++) {
            var data = _dataList[i];
            buffer.put(data.getMode(), 4);
            buffer.put(data.getLength(), QRUtil.getLengthInBits(data.getMode(), typeNumber) );
            data.write(buffer);
          }

          var totalDataCount = 0;
          for (var i = 0; i < rsBlocks.length; i++) {
            totalDataCount += rsBlocks[i].dataCount;
          }

          if (buffer.getLengthInBits() <= totalDataCount * 8) {
            break;
          }
        }

        _typeNumber = typeNumber;
      }

      makeImpl(false, getBestMaskPattern() );
    };

    _this.createTableTag = function(cellSize, margin) {

      cellSize = cellSize || 2;
      margin = (typeof margin == 'undefined')? cellSize * 4 : margin;

      var qrHtml = '';

      qrHtml += '<table style="';
      qrHtml += ' border-width: 0px; border-style: none;';
      qrHtml += ' border-collapse: collapse;';
      qrHtml += ' padding: 0px; margin: ' + margin + 'px;';
      qrHtml += '">';
      qrHtml += '<tbody>';

      for (var r = 0; r < _this.getModuleCount(); r += 1) {

        qrHtml += '<tr>';

        for (var c = 0; c < _this.getModuleCount(); c += 1) {
          qrHtml += '<td style="';
          qrHtml += ' border-width: 0px; border-style: none;';
          qrHtml += ' border-collapse: collapse;';
          qrHtml += ' padding: 0px; margin: 0px;';
          qrHtml += ' width: ' + cellSize + 'px;';
          qrHtml += ' height: ' + cellSize + 'px;';
          qrHtml += ' background-color: ';
          qrHtml += _this.isDark(r, c)? '#000000' : '#ffffff';
          qrHtml += ';';
          qrHtml += '"/>';
        }

        qrHtml += '</tr>';
      }

      qrHtml += '</tbody>';
      qrHtml += '</table>';

      return qrHtml;
    };

    _this.createSvgTag = function(cellSize, margin, alt, title) {

      var opts = {};
      if (typeof arguments[0] == 'object') {
        // Called by options.
        opts = arguments[0];
        // overwrite cellSize and margin.
        cellSize = opts.cellSize;
        margin = opts.margin;
        alt = opts.alt;
        title = opts.title;
      }

      cellSize = cellSize || 2;
      margin = (typeof margin == 'undefined')? cellSize * 4 : margin;

      // Compose alt property surrogate
      alt = (typeof alt === 'string') ? {text: alt} : alt || {};
      alt.text = alt.text || null;
      alt.id = (alt.text) ? alt.id || 'qrcode-description' : null;

      // Compose title property surrogate
      title = (typeof title === 'string') ? {text: title} : title || {};
      title.text = title.text || null;
      title.id = (title.text) ? title.id || 'qrcode-title' : null;

      var size = _this.getModuleCount() * cellSize + margin * 2;
      var c, mc, r, mr, qrSvg='', rect;

      rect = 'l' + cellSize + ',0 0,' + cellSize +
        ' -' + cellSize + ',0 0,-' + cellSize + 'z ';

      qrSvg += '<svg version="1.1" xmlns="http://www.w3.org/2000/svg"';
      qrSvg += !opts.scalable ? ' width="' + size + 'px" height="' + size + 'px"' : '';
      qrSvg += ' viewBox="0 0 ' + size + ' ' + size + '" ';
      qrSvg += ' preserveAspectRatio="xMinYMin meet"';
      qrSvg += (title.text || alt.text) ? ' role="img" aria-labelledby="' +
          escapeXml([title.id, alt.id].join(' ').trim() ) + '"' : '';
      qrSvg += '>';
      qrSvg += (title.text) ? '<title id="' + escapeXml(title.id) + '">' +
          escapeXml(title.text) + '</title>' : '';
      qrSvg += (alt.text) ? '<description id="' + escapeXml(alt.id) + '">' +
          escapeXml(alt.text) + '</description>' : '';
      qrSvg += '<rect width="100%" height="100%" fill="white" cx="0" cy="0"/>';
      qrSvg += '<path d="';

      for (r = 0; r < _this.getModuleCount(); r += 1) {
        mr = r * cellSize + margin;
        for (c = 0; c < _this.getModuleCount(); c += 1) {
          if (_this.isDark(r, c) ) {
            mc = c*cellSize+margin;
            qrSvg += 'M' + mc + ',' + mr + rect;
          }
        }
      }

      qrSvg += '" stroke="transparent" fill="black"/>';
      qrSvg += '</svg>';

      return qrSvg;
    };

    _this.createDataURL = function(cellSize, margin) {

      cellSize = cellSize || 2;
      margin = (typeof margin == 'undefined')? cellSize * 4 : margin;

      var size = _this.getModuleCount() * cellSize + margin * 2;
      var min = margin;
      var max = size - margin;

      return createDataURL(size, size, function(x, y) {
        if (min <= x && x < max && min <= y && y < max) {
          var c = Math.floor( (x - min) / cellSize);
          var r = Math.floor( (y - min) / cellSize);
          return _this.isDark(r, c)? 0 : 1;
        } else {
          return 1;
        }
      } );
    };

    _this.createImgTag = function(cellSize, margin, alt) {

      cellSize = cellSize || 2;
      margin = (typeof margin == 'undefined')? cellSize * 4 : margin;

      var size = _this.getModuleCount() * cellSize + margin * 2;

      var img = '';
      img += '<img';
      img += '\u0020src="';
      img += _this.createDataURL(cellSize, margin);
      img += '"';
      img += '\u0020width="';
      img += size;
      img += '"';
      img += '\u0020height="';
      img += size;
      img += '"';
      if (alt) {
        img += '\u0020alt="';
        img += escapeXml(alt);
        img += '"';
      }
      img += '/>';

      return img;
    };

    var escapeXml = function(s) {
      var escaped = '';
      for (var i = 0; i < s.length; i += 1) {
        var c = s.charAt(i);
        switch(c) {
        case '<': escaped += '&lt;'; break;
        case '>': escaped += '&gt;'; break;
        case '&': escaped += '&amp;'; break;
        case '"': escaped += '&quot;'; break;
        default : escaped += c; break;
        }
      }
      return escaped;
    };

    var _createHalfASCII = function(margin) {
      var cellSize = 1;
      margin = (typeof margin == 'undefined')? cellSize * 2 : margin;

      var size = _this.getModuleCount() * cellSize + margin * 2;
      var min = margin;
      var max = size - margin;

      var y, x, r1, r2, p;

      var blocks = {
        '██': '█',
        '█ ': '▀',
        ' █': '▄',
        '  ': ' '
      };

      var blocksLastLineNoMargin = {
        '██': '▀',
        '█ ': '▀',
        ' █': ' ',
        '  ': ' '
      };

      var ascii = '';
      for (y = 0; y < size; y += 2) {
        r1 = Math.floor((y - min) / cellSize);
        r2 = Math.floor((y + 1 - min) / cellSize);
        for (x = 0; x < size; x += 1) {
          p = '█';

          if (min <= x && x < max && min <= y && y < max && _this.isDark(r1, Math.floor((x - min) / cellSize))) {
            p = ' ';
          }

          if (min <= x && x < max && min <= y+1 && y+1 < max && _this.isDark(r2, Math.floor((x - min) / cellSize))) {
            p += ' ';
          }
          else {
            p += '█';
          }

          // Output 2 characters per pixel, to create full square. 1 character per pixels gives only half width of square.
          ascii += (margin < 1 && y+1 >= max) ? blocksLastLineNoMargin[p] : blocks[p];
        }

        ascii += '\\n';
      }

      if (size % 2 && margin > 0) {
        return ascii.substring(0, ascii.length - size - 1) + Array(size+1).join('▀');
      }

      return ascii.substring(0, ascii.length-1);
    };

    _this.createASCII = function(cellSize, margin) {
      cellSize = cellSize || 1;

      if (cellSize < 2) {
        return _createHalfASCII(margin);
      }

      cellSize -= 1;
      margin = (typeof margin == 'undefined')? cellSize * 2 : margin;

      var size = _this.getModuleCount() * cellSize + margin * 2;
      var min = margin;
      var max = size - margin;

      var y, x, r, p;

      var white = Array(cellSize+1).join('██');
      var black = Array(cellSize+1).join('  ');

      var ascii = '';
      var line = '';
      for (y = 0; y < size; y += 1) {
        r = Math.floor( (y - min) / cellSize);
        line = '';
        for (x = 0; x < size; x += 1) {
          p = 1;

          if (min <= x && x < max && min <= y && y < max && _this.isDark(r, Math.floor((x - min) / cellSize))) {
            p = 0;
          }

          // Output 2 characters per pixel, to create full square. 1 character per pixels gives only half width of square.
          line += p ? white : black;
        }

        for (r = 0; r < cellSize; r += 1) {
          ascii += line + '\\n';
        }
      }

      return ascii.substring(0, ascii.length-1);
    };

    _this.renderTo2dContext = function(context, cellSize) {
      cellSize = cellSize || 2;
      var length = _this.getModuleCount();
      for (var row = 0; row < length; row++) {
        for (var col = 0; col < length; col++) {
          context.fillStyle = _this.isDark(row, col) ? 'black' : 'white';
          context.fillRect(col * cellSize, row * cellSize, cellSize, cellSize);
        }
      }
    }

    return _this;
  };

  //---------------------------------------------------------------------
  // qrcode.stringToBytes
  //---------------------------------------------------------------------

  qrcode.stringToBytesFuncs = {
    'default' : function(s) {
      var bytes = [];
      for (var i = 0; i < s.length; i += 1) {
        var c = s.charCodeAt(i);
        bytes.push(c & 0xff);
      }
      return bytes;
    }
  };

  qrcode.stringToBytes = qrcode.stringToBytesFuncs['default'];

  //---------------------------------------------------------------------
  // qrcode.createStringToBytes
  //---------------------------------------------------------------------

  /**
   * @param unicodeData base64 string of byte array.
   * [16bit Unicode],[16bit Bytes], ...
   * @param numChars
   */
  qrcode.createStringToBytes = function(unicodeData, numChars) {

    // create conversion map.

    var unicodeMap = function() {

      var bin = base64DecodeInputStream(unicodeData);
      var read = function() {
        var b = bin.read();
        if (b == -1) throw 'eof';
        return b;
      };

      var count = 0;
      var unicodeMap = {};
      while (true) {
        var b0 = bin.read();
        if (b0 == -1) break;
        var b1 = read();
        var b2 = read();
        var b3 = read();
        var k = String.fromCharCode( (b0 << 8) | b1);
        var v = (b2 << 8) | b3;
        unicodeMap[k] = v;
        count += 1;
      }
      if (count != numChars) {
        throw count + ' != ' + numChars;
      }

      return unicodeMap;
    }();

    var unknownChar = '?'.charCodeAt(0);

    return function(s) {
      var bytes = [];
      for (var i = 0; i < s.length; i += 1) {
        var c = s.charCodeAt(i);
        if (c < 128) {
          bytes.push(c);
        } else {
          var b = unicodeMap[s.charAt(i)];
          if (typeof b == 'number') {
            if ( (b & 0xff) == b) {
              // 1byte
              bytes.push(b);
            } else {
              // 2bytes
              bytes.push(b >>> 8);
              bytes.push(b & 0xff);
            }
          } else {
            bytes.push(unknownChar);
          }
        }
      }
      return bytes;
    };
  };

  //---------------------------------------------------------------------
  // QRMode
  //---------------------------------------------------------------------

  var QRMode = {
    MODE_NUMBER :    1 << 0,
    MODE_ALPHA_NUM : 1 << 1,
    MODE_8BIT_BYTE : 1 << 2,
    MODE_KANJI :     1 << 3
  };

  //---------------------------------------------------------------------
  // QRErrorCorrectionLevel
  //---------------------------------------------------------------------

  var QRErrorCorrectionLevel = {
    L : 1,
    M : 0,
    Q : 3,
    H : 2
  };

  //---------------------------------------------------------------------
  // QRMaskPattern
  //---------------------------------------------------------------------

  var QRMaskPattern = {
    PATTERN000 : 0,
    PATTERN001 : 1,
    PATTERN010 : 2,
    PATTERN011 : 3,
    PATTERN100 : 4,
    PATTERN101 : 5,
    PATTERN110 : 6,
    PATTERN111 : 7
  };

  //---------------------------------------------------------------------
  // QRUtil
  //---------------------------------------------------------------------

  var QRUtil = function() {

    var PATTERN_POSITION_TABLE = [
      [],
      [6, 18],
      [6, 22],
      [6, 26],
      [6, 30],
      [6, 34],
      [6, 22, 38],
      [6, 24, 42],
      [6, 26, 46],
      [6, 28, 50],
      [6, 30, 54],
      [6, 32, 58],
      [6, 34, 62],
      [6, 26, 46, 66],
      [6, 26, 48, 70],
      [6, 26, 50, 74],
      [6, 30, 54, 78],
      [6, 30, 56, 82],
      [6, 30, 58, 86],
      [6, 34, 62, 90],
      [6, 28, 50, 72, 94],
      [6, 26, 50, 74, 98],
      [6, 30, 54, 78, 102],
      [6, 28, 54, 80, 106],
      [6, 32, 58, 84, 110],
      [6, 30, 58, 86, 114],
      [6, 34, 62, 90, 118],
      [6, 26, 50, 74, 98, 122],
      [6, 30, 54, 78, 102, 126],
      [6, 26, 52, 78, 104, 130],
      [6, 30, 56, 82, 108, 134],
      [6, 34, 60, 86, 112, 138],
      [6, 30, 58, 86, 114, 142],
      [6, 34, 62, 90, 118, 146],
      [6, 30, 54, 78, 102, 126, 150],
      [6, 24, 50, 76, 102, 128, 154],
      [6, 28, 54, 80, 106, 132, 158],
      [6, 32, 58, 84, 110, 136, 162],
      [6, 26, 54, 82, 110, 138, 166],
      [6, 30, 58, 86, 114, 142, 170]
    ];
    var G15 = (1 << 10) | (1 << 8) | (1 << 5) | (1 << 4) | (1 << 2) | (1 << 1) | (1 << 0);
    var G18 = (1 << 12) | (1 << 11) | (1 << 10) | (1 << 9) | (1 << 8) | (1 << 5) | (1 << 2) | (1 << 0);
    var G15_MASK = (1 << 14) | (1 << 12) | (1 << 10) | (1 << 4) | (1 << 1);

    var _this = {};

    var getBCHDigit = function(data) {
      var digit = 0;
      while (data != 0) {
        digit += 1;
        data >>>= 1;
      }
      return digit;
    };

    _this.getBCHTypeInfo = function(data) {
      var d = data << 10;
      while (getBCHDigit(d) - getBCHDigit(G15) >= 0) {
        d ^= (G15 << (getBCHDigit(d) - getBCHDigit(G15) ) );
      }
      return ( (data << 10) | d) ^ G15_MASK;
    };

    _this.getBCHTypeNumber = function(data) {
      var d = data << 12;
      while (getBCHDigit(d) - getBCHDigit(G18) >= 0) {
        d ^= (G18 << (getBCHDigit(d) - getBCHDigit(G18) ) );
      }
      return (data << 12) | d;
    };

    _this.getPatternPosition = function(typeNumber) {
      return PATTERN_POSITION_TABLE[typeNumber - 1];
    };

    _this.getMaskFunction = function(maskPattern) {

      switch (maskPattern) {

      case QRMaskPattern.PATTERN000 :
        return function(i, j) { return (i + j) % 2 == 0; };
      case QRMaskPattern.PATTERN001 :
        return function(i, j) { return i % 2 == 0; };
      case QRMaskPattern.PATTERN010 :
        return function(i, j) { return j % 3 == 0; };
      case QRMaskPattern.PATTERN011 :
        return function(i, j) { return (i + j) % 3 == 0; };
      case QRMaskPattern.PATTERN100 :
        return function(i, j) { return (Math.floor(i / 2) + Math.floor(j / 3) ) % 2 == 0; };
      case QRMaskPattern.PATTERN101 :
        return function(i, j) { return (i * j) % 2 + (i * j) % 3 == 0; };
      case QRMaskPattern.PATTERN110 :
        return function(i, j) { return ( (i * j) % 2 + (i * j) % 3) % 2 == 0; };
      case QRMaskPattern.PATTERN111 :
        return function(i, j) { return ( (i * j) % 3 + (i + j) % 2) % 2 == 0; };

      default :
        throw 'bad maskPattern:' + maskPattern;
      }
    };

    _this.getErrorCorrectPolynomial = function(errorCorrectLength) {
      var a = qrPolynomial([1], 0);
      for (var i = 0; i < errorCorrectLength; i += 1) {
        a = a.multiply(qrPolynomial([1, QRMath.gexp(i)], 0) );
      }
      return a;
    };

    _this.getLengthInBits = function(mode, type) {

      if (1 <= type && type < 10) {

        // 1 - 9

        switch(mode) {
        case QRMode.MODE_NUMBER    : return 10;
        case QRMode.MODE_ALPHA_NUM : return 9;
        case QRMode.MODE_8BIT_BYTE : return 8;
        case QRMode.MODE_KANJI     : return 8;
        default :
          throw 'mode:' + mode;
        }

      } else if (type < 27) {

        // 10 - 26

        switch(mode) {
        case QRMode.MODE_NUMBER    : return 12;
        case QRMode.MODE_ALPHA_NUM : return 11;
        case QRMode.MODE_8BIT_BYTE : return 16;
        case QRMode.MODE_KANJI     : return 10;
        default :
          throw 'mode:' + mode;
        }

      } else if (type < 41) {

        // 27 - 40

        switch(mode) {
        case QRMode.MODE_NUMBER    : return 14;
        case QRMode.MODE_ALPHA_NUM : return 13;
        case QRMode.MODE_8BIT_BYTE : return 16;
        case QRMode.MODE_KANJI     : return 12;
        default :
          throw 'mode:' + mode;
        }

      } else {
        throw 'type:' + type;
      }
    };

    _this.getLostPoint = function(qrcode) {

      var moduleCount = qrcode.getModuleCount();

      var lostPoint = 0;

      // LEVEL1

      for (var row = 0; row < moduleCount; row += 1) {
        for (var col = 0; col < moduleCount; col += 1) {

          var sameCount = 0;
          var dark = qrcode.isDark(row, col);

          for (var r = -1; r <= 1; r += 1) {

            if (row + r < 0 || moduleCount <= row + r) {
              continue;
            }

            for (var c = -1; c <= 1; c += 1) {

              if (col + c < 0 || moduleCount <= col + c) {
                continue;
              }

              if (r == 0 && c == 0) {
                continue;
              }

              if (dark == qrcode.isDark(row + r, col + c) ) {
                sameCount += 1;
              }
            }
          }

          if (sameCount > 5) {
            lostPoint += (3 + sameCount - 5);
          }
        }
      };

      // LEVEL2

      for (var row = 0; row < moduleCount - 1; row += 1) {
        for (var col = 0; col < moduleCount - 1; col += 1) {
          var count = 0;
          if (qrcode.isDark(row, col) ) count += 1;
          if (qrcode.isDark(row + 1, col) ) count += 1;
          if (qrcode.isDark(row, col + 1) ) count += 1;
          if (qrcode.isDark(row + 1, col + 1) ) count += 1;
          if (count == 0 || count == 4) {
            lostPoint += 3;
          }
        }
      }

      // LEVEL3

      for (var row = 0; row < moduleCount; row += 1) {
        for (var col = 0; col < moduleCount - 6; col += 1) {
          if (qrcode.isDark(row, col)
              && !qrcode.isDark(row, col + 1)
              &&  qrcode.isDark(row, col + 2)
              &&  qrcode.isDark(row, col + 3)
              &&  qrcode.isDark(row, col + 4)
              && !qrcode.isDark(row, col + 5)
              &&  qrcode.isDark(row, col + 6) ) {
            lostPoint += 40;
          }
        }
      }

      for (var col = 0; col < moduleCount; col += 1) {
        for (var row = 0; row < moduleCount - 6; row += 1) {
          if (qrcode.isDark(row, col)
              && !qrcode.isDark(row + 1, col)
              &&  qrcode.isDark(row + 2, col)
              &&  qrcode.isDark(row + 3, col)
              &&  qrcode.isDark(row + 4, col)
              && !qrcode.isDark(row + 5, col)
              &&  qrcode.isDark(row + 6, col) ) {
            lostPoint += 40;
          }
        }
      }

      // LEVEL4

      var darkCount = 0;

      for (var col = 0; col < moduleCount; col += 1) {
        for (var row = 0; row < moduleCount; row += 1) {
          if (qrcode.isDark(row, col) ) {
            darkCount += 1;
          }
        }
      }

      var ratio = Math.abs(100 * darkCount / moduleCount / moduleCount - 50) / 5;
      lostPoint += ratio * 10;

      return lostPoint;
    };

    return _this;
  }();

  //---------------------------------------------------------------------
  // QRMath
  //---------------------------------------------------------------------

  var QRMath = function() {

    var EXP_TABLE = new Array(256);
    var LOG_TABLE = new Array(256);

    // initialize tables
    for (var i = 0; i < 8; i += 1) {
      EXP_TABLE[i] = 1 << i;
    }
    for (var i = 8; i < 256; i += 1) {
      EXP_TABLE[i] = EXP_TABLE[i - 4]
        ^ EXP_TABLE[i - 5]
        ^ EXP_TABLE[i - 6]
        ^ EXP_TABLE[i - 8];
    }
    for (var i = 0; i < 255; i += 1) {
      LOG_TABLE[EXP_TABLE[i] ] = i;
    }

    var _this = {};

    _this.glog = function(n) {

      if (n < 1) {
        throw 'glog(' + n + ')';
      }

      return LOG_TABLE[n];
    };

    _this.gexp = function(n) {

      while (n < 0) {
        n += 255;
      }

      while (n >= 256) {
        n -= 255;
      }

      return EXP_TABLE[n];
    };

    return _this;
  }();

  //---------------------------------------------------------------------
  // qrPolynomial
  //---------------------------------------------------------------------

  function qrPolynomial(num, shift) {

    if (typeof num.length == 'undefined') {
      throw num.length + '/' + shift;
    }

    var _num = function() {
      var offset = 0;
      while (offset < num.length && num[offset] == 0) {
        offset += 1;
      }
      var _num = new Array(num.length - offset + shift);
      for (var i = 0; i < num.length - offset; i += 1) {
        _num[i] = num[i + offset];
      }
      return _num;
    }();

    var _this = {};

    _this.getAt = function(index) {
      return _num[index];
    };

    _this.getLength = function() {
      return _num.length;
    };

    _this.multiply = function(e) {

      var num = new Array(_this.getLength() + e.getLength() - 1);

      for (var i = 0; i < _this.getLength(); i += 1) {
        for (var j = 0; j < e.getLength(); j += 1) {
          num[i + j] ^= QRMath.gexp(QRMath.glog(_this.getAt(i) ) + QRMath.glog(e.getAt(j) ) );
        }
      }

      return qrPolynomial(num, 0);
    };

    _this.mod = function(e) {

      if (_this.getLength() - e.getLength() < 0) {
        return _this;
      }

      var ratio = QRMath.glog(_this.getAt(0) ) - QRMath.glog(e.getAt(0) );

      var num = new Array(_this.getLength() );
      for (var i = 0; i < _this.getLength(); i += 1) {
        num[i] = _this.getAt(i);
      }

      for (var i = 0; i < e.getLength(); i += 1) {
        num[i] ^= QRMath.gexp(QRMath.glog(e.getAt(i) ) + ratio);
      }

      // recursive call
      return qrPolynomial(num, 0).mod(e);
    };

    return _this;
  };

  //---------------------------------------------------------------------
  // QRRSBlock
  //---------------------------------------------------------------------

  var QRRSBlock = function() {

    var RS_BLOCK_TABLE = [

      // L
      // M
      // Q
      // H

      // 1
      [1, 26, 19],
      [1, 26, 16],
      [1, 26, 13],
      [1, 26, 9],

      // 2
      [1, 44, 34],
      [1, 44, 28],
      [1, 44, 22],
      [1, 44, 16],

      // 3
      [1, 70, 55],
      [1, 70, 44],
      [2, 35, 17],
      [2, 35, 13],

      // 4
      [1, 100, 80],
      [2, 50, 32],
      [2, 50, 24],
      [4, 25, 9],

      // 5
      [1, 134, 108],
      [2, 67, 43],
      [2, 33, 15, 2, 34, 16],
      [2, 33, 11, 2, 34, 12],

      // 6
      [2, 86, 68],
      [4, 43, 27],
      [4, 43, 19],
      [4, 43, 15],

      // 7
      [2, 98, 78],
      [4, 49, 31],
      [2, 32, 14, 4, 33, 15],
      [4, 39, 13, 1, 40, 14],

      // 8
      [2, 121, 97],
      [2, 60, 38, 2, 61, 39],
      [4, 40, 18, 2, 41, 19],
      [4, 40, 14, 2, 41, 15],

      // 9
      [2, 146, 116],
      [3, 58, 36, 2, 59, 37],
      [4, 36, 16, 4, 37, 17],
      [4, 36, 12, 4, 37, 13],

      // 10
      [2, 86, 68, 2, 87, 69],
      [4, 69, 43, 1, 70, 44],
      [6, 43, 19, 2, 44, 20],
      [6, 43, 15, 2, 44, 16],

      // 11
      [4, 101, 81],
      [1, 80, 50, 4, 81, 51],
      [4, 50, 22, 4, 51, 23],
      [3, 36, 12, 8, 37, 13],

      // 12
      [2, 116, 92, 2, 117, 93],
      [6, 58, 36, 2, 59, 37],
      [4, 46, 20, 6, 47, 21],
      [7, 42, 14, 4, 43, 15],

      // 13
      [4, 133, 107],
      [8, 59, 37, 1, 60, 38],
      [8, 44, 20, 4, 45, 21],
      [12, 33, 11, 4, 34, 12],

      // 14
      [3, 145, 115, 1, 146, 116],
      [4, 64, 40, 5, 65, 41],
      [11, 36, 16, 5, 37, 17],
      [11, 36, 12, 5, 37, 13],

      // 15
      [5, 109, 87, 1, 110, 88],
      [5, 65, 41, 5, 66, 42],
      [5, 54, 24, 7, 55, 25],
      [11, 36, 12, 7, 37, 13],

      // 16
      [5, 122, 98, 1, 123, 99],
      [7, 73, 45, 3, 74, 46],
      [15, 43, 19, 2, 44, 20],
      [3, 45, 15, 13, 46, 16],

      // 17
      [1, 135, 107, 5, 136, 108],
      [10, 74, 46, 1, 75, 47],
      [1, 50, 22, 15, 51, 23],
      [2, 42, 14, 17, 43, 15],

      // 18
      [5, 150, 120, 1, 151, 121],
      [9, 69, 43, 4, 70, 44],
      [17, 50, 22, 1, 51, 23],
      [2, 42, 14, 19, 43, 15],

      // 19
      [3, 141, 113, 4, 142, 114],
      [3, 70, 44, 11, 71, 45],
      [17, 47, 21, 4, 48, 22],
      [9, 39, 13, 16, 40, 14],

      // 20
      [3, 135, 107, 5, 136, 108],
      [3, 67, 41, 13, 68, 42],
      [15, 54, 24, 5, 55, 25],
      [15, 43, 15, 10, 44, 16],

      // 21
      [4, 144, 116, 4, 145, 117],
      [17, 68, 42],
      [17, 50, 22, 6, 51, 23],
      [19, 46, 16, 6, 47, 17],

      // 22
      [2, 139, 111, 7, 140, 112],
      [17, 74, 46],
      [7, 54, 24, 16, 55, 25],
      [34, 37, 13],

      // 23
      [4, 151, 121, 5, 152, 122],
      [4, 75, 47, 14, 76, 48],
      [11, 54, 24, 14, 55, 25],
      [16, 45, 15, 14, 46, 16],

      // 24
      [6, 147, 117, 4, 148, 118],
      [6, 73, 45, 14, 74, 46],
      [11, 54, 24, 16, 55, 25],
      [30, 46, 16, 2, 47, 17],

      // 25
      [8, 132, 106, 4, 133, 107],
      [8, 75, 47, 13, 76, 48],
      [7, 54, 24, 22, 55, 25],
      [22, 45, 15, 13, 46, 16],

      // 26
      [10, 142, 114, 2, 143, 115],
      [19, 74, 46, 4, 75, 47],
      [28, 50, 22, 6, 51, 23],
      [33, 46, 16, 4, 47, 17],

      // 27
      [8, 152, 122, 4, 153, 123],
      [22, 73, 45, 3, 74, 46],
      [8, 53, 23, 26, 54, 24],
      [12, 45, 15, 28, 46, 16],

      // 28
      [3, 147, 117, 10, 148, 118],
      [3, 73, 45, 23, 74, 46],
      [4, 54, 24, 31, 55, 25],
      [11, 45, 15, 31, 46, 16],

      // 29
      [7, 146, 116, 7, 147, 117],
      [21, 73, 45, 7, 74, 46],
      [1, 53, 23, 37, 54, 24],
      [19, 45, 15, 26, 46, 16],

      // 30
      [5, 145, 115, 10, 146, 116],
      [19, 75, 47, 10, 76, 48],
      [15, 54, 24, 25, 55, 25],
      [23, 45, 15, 25, 46, 16],

      // 31
      [13, 145, 115, 3, 146, 116],
      [2, 74, 46, 29, 75, 47],
      [42, 54, 24, 1, 55, 25],
      [23, 45, 15, 28, 46, 16],

      // 32
      [17, 145, 115],
      [10, 74, 46, 23, 75, 47],
      [10, 54, 24, 35, 55, 25],
      [19, 45, 15, 35, 46, 16],

      // 33
      [17, 145, 115, 1, 146, 116],
      [14, 74, 46, 21, 75, 47],
      [29, 54, 24, 19, 55, 25],
      [11, 45, 15, 46, 46, 16],

      // 34
      [13, 145, 115, 6, 146, 116],
      [14, 74, 46, 23, 75, 47],
      [44, 54, 24, 7, 55, 25],
      [59, 46, 16, 1, 47, 17],

      // 35
      [12, 151, 121, 7, 152, 122],
      [12, 75, 47, 26, 76, 48],
      [39, 54, 24, 14, 55, 25],
      [22, 45, 15, 41, 46, 16],

      // 36
      [6, 151, 121, 14, 152, 122],
      [6, 75, 47, 34, 76, 48],
      [46, 54, 24, 10, 55, 25],
      [2, 45, 15, 64, 46, 16],

      // 37
      [17, 152, 122, 4, 153, 123],
      [29, 74, 46, 14, 75, 47],
      [49, 54, 24, 10, 55, 25],
      [24, 45, 15, 46, 46, 16],

      // 38
      [4, 152, 122, 18, 153, 123],
      [13, 74, 46, 32, 75, 47],
      [48, 54, 24, 14, 55, 25],
      [42, 45, 15, 32, 46, 16],

      // 39
      [20, 147, 117, 4, 148, 118],
      [40, 75, 47, 7, 76, 48],
      [43, 54, 24, 22, 55, 25],
      [10, 45, 15, 67, 46, 16],

      // 40
      [19, 148, 118, 6, 149, 119],
      [18, 75, 47, 31, 76, 48],
      [34, 54, 24, 34, 55, 25],
      [20, 45, 15, 61, 46, 16]
    ];

    var qrRSBlock = function(totalCount, dataCount) {
      var _this = {};
      _this.totalCount = totalCount;
      _this.dataCount = dataCount;
      return _this;
    };

    var _this = {};

    var getRsBlockTable = function(typeNumber, errorCorrectionLevel) {

      switch(errorCorrectionLevel) {
      case QRErrorCorrectionLevel.L :
        return RS_BLOCK_TABLE[(typeNumber - 1) * 4 + 0];
      case QRErrorCorrectionLevel.M :
        return RS_BLOCK_TABLE[(typeNumber - 1) * 4 + 1];
      case QRErrorCorrectionLevel.Q :
        return RS_BLOCK_TABLE[(typeNumber - 1) * 4 + 2];
      case QRErrorCorrectionLevel.H :
        return RS_BLOCK_TABLE[(typeNumber - 1) * 4 + 3];
      default :
        return undefined;
      }
    };

    _this.getRSBlocks = function(typeNumber, errorCorrectionLevel) {

      var rsBlock = getRsBlockTable(typeNumber, errorCorrectionLevel);

      if (typeof rsBlock == 'undefined') {
        throw 'bad rs block @ typeNumber:' + typeNumber +
            '/errorCorrectionLevel:' + errorCorrectionLevel;
      }

      var length = rsBlock.length / 3;

      var list = [];

      for (var i = 0; i < length; i += 1) {

        var count = rsBlock[i * 3 + 0];
        var totalCount = rsBlock[i * 3 + 1];
        var dataCount = rsBlock[i * 3 + 2];

        for (var j = 0; j < count; j += 1) {
          list.push(qrRSBlock(totalCount, dataCount) );
        }
      }

      return list;
    };

    return _this;
  }();

  //---------------------------------------------------------------------
  // qrBitBuffer
  //---------------------------------------------------------------------

  var qrBitBuffer = function() {

    var _buffer = [];
    var _length = 0;

    var _this = {};

    _this.getBuffer = function() {
      return _buffer;
    };

    _this.getAt = function(index) {
      var bufIndex = Math.floor(index / 8);
      return ( (_buffer[bufIndex] >>> (7 - index % 8) ) & 1) == 1;
    };

    _this.put = function(num, length) {
      for (var i = 0; i < length; i += 1) {
        _this.putBit( ( (num >>> (length - i - 1) ) & 1) == 1);
      }
    };

    _this.getLengthInBits = function() {
      return _length;
    };

    _this.putBit = function(bit) {

      var bufIndex = Math.floor(_length / 8);
      if (_buffer.length <= bufIndex) {
        _buffer.push(0);
      }

      if (bit) {
        _buffer[bufIndex] |= (0x80 >>> (_length % 8) );
      }

      _length += 1;
    };

    return _this;
  };

  //---------------------------------------------------------------------
  // qrNumber
  //---------------------------------------------------------------------

  var qrNumber = function(data) {

    var _mode = QRMode.MODE_NUMBER;
    var _data = data;

    var _this = {};

    _this.getMode = function() {
      return _mode;
    };

    _this.getLength = function(buffer) {
      return _data.length;
    };

    _this.write = function(buffer) {

      var data = _data;

      var i = 0;

      while (i + 2 < data.length) {
        buffer.put(strToNum(data.substring(i, i + 3) ), 10);
        i += 3;
      }

      if (i < data.length) {
        if (data.length - i == 1) {
          buffer.put(strToNum(data.substring(i, i + 1) ), 4);
        } else if (data.length - i == 2) {
          buffer.put(strToNum(data.substring(i, i + 2) ), 7);
        }
      }
    };

    var strToNum = function(s) {
      var num = 0;
      for (var i = 0; i < s.length; i += 1) {
        num = num * 10 + chatToNum(s.charAt(i) );
      }
      return num;
    };

    var chatToNum = function(c) {
      if ('0' <= c && c <= '9') {
        return c.charCodeAt(0) - '0'.charCodeAt(0);
      }
      throw 'illegal char :' + c;
    };

    return _this;
  };

  //---------------------------------------------------------------------
  // qrAlphaNum
  //---------------------------------------------------------------------

  var qrAlphaNum = function(data) {

    var _mode = QRMode.MODE_ALPHA_NUM;
    var _data = data;

    var _this = {};

    _this.getMode = function() {
      return _mode;
    };

    _this.getLength = function(buffer) {
      return _data.length;
    };

    _this.write = function(buffer) {

      var s = _data;

      var i = 0;

      while (i + 1 < s.length) {
        buffer.put(
          getCode(s.charAt(i) ) * 45 +
          getCode(s.charAt(i + 1) ), 11);
        i += 2;
      }

      if (i < s.length) {
        buffer.put(getCode(s.charAt(i) ), 6);
      }
    };

    var getCode = function(c) {

      if ('0' <= c && c <= '9') {
        return c.charCodeAt(0) - '0'.charCodeAt(0);
      } else if ('A' <= c && c <= 'Z') {
        return c.charCodeAt(0) - 'A'.charCodeAt(0) + 10;
      } else {
        switch (c) {
        case ' ' : return 36;
        case '$' : return 37;
        case '%' : return 38;
        case '*' : return 39;
        case '+' : return 40;
        case '-' : return 41;
        case '.' : return 42;
        case '/' : return 43;
        case ':' : return 44;
        default :
          throw 'illegal char :' + c;
        }
      }
    };

    return _this;
  };

  //---------------------------------------------------------------------
  // qr8BitByte
  //---------------------------------------------------------------------

  var qr8BitByte = function(data) {

    var _mode = QRMode.MODE_8BIT_BYTE;
    var _data = data;
    var _bytes = qrcode.stringToBytes(data);

    var _this = {};

    _this.getMode = function() {
      return _mode;
    };

    _this.getLength = function(buffer) {
      return _bytes.length;
    };

    _this.write = function(buffer) {
      for (var i = 0; i < _bytes.length; i += 1) {
        buffer.put(_bytes[i], 8);
      }
    };

    return _this;
  };

  //---------------------------------------------------------------------
  // qrKanji
  //---------------------------------------------------------------------

  var qrKanji = function(data) {

    var _mode = QRMode.MODE_KANJI;
    var _data = data;

    var stringToBytes = qrcode.stringToBytesFuncs['SJIS'];
    if (!stringToBytes) {
      throw 'sjis not supported.';
    }
    !function(c, code) {
      // self test for sjis support.
      var test = stringToBytes(c);
      if (test.length != 2 || ( (test[0] << 8) | test[1]) != code) {
        throw 'sjis not supported.';
      }
    }('\u53cb', 0x9746);

    var _bytes = stringToBytes(data);

    var _this = {};

    _this.getMode = function() {
      return _mode;
    };

    _this.getLength = function(buffer) {
      return ~~(_bytes.length / 2);
    };

    _this.write = function(buffer) {

      var data = _bytes;

      var i = 0;

      while (i + 1 < data.length) {

        var c = ( (0xff & data[i]) << 8) | (0xff & data[i + 1]);

        if (0x8140 <= c && c <= 0x9FFC) {
          c -= 0x8140;
        } else if (0xE040 <= c && c <= 0xEBBF) {
          c -= 0xC140;
        } else {
          throw 'illegal char at ' + (i + 1) + '/' + c;
        }

        c = ( (c >>> 8) & 0xff) * 0xC0 + (c & 0xff);

        buffer.put(c, 13);

        i += 2;
      }

      if (i < data.length) {
        throw 'illegal char at ' + (i + 1);
      }
    };

    return _this;
  };

  //=====================================================================
  // GIF Support etc.
  //

  //---------------------------------------------------------------------
  // byteArrayOutputStream
  //---------------------------------------------------------------------

  var byteArrayOutputStream = function() {

    var _bytes = [];

    var _this = {};

    _this.writeByte = function(b) {
      _bytes.push(b & 0xff);
    };

    _this.writeShort = function(i) {
      _this.writeByte(i);
      _this.writeByte(i >>> 8);
    };

    _this.writeBytes = function(b, off, len) {
      off = off || 0;
      len = len || b.length;
      for (var i = 0; i < len; i += 1) {
        _this.writeByte(b[i + off]);
      }
    };

    _this.writeString = function(s) {
      for (var i = 0; i < s.length; i += 1) {
        _this.writeByte(s.charCodeAt(i) );
      }
    };

    _this.toByteArray = function() {
      return _bytes;
    };

    _this.toString = function() {
      var s = '';
      s += '[';
      for (var i = 0; i < _bytes.length; i += 1) {
        if (i > 0) {
          s += ',';
        }
        s += _bytes[i];
      }
      s += ']';
      return s;
    };

    return _this;
  };

  //---------------------------------------------------------------------
  // base64EncodeOutputStream
  //---------------------------------------------------------------------

  var base64EncodeOutputStream = function() {

    var _buffer = 0;
    var _buflen = 0;
    var _length = 0;
    var _base64 = '';

    var _this = {};

    var writeEncoded = function(b) {
      _base64 += String.fromCharCode(encode(b & 0x3f) );
    };

    var encode = function(n) {
      if (n < 0) {
        // error.
      } else if (n < 26) {
        return 0x41 + n;
      } else if (n < 52) {
        return 0x61 + (n - 26);
      } else if (n < 62) {
        return 0x30 + (n - 52);
      } else if (n == 62) {
        return 0x2b;
      } else if (n == 63) {
        return 0x2f;
      }
      throw 'n:' + n;
    };

    _this.writeByte = function(n) {

      _buffer = (_buffer << 8) | (n & 0xff);
      _buflen += 8;
      _length += 1;

      while (_buflen >= 6) {
        writeEncoded(_buffer >>> (_buflen - 6) );
        _buflen -= 6;
      }
    };

    _this.flush = function() {

      if (_buflen > 0) {
        writeEncoded(_buffer << (6 - _buflen) );
        _buffer = 0;
        _buflen = 0;
      }

      if (_length % 3 != 0) {
        // padding
        var padlen = 3 - _length % 3;
        for (var i = 0; i < padlen; i += 1) {
          _base64 += '=';
        }
      }
    };

    _this.toString = function() {
      return _base64;
    };

    return _this;
  };

  //---------------------------------------------------------------------
  // base64DecodeInputStream
  //---------------------------------------------------------------------

  var base64DecodeInputStream = function(str) {

    var _str = str;
    var _pos = 0;
    var _buffer = 0;
    var _buflen = 0;

    var _this = {};

    _this.read = function() {

      while (_buflen < 8) {

        if (_pos >= _str.length) {
          if (_buflen == 0) {
            return -1;
          }
          throw 'unexpected end of file./' + _buflen;
        }

        var c = _str.charAt(_pos);
        _pos += 1;

        if (c == '=') {
          _buflen = 0;
          return -1;
        } else if (c.match(/^\s$/) ) {
          // ignore if whitespace.
          continue;
        }

        _buffer = (_buffer << 6) | decode(c.charCodeAt(0) );
        _buflen += 6;
      }

      var n = (_buffer >>> (_buflen - 8) ) & 0xff;
      _buflen -= 8;
      return n;
    };

    var decode = function(c) {
      if (0x41 <= c && c <= 0x5a) {
        return c - 0x41;
      } else if (0x61 <= c && c <= 0x7a) {
        return c - 0x61 + 26;
      } else if (0x30 <= c && c <= 0x39) {
        return c - 0x30 + 52;
      } else if (c == 0x2b) {
        return 62;
      } else if (c == 0x2f) {
        return 63;
      } else {
        throw 'c:' + c;
      }
    };

    return _this;
  };

  //---------------------------------------------------------------------
  // gifImage (B/W)
  //---------------------------------------------------------------------

  var gifImage = function(width, height) {

    var _width = width;
    var _height = height;
    var _data = new Array(width * height);

    var _this = {};

    _this.setPixel = function(x, y, pixel) {
      _data[y * _width + x] = pixel;
    };

    _this.write = function(out) {

      //---------------------------------
      // GIF Signature

      out.writeString('GIF87a');

      //---------------------------------
      // Screen Descriptor

      out.writeShort(_width);
      out.writeShort(_height);

      out.writeByte(0x80); // 2bit
      out.writeByte(0);
      out.writeByte(0);

      //---------------------------------
      // Global Color Map

      // black
      out.writeByte(0x00);
      out.writeByte(0x00);
      out.writeByte(0x00);

      // white
      out.writeByte(0xff);
      out.writeByte(0xff);
      out.writeByte(0xff);

      //---------------------------------
      // Image Descriptor

      out.writeString(',');
      out.writeShort(0);
      out.writeShort(0);
      out.writeShort(_width);
      out.writeShort(_height);
      out.writeByte(0);

      //---------------------------------
      // Local Color Map

      //---------------------------------
      // Raster Data

      var lzwMinCodeSize = 2;
      var raster = getLZWRaster(lzwMinCodeSize);

      out.writeByte(lzwMinCodeSize);

      var offset = 0;

      while (raster.length - offset > 255) {
        out.writeByte(255);
        out.writeBytes(raster, offset, 255);
        offset += 255;
      }

      out.writeByte(raster.length - offset);
      out.writeBytes(raster, offset, raster.length - offset);
      out.writeByte(0x00);

      //---------------------------------
      // GIF Terminator
      out.writeString(';');
    };

    var bitOutputStream = function(out) {

      var _out = out;
      var _bitLength = 0;
      var _bitBuffer = 0;

      var _this = {};

      _this.write = function(data, length) {

        if ( (data >>> length) != 0) {
          throw 'length over';
        }

        while (_bitLength + length >= 8) {
          _out.writeByte(0xff & ( (data << _bitLength) | _bitBuffer) );
          length -= (8 - _bitLength);
          data >>>= (8 - _bitLength);
          _bitBuffer = 0;
          _bitLength = 0;
        }

        _bitBuffer = (data << _bitLength) | _bitBuffer;
        _bitLength = _bitLength + length;
      };

      _this.flush = function() {
        if (_bitLength > 0) {
          _out.writeByte(_bitBuffer);
        }
      };

      return _this;
    };

    var getLZWRaster = function(lzwMinCodeSize) {

      var clearCode = 1 << lzwMinCodeSize;
      var endCode = (1 << lzwMinCodeSize) + 1;
      var bitLength = lzwMinCodeSize + 1;

      // Setup LZWTable
      var table = lzwTable();

      for (var i = 0; i < clearCode; i += 1) {
        table.add(String.fromCharCode(i) );
      }
      table.add(String.fromCharCode(clearCode) );
      table.add(String.fromCharCode(endCode) );

      var byteOut = byteArrayOutputStream();
      var bitOut = bitOutputStream(byteOut);

      // clear code
      bitOut.write(clearCode, bitLength);

      var dataIndex = 0;

      var s = String.fromCharCode(_data[dataIndex]);
      dataIndex += 1;

      while (dataIndex < _data.length) {

        var c = String.fromCharCode(_data[dataIndex]);
        dataIndex += 1;

        if (table.contains(s + c) ) {

          s = s + c;

        } else {

          bitOut.write(table.indexOf(s), bitLength);

          if (table.size() < 0xfff) {

            if (table.size() == (1 << bitLength) ) {
              bitLength += 1;
            }

            table.add(s + c);
          }

          s = c;
        }
      }

      bitOut.write(table.indexOf(s), bitLength);

      // end code
      bitOut.write(endCode, bitLength);

      bitOut.flush();

      return byteOut.toByteArray();
    };

    var lzwTable = function() {

      var _map = {};
      var _size = 0;

      var _this = {};

      _this.add = function(key) {
        if (_this.contains(key) ) {
          throw 'dup key:' + key;
        }
        _map[key] = _size;
        _size += 1;
      };

      _this.size = function() {
        return _size;
      };

      _this.indexOf = function(key) {
        return _map[key];
      };

      _this.contains = function(key) {
        return typeof _map[key] != 'undefined';
      };

      return _this;
    };

    return _this;
  };

  var createDataURL = function(width, height, getPixel) {
    var gif = gifImage(width, height);
    for (var y = 0; y < height; y += 1) {
      for (var x = 0; x < width; x += 1) {
        gif.setPixel(x, y, getPixel(x, y) );
      }
    }

    var b = byteArrayOutputStream();
    gif.write(b);

    var base64 = base64EncodeOutputStream();
    var bytes = b.toByteArray();
    for (var i = 0; i < bytes.length; i += 1) {
      base64.writeByte(bytes[i]);
    }
    base64.flush();

    return 'data:image/gif;base64,' + base64;
  };

  //---------------------------------------------------------------------
  // returns qrcode function.

  return qrcode;
}();

// multibyte support
!function() {

  qrcode.stringToBytesFuncs['UTF-8'] = function(s) {
    // http://stackoverflow.com/questions/18729405/how-to-convert-utf8-string-to-byte-array
    function toUTF8Array(str) {
      var utf8 = [];
      for (var i=0; i < str.length; i++) {
        var charcode = str.charCodeAt(i);
        if (charcode < 0x80) utf8.push(charcode);
        else if (charcode < 0x800) {
          utf8.push(0xc0 | (charcode >> 6),
              0x80 | (charcode & 0x3f));
        }
        else if (charcode < 0xd800 || charcode >= 0xe000) {
          utf8.push(0xe0 | (charcode >> 12),
              0x80 | ((charcode>>6) & 0x3f),
              0x80 | (charcode & 0x3f));
        }
        // surrogate pair
        else {
          i++;
          // UTF-16 encodes 0x10000-0x10FFFF by
          // subtracting 0x10000 and splitting the
          // 20 bits of 0x0-0xFFFFF into two halves
          charcode = 0x10000 + (((charcode & 0x3ff)<<10)
            | (str.charCodeAt(i) & 0x3ff));
          utf8.push(0xf0 | (charcode >>18),
              0x80 | ((charcode>>12) & 0x3f),
              0x80 | ((charcode>>6) & 0x3f),
              0x80 | (charcode & 0x3f));
        }
      }
      return utf8;
    }
    return toUTF8Array(s);
  };

}();

(function (factory) {
  if (typeof define === 'function' && define.amd) {
      define([], factory);
  } else if (typeof exports === 'object') {
      module.exports = factory();
  }
}(function () {
    return qrcode;
}));

//---------------------------------------------------------------------
//
// QR Code Generator for JavaScript UTF8 Support (optional)
//
// Copyright (c) 2011 Kazuhiko Arase
//
// URL: http://www.d-project.com/
//
// Licensed under the MIT license:
//  http://www.opensource.org/licenses/mit-license.php
//
// The word 'QR Code' is registered trademark of
// DENSO WAVE INCORPORATED
//  http://www.denso-wave.com/qrcode/faqpatent-e.html
//
//---------------------------------------------------------------------

!function(qrcode) {

  //---------------------------------------------------------------------
  // overwrite qrcode.stringToBytes
  //---------------------------------------------------------------------

  qrcode.stringToBytes = qrcode.stringToBytesFuncs['UTF-8'];

}(qrcode);

`;

// ─── Utils ─────────────────────────────────────────────────────────────────
const fmt=b=>{b=Number(b)||0;if(b<1024)return b+" B";if(b<1048576)return(b/1024).toFixed(1)+" KB";if(b<1073741824)return(b/1048576).toFixed(1)+" MB";return(b/1073741824).toFixed(2)+" GB";};
const daysLeft=s=>Math.max(0,Math.floor((s.created_at+s.expire_days*86400000-Date.now())/86400000));
const isExp=s=>Date.now()>s.created_at+s.expire_days*86400000||(s.used_bytes||0)>=s.quota_gb*1073741824;

// ═══════════════════════════════════════════════════════════════════════════
//  SVG ICONS (inline, no CDN)
// ═══════════════════════════════════════════════════════════════════════════
const ic=(d,s=16)=>`<svg width="${s}" height="${s}" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">${d}</svg>`;
const I={
  zap:    ic(`<polygon points="13 2 3 14 12 14 11 22 21 10 12 10 13 2"/>`,18),
  zapS:   ic(`<polygon points="13 2 3 14 12 14 11 22 21 10 12 10 13 2"/>`,15),
  users:  ic(`<path d="M17 21v-2a4 4 0 0 0-4-4H5a4 4 0 0 0-4 4v2"/><circle cx="9" cy="7" r="4"/><path d="M23 21v-2a4 4 0 0 0-3-3.87"/><path d="M16 3.13a4 4 0 0 1 0 7.75"/>`),
  act:    ic(`<polyline points="22 12 18 12 15 21 9 3 6 12 2 12"/>`),
  clock:  ic(`<circle cx="12" cy="12" r="10"/><polyline points="12 6 12 12 16 14"/>`,14),
  link:   ic(`<path d="M10 13a5 5 0 0 0 7.54.54l3-3a5 5 0 0 0-7.07-7.07l-1.72 1.71"/><path d="M14 11a5 5 0 0 0-7.54-.54l-3 3a5 5 0 0 0 7.07 7.07l1.71-1.71"/>`,14),
  trash:  ic(`<polyline points="3 6 5 6 21 6"/><path d="M19 6v14a2 2 0 0 1-2 2H7a2 2 0 0 1-2-2V6m3 0V4a2 2 0 0 1 2-2h4a2 2 0 0 1 2 2v2"/>`,14),
  pause:  ic(`<rect x="6" y="4" width="4" height="16"/><rect x="14" y="4" width="4" height="16"/>`,14),
  play:   ic(`<polygon points="5 3 19 12 5 21 5 3"/>`,14),
  shuf:   ic(`<polyline points="16 3 21 3 21 8"/><line x1="4" y1="20" x2="21" y2="3"/><polyline points="21 16 21 21 16 21"/><line x1="15" y1="15" x2="21" y2="21"/><line x1="4" y1="4" x2="9" y2="9"/>`,14),
  plus:   ic(`<line x1="12" y1="5" x2="12" y2="19"/><line x1="5" y1="12" x2="19" y2="12"/>`),
  dl:     ic(`<path d="M21 15v4a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2v-4"/><polyline points="7 10 12 15 17 10"/><line x1="12" y1="15" x2="12" y2="3"/>`,20),
  copy:   ic(`<rect x="9" y="9" width="13" height="13" rx="2"/><path d="M5 15H4a2 2 0 0 1-2-2V4a2 2 0 0 1 2-2h9a2 2 0 0 1 2 2v1"/>`,14),
  sync:   ic(`<polyline points="23 4 23 10 17 10"/><polyline points="1 20 1 14 7 14"/><path d="M3.51 9a9 9 0 0 1 14.85-3.36L23 10M1 14l4.64 4.36A9 9 0 0 0 20.49 15"/>`,14),
  set:    ic(`<circle cx="12" cy="12" r="3"/><path d="M19.4 15a1.65 1.65 0 0 0 .33 1.82l.06.06a2 2 0 0 1-2.83 2.83l-.06-.06a1.65 1.65 0 0 0-1.82-.33 1.65 1.65 0 0 0-1 1.51V21a2 2 0 0 1-4 0v-.09A1.65 1.65 0 0 0 9 19.4a1.65 1.65 0 0 0-1.82.33l-.06.06a2 2 0 0 1-2.83-2.83l.06-.06A1.65 1.65 0 0 0 4.68 15a1.65 1.65 0 0 0-1.51-1H3a2 2 0 0 1 0-4h.09A1.65 1.65 0 0 0 4.6 9a1.65 1.65 0 0 0-.33-1.82l-.06-.06a2 2 0 0 1 2.83-2.83l.06.06A1.65 1.65 0 0 0 9 4.68a1.65 1.65 0 0 0 1-1.51V3a2 2 0 0 1 4 0v.09a1.65 1.65 0 0 0 1 1.51 1.65 1.65 0 0 0 1.82-.33l.06-.06a2 2 0 0 1 2.83 2.83l-.06.06A1.65 1.65 0 0 0 19.4 9a1.65 1.65 0 0 0 1.51 1H21a2 2 0 0 1 0 4h-.09a1.65 1.65 0 0 0-1.51 1z"/>`,16),
  logout: ic(`<path d="M9 21H5a2 2 0 0 1-2-2V5a2 2 0 0 1 2-2h4"/><polyline points="16 17 21 12 16 7"/><line x1="21" y1="12" x2="9" y2="12"/>`,14),
  lock:   ic(`<rect x="3" y="11" width="18" height="11" rx="2"/><path d="M7 11V7a5 5 0 0 1 10 0v4"/>`,42),
  ref:    ic(`<polyline points="1 4 1 10 7 10"/><path d="M3.51 15a9 9 0 1 0 .49-3.33"/>`,14),
  cal:    ic(`<rect x="3" y="4" width="18" height="18" rx="2"/><line x1="16" y1="2" x2="16" y2="6"/><line x1="8" y1="2" x2="8" y2="6"/><line x1="3" y1="10" x2="21" y2="10"/>`,14),
  eye:    ic(`<path d="M1 12s4-8 11-8 11 8 11 8-4 8-11 8-11-8-11-8z"/><circle cx="12" cy="12" r="3"/>`,14),
  eyeOff: ic(`<path d="M17.94 17.94A10.07 10.07 0 0 1 12 20c-7 0-11-8-11-8a18.45 18.45 0 0 1 5.06-5.94M9.9 4.24A9.12 9.12 0 0 1 12 4c7 0 11 8 11 8a18.5 18.5 0 0 1-2.16 3.19m-6.72-1.07a3 3 0 1 1-4.24-4.24"/><line x1="1" y1="1" x2="23" y2="23"/>`,14),
  qr:     ic(`<rect x="3" y="3" width="7" height="7"/><rect x="14" y="3" width="7" height="7"/><rect x="14" y="14" width="7" height="7"/><rect x="3" y="14" width="7" height="7"/><rect x="5" y="5" width="3" height="3"/><rect x="16" y="5" width="3" height="3"/><rect x="16" y="16" width="3" height="3"/><rect x="5" y="16" width="3" height="3"/>`,18),
  key:    ic(`<path d="M21 2l-2 2m-7.61 7.61a5.5 5.5 0 1 1-7.778 7.778 5.5 5.5 0 0 1 7.777-7.777zm0 0L15.5 7.5m0 0l3 3L22 7l-3-3m-3.5 3.5L19 4"/>`,16),
  shield: ic(`<path d="M12 22s8-4 8-10V5l-8-3-8 3v7c0 6 8 10 8 10z"/>`,16),
  check:  ic(`<polyline points="20 6 9 17 4 12"/>`,14),
  close:  ic(`<line x1="18" y1="6" x2="6" y2="18"/><line x1="6" y1="6" x2="18" y2="18"/>`,14),
  palette:ic(`<circle cx="13.5" cy="6.5" r="1"/><circle cx="17.5" cy="10.5" r="1"/><circle cx="8.5" cy="7.5" r="1"/><circle cx="6.5" cy="12.5" r="1"/><path d="M12 2C6.5 2 2 6.5 2 12s4.5 10 10 10c1.1 0 2-.9 2-2 0-.5-.2-1-.5-1.4-.3-.4-.5-.8-.5-1.4 0-1.1.9-2 2-2h2.4c2.5 0 4.6-2.1 4.6-4.6C22 6.1 17.5 2 12 2z"/>`,14),
  sun:    ic(`<circle cx="12" cy="12" r="5"/><line x1="12" y1="1" x2="12" y2="3"/><line x1="12" y1="21" x2="12" y2="23"/><line x1="4.22" y1="4.22" x2="5.64" y2="5.64"/><line x1="18.36" y1="18.36" x2="19.78" y2="19.78"/><line x1="1" y1="12" x2="3" y2="12"/><line x1="21" y1="12" x2="23" y2="12"/><line x1="4.22" y1="19.78" x2="5.64" y2="18.36"/><line x1="18.36" y1="5.64" x2="19.78" y2="4.22"/>`,16),
  moon:   ic(`<path d="M21 12.79A9 9 0 1 1 11.21 3 7 7 0 0 0 21 12.79z"/>`,16),
  globe:  ic(`<circle cx="12" cy="12" r="10"/><line x1="2" y1="12" x2="22" y2="12"/><path d="M12 2a15.3 15.3 0 0 1 4 10 15.3 15.3 0 0 1-4 10 15.3 15.3 0 0 1-4-10 15.3 15.3 0 0 1 4-10z"/>`,16),
  db:     ic(`<ellipse cx="12" cy="5" rx="9" ry="3"/><path d="M21 12c0 1.66-4 3-9 3s-9-1.34-9-3"/><path d="M3 5v14c0 1.66 4 3 9 3s9-1.34 9-3V5"/>`,14),
  warn:   ic(`<path d="M10.29 3.86L1.82 18a2 2 0 0 0 1.71 3h16.94a2 2 0 0 0 1.71-3L13.71 3.86a2 2 0 0 0-3.42 0z"/><line x1="12" y1="9" x2="12" y2="13"/><line x1="12" y1="17" x2="12.01" y2="17"/>`,30),
  ban:    ic(`<circle cx="12" cy="12" r="10"/><line x1="4.93" y1="4.93" x2="19.07" y2="19.07"/>`,30),
  edit:   ic(`<path d="M11 4H4a2 2 0 0 0-2 2v14a2 2 0 0 0 2 2h14a2 2 0 0 0 2-2v-7"/><path d="M18.5 2.5a2.121 2.121 0 0 1 3 3L12 15l-4 1 1-4 9.5-9.5z"/>`,14),
  crown:  ic(`<path d="M2 20h20M4 20l-1-9 5 4 4-7 4 7 5-4-1 9"/>`,15),
  panel:  ic(`<rect x="3" y="3" width="18" height="18" rx="2"/><line x1="3" y1="9" x2="21" y2="9"/><line x1="9" y1="21" x2="9" y2="9"/>`,16),
  retry:  ic(`<polyline points="1 4 1 10 7 10"/><path d="M3.51 15a9 9 0 1 0 .49-3.33L1 10"/>`,14),
  userP:  ic(`<circle cx="12" cy="8" r="4"/><path d="M4 20c0-4 4-6 8-6s8 2 8 6"/>`,14),
  mask:   ic(`<path d="M4 9c0-2 2-3 4-3s3 1 4 3c1-2 2-3 4-3s4 1 4 3v3c0 5-4 9-8 9s-8-4-8-9V9z"/><circle cx="8.5" cy="11" r="1"/><circle cx="15.5" cy="11" r="1"/>`,15),
  sat:    ic(`<path d="M13 7l4 4-1.5 1.5L11 8.5 13 7z"/><path d="M8 12l4 4-3 3-4-4 3-3z"/><path d="M2 22l4-4"/><path d="M16 2c2 2 3 4 3 6"/><path d="M13 4c1.2 1 2 2.4 2 4"/>`,15),
  grid:   ic(`<rect x="3" y="3" width="7" height="7" rx="1.5"/><rect x="14" y="3" width="7" height="7" rx="1.5"/><rect x="3" y="14" width="7" height="7" rx="1.5"/><rect x="14" y="14" width="7" height="7" rx="1.5"/>`,15),
};

// ═══════════════════════════════════════════════════════════════════════════
//  CSS
// ═══════════════════════════════════════════════════════════════════════════
const CSS=`
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700;800;900&family=Vazirmatn:wght@400;600;800;900&display=swap');
*{box-sizing:border-box;margin:0;padding:0}
:root{
  --bg:#05060f;--s1:#0c0e1c;--s2:#13162b;--s3:#1c2040;
  --bd:rgba(255,255,255,.08);--bdh:rgba(255,255,255,.16);
  --in:#7c5cff;--vi:#c026d3;--cy:#22e0ff;--pk:#ff4d8d;
  --em:#10b981;--ro:#f43f5e;--am:#f59e0b;
  --t1:#f8f9ff;--t2:#9aa3c4;--t3:#4e5678;
  --gi:linear-gradient(135deg,#7c5cff,#ff4d8d 55%,#22e0ff);
  --gi2:linear-gradient(120deg,#ff4d8d,#c026d3,#7c5cff);
  --r:14px;--sh:0 10px 36px rgba(0,0,0,.6);--sh-glow:0 0 0 1px color-mix(in srgb,var(--vi) 18%,transparent),0 16px 48px color-mix(in srgb,var(--in) 22%,transparent);
}
[data-theme=light]{
  --bg:#f0f2fb;--s1:#ffffff;--s2:#f5f7ff;--s3:#eaecf7;
  --bd:rgba(0,0,0,.07);--bdh:rgba(0,0,0,.14);
  --t1:#0f1629;--t2:#3d4a6b;--t3:#7b8ab0;
  --sh:0 4px 20px color-mix(in srgb,var(--in) 8%,transparent);--sh-glow:0 4px 20px color-mix(in srgb,var(--in) 12%,transparent);
}
html,body{min-height:100%;background:var(--bg)}
body{font-family:'Vazirmatn','Inter',Tahoma,sans-serif;color:var(--t1);font-size:14px;
  line-height:1.55;direction:rtl;-webkit-font-smoothing:antialiased;
  transition:background .25s,color .25s;overflow-x:hidden}
body::after{content:'';position:fixed;inset:0;z-index:0;pointer-events:none;opacity:.5;
  background-image:radial-gradient(rgba(255,255,255,.045) 1px,transparent 1px);
  background-size:26px 26px;mask-image:radial-gradient(ellipse 80% 60% at 50% 0%,#000 40%,transparent 100%)}
[data-theme=light] body::after{display:none}
body::before{content:'';position:fixed;inset:-10%;z-index:0;pointer-events:none;filter:blur(10px);
  background:
    radial-gradient(ellipse 900px 600px at -10% -5%,color-mix(in srgb,var(--in) 14%,transparent) 0%,transparent 60%),
    radial-gradient(ellipse 700px 500px at 110% 15%,color-mix(in srgb,var(--vi) 10%,transparent) 0%,transparent 60%),
    radial-gradient(ellipse 800px 550px at 100% 100%,color-mix(in srgb,var(--cy) 10%,transparent) 0%,transparent 60%),
    radial-gradient(ellipse 600px 500px at 0% 100%,color-mix(in srgb,var(--vi) 10%,transparent) 0%,transparent 60%);
  animation:auroraDrift 22s ease-in-out infinite alternate}
@keyframes auroraDrift{0%{transform:translate3d(0,0,0) scale(1)}100%{transform:translate3d(-2%,2%,0) scale(1.06)}}
[data-theme=light] body::before{opacity:.35}
.wrap{position:relative;z-index:1}

/* NAV */
.nav{position:sticky;top:0;z-index:200;height:58px;
  display:flex;align-items:center;justify-content:space-between;padding:0 24px;
  background:rgba(5,6,12,.72);border-bottom:1px solid var(--bd);
  backdrop-filter:blur(26px) saturate(160%);-webkit-backdrop-filter:blur(26px) saturate(160%);
  transition:background .25s;position:sticky}
.nav::after{content:'';position:absolute;left:0;right:0;bottom:-1px;height:1px;
  background:var(--gi2);opacity:.55}
[data-theme=light] .nav{background:rgba(255,255,255,.85)}
.nav-l{display:flex;align-items:center;gap:10px}
.nav-logo{width:34px;height:34px;border-radius:10px;background:var(--gi);
  display:flex;align-items:center;justify-content:center;color:#fff;flex-shrink:0;
  box-shadow:0 0 18px color-mix(in srgb,var(--vi) 55%,transparent);animation:logoPulse 3s ease-in-out infinite}
@keyframes logoPulse{0%,100%{box-shadow:0 0 14px color-mix(in srgb,var(--in) 45%,transparent)}50%{box-shadow:0 0 26px color-mix(in srgb,var(--vi) 75%,transparent)}}
.nav-name{font-size:15px;font-weight:800;letter-spacing:-.3px;
  background:var(--gi);-webkit-background-clip:text;background-clip:text;color:transparent}
.nav-sub{font-size:11px;color:var(--t3);margin-top:1px}
.nav-r{display:flex;align-items:center;gap:6px}
.status-pill{display:none;align-items:center;gap:5px;margin-inline-start:14px;padding:4px 11px;
  border-radius:20px;background:rgba(16,185,129,.12);border:1px solid rgba(16,185,129,.25);
  color:var(--em);font-size:11px;font-weight:700}
@media(min-width:640px){.status-pill{display:inline-flex}}
.ham-item.active,.ham-item:hover{background:color-mix(in srgb,var(--in) 10%,transparent);color:var(--in);
  box-shadow:inset 0 0 0 1px color-mix(in srgb,var(--in) 35%,transparent)}
.ham-wrap{position:relative}
.ham-menu{position:absolute;top:calc(100% + 8px);left:0;min-width:190px;background:var(--bg2,#161a22);
  border:1px solid var(--bd,rgba(255,255,255,.08));border-radius:12px;padding:6px;display:none;flex-direction:column;
  gap:2px;z-index:50;box-shadow:0 12px 32px rgba(0,0,0,.35)}
.ham-menu.show{display:flex}
.ham-item{display:flex;align-items:center;gap:8px;padding:9px 10px;border-radius:8px;font-size:13px;
  color:var(--t2,#c7ccd6);text-decoration:none;white-space:nowrap;transition:background .15s,box-shadow .15s}

/* DRAWER — منوی کشویی اصلی پنل */
.drawer-backdrop{position:fixed;inset:0;z-index:590;background:rgba(0,0,0,.55);
  backdrop-filter:blur(3px);-webkit-backdrop-filter:blur(3px);
  opacity:0;pointer-events:none;transition:opacity .25s}
.drawer-backdrop.show{opacity:1;pointer-events:auto}
.drawer{position:fixed;top:0;bottom:0;right:0;z-index:600;width:min(310px,86vw);
  background:var(--s1);border-left:1px solid var(--bd);
  display:flex;flex-direction:column;
  transform:translateX(100%);transition:transform .28s cubic-bezier(.22,1,.36,1);
  box-shadow:-24px 0 60px rgba(0,0,0,.5)}
.drawer.open{transform:translateX(0)}
.drawer-hd{display:flex;align-items:center;gap:12px;padding:20px 18px;border-bottom:1px solid var(--bd);
  position:relative}
.drawer-hd::after{content:'';position:absolute;bottom:-1px;left:0;right:0;height:2px;background:var(--gi2);opacity:.5}
.drawer-avatar{width:42px;height:42px;border-radius:12px;background:var(--gi);flex-shrink:0;
  display:flex;align-items:center;justify-content:center;color:#fff;font-weight:800;font-size:17px;
  box-shadow:0 8px 20px color-mix(in srgb,var(--in) 40%,transparent)}
.drawer-uname{font-size:14px;font-weight:800;color:var(--t1)}
.drawer-urole{font-size:11px;color:var(--am);margin-top:2px;display:flex;align-items:center;gap:4px}
.drawer-body{flex:1;overflow-y:auto;padding:14px 12px;display:flex;flex-direction:column;gap:2px}
.drawer-sec{font-size:10px;font-weight:700;color:var(--t3);text-transform:uppercase;
  letter-spacing:1px;margin:14px 10px 4px;}
.drawer-sec:first-child{margin-top:2px}
.drawer-item{display:flex;align-items:center;gap:11px;padding:11px 12px;border-radius:10px;
  font-size:13px;font-weight:500;color:var(--t2);text-decoration:none;
  transition:background .15s,color .15s,transform .15s}
.drawer-item:hover{background:color-mix(in srgb,var(--in) 10%,transparent);color:var(--in);transform:translateX(-2px)}
.drawer-item svg{flex-shrink:0}
.drawer-ft{padding:14px 12px;border-top:1px solid var(--bd)}
.drawer-logout{color:var(--ro)}
.drawer-logout:hover{background:rgba(244,63,94,.1);color:var(--ro);transform:translateX(-2px)}

/* ROW TOGGLE SWITCH + ROW MENU (داشبورد) */
.tgl-sw{width:40px;height:22px;border-radius:20px;border:none;cursor:pointer;flex-shrink:0;
  background:rgba(244,63,94,.25);display:flex;align-items:center;padding:2px;transition:background .2s}
.tgl-sw.on{background:linear-gradient(135deg,#10b981,#22d3ee)}
.tgl-dot{width:18px;height:18px;border-radius:50%;background:#fff;box-shadow:0 1px 3px rgba(0,0,0,.4);
  transition:margin-inline-start .2s}
.tgl-sw.on .tgl-dot{margin-inline-start:18px}
.acts-wrap{position:relative;display:inline-block}
/* position:fixed عمداً — چون این منو داخل یه جدول با اسکرول افقیه، اگه
   absolute نسبت به سلول جدول باشه، گیر می‌کنه توی stacking context ردیف و
   پشت ردیف‌های بعدی محو/بریده می‌شه (خصوصاً موبایل). با fixed + مختصات JS
   این مشکل کلاً حل می‌شه چون دیگه به جدول وابسته نیست */
.row-menu{display:none;position:fixed;z-index:900;width:230px;max-height:calc(100dvh - 16px);overflow-y:auto;overscroll-behavior:contain;-webkit-overflow-scrolling:touch;
  background:var(--s1);border:1px solid var(--bdh);border-radius:12px;padding:8px;
  flex-direction:column;gap:2px;box-shadow:0 20px 50px rgba(0,0,0,.65),0 0 0 1px rgba(0,0,0,.3)}
.row-menu.show{display:flex}
.row-menu-backdrop{display:none;position:fixed;inset:0;z-index:895;background:rgba(0,0,0,.35)}
.row-menu-backdrop.show{display:block}
.row-menu-form{display:flex;align-items:center;gap:6px;padding:6px 8px}
.row-menu-form svg{flex-shrink:0;color:var(--t3)}
.row-menu-form input{flex:1;min-width:0;background:var(--s2);border:1px solid var(--bd);border-radius:7px;
  padding:6px 8px;color:var(--t1);font-size:11px;font-family:inherit;outline:none}

/* ACCENT PICKER */
.accent-menu{min-width:auto;padding:10px}
.accent-row{display:flex;gap:8px}
.accent-dot{width:24px;height:24px;border-radius:50%;cursor:pointer;flex-shrink:0;
  border:2px solid transparent;transition:transform .15s,border-color .15s;box-shadow:0 0 0 1px rgba(255,255,255,.06) inset}
.accent-dot:hover{transform:scale(1.15)}
.accent-dot.sel{border-color:var(--t1)}

/* LAYOUT */
.pg{max-width:1180px;margin:0 auto;padding:24px 18px}
.pg-sm{max-width:560px;margin:0 auto;padding:24px 16px}

/* STATS */
.stats{display:grid;grid-template-columns:repeat(4,1fr);gap:12px;margin-bottom:24px}
@media(max-width:680px){.stats{grid-template-columns:1fr 1fr}}
.sc{position:relative;background:var(--s1);border:1px solid var(--bd);border-radius:var(--r);
  padding:16px;overflow:hidden;transition:border-color .2s,transform .2s,box-shadow .2s}
.sc::before{content:'';position:absolute;inset:0;opacity:0;transition:opacity .25s;
  background:radial-gradient(120px 80px at 15% 0%,color-mix(in srgb,var(--vi) 16%,transparent),transparent 70%)}
.sc:hover{border-color:var(--bdh);transform:translateY(-3px);box-shadow:var(--sh-glow)}
.sc:hover::before{opacity:1}
.sc-ic{width:36px;height:36px;border-radius:10px;display:flex;align-items:center;
  justify-content:center;margin-bottom:12px;box-shadow:0 0 0 1px rgba(255,255,255,.04) inset}
.ii{background:color-mix(in srgb,var(--in) 16%,transparent);color:var(--in);box-shadow:0 0 16px color-mix(in srgb,var(--in) 25%,transparent)}
.ig{background:rgba(16,185,129,.14);color:var(--em);box-shadow:0 0 16px rgba(16,185,129,.22)}
.ic{background:color-mix(in srgb,var(--cy) 12%,transparent);color:var(--cy);box-shadow:0 0 16px color-mix(in srgb,var(--cy) 22%,transparent)}
.ir{background:rgba(244,63,94,.12);color:var(--ro);box-shadow:0 0 16px rgba(244,63,94,.22)}
.ia{background:rgba(245,158,11,.12);color:var(--am);box-shadow:0 0 16px rgba(245,158,11,.22)}
.sc-v{position:relative;font-size:25px;font-weight:900;letter-spacing:-1px;color:var(--t1);line-height:1}
.sc-l{position:relative;font-size:11px;color:var(--t3);margin-top:4px;font-weight:500;
  text-transform:uppercase;letter-spacing:.4px}
.sc-bar{position:relative;margin-top:10px;height:5px;border-radius:4px;background:var(--s2);overflow:hidden}
.sc-bar-f{height:100%;border-radius:4px;transition:width .5s ease}
.refresh-bar{display:flex;align-items:center;justify-content:space-between;gap:10px;
  background:var(--s1);border:1px solid var(--bd);border-radius:12px;padding:10px 16px;
  margin-bottom:16px;font-size:12px;color:var(--t3)}
.refresh-bar b{color:var(--t2);font-weight:600}

/* CARD */
.card{position:relative;background:var(--s1);border:1px solid var(--bd);border-radius:var(--r);
  overflow:hidden;margin-bottom:14px;transition:box-shadow .25s,border-color .25s}
.card::before{content:'';position:absolute;top:0;left:0;right:0;height:2px;background:var(--gi);
  opacity:.55}
.card:hover{border-color:var(--bdh)}
.card-hd{padding:13px 18px;border-bottom:1px solid var(--bd);
  display:flex;align-items:center;justify-content:space-between}
.card-t{font-size:11px;font-weight:700;color:var(--t3);
  text-transform:uppercase;letter-spacing:.8px;display:flex;align-items:center;gap:7px}
.card-b{padding:18px}

/* TABLE */
.tbw{overflow-x:auto}
table{width:100%;border-collapse:collapse}
th{background:var(--s2);padding:9px 14px;text-align:right;font-size:11px;font-weight:600;
  letter-spacing:.5px;text-transform:uppercase;color:var(--t3);border-bottom:1px solid var(--bd)}
td{padding:11px 14px;border-bottom:1px solid var(--bd);vertical-align:middle}
[data-theme=light] td{border-bottom:1px solid rgba(0,0,0,.04)}
tr:last-child td{border-bottom:none}
tbody tr{transition:background .1s}
tbody tr:hover td{background:color-mix(in srgb,var(--in) 3%,transparent)}
.nm{font-size:14px;font-weight:700;color:var(--t1);letter-spacing:-.2px}
.dt{font-size:11px;color:var(--t3);margin-top:2px}

/* BADGE */
.badge{display:inline-flex;align-items:center;gap:4px;
  padding:3px 8px;border-radius:20px;font-size:11px;font-weight:600}
.dot{width:6px;height:6px;border-radius:50%;flex-shrink:0}
.bg{background:rgba(16,185,129,.1);color:var(--em);border:1px solid rgba(16,185,129,.18)}
.bg .dot{background:var(--em);animation:pl 2s infinite;box-shadow:0 0 5px var(--em)}
.br{background:rgba(244,63,94,.09);color:var(--ro);border:1px solid rgba(244,63,94,.18)}
.br .dot{background:var(--ro)}
.ba{background:rgba(245,158,11,.08);color:var(--am);border:1px solid rgba(245,158,11,.15)}
.ba .dot{background:var(--am)}
.bd2{background:var(--s2);color:var(--t3);border:1px solid var(--bd)}
.bd2 .dot{background:var(--t3)}
@keyframes pl{0%,100%{opacity:1}50%{opacity:.35}}

/* PROGRESS */
.pt{height:5px;background:var(--s3);border-radius:99px;overflow:hidden;margin-top:4px}
.pb{height:100%;border-radius:99px;transition:width .5s;
  background:linear-gradient(90deg,var(--in),var(--vi))}
.pb.hot{background:linear-gradient(90deg,#f97316,var(--ro))}
.uc{display:flex;justify-content:space-between;font-size:11px;color:var(--t2);margin-bottom:2px}
.ur{font-size:11px;color:var(--t3);margin-top:3px}

/* ACTIONS */
.acts{display:flex;align-items:center;gap:4px;flex-wrap:wrap}

/* BUTTONS */
.btn{display:inline-flex;align-items:center;justify-content:center;gap:5px;
  padding:6px 12px;border-radius:8px;border:none;cursor:pointer;font-size:12px;
  font-weight:600;font-family:inherit;text-decoration:none;transition:all .15s;white-space:nowrap}
.bp{position:relative;overflow:hidden;background:var(--gi);background-size:200% 200%;color:#fff;
  box-shadow:0 1px 3px rgba(0,0,0,.3);animation:gradShift 6s ease infinite}
@keyframes gradShift{0%,100%{background-position:0% 50%}50%{background-position:100% 50%}}
.bp::after{content:'';position:absolute;inset:0;background:linear-gradient(115deg,transparent 30%,rgba(255,255,255,.35) 50%,transparent 70%);
  transform:translateX(-120%);transition:transform .5s}
.bp:hover::after{transform:translateX(120%)}
.bp:hover{transform:translateY(-1px);box-shadow:0 6px 20px color-mix(in srgb,var(--vi) 45%,transparent)}
.bo{background:transparent;color:var(--t2);border:1px solid var(--bd)}
.bo.active{background:color-mix(in srgb,var(--in) 12%,transparent);color:#818cf8;border-color:color-mix(in srgb,var(--in) 35%,transparent)}
.bo:hover{border-color:var(--bdh);color:var(--t1);background:var(--s2)}
.be{background:rgba(16,185,129,.08);color:var(--em);border:1px solid rgba(16,185,129,.18)}
.be:hover{background:rgba(16,185,129,.16)}
.bro{background:rgba(244,63,94,.08);color:var(--ro);border:1px solid rgba(244,63,94,.18)}
.bro:hover{background:rgba(244,63,94,.16)}
.bam{background:rgba(245,158,11,.07);color:var(--am);border:1px solid rgba(245,158,11,.15)}
.bam:hover{background:rgba(245,158,11,.14)}
.bcy{background:color-mix(in srgb,var(--cy) 7%,transparent);color:var(--cy);border:1px solid color-mix(in srgb,var(--cy) 14%,transparent)}
.bcy:hover{background:color-mix(in srgb,var(--cy) 14%,transparent)}
.bsm{padding:5px 9px;font-size:12px}
.bic{padding:5px 8px}
.btn:disabled{opacity:.4;cursor:not-allowed;transform:none!important}

/* GLOW CARD — کارت ساخت اشتراک با یه لبه‌ی گرادیانی ظریف */
.glow-card{position:relative;border:1px solid transparent;background:
  linear-gradient(var(--s1),var(--s1)) padding-box,
  linear-gradient(120deg,var(--in),var(--vi),var(--cy)) border-box}
.glow-card .card-hd{border-bottom:1px solid var(--bd)}

/* V2 EXTRA — بخش کانفیگ اختصاصی (کشور/پروکسی) */
.v2-extra{margin-top:16px;padding:14px;border-radius:10px;
  border:1px dashed color-mix(in srgb,var(--cy) 30%,transparent);background:color-mix(in srgb,var(--cy) 5%,transparent);
  display:flex;flex-direction:column;gap:10px}
.v2-extra-hd{display:flex;flex-direction:column;gap:2px}
.v2-extra-title{font-size:12px;font-weight:700;color:var(--cy)}
.v2-extra-sub{font-size:11px;color:var(--t3);line-height:1.6}
select{width:100%;background:var(--s2);border:1px solid var(--bd);border-radius:8px;
  padding:7px 11px;color:var(--t1);font-size:13px;font-family:inherit;
  outline:none;transition:border-color .15s,box-shadow .15s;cursor:pointer}
select:focus,input:focus{border-color:var(--in);box-shadow:0 0 0 3px color-mix(in srgb,var(--in) 15%,transparent)}

/* FORM */
.fg{display:grid;grid-template-columns:1fr 110px 100px auto;gap:9px;align-items:end}
@media(max-width:700px){.fg{grid-template-columns:1fr 1fr;row-gap:9px}}
.fl label{display:block;font-size:11px;font-weight:600;color:var(--t3);
  margin-bottom:5px;letter-spacing:.4px;text-transform:uppercase}
.igrp{display:flex;gap:5px}
.igrp input{flex:1}
input[type=text],input[type=number],input[type=password]{
  width:100%;background:var(--s2);border:1px solid var(--bd);border-radius:8px;
  padding:7px 11px;color:var(--t1);font-size:13px;font-family:inherit;
  outline:none;transition:border-color .15s,box-shadow .15s}
input:focus{border-color:var(--in);box-shadow:0 0 0 3px color-mix(in srgb,var(--in) 12%,transparent)}
input::placeholder{color:var(--t3)}

/* INLINE EDIT FORMS */
.ief{display:flex;gap:4px;align-items:center}
.ief input{width:65px;padding:4px 7px;font-size:11px}

/* LOGIN */
.login-pg{min-height:100vh;display:flex;flex-direction:column;align-items:center;justify-content:center;padding:20px 16px;
  background:radial-gradient(ellipse 80% 70% at 50% 45%,color-mix(in srgb,var(--in) 14%,transparent) 0%,var(--bg) 65%)}
.login-bx{position:relative;background:var(--s1);border:1px solid var(--bd);border-radius:20px;
  padding:36px 32px;width:100%;max-width:380px;overflow:hidden;
  box-shadow:0 28px 56px rgba(0,0,0,.55),0 0 0 1px rgba(255,255,255,.03)}
.login-bx::before{content:'';position:absolute;top:0;left:0;right:0;height:3px;background:var(--gi2)}
.l-ic-wrap{position:relative;width:76px;height:76px;margin:4px auto 18px}
.l-ic-glow{position:absolute;inset:-14px;border-radius:50%;background:var(--gi);opacity:.35;filter:blur(20px);
  animation:logoPulse 3s ease-in-out infinite}
.l-ic{position:relative;width:76px;height:76px;border-radius:20px;background:var(--gi);
  display:flex;align-items:center;justify-content:center;color:#fff;
  box-shadow:0 14px 34px color-mix(in srgb,var(--in) 50%,transparent),inset 0 1px 0 rgba(255,255,255,.25)}
.l-t{text-align:center;font-size:22px;font-weight:900;letter-spacing:-.6px;margin-bottom:5px;
  background:var(--gi);-webkit-background-clip:text;background-clip:text;color:transparent}
.l-s{text-align:center;font-size:12px;color:var(--t2);margin-bottom:10px}
.l-ver{text-align:center;font-size:10px;font-weight:700;letter-spacing:1.5px;color:var(--t3);
  text-transform:uppercase;margin-bottom:24px}
.l-err{background:rgba(244,63,94,.07);border:1px solid rgba(244,63,94,.18);
  color:var(--ro);border-radius:8px;padding:9px 14px;
  font-size:12px;text-align:center;margin-bottom:14px}
.l-field{position:relative}
.l-field input{width:100%;box-sizing:border-box;padding:12px 40px 12px 40px;border-radius:12px;
  background:var(--s2);border:1px solid var(--bd);color:var(--t1);font-size:14px;font-family:inherit;
  text-align:right;outline:none;transition:border-color .15s,box-shadow .15s}
.l-field input:focus{border-color:var(--in);box-shadow:0 0 0 3px color-mix(in srgb,var(--in) 15%,transparent)}
.l-field svg{position:absolute;right:14px;top:50%;transform:translateY(-50%);color:var(--t3);pointer-events:none}
.l-status{display:flex;align-items:center;justify-content:center;gap:6px;margin-top:20px;
  font-size:11px;color:var(--em)}
.pw{position:relative}.pw input{padding-left:36px}
.pey{position:absolute;left:10px;top:50%;transform:translateY(-50%);
  background:none;border:none;cursor:pointer;color:var(--t3);display:flex;padding:0}
.pey:hover{color:var(--t2)}

/* SETTINGS */
.set-row{display:flex;align-items:center;justify-content:space-between;
  padding:14px;background:var(--s2);border:1px solid var(--bd);border-radius:10px;
  gap:16px;margin-bottom:8px}
.si-t{font-size:13px;font-weight:600;color:var(--t1)}
.si-d{font-size:11px;color:var(--t3);margin-top:2px}
.si-ctl{display:flex;align-items:center;gap:7px;flex-shrink:0}

/* SUB PAGE */
.sub-hero{text-align:center;padding:32px 0 20px}
.sub-ic{width:72px;height:72px;margin:0 auto 16px;border-radius:20px;
  border:1px solid color-mix(in srgb,var(--cy) 20%,transparent);background:color-mix(in srgb,var(--cy) 5%,transparent);
  display:flex;align-items:center;justify-content:center;color:var(--cy);
  box-shadow:0 0 36px color-mix(in srgb,var(--cy) 10%,transparent)}
.sub-t{font-size:24px;font-weight:900;letter-spacing:-.6px}
.two-up{display:grid;grid-template-columns:1fr 1fr;gap:10px;margin-bottom:12px}
.vb{background:var(--s2);border:1px solid var(--bd);border-radius:10px;padding:14px;text-align:center}
.vb-v{font-size:19px;font-weight:800;letter-spacing:-.4px}
.vb-l{font-size:11px;color:var(--t3);margin-top:3px;text-transform:uppercase;
  letter-spacing:.4px;font-weight:500}
.dl-r{display:flex;align-items:center;gap:12px;padding:13px 15px;border-radius:10px;
  background:var(--s2);border:1px solid var(--bd);color:var(--t1);text-decoration:none;
  transition:border-color .15s,box-shadow .15s,transform .15s;margin-bottom:8px}
.dl-r:hover{border-color:color-mix(in srgb,var(--in) 35%,transparent);box-shadow:0 0 18px color-mix(in srgb,var(--in) 10%,transparent);transform:translateX(-2px)}
.dl-ic{width:38px;height:38px;border-radius:9px;flex-shrink:0;
  background:color-mix(in srgb,var(--in) 12%,transparent);border:1px solid color-mix(in srgb,var(--in) 18%,transparent);
  display:flex;align-items:center;justify-content:center;color:var(--in)}
.dl-t{font-size:13px;font-weight:700}
.dl-h{font-size:11px;color:var(--t3);margin-top:1px}
.qr-wrap{display:flex;justify-content:center;padding:6px}
.qr-box{background:#fff;padding:14px;border-radius:10px;
  display:inline-block;box-shadow:0 4px 18px rgba(0,0,0,.25)}

/* V2 CONFIG ROW — اسم کانفیگ + دکمه کپی + دکمه کیوآر جدا */
.v2-row{cursor:default}
.v2-row-b{flex:1;min-width:0;cursor:pointer}
.v2-row .bic{width:34px;height:34px;flex-shrink:0}

/* QR MODAL — نمایش کیوآر تک‌کانفیگ */
.qr-modal{position:fixed;inset:0;z-index:500;display:none;align-items:center;justify-content:center;
  background:rgba(0,0,0,.6);backdrop-filter:blur(6px);-webkit-backdrop-filter:blur(6px);padding:20px}
.qr-modal.open{display:flex}
.qr-modal-box{background:var(--s1);border:1px solid var(--bd);border-radius:16px;
  padding:18px;max-width:320px;width:100%;box-shadow:var(--sh-glow);animation:qrPop .2s ease}
@keyframes qrPop{from{transform:scale(.92);opacity:0}to{transform:scale(1);opacity:1}}
.qr-modal-hd{display:flex;align-items:center;justify-content:space-between;
  margin-bottom:12px;font-size:13px;font-weight:700}
.qr-modal-box .qr-box{display:block;width:100%;box-sizing:border-box}

/* STATE BOXES */
.st-box{text-align:center;padding:48px 20px;border-radius:14px}
.st-box.dis{background:rgba(244,63,94,.03);border:1px solid rgba(244,63,94,.12)}
.st-box.exp{background:rgba(245,158,11,.03);border:1px solid rgba(245,158,11,.1)}
.st-ic{width:64px;height:64px;margin:0 auto 16px;border-radius:18px;
  display:flex;align-items:center;justify-content:center}
.st-ic.dis{background:rgba(244,63,94,.1);border:1px solid rgba(244,63,94,.2);color:var(--ro)}
.st-ic.exp{background:rgba(245,158,11,.08);border:1px solid rgba(245,158,11,.15);color:var(--am)}
.st-t{font-size:16px;font-weight:700;margin-bottom:6px}
.st-s{font-size:12px;color:var(--t3)}

/* TOAST */
.toast{position:fixed;bottom:22px;left:50%;z-index:9999;
  transform:translateX(-50%) translateY(80px);
  background:rgba(16,185,129,.1);border:1px solid rgba(16,185,129,.22);
  color:var(--em);padding:9px 20px;border-radius:99px;font-size:13px;font-weight:600;
  display:flex;align-items:center;gap:7px;
  transition:transform .28s cubic-bezier(.34,1.56,.64,1);
  pointer-events:none;backdrop-filter:blur(18px);box-shadow:0 6px 28px rgba(0,0,0,.28)}
.toast.show{transform:translateX(-50%) translateY(0)}

/* ALERTS */
.alr{padding:11px 14px;border-radius:8px;font-size:13px;margin-bottom:14px;
  display:flex;align-items:center;gap:8px}
.alr-ok{background:rgba(16,185,129,.07);border:1px solid rgba(16,185,129,.18);color:var(--em)}
.alr-er{background:rgba(244,63,94,.07);border:1px solid rgba(244,63,94,.18);color:var(--ro)}

/* EMPTY */
.empty{text-align:center;padding:56px 24px;color:var(--t3)}
.empty svg{opacity:.2;margin:0 auto 14px;display:block}
.empty-t{font-size:14px;font-weight:600;color:var(--t2);margin-bottom:5px}
.empty-s{font-size:13px}

/* SCROLLBAR */
::-webkit-scrollbar{width:5px;height:5px}
::-webkit-scrollbar-track{background:transparent}
::-webkit-scrollbar-thumb{background:var(--s3);border-radius:3px}

/* ANIM */
@keyframes fu{from{opacity:0;transform:translateY(8px)}to{opacity:1;transform:translateY(0)}}
.fu{animation:fu .22s ease both}
.f1{animation-delay:.04s}.f2{animation-delay:.08s}.f3{animation-delay:.12s}
`;

// ─── SHARED JS ─────────────────────────────────────────────────────────────
const JSG=`
const _T={
  fa:{dashboard:'داشبورد',subs:'اشتراک‌ها',total:'کل اشتراک‌ها',active:'فعال',
    usage:'مصرف کل',inactive:'غیرفعال / منقضی',newSub:'اشتراک جدید',
    subName:'نام اشتراک',quota:'حجم (GB)',days:'روز',create:'بساز',
    status:'وضعیت',usageCol:'مصرف',expire:'انقضا',actions:'عملیات',
    on:'فعال',off:'غیرفعال',expiredB:'منقضی',soon:'رو به اتمام',
    remaining:'باقی',renew:'تمدید',addQuota:'+ حجم',reset:'ریست',
    toggle:'قطع / فعال',del:'حذف',settings:'تنظیمات',logout:'خروج',
    security:'امنیت',sysInfo:'اطلاعات سیستم',panelPwd:'رمز عبور پنل',
    pwdDesc:'رمز ورود به پنل مدیریت را تغییر دهید',savePwd:'ذخیره رمز جدید',
    newPwd:'رمز جدید',confirmPwd:'تکرار رمز',login:'ورود به پنل',
    loginSub:'مدیریت اشتراک‌های وی‌توری',pwd:'رمز عبور',
    dlConf:'دانلود کانفیگ',scanQr:'اسکن با اپ کلاینت',
    disTitle:'اشتراک شما غیرفعال است',disSub:'برای فعال‌سازی با پشتیبانی تماس بگیرید',
    expTitle:'اشتراک منقضی شده',expSub:'جهت تمدید با پشتیبانی تماس بگیرید',
    setUsage:'تنظیم مصرف (MB)'},
  en:{dashboard:'Dashboard',subs:'Subscriptions',total:'Total Subs',active:'Active',
    usage:'Total Usage',inactive:'Inactive / Expired',newSub:'New Subscription',
    subName:'Name',quota:'Quota (GB)',days:'Days',create:'Create',
    status:'Status',usageCol:'Usage',expire:'Expiry',actions:'Actions',
    on:'Active',off:'Disabled',expiredB:'Expired',soon:'Expiring Soon',
    remaining:'Left',renew:'Renew',addQuota:'+ GB',reset:'Reset',
    toggle:'Pause / Resume',del:'Delete',settings:'Settings',logout:'Logout',
    security:'Security',sysInfo:'System Info',panelPwd:'Panel Password',
    pwdDesc:'Change the admin panel login password',savePwd:'Save Password',
    newPwd:'New Password',confirmPwd:'Confirm Password',login:'Login',
    loginSub:'Subscription Manager',pwd:'Password',
    dlConf:'Download Config',scanQr:'Scan with client app',
    disTitle:'Your subscription is disabled',disSub:'Contact support to re-enable',
    expTitle:'Subscription expired',expSub:'Contact support to renew',
    setUsage:'Set Usage (MB)'}
};
let _lang=localStorage.getItem('ep_lang')||'fa';
function t(k){return _T[_lang][k]||k}
function applyLang(){
  document.querySelectorAll('[data-i]').forEach(el=>{const k=el.dataset.i;if(_T[_lang][k])el.textContent=_T[_lang][k]});
  document.documentElement.lang=_lang==='fa'?'fa':'en';
  document.documentElement.dir=_lang==='fa'?'rtl':'ltr';
  document.querySelectorAll('.l-tog').forEach(b=>{b.textContent=_lang==='fa'?'EN':'FA'});
}
function toggleLang(){_lang=_lang==='fa'?'en':'fa';localStorage.setItem('ep_lang',_lang);applyLang();}

let _theme=localStorage.getItem('ep_theme')||'dark';
function applyTheme(){
  document.documentElement.setAttribute('data-theme',_theme==='light'?'light':'');
  document.querySelectorAll('.t-tog svg').forEach(s=>{});
  document.querySelectorAll('.t-tog').forEach(b=>{
    b.innerHTML=_theme==='dark'?'${I.sun.replace(/`/g,"'")}':'${I.moon.replace(/`/g,"'")}';
  });
}
function toggleTheme(){_theme=_theme==='dark'?'light':'dark';localStorage.setItem('ep_theme',_theme);applyTheme();}

// ─── رنگ اصلی پنل (Accent) — قابل انتخاب و ذخیره‌شونده روی مرورگر خودت ──
var ACCENTS={
  violet:['#8b5cf6','#a78bfa','#a78bfa'],
  rose:  ['#f43f9e','#fb7185','#fda4af'],
  emerald:['#10b981','#34d399','#22d3ee'],
  amber: ['#f59e0b','#fb923c','#f43f5e'],
  sky:   ['#0ea5e9','#38bdf8','#a855f7']
};
var _accent=localStorage.getItem('ep_accent')||'violet';
function applyAccent(){
  var a=ACCENTS[_accent]||ACCENTS.violet;
  var r=document.documentElement.style;
  r.setProperty('--in',a[0]);r.setProperty('--vi',a[1]);r.setProperty('--pk',a[1]);r.setProperty('--cy',a[2]);
  r.setProperty('--gi','linear-gradient(135deg,'+a[0]+','+a[0]+')');
  r.setProperty('--gi2','linear-gradient(135deg,'+a[0]+','+a[0]+')');
  document.querySelectorAll('.accent-dot').forEach(function(d){
    d.classList.toggle('sel',d.dataset.accent===_accent);
  });
}
function setAccent(name){_accent=name;localStorage.setItem('ep_accent',name);applyAccent();
  var m=document.getElementById('accent-menu');if(m)m.classList.remove('show');}
function toggleAccentMenu(ev){
  if(ev)ev.stopPropagation();
  var m=document.getElementById('accent-menu');
  if(m)m.classList.toggle('show');
}
document.addEventListener('click',function(e){
  var m=document.getElementById('accent-menu');
  if(m&&m.classList.contains('show')&&!m.contains(e.target)&&!e.target.closest('.accent-btn'))m.classList.remove('show');
});

function openDrawer(ev){
  if(ev)ev.stopPropagation();
  const d=document.getElementById('drawer'),b=document.getElementById('drawer-bd');
  if(!d||!b)return;
  d.classList.add('open');b.classList.add('show');
  document.body.style.overflow='hidden';
}
function closeDrawer(){
  const d=document.getElementById('drawer'),b=document.getElementById('drawer-bd');
  if(!d||!b)return;
  d.classList.remove('open');b.classList.remove('show');
  document.body.style.overflow='';
}
document.addEventListener('keydown',function(e){if(e.key==='Escape')closeDrawer();});
function grpAll(el){document.querySelectorAll('.grp-i').forEach(function(c){c.checked=el.checked;});}
function grpSync(){var a=document.querySelectorAll('.grp-i'),n=0;a.forEach(function(c){if(c.checked)n++;});var m=document.getElementById('grp-all');if(m)m.checked=(n===a.length);}
function closeAllRowMenus(){
  document.querySelectorAll('.row-menu.show').forEach(function(m){m.classList.remove('show');});
  var bd=document.getElementById('row-menu-bd');if(bd)bd.classList.remove('show');
}
function toggleRowMenu(ev,id){
  if(ev)ev.stopPropagation();
  const m=document.getElementById(id);
  if(!m)return;
  const wasOpen=m.classList.contains('show');
  closeAllRowMenus();
  if(wasOpen)return; // فقط بسته بمونه
  // نکته‌ی مهم: هر ردیف جدول کلاس fu داره (انیمیشن ورود) که با
  // animation:...both یه transform:translateY(0) رو برای همیشه روی
  // ردیف نگه می‌داره. طبق اسپک CSS، هر transform روی یه پدر، باعث میشه
  // position:fixed فرزندش دیگه نسبت به کل صفحه نباشه و گیر بیفته/بریده بشه
  // (دقیقاً چیزی که باعث میشد پس‌زمینه‌ی تیره بیاد ولی خودِ منو دیده نشه).
  // راه‌حل قطعی: منو رو موقع باز شدن مستقیم می‌بریم زیر <body>.
  if(m.parentElement!==document.body)document.body.appendChild(m);
  // موقعیت: اول نامرئی نشونش بده، ارتفاع واقعی رو اندازه بگیر، بعد داخل صفحه جاش بده
  const btn=ev&&ev.currentTarget?ev.currentTarget:null;
  m.style.visibility='hidden';m.classList.add('show');
  const vv=window.visualViewport,vh=vv?vv.height:window.innerHeight,menuW=230,pad=8;
  m.style.maxHeight=(vh-pad*2)+'px';
  const mh=m.offsetHeight;
  if(btn){
    const r=btn.getBoundingClientRect();
    let left=r.left-menuW+r.width;
    left=Math.max(pad,Math.min(left,window.innerWidth-menuW-pad));
    let top=r.bottom+6;
    if(top+mh>vh-pad)top=r.top-6-mh;
    top=Math.max(pad,Math.min(top,vh-mh-pad));
    m.style.left=left+'px';
    m.style.top=top+'px';
  }
  m.style.visibility='';
  const bd=document.getElementById('row-menu-bd');if(bd)bd.classList.add('show');
}
// باز شدن کیبورد فقط ارتفاع رو عوض می‌کنه؛ پس فقط با تغییر عرض منو رو ببند
var _lw=window.innerWidth;
window.addEventListener('resize',function(){if(window.innerWidth!==_lw){_lw=window.innerWidth;closeAllRowMenus();}});
function _inMenu(n){return !!(n&&n.closest&&n.closest('.row-menu'));}
window.addEventListener('scroll',function(e){if(_inMenu(e.target)||_inMenu(document.activeElement))return;closeAllRowMenus();},true);
document.addEventListener('focusin',function(e){var t=e.target;if(_inMenu(t))setTimeout(function(){try{t.scrollIntoView({block:'nearest'});}catch(x){}},300);});

window.addEventListener('DOMContentLoaded',()=>{
  applyLang();applyTheme();applyAccent();
  try{
    const p=new URLSearchParams(location.search);
    const err=p.get('err'),ok=p.get('ok');
    if(err){toast(decodeURIComponent(err),1);p.delete('err');history.replaceState({},'',location.pathname+(p.toString()?'?'+p.toString():''));}
    else if(ok){toast(decodeURIComponent(ok));p.delete('ok');history.replaceState({},'',location.pathname+(p.toString()?'?'+p.toString():''));}
  }catch(e){}
});

function toast(msg,err){
  const t=document.getElementById('toast');
  if(err){t.style.background='rgba(244,63,94,.1)';t.style.color='var(--ro)';t.style.borderColor='rgba(244,63,94,.2)'}
  else{t.style.background='rgba(16,185,129,.1)';t.style.color='var(--em)';t.style.borderColor='rgba(16,185,129,.22)'}
  t.querySelector('.msg').textContent=msg;
  t.classList.add('show');setTimeout(()=>t.classList.remove('show'),2500);
}
function cp(v){navigator.clipboard.writeText(v).then(()=>toast('کپی شد ✓')).catch(()=>toast('خطا',1));}
function randName(){const c='abcdefghijklmnopqrstuvwxyz0123456789';const el=document.getElementById('sn');if(el)el.value=Array.from({length:6},()=>c[Math.floor(Math.random()*c.length)]).join('');}
function onQtyUnitChange(){
  var u=document.getElementById('qty-unit'),i=document.getElementById('qty-input'),n=document.getElementById('qty-note');
  if(!u||!i||!n)return;
  if(u.value==='req'){i.placeholder='مثلاً 180000';i.step='1000';n.style.display='block';}
  else{i.placeholder='30';i.step='0.01';n.style.display='none';}
}
function togglePwd(id,btn){const i=document.getElementById(id);const s=i.type==='password';i.type=s?'text':'password';}
`;

// ═══════════════════════════════════════════════════════════════════════════
//  NAV COMPONENT
// ═══════════════════════════════════════════════════════════════════════════
function nav(sub='داشبورد',admin=null){
  const isSuper=admin?.is_super;
  const mgmtItems=[
    isSuper?{href:"/panel/admins",icon:I.panel,label:"مدیریت پنل‌ها"}:null,
    isSuper?{href:"/panel/shop-settings",icon:I.zap,label:"بات فروش تلگرام"}:null,
    isSuper?{href:"/panel/v2-settings",icon:I.globe,label:"تنظیمات وی‌توری"}:null,
  ].filter(Boolean);
  const acctItems=[
    {href:"/panel/settings",icon:I.set,label:"تنظیمات حساب"},
  ];
  const initial=(admin?.username||"?").trim().charAt(0).toUpperCase();
  return `<nav class="nav">
  <div class="nav-l">
    <img class="nav-logo-img" src="/logo.svg" alt="Epic" width="36" height="36">
    <div><div class="nav-name">${PANEL_NAME}</div><div class="nav-sub">${sub}${admin?` · ${admin.username}${isSuper?' <span style="color:var(--am)">'+I.crown.replace('width="15" height="15"','width="13" height="13"')+'</span>':''}`:''}</div></div>
    <span class="status-pill">${I.check.replace('width="14" height="14"','width="11" height="11"')} <span data-i="panelOnline">پنل آنلاین</span></span>
  </div>
  <div class="nav-r">
    <div class="ham-wrap">
      <button class="btn bo bic accent-btn" onclick="toggleAccentMenu(event)" title="رنگ اصلی پنل">${I.palette}</button>
      <div class="ham-menu accent-menu" id="accent-menu">
        <div class="accent-row">
          <span class="accent-dot" data-accent="violet" style="background:#8b5cf6" onclick="setAccent('violet')"></span>
          <span class="accent-dot" data-accent="rose" style="background:linear-gradient(135deg,#f43f9e,#fb7185)" onclick="setAccent('rose')"></span>
          <span class="accent-dot" data-accent="emerald" style="background:linear-gradient(135deg,#10b981,#22d3ee)" onclick="setAccent('emerald')"></span>
          <span class="accent-dot" data-accent="amber" style="background:linear-gradient(135deg,#f59e0b,#f43f5e)" onclick="setAccent('amber')"></span>
          <span class="accent-dot" data-accent="sky" style="background:linear-gradient(135deg,#0ea5e9,#a855f7)" onclick="setAccent('sky')"></span>
        </div>
      </div>
    </div>
    <button class="btn bo bic t-tog" onclick="toggleTheme()" title="تغییر تم">${I.moon}</button>
    <button class="btn bo bic l-tog" onclick="toggleLang()" title="تغییر زبان">EN</button>
    <button class="btn bo bic ham-hide" onclick="openDrawer(event)" title="منو">
      <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.2" stroke-linecap="round"><line x1="3" y1="6" x2="21" y2="6"/><line x1="3" y1="12" x2="21" y2="12"/><line x1="3" y1="18" x2="21" y2="18"/></svg>
    </button>
  </div>
</nav>

<div class="drawer-backdrop" id="drawer-bd" onclick="closeDrawer()"></div>
<aside class="drawer" id="drawer">
  <div class="drawer-hd">
    <div class="drawer-avatar">${initial||I.userP}</div>
    <div style="flex:1;min-width:0">
      <div class="drawer-uname">${admin?.username||'مهمان'}</div>
      <div class="drawer-urole">${isSuper?I.crown.replace('width="15" height="15"','width="12" height="12"')+' سوپرادمین':'ادمین پنل'}</div>
    </div>
    <button class="btn bo bic bsm" onclick="closeDrawer()">${I.close}</button>
  </div>
  <div class="drawer-body">
    <a class="drawer-item" href="/panel/dashboard">${I.grid} <span>داشبورد</span></a>
    <a class="drawer-item" href="/panel/stats">${I.act} <span>اطلاعات و آمار</span></a>
    <div class="drawer-sec">پروتکل‌ها</div>
    <a class="drawer-item" href="/panel/proto-vless">${PI.vl} <span>VLESS</span></a>
    <a class="drawer-item" href="/panel/proto-trojan">${PI.tj} <span>Trojan</span></a>
    ${mgmtItems.length?`<div class="drawer-sec">مدیریت</div>`:''}
    ${mgmtItems.map(it=>`<a class="drawer-item" href="${it.href}">${it.icon} <span>${it.label}</span></a>`).join('')}
    <div class="drawer-sec">حساب کاربری</div>
    ${acctItems.map(it=>`<a class="drawer-item" href="${it.href}">${it.icon} <span>${it.label}</span></a>`).join('')}
  </div>
  <div class="drawer-ft">
    <a class="drawer-item drawer-logout" href="/panel/logout">${I.logout} <span>خروج از حساب</span></a>
  </div>
</aside>
<nav class="dock" id="dock">
  <a class="dk" href="/panel/dashboard">${I.grid}<span>داشبورد</span></a>
  <a class="dk" href="/panel/proto-vless">${PI.vl.replace('width="16" height="16"','width="22" height="22"')}<span>VLESS</span></a>
  <a class="dk" href="/panel/proto-trojan">${PI.tj.replace('width="16" height="16"','width="22" height="22"')}<span>Trojan</span></a>
  <a class="dk" href="/panel/stats"><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.2" stroke-linecap="round"><path d="M4 20V10M10 20V4M16 20v-7M22 20H2"/></svg><span>آمار</span></a>
  <button class="dk" onclick="openDrawer(event)"><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.2" stroke-linecap="round"><circle cx="5" cy="12" r="1.6"/><circle cx="12" cy="12" r="1.6"/><circle cx="19" cy="12" r="1.6"/></svg><span>بیشتر</span></button>
</nav>
<script>(function(){var p=location.pathname;document.querySelectorAll('#dock .dk[href]').forEach(function(a){if(p===a.getAttribute('href')||(a.getAttribute('href')!=='/panel/dashboard'&&p.indexOf(a.getAttribute('href'))===0))a.classList.add('on');});})();</script>`;
}

const TOAST=`<div class="toast" id="toast"><svg width="13" height="13" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="3"><polyline points="20 6 9 17 4 12"/></svg><span class="msg"></span></div>`;

// ═══════════════════════════════════════════════════════════════════════════
//  PAGE: LOGIN
// ═══════════════════════════════════════════════════════════════════════════
function loginPage(err=""){
  return `<!DOCTYPE html><html lang="fa" dir="rtl"><head>
<meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>${PANEL_NAME}</title><link rel="icon" type="image/svg+xml" href="/logo.svg"><script src="https://telegram.org/js/telegram-web-app.js"></script><script>try{var w=window.Telegram&&Telegram.WebApp;if(w){w.ready();w.expand();}}catch(e){}</script><style>${CSS}${CSSX}${DOCKCSS}${LAYOUTCSS}${NATURALCSS}</style></head>
<body>
<div class="login-pg">
  <div style="display:flex;justify-content:center;gap:8px;margin-bottom:14px">
    <button class="btn bo bic t-tog" onclick="toggleTheme()">${I.moon}</button>
    <button class="btn bo bic l-tog" onclick="toggleLang()">EN</button>
  </div>
<div class="login-bx fu">
  <div class="l-ic-wrap"><img class="login-logo" src="/logo.svg" alt="Epic" width="92" height="92"></div>
  <div class="l-t">${PANEL_NAME}</div>
  <div class="l-s" data-i="loginSub">مدیریت اشتراک‌های وی‌توری</div>
  <div class="l-ver">V7 · PANEL EDITION</div>
  ${err?`<div class="l-err">${err}</div>`:''}
  <form method="POST" action="/panel/admin" style="display:flex;flex-direction:column;gap:11px;margin-top:6px">
    <div class="l-field">
      <input type="text" name="username" placeholder="نام کاربری" required autofocus/>
      ${I.userP}
    </div>
    <div class="l-field">
      <input type="password" id="pw" name="password" data-i-placeholder="pwd" placeholder="رمز عبور" required/>
      ${I.lock.replace(/width="\d+" height="\d+"/,'width="16" height="16"')}
      <button type="button" class="pey" onclick="togglePwd('pw',this)">${I.eye}</button>
    </div>
    <button class="btn bp" style="padding:12px;font-size:14px;width:100%;justify-content:center;margin-top:4px">
      <span data-i="login">ورود به پنل</span> ${I.zapS}
    </button>
  </form>
  <div class="l-status">${I.check} <span data-i="secure">اتصال امن · نشست محافظت‌شده</span></div>
</div>
</div>
${TOAST}<script>${JSG}</script></body></html>`;
}

// ═══════════════════════════════════════════════════════════════════════════
//  PAGE: DASHBOARD
// ═══════════════════════════════════════════════════════════════════════════
// ─── لیست کشورها برای انتخاب پروکسی/کانفیگ اختصاصی هر اشتراک ──────────────
const COUNTRIES=[
  {code:"",   flag:"🌐",name:"پیش‌فرض (بدون کشور)"},
  {code:"DE", flag:"🇩🇪",name:"آلمان"},
  {code:"NL", flag:"🇳🇱",name:"هلند"},
  {code:"FR", flag:"🇫🇷",name:"فرانسه"},
  {code:"GB", flag:"🇬🇧",name:"انگلیس"},
  {code:"US", flag:"🇺🇸",name:"آمریکا"},
  {code:"CA", flag:"🇨🇦",name:"کانادا"},
  {code:"FI", flag:"🇫🇮",name:"فنلاند"},
  {code:"SE", flag:"🇸🇪",name:"سوئد"},
  {code:"CH", flag:"🇨🇭",name:"سوئیس"},
  {code:"TR", flag:"🇹🇷",name:"ترکیه"},
  {code:"AE", flag:"🇦🇪",name:"امارات"},
  {code:"SG", flag:"🇸🇬",name:"سنگاپور"},
  {code:"JP", flag:"🇯🇵",name:"ژاپن"},
  {code:"KR", flag:"🇰🇷",name:"کره جنوبی"},
  {code:"IN", flag:"🇮🇳",name:"هند"},
  {code:"AU", flag:"🇦🇺",name:"استرالیا"},
];
const countryOf=(code)=>COUNTRIES.find(c=>c.code===code)||COUNTRIES[0];
function countryOptions(sel=""){
  return COUNTRIES.map(c=>`<option value="${c.code}"${c.code===sel?" selected":""}>${c.flag} ${c.name}</option>`).join('');
}
function countryChip(code){
  if(!code)return"";
  const c=countryOf(code);
  return `<span class="badge" style="background:rgba(34,211,238,.1);color:var(--cy);border-color:rgba(34,211,238,.22)">${c.flag} ${c.name}</span>`;
}

const PROTO_META={
 vless:{title:"VLESS",ic:"vl",color:"#7c5cff",desc:"وی‌توری",path:"/panel/proto-vless"},
 trojan:{title:"Trojan",ic:"tj",color:"#ff4d8d",desc:"تروجان",path:"/panel/proto-trojan"}
};
const _gb=n=>{n=n||0;return "<bdi dir=\"ltr\">"+(n>=1073741824?(n/1073741824).toFixed(2)+" GB":n>=1048576?(n/1048576).toFixed(1)+" MB":Math.round(n/1024)+" KB")+"</bdi>";};
function protoSubs(subs,pk){return subs.filter(s=>s.protocol===pk||s.protocol==="all");}
function statsPage(subs,baseUrl,admin){
  const tot=subs.length;let act=0,off=0,exp=0,soon=0,used=0,quota=0;
  subs.forEach(s=>{used+=s.used_bytes||0;quota+=(s.quota_gb||0)*1073741824;
    if(s.disabled){off++;return;} if(isExp(s)){exp++;return;} act++; if(daysLeft(s)<4)soon++;});
  const pct=quota?Math.min(100,used/quota*100):0;
  const card=(ic,v,l,c)=>`<div class="st-c" style="--c:${c}"><div class="st-i">${ic}</div><div class="st-v">${v}</div><div class="st-l">${l}</div></div>`;
  const prot=Object.keys(PROTO_META).map(k=>{const a=protoSubs(subs,k);const u=a.reduce((x,s)=>x+(s.used_bytes||0),0);
    return `<a class="st-p" href="${PROTO_META[k].path}" style="--c:${PROTO_META[k].color}"><div class="st-pi">${PI[PROTO_META[k].ic].replace('width="16" height="16"','width="30" height="30"')}</div><div><b>${PROTO_META[k].title}</b><div class="st-ps">${a.length} اشتراک · ${_gb(u)}</div></div></a>`;}).join("");
  const top=[...subs].sort((a,b)=>(b.used_bytes||0)-(a.used_bytes||0)).slice(0,5).map((s,i)=>{const q=(s.quota_gb||0)*1073741824;const pc=q?Math.min(100,(s.used_bytes||0)/q*100):0;
    return `<div class="st-r"><span class="st-n">${i+1}. ${_he(s.name)}</span><span class="st-u">${_gb(s.used_bytes)}</span><div class="st-b"><i style="width:${pc.toFixed(0)}%"></i></div></div>`;}).join("")||'<div style="color:var(--t3);font-size:13px">هنوز اشتراکی نیست</div>';
  return `<!DOCTYPE html><html lang="fa" dir="rtl"><head><meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>${PANEL_NAME}</title><link rel="icon" type="image/svg+xml" href="/logo.svg"><style>${CSS}${CSSX}${DOCKCSS}${LAYOUTCSS}${NATURALCSS}${STATCSS}${LAYOUTCSS}${NATURALCSS}</style></head><body>
${nav('اطلاعات و آمار',admin)}<div class="pg wrap">
<div class="proto-hero fu" style="--pc:#22e0ff"><div class="proto-ic">${I.act}</div><div><div class="proto-t">اطلاعات و آمار</div><div class="proto-s">نمای کلی از اشتراک‌ها و مصرف</div></div></div>
<div class="st-g fu">${card(I.users,tot,"کل اشتراک‌ها","#8b5cf6")}${card(I.check,act,"فعال","#34d399")}${card(I.clock,soon,"رو به اتمام","#f59e0b")}${card(I.pause,exp+off,"منقضی/غیرفعال","#f87171")}</div>
<div class="card fu" style="padding:16px;margin-top:12px"><div style="display:flex;justify-content:space-between;font-weight:800;margin-bottom:10px"><span>مصرف کل</span><span>${_gb(used)} / ${_gb(quota)}</span></div><div class="st-b big"><i style="width:${pct.toFixed(1)}%"></i></div></div>
<div class="st-pg fu">${prot}</div>
<div class="card fu" style="padding:16px;margin-top:12px"><div style="font-weight:800;margin-bottom:10px">پرمصرف‌ترین‌ها</div>${top}</div>
</div></body></html>`;
}
const STATCSS=`
.proto-hero{display:flex;align-items:center;gap:14px;padding:18px;margin-bottom:14px;border-radius:20px;background:linear-gradient(135deg,color-mix(in srgb,var(--pc) 22%,transparent),transparent 70%);border:1px solid color-mix(in srgb,var(--pc) 40%,transparent)}
.proto-ic{width:56px;height:56px;border-radius:18px;display:flex;align-items:center;justify-content:center;background:rgba(255,255,255,.07);box-shadow:0 8px 24px color-mix(in srgb,var(--pc) 45%,transparent)}
.proto-t{font-size:20px;font-weight:900}.proto-s{font-size:12px;color:var(--t2);margin-top:2px}
.st-g{display:grid;grid-template-columns:repeat(2,1fr);gap:10px}
@media(min-width:700px){.st-g{grid-template-columns:repeat(4,1fr)}}
.st-c{padding:16px;border-radius:18px;background:var(--s2);border:1px solid var(--bd);position:relative;overflow:hidden}
.st-c::after{content:'';position:absolute;inset:auto -20px -30px auto;width:90px;height:90px;border-radius:50%;background:var(--c);opacity:.18;filter:blur(18px)}
.st-i{font-size:22px}.st-v{font-size:28px;font-weight:900;margin-top:6px;color:var(--c)}.st-l{font-size:12px;color:var(--t2)}
.st-pg{display:grid;gap:10px;margin-top:12px}@media(min-width:700px){.st-pg{grid-template-columns:repeat(3,1fr)}}
.st-p{display:flex;align-items:center;gap:12px;padding:14px;border-radius:18px;background:var(--s2);border:1px solid color-mix(in srgb,var(--c) 40%,transparent);text-decoration:none;color:inherit}
.st-ps{font-size:12px;color:var(--t2);margin-top:2px}
.st-r{display:grid;grid-template-columns:1fr auto;gap:4px 10px;margin-bottom:12px;font-size:13px}.st-u{color:var(--t2);font-size:12px}
.st-b{grid-column:1/-1;height:6px;border-radius:9px;background:var(--s3);overflow:hidden}.st-b.big{height:10px}
.st-b i{display:block;height:100%;border-radius:9px;background:linear-gradient(90deg,var(--in),var(--vi))}
`;
function dashPage(subs,baseUrl,admin,showOwner=false,pk=null){
  const now=Date.now();
  let cA=0,cO=0,cE=0,tU=0;
  subs.forEach(s=>{
    tU+=(s.used_bytes||0);
    if(s.disabled){cO++;return;}
    isExp(s)?cE++:cA++;
  });

  const rows=subs.map((s,i)=>{
    const qb=s.quota_gb*1073741824,ub=s.used_bytes||0;
    const rem=Math.max(0,qb-ub),pct=Math.min(100,qb>0?ub/qb*100:0);
    const dl=daysLeft(s),exp=isExp(s);
    const owner=s.owner||admin.username;
    const sub_url=`${baseUrl}/sublink/${encodeURIComponent(owner)}/${encodeURIComponent(s.name)}`;
    const rowBase=`/panel/${encodeURIComponent(owner)}`;

    let badge;
    if(s.disabled)     badge=`<span class="badge bd2"><span class="dot"></span><span data-i="off">غیرفعال</span></span>`;
    else if(exp)       badge=`<span class="badge br"><span class="dot"></span><span data-i="expiredB">منقضی</span></span>`;
    else if(dl<4)      badge=`<span class="badge ba"><span class="dot"></span><span data-i="soon">رو به اتمام</span></span>`;
    else               badge=`<span class="badge bg"><span class="dot"></span><span data-i="on">فعال</span></span>`;


    const badgeVL=`<span class="badge" style="background:rgba(124,92,255,.12);color:#a594ff;border-color:rgba(124,92,255,.3)">${PI.vl} VLESS</span>`;
    const badgeTJ=`<span class="badge" style="background:rgba(255,77,141,.12);color:#ff7fb0;border-color:rgba(255,77,141,.3)">${PI.tj} Trojan</span>`;
    const protoTag = s.protocol==="all"?`${badgeVL} ${badgeTJ}`
      : s.protocol==="vless"?badgeVL
      : s.protocol==="trojan"?badgeTJ
      : `<span class="badge">قدیمی</span>`;
    const cChip=countryChip(s.country);
    const pIpTag=s.proxyIp?`<span class="badge bd2" title="پروکسی IP اختصاصی">${I.sat.replace('width="15" height="15"','width="12" height="12"')} ${s.proxyIp}</span>`:"";

    return `<tr class="fu" style="animation-delay:${i*.025}s">
<td><div class="nm">${s.name}</div><div class="dt">${new Date(s.created_at).toLocaleDateString('fa-IR')}${showOwner?` · <span style="color:var(--in)">${owner}</span>`:''}</div><div style="margin-top:4px;display:flex;gap:4px;flex-wrap:wrap">${protoTag} ${cChip} ${pIpTag}</div></td>
<td>${badge}</td>
<td style="min-width:170px">
  <div class="uc"><span>${fmt(ub)}</span><span>${s.quota_gb} GB</span></div>
  <div class="pt"><div class="pb${pct>80?' hot':''}" style="width:${pct.toFixed(1)}%"></div></div>
  <div class="ur"><span data-i="remaining">باقی</span>: <b style="color:${rem<1073741824?'var(--am)':'var(--t2)'}">${fmt(rem)}</b></div>
  ${s.dailyLimitGb?(()=>{
    const dActive=s.dailyResetAt&&Date.now()<=s.dailyResetAt;
    const dUsed=dActive?(s.dailyUsed||0):0;
    const dPct=Math.min(100,dUsed/(s.dailyLimitGb*1073741824)*100);
    return `<div class="ur" style="margin-top:3px">${I.cal.replace('width="14" height="14"','width="11" height="11"')} امروز: <b style="color:${dPct>=100?'var(--ro)':'var(--t2)'}">${fmt(dUsed)} / ${s.dailyLimitGb} GB</b></div>`;
  })():''}
</td>
<td>
  <div style="display:flex;align-items:center;gap:4px;font-size:12px;color:${dl<4&&!exp?'var(--am)':'var(--t3)'}">
    ${I.clock} ${dl} <span style="font-size:10px;color:var(--t3)">روز</span>
  </div>
</td>
<td>
  <div class="acts">
    <form method="POST" action="${rowBase}/toggle/${encodeURIComponent(s.name)}" style="display:inline">
      <button type="submit" class="tgl-sw ${s.disabled?'':'on'}" title="${s.disabled?'فعال‌سازی':'غیرفعال کردن'}"><span class="tgl-dot"></span></button>
    </form>
    <div class="acts-wrap">
      <button type="button" class="btn bo bsm bic" onclick="toggleRowMenu(event,'rm${i}')" title="عملیات بیشتر">
        <svg width="15" height="15" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.3" stroke-linecap="round"><circle cx="12" cy="5" r="1.2"/><circle cx="12" cy="12" r="1.2"/><circle cx="12" cy="19" r="1.2"/></svg>
      </button>
      <div class="row-menu" id="rm${i}">
        <a class="drawer-item" href="${sub_url}" target="_blank">${I.link} <span>صفحه کاربر</span></a>
        ${s.subTok?`<a class="drawer-item" href="javascript:void(0)" onclick="cp('${baseUrl}/s/${s.subTok}')">${I.copy} <span>کپی لینک کانفیگ</span></a>`:''}
        <a class="drawer-item" href="javascript:void(0)" onclick="openHist('${s.name.replace(/'/g,"\\'")}')">${I.cal} <span>تاریخچه مصرف</span></a>
        <div class="drawer-sec">مدیریت مصرف</div>
        <form method="POST" action="${rowBase}/reset/${encodeURIComponent(s.name)}" onsubmit="return confirm('ریست مصرف «${s.name}»؟')">
          <button class="drawer-item" style="width:100%;border:none;background:none;cursor:pointer;font:inherit;text-align:right">${I.ref} <span>ریست مصرف</span></button>
        </form>
        <form method="POST" action="${rowBase}/setusage/${encodeURIComponent(s.name)}" class="row-menu-form">
          ${I.db}<input type="number" name="mb" placeholder="تنظیم دستی (MB)" min="0" step="1"/>
          <button class="btn bcy bsm" type="submit">${I.check}</button>
        </form>
        <div class="drawer-sec">زمان و حجم</div>
        <form method="POST" action="${rowBase}/renew/${encodeURIComponent(s.name)}" class="row-menu-form">
          ${I.cal}<input type="number" name="days" placeholder="تمدید (روز)" min="1" max="3650"/>
          <button class="btn bam bsm" type="submit">${I.check}</button>
        </form>
        <form method="POST" action="${rowBase}/addquota/${encodeURIComponent(s.name)}" class="row-menu-form">
          ${I.db}<input type="number" name="gb" placeholder="افزودن حجم (GB)" min="0.1" step="0.1"/>
          <button class="btn be bsm" type="submit">+</button>
        </form>
        <div class="drawer-sec"></div>
        <form method="POST" action="${rowBase}/delete/${encodeURIComponent(s.name)}"
          onsubmit="return confirm('حذف «${s.name}»؟ برگشت‌پذیر نیست.')">
          <button class="drawer-item drawer-logout" style="width:100%;border:none;background:none;cursor:pointer;font:inherit;text-align:right">${I.trash} <span>حذف اشتراک</span></button>
        </form>
      </div>
    </div>
  </div>
</td>
</tr>`;
  }).join('');

  return `<!DOCTYPE html><html lang="fa" dir="rtl"><head>
<meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>${PANEL_NAME}</title><link rel="icon" type="image/svg+xml" href="/logo.svg"><style>${CSS}${CSSX}${DOCKCSS}${LAYOUTCSS}${NATURALCSS}</style></head>
<body>
${nav(showOwner?'همه پنل‌ها':(pk?PROTO_META[pk].title:'داشبورد'),admin)}
<div class="pg wrap">
${pk?`<div class="proto-hero fu" style="--pc:${PROTO_META[pk].color}"><div class="proto-ic">${PI[PROTO_META[pk].ic].replace('width="16" height="16"','width="34" height="34"')}</div><div><div class="proto-t">${PROTO_META[pk].title}</div><div class="proto-s">${subs.length} اشتراک · ${PROTO_META[pk].desc}</div></div></div>`:''}

<div class="refresh-bar fu">
  <span>${I.clock} <span data-i="lastUpdate">آخرین بروزرسانی</span>: <b data-i="now">همین الان</b></span>
  <button type="button" class="btn bo bsm" onclick="location.reload()">${I.sync} <span data-i="refresh">رفرش کن</span></button>
</div>

<div class="stats fu">
  <div class="sc"><div class="sc-ic ii">${I.users}</div><div class="sc-v">${subs.length}</div><div class="sc-l" data-i="total">کل اشتراک‌ها</div>
    <div class="sc-bar"><div class="sc-bar-f" style="width:${subs.length?Math.round(cA/subs.length*100):0}%;background:var(--in)"></div></div></div>
  <div class="sc"><div class="sc-ic ig">${I.zapS}</div><div class="sc-v">${cA}</div><div class="sc-l" data-i="active">فعال</div>
    <div class="sc-bar"><div class="sc-bar-f" style="width:${subs.length?Math.round(cA/subs.length*100):0}%;background:var(--em)"></div></div></div>
  <div class="sc"><div class="sc-ic ic">${I.act}</div><div class="sc-v">${fmt(tU)}</div><div class="sc-l" data-i="usage">مصرف کل</div>
    <div class="sc-bar"><div class="sc-bar-f" style="width:${(()=>{const q=subs.reduce((a,s)=>a+(s.quota_gb||0)*1073741824,0);return q?Math.min(100,Math.round(tU/q*100)):0;})()}%;background:var(--cy)"></div></div></div>
  <div class="sc"><div class="sc-ic ir">${I.pause}</div><div class="sc-v">${cO+cE}</div><div class="sc-l" data-i="inactive">غیرفعال / منقضی</div>
    <div class="sc-bar"><div class="sc-bar-f" style="width:${subs.length?Math.round((cO+cE)/subs.length*100):0}%;background:var(--ro)"></div></div></div>
</div>

${(()=>{
  const today=new Date().toISOString().slice(0,10);
  let todayTotal=0,activeToday=0;
  subs.forEach(s=>{
    const h=Array.isArray(s.dailyHistory)?s.dailyHistory:[];
    const last=h[h.length-1];
    if(last&&last.d===today){todayTotal+=last.b;activeToday++;}
  });
  if(!subs.length)return'';
  return `<div class="card fu f1" style="margin-bottom:16px">
    <div class="card-hd"><div class="card-t">${I.cal} <span data-i="todayGlance">امروز در یک نگاه</span></div></div>
    <div class="card-b" style="display:flex;gap:24px;flex-wrap:wrap">
      <div><div class="sc-v" style="font-size:20px">${fmt(todayTotal)}</div><div class="sc-l" data-i="todayUsage">مصرف کل امروز</div></div>
      <div><div class="sc-v" style="font-size:20px">${activeToday}</div><div class="sc-l" data-i="todayActiveSubs">اشتراک فعال امروز</div></div>
    </div>
  </div>`;
})()}

${!showOwner?`<div class="card fu f1 glow-card">
  <div class="card-hd"><div class="card-t">${I.plus} <span data-i="newSub">اشتراک جدید</span></div></div>

  <div class="card-b">
    <form method="POST" action="/panel/create" id="create-form">
      <div class="fg">
        <input type="hidden" name="protocol" id="proto-sel" value="${pk||'all'}"/>
        <div class="fl"><label>ساخت برای</label>
          <div class="proto-pick">${pk?`${PI[PROTO_META[pk].ic].replace('width="16" height="16"','width="22" height="22"')} <b>${PROTO_META[pk].title}</b>`:'<b>همه‌ی پروتکل‌ها</b> <span style="color:var(--t3);font-size:11px">VLESS + Trojan</span>'}</div>
        </div>
        <div class="fl"><label data-i="subName">نام اشتراک</label>
          <div class="igrp">
            <input type="text" name="name" id="sn" placeholder="user01"
              pattern="[a-zA-Z0-9]{1,6}" maxlength="6" title="فقط حروف انگلیسی و عدد، حداکثر ۶ کاراکتر" required/>
            <button type="button" class="btn bo bic" onclick="randName()" title="رندوم">${I.shuf}</button>
          </div>
        </div>
        <div class="fl"><label data-i="quota">حجم</label>
          <div class="igrp">
            <input type="number" name="quota_gb" id="qty-input" placeholder="30" min="0.01" step="0.01" required/>
            <select name="qtyUnit" id="qty-unit" style="max-width:88px" onchange="onQtyUnitChange()">
              <option value="gb">GB</option>
              <option value="req">ریکوئست</option>
            </select>
          </div>
          <div id="qty-note" style="font-size:10px;color:var(--t3);margin-top:3px;display:none">
            هر ${REQ_PER_GB.toLocaleString('fa-IR')} ریکوئست ≈ ۱ گیگابایت — همین الان به گیگ تبدیل می‌شه
          </div>
        </div>
        <div class="fl"><label data-i="days">روز</label>
          <input type="number" name="expire_days" placeholder="30" min="1" required/>
        </div>
        <div class="fl"><label>&nbsp;</label>
          <button class="btn bp" style="width:100%;justify-content:center">
            ${I.plus} <span data-i="create">بساز</span>
          </button>
        </div>
      </div>

      <div id="v2-extra-wrap" class="v2-extra">
        ${_V2GROUPS.length>1?`<div class="grp-box">
          <div class="grp-hd"><span>گروه کانفیگ‌ها</span>
            <label class="grp-all"><input type="checkbox" id="grp-all" checked onchange="grpAll(this)"/> انتخاب همه</label></div>
          <div class="grp-list">${_V2GROUPS.map(g=>`<label class="grp-chip"><input type="checkbox" class="grp-i" name="groups" value="${_he(g)}" checked onchange="grpSync()"/> ${_he(g)}</label>`).join("")}</div>
        </div>`:""}
        <div class="v2-extra-hd">
          <span class="v2-extra-title">${I.zap} کانفیگ اختصاصی این اشتراک</span>
          <span class="v2-extra-sub">اختیاری — اگه پر کنی، این اشتراک به‌جای لیست عمومی، از کشور و پروکسی IP خودش استفاده می‌کنه</span>
        </div>
        <div class="fg" style="grid-template-columns:1fr 1fr">
          <div class="fl"><label>${I.globe.replace('width="16" height="16"','width="13" height="13"')} کشور اختصاصی</label>
            <select name="country" style="width:100%">${countryOptions()}</select>
          </div>
          <div class="fl"><label>پروکسی IP اختصاصی</label>
            <input type="text" name="proxyIp" placeholder="مثلاً 1.2.3.4 (خالی = پیش‌فرض)" style="direction:ltr;font-family:monospace"/>
          </div>
        </div>
        <div class="fl" style="margin-top:10px">
          <label>${I.cal.replace('width="14" height="14"','width="13" height="13"')} حداکثر مصرف روزانه (GB) — اختیاری</label>
          <input type="number" name="dailyLimitGb" placeholder="خالی = بدون سقف روزانه" min="0.01" step="0.01" style="width:100%"/>
        </div>
      </div>
    </form>
  </div>
</div>`:''}

<div class="card fu f2">
  <div class="card-hd">
    <div class="card-t">${I.users} <span data-i="subs">اشتراک‌ها</span></div>
    <span style="font-size:11px;color:var(--t3)">${subs.length}</span>
  </div>
  ${subs.length===0
    ?`<div class="empty">${I.users.replace(/width="\d+" height="\d+"/,'width="36" height="36"')}<div class="empty-t">هنوز اشتراکی نیست</div><div class="empty-s">از فرم بالا بساز</div></div>`
    :`<div class="tbw"><table class="dtab">
      <thead><tr>
        <th data-i="subName">نام</th><th data-i="status">وضعیت</th>
        <th data-i="usageCol">مصرف</th><th data-i="expire">انقضا</th>
        <th data-i="actions">عملیات</th>
      </tr></thead>
      <tbody>${rows}</tbody>
    </table></div>`}
</div>

</div>

<div class="row-menu-backdrop" id="row-menu-bd" onclick="closeAllRowMenus()"></div>

<!-- مودال تاریخچه‌ی مصرف روزانه -->
<div class="qr-modal" id="hist-modal" onclick="if(event.target===this)document.getElementById('hist-modal').classList.remove('open')">
  <div class="qr-modal-box" style="max-width:380px">
    <div class="qr-modal-hd">
      <span id="hist-modal-title">تاریخچه مصرف</span>
      <button type="button" class="btn bo bic bsm" onclick="document.getElementById('hist-modal').classList.remove('open')">${I.close}</button>
    </div>
    <div id="hist-modal-body" style="max-height:340px;overflow-y:auto"></div>
  </div>
</div>
<script>
var HIST_DATA=${JSON.stringify(Object.fromEntries(subs.map(s=>[s.name,{
  hist:s.dailyHistory||[],
  dailyUsed:s.dailyUsed||0,
  dailyLimitGb:s.dailyLimitGb||0,
  dailyResetAt:s.dailyResetAt||0
}])))};
function fmtB(n){
  if(n>=1073741824)return (n/1073741824).toFixed(2)+' GB';
  if(n>=1048576)return (n/1048576).toFixed(1)+' MB';
  if(n>=1024)return (n/1024).toFixed(0)+' KB';
  return n+' B';
}
function openHist(name){
  var d=HIST_DATA[name];if(!d)return;
  document.getElementById('hist-modal-title').textContent='تاریخچه مصرف — '+name;
  var body=document.getElementById('hist-modal-body');
  var dActive=d.dailyResetAt&&Date.now()<=d.dailyResetAt;
  var todayUsed=dActive?d.dailyUsed:0;
  var todayReqEst=Math.round(todayUsed*${REQ_PER_GB}/1073741824);
  var html='';
  if(d.dailyLimitGb>0){
    var todayRemB=Math.max(0,d.dailyLimitGb*1073741824-todayUsed);
    var todayRemReq=Math.round(todayRemB*${REQ_PER_GB}/1073741824);
    html+='<div class="ur" style="margin-bottom:10px;font-size:12px">'
      +'📅 امروز: <b>'+fmtB(todayUsed)+'</b> از '+d.dailyLimitGb+' GB'
      +' (≈'+todayReqEst.toLocaleString('fa-IR')+' ریکوئست)<br>'
      +'باقی‌مونده امروز: <b style="color:var(--em)">'+fmtB(todayRemB)+'</b>'
      +' (≈'+todayRemReq.toLocaleString('fa-IR')+' ریکوئست)</div>';
  }else{
    html+='<div class="ur" style="margin-bottom:10px;font-size:12px">📅 امروز: <b>'+fmtB(todayUsed)+'</b> (≈'+todayReqEst.toLocaleString('fa-IR')+' ریکوئست) — بدون سقف روزانه</div>';
  }
  if(!d.hist||!d.hist.length){
    html+='<div style="font-size:12px;color:var(--t3);text-align:center;padding:16px">هنوز مصرفی ثبت نشده</div>';
  }else{
    var maxB=Math.max.apply(null,d.hist.map(function(h){return h.b;}))||1;
    html+='<div style="display:flex;flex-direction:column;gap:6px">';
    d.hist.slice().reverse().forEach(function(h){
      var pct=Math.max(2,h.b/maxB*100);
      html+='<div style="display:flex;align-items:center;gap:8px;font-size:11px">'
        +'<span style="width:70px;flex-shrink:0;color:var(--t3)">'+h.d+'</span>'
        +'<div style="flex:1;background:var(--s2);border-radius:6px;overflow:hidden;height:14px">'
          +'<div style="width:'+pct+'%;height:100%;background:var(--gi)"></div>'
        +'</div>'
        +'<span style="width:64px;flex-shrink:0;text-align:left;color:var(--t2)">'+fmtB(h.b)+'</span>'
      +'</div>';
    });
    html+='</div>';
  }
  body.innerHTML=html;
  document.getElementById('hist-modal').classList.add('open');
}
</script>
${TOAST}<script>${JSG}
(function(){
  var proto=document.getElementById('proto-sel');
  var extra=document.getElementById('v2-extra-wrap');
  if(!proto||!extra)return;
  function sync(){extra.style.display=(proto.value==='wireguard')?'none':'flex';}
  proto.addEventListener('change',sync);sync();
})()
</script></body></html>`;
}

// ═══════════════════════════════════════════════════════════════════════════
//  PAGE: ADMINS (سوپرادمین — مدیریت پنل‌ها)
// ═══════════════════════════════════════════════════════════════════════════
function adminsPage(admins,ok="",err=""){
  const rows=admins.map((a,i)=>{
    const exp=isAdminExpired(a);
    const dl=adminDaysLeft(a);
    let statusBadge;
    if(a.is_super) statusBadge=`<span class="badge bg"><span class="dot"></span>نامحدود</span>`;
    else if(!a.expire_days) statusBadge=`<span class="badge bd2"><span class="dot"></span>نامحدود</span>`;
    else if(exp) statusBadge=`<span class="badge br"><span class="dot"></span>منقضی</span>`;
    else if(dl<4) statusBadge=`<span class="badge ba"><span class="dot"></span>${dl} روز مانده</span>`;
    else statusBadge=`<span class="badge bg"><span class="dot"></span>${dl} روز مانده</span>`;

    return `
<tr class="fu" style="animation-delay:${i*.03}s">
<td><div class="nm">${a.username}${a.is_super?' <span style="color:var(--am);font-size:11px">'+I.crown.replace('width="15" height="15"','width="12" height="12"')+' سوپرادمین</span>':''}</div>
  <div class="dt">${new Date(a.created_at).toLocaleDateString('fa-IR')}</div></td>
<td><span style="font-family:monospace;font-size:12px;color:var(--t2)">${'•'.repeat(Math.min(a.password.length,10))}</span></td>
<td>${statusBadge}</td>
<td>
  <div class="acts">
    <a class="btn bo bsm bic" href="/panel/${encodeURIComponent(a.username)}" title="ورود به داشبورد این پنل">${I.link}</a>
    ${!a.is_super?`
    <!-- تمدید روزهای پنل -->
    <form method="POST" action="/panel/admins/renew/${encodeURIComponent(a.username)}" style="display:inline">
      <div class="ief" title="تمدید روزهای این پنل">
        ${I.cal}
        <input type="number" name="days" placeholder="روز" min="1" max="3650"/>
        <button class="btn bam bsm" type="submit">${I.check}</button>
      </div>
    </form>
    <form method="POST" action="/panel/admins/delete/${encodeURIComponent(a.username)}"
      onsubmit="return confirm('حذف پنل «${a.username}»؟ همه اشتراک‌های این پنل هم حذف میشن. برگشت‌پذیر نیست.')" style="display:inline">
      <button class="btn bro bsm bic" title="حذف پنل">${I.trash}</button>
    </form>`:''}
  </div>
</td>
</tr>`;}).join('');

  return `<!DOCTYPE html><html lang="fa" dir="rtl"><head>
<meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>مدیریت پنل‌ها — ${PANEL_NAME}</title><link rel="icon" type="image/svg+xml" href="/logo.svg"><style>${CSS}${CSSX}${DOCKCSS}${LAYOUTCSS}${NATURALCSS}</style></head>
<body>
${nav('مدیریت پنل‌ها',{username:SUPERADMIN_USER,is_super:true})}
<div class="pg wrap">
${ok?`<div class="alr alr-ok fu">${I.check} ${ok}</div>`:''}
${err?`<div class="alr alr-er fu">${err}</div>`:''}

<div class="card fu f1">
  <div class="card-hd"><div class="card-t">${I.plus} پنل مدیریتی جدید</div></div>
  <div class="card-b">
    <form method="POST" action="/panel/admins/create">
      <div class="fg" style="grid-template-columns:1fr 1fr 100px auto">
        <div class="fl"><label>یوزرنیم</label>
          <input type="text" name="username" placeholder="مثلا reseller1" pattern="[a-zA-Z0-9_\\-]+" required/>
        </div>
        <div class="fl"><label>پسورد</label>
          <input type="text" name="password" placeholder="رمز عبور" required minlength="4"/>
        </div>
        <div class="fl"><label>مدت (روز)</label>
          <input type="number" name="expire_days" placeholder="30" min="1" max="3650" required/>
        </div>
        <div class="fl"><label>&nbsp;</label>
          <button class="btn bp" style="width:100%;justify-content:center">${I.plus} بساز</button>
        </div>
      </div>
      <div style="font-size:11px;color:var(--t3);margin-top:8px">
        پس از پایان این مدت، این پنل دیگه نمی‌تونه وارد بشه — تا وقتی که از همینجا تمدیدش کنی.
      </div>
    </form>
  </div>
</div>

<div class="card fu f2">
  <div class="card-hd">
    <div class="card-t">${I.panel} پنل‌های مدیریتی</div>
    <span style="font-size:11px;color:var(--t3)">${admins.length}</span>
  </div>
  <div class="tbw"><table>
    <thead><tr><th>یوزرنیم</th><th>پسورد</th><th>وضعیت</th><th>عملیات</th></tr></thead>
    <tbody>${rows}</tbody>
  </table></div>
</div>

</div>
${TOAST}<script>${JSG}</script></body></html>`;
}

// ═══════════════════════════════════════════════════════════════════════════
//  PAGE: SETTINGS
// ═══════════════════════════════════════════════════════════════════════════
function settingsPage(admin,ok="",err=""){
  return `<!DOCTYPE html><html lang="fa" dir="rtl"><head>
<meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>تنظیمات — ${PANEL_NAME}</title><link rel="icon" type="image/svg+xml" href="/logo.svg"><style>${CSS}${CSSX}${DOCKCSS}${LAYOUTCSS}${NATURALCSS}</style></head>
<body>
${nav('تنظیمات',admin)}
<div class="pg wrap" style="max-width:680px">
${ok?`<div class="alr alr-ok fu">${I.check} ${ok}</div>`:''}
${err?`<div class="alr alr-er fu">${err}</div>`:''}

<div class="card fu">
  <div class="card-hd"><div class="card-t">${I.key} <span data-i="security">امنیت</span></div></div>
  <div class="card-b">
    <div class="set-row">
      <div><div class="si-t">نام کاربری پنل</div>
        <div class="si-d">این نام برای ورود استفاده میشه — یوزرنیم فعلی: <b style="color:var(--t1)">${admin.username}</b></div></div>
    </div>
    <div class="set-row">
      <div><div class="si-t" data-i="panelPwd">رمز عبور پنل</div>
        <div class="si-d" data-i="pwdDesc">رمز ورود به پنل مدیریت را تغییر دهید</div></div>
      <form method="POST" action="/panel/settings" style="display:flex;flex-direction:column;gap:7px;min-width:210px">
        <div class="pw"><input type="password" id="np" name="newPassword" data-i-placeholder="newPwd" placeholder="رمز جدید" required style="padding-left:36px"/><button type="button" class="pey" onclick="togglePwd('np',this)">${I.eye}</button></div>
        <div class="pw"><input type="password" id="cp2" name="confirmPassword" data-i-placeholder="confirmPwd" placeholder="تکرار رمز" required style="padding-left:36px"/><button type="button" class="pey" onclick="togglePwd('cp2',this)">${I.eye}</button></div>
        <button class="btn bp bsm" style="width:100%;justify-content:center">${I.check} <span data-i="savePwd">ذخیره</span></button>
      </form>
    </div>
  </div>
</div>

<div class="card fu f1">
  <div class="card-hd"><div class="card-t">${I.shield} <span data-i="sysInfo">اطلاعات سیستم</span></div></div>
  <div class="card-b" style="display:grid;gap:8px">
    <div class="set-row"><div><div class="si-t">Endpoint</div><div class="si-d" style="direction:ltr;font-family:monospace;font-size:12px">8.6.112.165:928</div></div><span class="badge bg"><span class="dot"></span>Active</span></div>
    <div class="set-row"><div><div class="si-t">DNS</div><div class="si-d" style="direction:ltr;font-family:monospace;font-size:12px">1.1.1.1 (Cloudflare)</div></div><span class="badge bg"><span class="dot"></span>Active</span></div>
    <div class="set-row"><div><div class="si-t">Usage Tracking</div><div class="si-d" style="font-size:12px">مصرف موقع عبور ترافیک شمرده میشه — برای اصلاح دستی از فیلد MB در داشبورد استفاده کن</div></div><span class="badge ba"><span class="dot"></span>Manual</span></div>
  </div>
</div>

</div>
${TOAST}<script>${JSG}</script></body></html>`;
}

// ═══════════════════════════════════════════════════════════════════════════
//  PAGE: SUBLINK (کاربر)
// ═══════════════════════════════════════════════════════════════════════════
function subPage(sub,baseUrl,owner,v2Links,subTok){
  const dl=daysLeft(sub),qb=sub.quota_gb*1073741824;
  const ub=sub.used_bytes||0,rem=Math.max(0,qb-ub);
  const pct=Math.min(100,qb>0?ub/qb*100:0),exp=isExp(sub);
  const ownerName=owner||sub.owner||"";
  const hasV2=!!(v2Links&&v2Links.length);
  const protoLabel=sub.protocol==="vless"?"VLESS":sub.protocol==="trojan"?"Trojan":"VLESS + Trojan";
  // لینک کوتاه توکنی (فقط ASCII، بدون یوزرنیم/اسم) — چون خیلی از کلاینت‌ها/
  // اپ‌های پیام‌رسان با لینک‌های حاوی کاراکترهای اینکودشده (فارسی و…) درست
  // رفتار نمی‌کنن. اگه به هر دلیلی توکن نساخته شده بود، فرم قدیمی fallback میشه
  const subUrl=subTok?`${baseUrl}/s/${subTok}`:`${baseUrl}/v2sub/${encodeURIComponent(ownerName)}/${encodeURIComponent(sub.name)}`;
  const links=v2Links||[];

  if(sub.disabled) return `<!DOCTYPE html><html lang="fa" dir="rtl"><head>
<meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>${sub.name}</title><link rel="icon" type="image/svg+xml" href="/logo.svg"><style>${CSS}${CSSX}${DOCKCSS}${LAYOUTCSS}${NATURALCSS}</style></head>
<body><div class="pg-sm wrap">
<div class="sub-hero fu"><div class="sub-ic" style="border-color:rgba(244,63,94,.2);background:rgba(244,63,94,.05);color:var(--ro)">${I.lock.replace('42','36').replace('42','36')}</div>
<div class="sub-t">${sub.name}</div></div>
<div class="st-box dis fu f1"><div class="st-ic dis">${I.ban}</div>
<div class="st-t" style="color:var(--ro)" data-i="disTitle">اشتراک شما غیرفعال است</div>
<div class="st-s" data-i="disSub">برای فعال‌سازی با پشتیبانی تماس بگیرید</div></div>
</div>${TOAST}<script>${JSG}</script></body></html>`;


  // کتابخانه QR داخل خودِ ورکر باندل شده (بدون وابستگی به CDN خارجی مثل
  // jsdelivr که ممکنه در شبکه‌ی کاربر فیلتر باشه) → همیشه درجا و آفلاین کار میکنه
  const qrJs=hasV2?`
<script>${QR_LIB_JS}</script>
<script>
function renderQR(elId,data){
  var el=document.getElementById(elId);
  if(!el)return;
  if(typeof qrcode==='undefined'){el.innerHTML='<div style="font-size:11px;color:var(--ro);text-align:center;padding:12px">خطا در ساخت QR — از دانلود/کپی مستقیم استفاده کنید</div>';return;}
  try{
    // برای متن‌های طولانی (لینک VLESS/Trojan) سطح تصحیح خطا
    // رو پایین (L) نگه میداریم — سطح بالاتر (M/Q/H) باعث میشه تعداد ماژول‌های
    // کیوآر برای همون داده بیشتر بشه یعنی کیوآر شلوغ‌تر و اسکنش سخت‌تر بشه،
    // خصوصاً روی صفحه‌ی کوچیک گوشی. برای متن کوتاه (زیر ۵۰ کاراکتر) از M استفاده میشه.
    var qr=qrcode(0, data.length>50?'L':'M');
    qr.addData(data);
    qr.make();
    el.innerHTML=qr.createSvgTag({cellSize:6,margin:10});
    var svg=el.querySelector('svg');
    if(svg){svg.style.width='100%';svg.style.height='auto';svg.style.maxWidth='280px';svg.style.display='block';svg.style.margin='0 auto';}
  }catch(e){el.innerHTML='<div style="font-size:11px;color:var(--ro);text-align:center;padding:12px">خطا در ساخت QR — از دانلود/کپی مستقیم استفاده کنید</div>';}
}
</script>`:'';


  return `<!DOCTYPE html><html lang="fa" dir="rtl"><head>
<meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>${sub.name} — ${PANEL_NAME}</title><link rel="icon" type="image/svg+xml" href="/logo.svg"><style>${CSS}${CSSX}${DOCKCSS}${LAYOUTCSS}${NATURALCSS}</style></head>
<body><div class="pg-sm wrap">

<div style="display:flex;justify-content:flex-start;padding:8px 0 0;gap:6px">
  <button class="btn bo bic t-tog" onclick="toggleTheme()">${I.moon}</button>
  <button class="btn bo bic l-tog" onclick="toggleLang()">EN</button>
</div>

<div class="sub-hero fu">
  <img class="sub-logo" src="/logo.svg" alt="Epic" width="84" height="84">
  <div class="sub-t">${sub.name}</div>
  <div style="margin-top:8px">
    ${exp?`<span class="badge br"><span class="dot"></span><span data-i="expiredB">منقضی</span></span>`
        :dl<4?`<span class="badge ba"><span class="dot"></span><span data-i="soon">رو به اتمام</span></span>`
             :`<span class="badge bg"><span class="dot"></span><span data-i="on">فعال</span></span>`}
  </div>
</div>

<!-- مصرف -->
<div class="card fu f1">
  <div class="card-hd"><div class="card-t">${I.act} <span data-i="usageCol">مصرف</span></div></div>
  <div class="card-b">
    <div class="two-up">
      <div class="vb"><div class="vb-v" style="color:var(--em)" id="rem-val">${fmt(rem)}</div><div class="vb-l" data-i="remaining">باقیمانده</div></div>
      <div class="vb"><div class="vb-v" style="color:var(--cy)" id="days-val">${dl} <span style="font-size:13px">روز</span></div><div class="vb-l">زمان باقی</div></div>
    </div>
    <div style="display:flex;justify-content:space-between;font-size:11px;color:var(--t2);margin-bottom:5px">
      <span><span id="used-val">${fmt(ub)}</span> مصرف شده</span>
      <span style="color:var(--t3)"><span id="pct-val">${pct.toFixed(1)}</span>% از ${sub.quota_gb} GB</span>
    </div>
    <div class="pt" style="height:7px">
      <div class="pb${pct>80?' hot':''}" style="width:${pct.toFixed(1)}%" id="prog-bar"></div>
    </div>
  </div>
</div>

${exp
  ?`<div class="st-box exp fu f2"><div class="st-ic exp">${I.warn}</div>
     <div class="st-t" style="color:var(--am)" data-i="expTitle">اشتراک منقضی شده</div>
     <div class="st-s" data-i="expSub">جهت تمدید با پشتیبانی تماس بگیرید</div></div>`
  :`

   ${hasV2?`
   <!-- VLESS / Trojan -->
   <div class="card fu f2">
     <div class="card-hd"><div class="card-t">${I.dl.replace('20','14').replace('20','14')} <span>کانفیگ${links.length>1?'‌های ':' '}وی‌توری</span></div></div>
     <div class="card-b">
       <div class="dl-r" style="cursor:pointer;border:1px solid rgba(99,102,241,.35);background:rgba(99,102,241,.06)" onclick="cp(V2_SUB_URL)" id="v2-sub-box">
         <div class="dl-ic">${I.link}</div>
         <div><div class="dl-t">لینک ساب‌اسکریپشن (این رو داخل کلاینت بزن)</div>
           <div class="dl-h" style="direction:ltr;word-break:break-all;font-family:monospace;font-size:10px">${subUrl}</div></div>
         <svg style="margin-right:auto" width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="var(--in)" stroke-width="2.5" stroke-linecap="round"><rect x="9" y="9" width="13" height="13" rx="2"/><path d="M5 15H4a2 2 0 0 1-2-2V4a2 2 0 0 1 2-2h9a2 2 0 0 1 2 2v1"/></svg>
       </div>
       <div style="font-size:11px;color:var(--t3);text-align:center;margin:8px 0 14px">این لینک رو داخل «Add subscription / Subscribe» اپ (v2rayNG، NekoBox، Streisand، v2Box) بذار — همه‌ی کانفیگ‌ها + حجم و روز باقی‌مونده خودکار میاد و آپدیت میشه</div>
       <div style="font-size:11px;color:var(--t3);margin-bottom:6px">یا هر کانفیگ رو تک‌تک کپی یا با کیوآر اسکن کن:</div>
       ${links.map((lnk,i)=>{
         const rem=decodeURIComponent((lnk.split('#')[1]||`کانفیگ ${i+1}`));
         const port=(lnk.match(/[@:](\d+)\?/)||[])[1]||'';
         return `
       <div class="dl-r v2-row">
         <div class="dl-ic">${I.zapS}</div>
         <div class="v2-row-b" onclick="cp(V2_LINKS[${i}])">
           <div class="dl-t">${rem}</div>
           <div class="dl-h" style="direction:ltr;word-break:break-all;font-family:monospace;font-size:10px">${port?`پورت ${port} · `:''}${lnk.slice(0,46)}${lnk.length>46?'…':''}</div>
         </div>
         <button type="button" class="btn bo bic bsm" title="کپی لینک" onclick="cp(V2_LINKS[${i}])">${I.link}</button>
         <button type="button" class="btn bo bic bsm" title="نمایش کیوآر" onclick="openQr(${i})">${I.qr}</button>
       </div>`;}).join('')}
     </div>
   </div>

   <!-- مودال کیوآر تک‌کانفیگ -->
   <div class="qr-modal" id="qr-modal" onclick="if(event.target===this)closeQr()">
     <div class="qr-modal-box">
       <div class="qr-modal-hd">
         <span id="qr-modal-title">کیوآر کانفیگ</span>
         <button type="button" class="btn bo bic bsm" onclick="closeQr()">${I.close}</button>
       </div>
       <div class="qr-box" id="qrdiv-modal"></div>
       <button type="button" class="btn bp" style="width:100%;justify-content:center;margin-top:10px" onclick="cp(V2_LINKS[window.__qrIdx])">${I.link} کپی لینک</button>
     </div>
   </div>
   <script>
   var V2_SUB_URL=${JSON.stringify(subUrl)};var V2_LINKS=${JSON.stringify(links)};
   function openQr(i){
     window.__qrIdx=i;
     document.getElementById('qr-modal-title').textContent=V2_NAMES[i]||('کانفیگ '+(i+1));
     document.getElementById('qr-modal').classList.add('open');
     renderQR('qrdiv-modal',V2_LINKS[i]);
   }
   function closeQr(){document.getElementById('qr-modal').classList.remove('open');}
   var V2_NAMES=${JSON.stringify(links.map(l=>{try{return decodeURIComponent(l.split('#')[1]||'');}catch(e){return'';}}))};
   </script>
   `:''}
  `
}

</div>
${TOAST}<script>${JSG}</script>${qrJs}</body></html>`;
}

// ═══════════════════════════════════════════════════════════════════════════
//  SHOP BOT — فروش اشتراک وی‌توری از طریق تلگرام
// ═══════════════════════════════════════════════════════════════════════════
const SHOPP="shop:";
const SHOP_DEFAULTS={
  botToken:"",adminIds:[],ownerUsername:"",cardNumber:"",cardHolder:"",
  pricePerGB:0,testEnabled:true,testGB:1,testDays:1,
};
async function shopGetSettings(kv){
  const s=await kv.get(`${SHOPP}settings`,"json");
  return s?{...SHOP_DEFAULTS,...s}:{...SHOP_DEFAULTS};
}
const shopPutSettings=(kv,d)=>kv.put(`${SHOPP}settings`,JSON.stringify(d));

const shopGetUser=(kv,id)=>kv.get(`${SHOPP}user:${id}`,"json");
const shopPutUser=(kv,id,d)=>kv.put(`${SHOPP}user:${id}`,JSON.stringify(d));
async function shopListUsers(kv){
  const {keys}=await kv.list({prefix:`${SHOPP}user:`});
  const all=await Promise.all(keys.map(k=>kv.get(k.name,"json")));
  return all.filter(Boolean);
}
const shopGetState=(kv,id)=>kv.get(`${SHOPP}state:${id}`,"json");
const shopPutState=(kv,id,d)=>kv.put(`${SHOPP}state:${id}`,JSON.stringify(d));
const shopDelState=(kv,id)=>kv.delete(`${SHOPP}state:${id}`);

async function shopGetPlans(kv){
  let plans=(await kv.get(`${SHOPP}plans`,"json"))||[];
  if(!plans.some(p=>p.unlimited)){
    plans.push({id:shopRandId(),name:"🚀 نامحدود (ماهانه)",gb:10000,days:30,price:100,unlimited:true});
    await kv.put(`${SHOPP}plans`,JSON.stringify(plans));
  }
  return plans;
}
const shopPutPlans=(kv,arr)=>kv.put(`${SHOPP}plans`,JSON.stringify(arr));

async function shopGetCharges(kv){return (await kv.get(`${SHOPP}charges`,"json"))||[];}
const shopPutCharges=(kv,arr)=>kv.put(`${SHOPP}charges`,JSON.stringify(arr));

async function shopGetPurchases(kv,id){return (await kv.get(`${SHOPP}purchases:${id}`,"json"))||[];}
const shopPutPurchases=(kv,id,arr)=>kv.put(`${SHOPP}purchases:${id}`,JSON.stringify(arr));

function shopFmtT(n){return Math.round(n||0).toLocaleString("fa-IR");}
function shopRandId(){return Date.now().toString(36)+Math.random().toString(36).slice(2,6);}
function shopValidSubName(n){return /^[A-Za-z0-9]{1,6}$/.test(n||"");}
function shopRandSubName(){const c="abcdefghijklmnopqrstuvwxyz0123456789";return Array.from({length:6},()=>c[Math.floor(Math.random()*c.length)]).join("");}

// ─── Telegram API ────────────────────────────────────────────────────────
async function tg(token,method,payload){
  try{
    const r=await fetchT(`https://api.telegram.org/bot${token}/${method}`,{
      method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify(payload)
    });
    return await r.json();
  }catch(e){return {ok:false,error:String(e)};}
}
function tgSend(token,chatId,text,extra={}){
  return tg(token,"sendMessage",{chat_id:chatId,text,parse_mode:"HTML",disable_web_page_preview:true,...extra});
}
function tgAnswerCb(token,id,text="",alert=false){
  return tg(token,"answerCallbackQuery",{callback_query_id:id,text,show_alert:alert});
}
function tgSendPhoto(token,chatId,photo,caption="",extra={}){
  return tg(token,"sendPhoto",{chat_id:chatId,photo,caption,parse_mode:"HTML",...extra});
}
async function tgSetWebhook(token,url){return tg(token,"setWebhook",{url,drop_pending_updates:true});}

function isShopAdmin(settings,userId){return (settings.adminIds||[]).map(String).includes(String(userId));}

// ─── Keyboards ───────────────────────────────────────────────────────────
function shopMainKeyboard(settings,userId){
  const rows=[[{text:"🛍 خرید سرویس",callback_data:"buy_menu",style:"primary"}]];
  if(settings.testEnabled)rows.push([{text:"🎁 اکانت تست رایگان",callback_data:"get_test",style:"success"}]);
  rows.push([{text:"💰 کیف پول",callback_data:"balance",style:"primary"},{text:"📋 سوابق خرید",callback_data:"history",style:"primary"}]);
  rows.push([{text:"👤 حساب کاربری",callback_data:"account",style:"primary"},{text:"💬 پشتیبانی",callback_data:"support",style:"primary"}]);
  if(isShopAdmin(settings,userId)){
    if(settings.panelUrl)rows.push([{text:"🖥 باز کردن پنل (مینی‌اپ)",web_app:{url:settings.panelUrl}}]);
    rows.push([{text:"⚙️ پنل مدیریت",callback_data:"admin_panel",style:"danger"}]);
  }
  return {inline_keyboard:rows};
}
function shopAdminKeyboard(){
  return {inline_keyboard:[
    [{text:"➕ افزودن پلن",callback_data:"admin_add_plan",style:"success"},{text:"🗑 حذف پلن",callback_data:"admin_del_plan",style:"danger"}],
    [{text:"✏️ ویرایش قیمت پلن",callback_data:"admin_edit_plan_price",style:"primary"}],
    [{text:"💲 نرخ هر گیگ (سفارشی)",callback_data:"admin_set_gb_price",style:"primary"}],
    [{text:"🎁 تنظیم اکانت تست",callback_data:"admin_set_test",style:"primary"}],
    [{text:"💳 شماره کارت",callback_data:"admin_set_card",style:"primary"}],
    [{text:"👥 کاربران",callback_data:"admin_users",style:"primary"},{text:"🔍 اطلاعات کاربر",callback_data:"admin_user_info",style:"primary"}],
    [{text:"🚫 بلاک",callback_data:"admin_block",style:"danger"},{text:"✅ آنبلاک",callback_data:"admin_unblock",style:"success"}],
    [{text:"💵 تایید شارژها",callback_data:"admin_confirm_charge",style:"primary"}],
    [{text:"📢 پیام همگانی",callback_data:"admin_broadcast",style:"primary"}],
    [{text:"🔙 بازگشت",callback_data:"back_main"}],
  ]};
}

// ─── ساخت و تحویل اشتراک (از همون توابع پنل استفاده می‌کنه) ─────────────
async function shopCreateSub(kv,owner,quota_gb,expire_days,name){
  const creds=v2NewCreds();
  const row={name,owner,quota_gb,expire_days,created_at:Date.now(),used_bytes:0,disabled:false,protocol:"all",
    v2uuid:creds.v2uuid,v2pass:creds.v2pass,v2passHash:sha224Hex(creds.v2pass)};
  await kvPut(kv,owner,name,row);
  await v2IndexPut(kv,row,owner,name);
  return row;
}
function shopSubLink(base,owner,name){return `${base}/sublink/${encodeURIComponent(owner)}/${encodeURIComponent(name)}`;}

async function shopDeliverSub(token,chatId,base,owner,row,title){
  const link=shopSubLink(base,owner,row.name);
  let msg=`✅ <b>${title}</b>\n\n📦 حجم: <b>${row.quota_gb} GB</b>\n📅 مدت: <b>${row.expire_days} روز</b>\n\n`;
  msg+=`🔗 لینک اشتراک شما (کانفیگ + QR + دانلود):\n${link}\n\nبا باز کردن لینک بالا، QR و فایل کانفیگ به‌صورت خودکار آماده میشه.`;
  await tgSend(token,chatId,msg);
}

// ─── تکمیل ساخت اشتراک بعد از دریافت اسم دلخواه کاربر ───────────────────
async function shopFinalizeSubPurchase(kv,base,settings,token,chatId,userId,user,name,pending){
  const owner=settings.ownerUsername;
  if(!owner){await shopDelState(kv,userId);await tgSend(token,chatId,"⚠️ فروشگاه هنوز کامل تنظیم نشده. به ادمین اطلاع بده.",{reply_markup:shopMainKeyboard(settings,userId)});return;}
  const existing=await kvGet(kv,owner,name);
  if(existing){
    await tgSend(token,chatId,"❌ این اسم قبلاً استفاده شده. یه اسم دیگه وارد کن یا از دکمه نام رندوم استفاده کن:",
      {reply_markup:{inline_keyboard:[[{text:"🎲 نام رندوم",callback_data:"random_sub_name",style:"primary"}]]}});
    return;
  }
  let gb,days,price=0,title;
  if(pending.type==="plan"){
    const plans=await shopGetPlans(kv);
    const plan=plans.find(p=>p.id===pending.planId);
    if(!plan){await shopDelState(kv,userId);await tgSend(token,chatId,"❌ این پلن دیگه موجود نیست.",{reply_markup:shopMainKeyboard(settings,userId)});return;}
    if((user.balance||0)<plan.price){await shopDelState(kv,userId);await tgSend(token,chatId,"❌ موجودی کافی نیست.",{reply_markup:shopMainKeyboard(settings,userId)});return;}
    gb=plan.gb;days=plan.days;price=plan.price;title=plan.name;
  }else if(pending.type==="custom"){
    gb=pending.gb;days=pending.days;price=pending.price;
    if((user.balance||0)<price){await shopDelState(kv,userId);await tgSend(token,chatId,"❌ موجودی کافی نیست.",{reply_markup:shopMainKeyboard(settings,userId)});return;}
    title=`سفارشی ${gb}GB / ${days} روز`;
  }else{
    if(!settings.testEnabled){await shopDelState(kv,userId);await tgSend(token,chatId,"اکانت تست فعلاً غیرفعاله.",{reply_markup:shopMainKeyboard(settings,userId)});return;}
    if(user.usedTest){await shopDelState(kv,userId);await tgSend(token,chatId,"❌ شما قبلاً از اکانت تست استفاده کردی.",{reply_markup:shopMainKeyboard(settings,userId)});return;}
    gb=settings.testGB||1;days=settings.testDays||1;price=0;title="اکانت تست رایگان";
  }
  await shopDelState(kv,userId);
  await tgSend(token,chatId,"⏳ در حال ساخت کانفیگ...");
  const row=await shopCreateSub(kv,owner,gb,days,name);
  if(pending.type==="test"){
    user.usedTest=true;
  }else{
    user.balance=(user.balance||0)-price;
    user.purchases=(user.purchases||0)+1;
    const purchases=await shopGetPurchases(kv,userId);
    purchases.unshift({productName:title,price,date:Date.now(),config:shopSubLink(base,owner,row.name)});
    await shopPutPurchases(kv,userId,purchases);
  }
  await shopPutUser(kv,userId,user);
  await shopDeliverSub(token,chatId,base,owner,row,title);
}

async function shopEnsureUser(kv,from){
  const userId=from.id;
  let user=await shopGetUser(kv,userId);
  if(!user){
    user={id:userId,name:from.first_name||"کاربر",username:from.username||"",balance:0,purchases:0,usedTest:false,blocked:false,created_at:Date.now()};
    await shopPutUser(kv,userId,user);
  }
  return user;
}

// ─── پیام‌های متنی + استیت‌های چندمرحله‌ای ──────────────────────────────
async function handleShopMessage(kv,base,settings,msg){
  const token=settings.botToken,chatId=msg.chat.id,userId=msg.from.id;
  const text=(msg.text||"").trim();
  const user=await shopEnsureUser(kv,msg.from);
  if(user.blocked){await tgSend(token,chatId,"⛔️ شما مسدود شده‌اید.");return;}

  if(text==="/start"){
    await shopDelState(kv,userId);
    await tgSend(token,chatId,`👋 سلام ${user.name}!\nبه فروشگاه اشتراک خوش اومدی.`,{reply_markup:shopMainKeyboard(settings,userId)});
    return;
  }
  if(text==="/cancel"){
    await shopDelState(kv,userId);
    await tgSend(token,chatId,"❌ لغو شد.",{reply_markup:shopMainKeyboard(settings,userId)});
    return;
  }

  const state=await shopGetState(kv,userId);
  if(state)return handleShopState(kv,base,settings,msg,user,state);

  await tgSend(token,chatId,"از دکمه‌های زیر استفاده کن:",{reply_markup:shopMainKeyboard(settings,userId)});
}

async function handleShopState(kv,base,settings,msg,user,state){
  const token=settings.botToken,chatId=msg.chat.id,userId=user.id,text=(msg.text||"").trim();
  const isAdm=isShopAdmin(settings,userId);
  const data=state.data||{};

  switch(state.step){
    case "charge_amount":{
      const amt=parseInt(text.replace(/[^\d]/g,""));
      if(!amt||amt<=0){await tgSend(token,chatId,"❌ مبلغ نامعتبره، دوباره وارد کن (فقط عدد):");return;}
      await shopPutState(kv,userId,{step:"charge_receipt",data:{amount:amt}});
      let cardMsg=`💳 لطفاً مبلغ <b>${shopFmtT(amt)} تومان</b> رو به شماره کارت زیر واریز کن:\n\n<code>${settings.cardNumber||"— شماره کارت ثبت نشده —"}</code>\n`;
      if(settings.cardHolder)cardMsg+=`به نام: ${settings.cardHolder}\n`;
      cardMsg+=`\n📸 بعد از واریز، عکس رسید پرداخت رو همینجا برام بفرست تا برای ادمین ارسال بشه.\n\n/cancel برای لغو`;
      await tgSend(token,chatId,cardMsg);
      return;
    }
    case "charge_receipt":{
      const photos=msg.photo;
      const doc=msg.document;
      let fileId=null;
      if(photos&&photos.length>0)fileId=photos[photos.length-1].file_id;
      else if(doc&&doc.mime_type&&doc.mime_type.startsWith("image/"))fileId=doc.file_id;
      if(!fileId){await tgSend(token,chatId,"❌ لطفاً عکس رسید پرداخت رو ارسال کن (به‌صورت عکس، نه متن).\n\n/cancel برای لغو");return;}
      const amt=data.amount;
      const charges=await shopGetCharges(kv);
      const id=shopRandId();
      charges.push({id,userId,amount:amt,date:Date.now(),status:"pending",receiptFileId:fileId});
      await shopPutCharges(kv,charges);
      await shopDelState(kv,userId);
      await tgSend(token,chatId,"✅ رسید شما برای ادمین ارسال شد. بعد از تایید، موجودی کیف پولت خودکار اضافه میشه.",{reply_markup:shopMainKeyboard(settings,userId)});
      for(const aid of (settings.adminIds||[])){
        await tgSendPhoto(token,aid,fileId,
          `💵 درخواست شارژ جدید\n👤 کاربر: <code>${userId}</code>${user.username?` (@${user.username})`:""}\n💰 مبلغ: ${shopFmtT(amt)} تومان`,
          {reply_markup:{inline_keyboard:[[{text:"✅ تایید",callback_data:`confirm_charge:${id}`,style:"success"},{text:"❌ رد",callback_data:`reject_charge:${id}`,style:"danger"}]]}});
      }
      return;
    }
    case "custom_gb":{
      const gb=parseFloat(text);
      if(!gb||gb<=0){await tgSend(token,chatId,"❌ عدد نامعتبره، دوباره حجم رو به GB وارد کن:");return;}
      await shopPutState(kv,userId,{step:"custom_days",data:{gb}});
      await tgSend(token,chatId,"📅 تعداد روز مورد نظر رو وارد کن:");
      return;
    }
    case "custom_days":{
      const days=parseInt(text);
      if(!days||days<=0){await tgSend(token,chatId,"❌ عدد نامعتبره، دوباره تعداد روز رو وارد کن:");return;}
      const gb=data.gb,price=Math.round(gb*(settings.pricePerGB||0));
      await shopDelState(kv,userId);
      await tgSend(token,chatId,
        `🧾 <b>سفارش سفارشی</b>\n\n📦 حجم: ${gb} GB\n📅 مدت: ${days} روز\n💰 قیمت: <b>${shopFmtT(price)} تومان</b>\n\nتایید میکنی؟`,
        {reply_markup:{inline_keyboard:[
          [{text:"✅ خرید و پرداخت از کیف پول",callback_data:`confirm_custom:${gb}:${days}:${price}`,style:"success"}],
          [{text:"🔙 انصراف",callback_data:"back_main"}]
        ]}});
      return;
    }
    case "await_sub_name":{
      const name=text.trim();
      if(!shopValidSubName(name)){
        await tgSend(token,chatId,"❌ اسم نامعتبره. فقط حروف انگلیسی و عدد، حداکثر ۶ کاراکتر. دوباره وارد کن یا از دکمه زیر استفاده کن:",
          {reply_markup:{inline_keyboard:[[{text:"🎲 نام رندوم",callback_data:"random_sub_name",style:"primary"}]]}});
        return;
      }
      await shopFinalizeSubPurchase(kv,base,settings,token,chatId,userId,user,name,data);
      return;
    }
    case "support_message":{
      await shopDelState(kv,userId);
      await tgSend(token,chatId,"✅ پیام شما برای پشتیبانی ارسال شد.",{reply_markup:shopMainKeyboard(settings,userId)});
      for(const aid of (settings.adminIds||[]))
        await tgSend(token,aid,`💬 پیام پشتیبانی از <code>${userId}</code>${user.username?` (@${user.username})`:""}:\n\n${text}`);
      return;
    }
    case "admin_add_plan_name":{
      if(!isAdm)return;
      await shopPutState(kv,userId,{step:"admin_add_plan_gb",data:{name:text}});
      await tgSend(token,chatId,"📦 حجم پلن رو به GB وارد کن:");
      return;
    }
    case "admin_add_plan_gb":{
      if(!isAdm)return;
      const gb=parseFloat(text);
      if(!gb||gb<=0){await tgSend(token,chatId,"❌ عدد نامعتبر، دوباره وارد کن:");return;}
      await shopPutState(kv,userId,{step:"admin_add_plan_days",data:{...data,gb}});
      await tgSend(token,chatId,"📅 تعداد روز رو وارد کن:");
      return;
    }
    case "admin_add_plan_days":{
      if(!isAdm)return;
      const days=parseInt(text);
      if(!days||days<=0){await tgSend(token,chatId,"❌ عدد نامعتبر، دوباره وارد کن:");return;}
      await shopPutState(kv,userId,{step:"admin_add_plan_price",data:{...data,days}});
      await tgSend(token,chatId,"💰 قیمت رو به تومان وارد کن:");
      return;
    }
    case "admin_add_plan_price":{
      if(!isAdm)return;
      const price=parseInt(text.replace(/[^\d]/g,""));
      if(!price||price<=0){await tgSend(token,chatId,"❌ عدد نامعتبر، دوباره وارد کن:");return;}
      const plans=await shopGetPlans(kv);
      plans.push({id:shopRandId(),name:data.name,gb:data.gb,days:data.days,price});
      await shopPutPlans(kv,plans);
      await shopDelState(kv,userId);
      await tgSend(token,chatId,"✅ پلن اضافه شد.",{reply_markup:shopAdminKeyboard()});
      return;
    }
    case "admin_del_plan":{
      if(!isAdm)return;
      const plans=await shopGetPlans(kv);
      const idx=parseInt(text)-1;
      if(isNaN(idx)||idx<0||idx>=plans.length){await tgSend(token,chatId,"❌ شماره نامعتبر.");return;}
      plans.splice(idx,1);
      await shopPutPlans(kv,plans);
      await shopDelState(kv,userId);
      await tgSend(token,chatId,"✅ پلن حذف شد.",{reply_markup:shopAdminKeyboard()});
      return;
    }
    case "admin_edit_plan_price_idx":{
      if(!isAdm)return;
      const plans=await shopGetPlans(kv);
      const idx=parseInt(text)-1;
      if(isNaN(idx)||idx<0||idx>=plans.length){await tgSend(token,chatId,"❌ شماره نامعتبر، دوباره وارد کن:");return;}
      await shopPutState(kv,userId,{step:"admin_edit_plan_price_val",data:{idx}});
      await tgSend(token,chatId,`💰 قیمت جدید رو برای «${plans[idx].name}» به تومان وارد کن:`);
      return;
    }
    case "admin_edit_plan_price_val":{
      if(!isAdm)return;
      const price=parseInt(text.replace(/[^\d]/g,""));
      if(!price||price<=0){await tgSend(token,chatId,"❌ عدد نامعتبر، دوباره وارد کن:");return;}
      const plans=await shopGetPlans(kv);
      if(!plans[data.idx]){await shopDelState(kv,userId);await tgSend(token,chatId,"❌ خطا، دوباره تلاش کن.",{reply_markup:shopAdminKeyboard()});return;}
      plans[data.idx].price=price;
      await shopPutPlans(kv,plans);
      await shopDelState(kv,userId);
      await tgSend(token,chatId,"✅ قیمت پلن بروزرسانی شد.",{reply_markup:shopAdminKeyboard()});
      return;
    }
    case "admin_set_gb_price":{
      if(!isAdm)return;
      const p=parseInt(text.replace(/[^\d]/g,""));
      if(isNaN(p)||p<0){await tgSend(token,chatId,"❌ عدد نامعتبر.");return;}
      settings.pricePerGB=p;
      await shopPutSettings(kv,settings);
      await shopDelState(kv,userId);
      await tgSend(token,chatId,`✅ نرخ هر گیگ روی ${shopFmtT(p)} تومان تنظیم شد.`,{reply_markup:shopAdminKeyboard()});
      return;
    }
    case "admin_set_test_gb":{
      if(!isAdm)return;
      const gb=parseFloat(text);
      if(!gb||gb<=0){await tgSend(token,chatId,"❌ عدد نامعتبر.");return;}
      await shopPutState(kv,userId,{step:"admin_set_test_days",data:{gb}});
      await tgSend(token,chatId,"📅 تعداد روز اکانت تست رو وارد کن:");
      return;
    }
    case "admin_set_test_days":{
      if(!isAdm)return;
      const days=parseInt(text);
      if(!days||days<=0){await tgSend(token,chatId,"❌ عدد نامعتبر.");return;}
      settings.testGB=data.gb;settings.testDays=days;
      await shopPutSettings(kv,settings);
      await shopDelState(kv,userId);
      await tgSend(token,chatId,`✅ اکانت تست: ${settings.testGB} GB برای ${days} روز.`,{reply_markup:shopAdminKeyboard()});
      return;
    }
    case "admin_set_card_number":{
      if(!isAdm)return;
      settings.cardNumber=text;
      await shopPutSettings(kv,settings);
      await shopPutState(kv,userId,{step:"admin_set_card_holder",data:{}});
      await tgSend(token,chatId,"👤 نام صاحب کارت رو وارد کن:");
      return;
    }
    case "admin_set_card_holder":{
      if(!isAdm)return;
      settings.cardHolder=text;
      await shopPutSettings(kv,settings);
      await shopDelState(kv,userId);
      await tgSend(token,chatId,"✅ اطلاعات کارت ذخیره شد.",{reply_markup:shopAdminKeyboard()});
      return;
    }
    case "admin_user_info":{
      if(!isAdm)return;
      const uid=text.replace(/[^\d]/g,"");
      const u=await shopGetUser(kv,uid);
      await shopDelState(kv,userId);
      if(!u){await tgSend(token,chatId,"❌ کاربر یافت نشد.",{reply_markup:shopAdminKeyboard()});return;}
      await tgSend(token,chatId,
        `👤 <b>اطلاعات کاربر</b>\n\n🆔 ${u.id}\n📛 ${u.name}${u.username?` (@${u.username})`:""}\n💰 موجودی: ${shopFmtT(u.balance)} تومان\n🛒 خرید: ${u.purchases||0}\n🎁 تست: ${u.usedTest?"استفاده شده":"استفاده نشده"}\n🚫 بلاک: ${u.blocked?"بله":"خیر"}`,
        {reply_markup:shopAdminKeyboard()});
      return;
    }
    case "admin_block":{
      if(!isAdm)return;
      const uid=text.replace(/[^\d]/g,"");
      const u=await shopGetUser(kv,uid);
      await shopDelState(kv,userId);
      if(!u){await tgSend(token,chatId,"❌ کاربر یافت نشد.",{reply_markup:shopAdminKeyboard()});return;}
      u.blocked=true;await shopPutUser(kv,uid,u);
      await tgSend(token,chatId,"✅ کاربر بلاک شد.",{reply_markup:shopAdminKeyboard()});
      return;
    }
    case "admin_unblock":{
      if(!isAdm)return;
      const uid=text.replace(/[^\d]/g,"");
      const u=await shopGetUser(kv,uid);
      await shopDelState(kv,userId);
      if(!u){await tgSend(token,chatId,"❌ کاربر یافت نشد.",{reply_markup:shopAdminKeyboard()});return;}
      u.blocked=false;await shopPutUser(kv,uid,u);
      await tgSend(token,chatId,"✅ کاربر آنبلاک شد.",{reply_markup:shopAdminKeyboard()});
      return;
    }
    case "admin_broadcast":{
      if(!isAdm)return;
      await shopDelState(kv,userId);
      const users=await shopListUsers(kv);
      await tgSend(token,chatId,`⏳ در حال ارسال به ${users.length} کاربر...`);
      let ok=0;
      for(const u of users){const res=await tgSend(token,u.id,text);if(res&&res.ok)ok++;}
      await tgSend(token,chatId,`✅ پیام همگانی به ${ok} کاربر ارسال شد.`,{reply_markup:shopAdminKeyboard()});
      return;
    }
  }
}

// ─── دکمه‌های شیشه‌ای (callback_query) ──────────────────────────────────
async function handleShopCallback(kv,base,settings,cq){
  const token=settings.botToken,chatId=cq.message.chat.id,userId=cq.from.id;
  const cdata=cq.data||"";
  const user=await shopEnsureUser(kv,cq.from);
  if(user.blocked){await tgAnswerCb(token,cq.id,"⛔️ شما مسدود شده‌اید.",true);return;}
  const isAdm=isShopAdmin(settings,userId);
  await tgAnswerCb(token,cq.id);

  if(cdata==="back_main"){
    await shopDelState(kv,userId);
    await tgSend(token,chatId,"🏠 منوی اصلی",{reply_markup:shopMainKeyboard(settings,userId)});
    return;
  }
  if(cdata==="buy_menu"){
    const plans=await shopGetPlans(kv);
    const rows=plans.map(p=>[{text:`${p.name} — ${p.gb}GB/${p.days}روز — ${shopFmtT(p.price)}ت`,callback_data:`buy_plan:${p.id}`,style:"primary"}]);
    if(settings.pricePerGB>0)rows.push([{text:"🎛 سفارشی (خودت انتخاب کن)",callback_data:"buy_custom",style:"primary"}]);
    rows.push([{text:"🔙 بازگشت",callback_data:"back_main"}]);
    if(plans.length===0&&!(settings.pricePerGB>0)){
      await tgSend(token,chatId,"فعلاً محصولی برای فروش تعریف نشده.",{reply_markup:shopMainKeyboard(settings,userId)});
      return;
    }
    await tgSend(token,chatId,"🛍 <b>لیست سرویس‌ها</b>\n\nیکی رو انتخاب کن:",{reply_markup:{inline_keyboard:rows}});
    return;
  }
  if(cdata.startsWith("buy_plan:")){
    const id=cdata.split(":")[1];
    const plans=await shopGetPlans(kv);
    const plan=plans.find(p=>p.id===id);
    if(!plan){await tgSend(token,chatId,"❌ این پلن دیگه موجود نیست.");return;}
    if((user.balance||0)<plan.price){
      await tgSend(token,chatId,`❌ موجودی کافی نیست.\n💰 موجودی شما: ${shopFmtT(user.balance)} تومان\n💵 قیمت پلن: ${shopFmtT(plan.price)} تومان\n\nابتدا کیف پولت رو شارژ کن.`,
        {reply_markup:{inline_keyboard:[[{text:"➕ شارژ کیف پول",callback_data:"charge_wallet",style:"success"}],[{text:"🔙 بازگشت",callback_data:"back_main"}]]}});
      return;
    }
    if(!settings.ownerUsername){await tgSend(token,chatId,"⚠️ فروشگاه هنوز کامل تنظیم نشده. به ادمین اطلاع بده.");return;}
    await shopPutState(kv,userId,{step:"await_sub_name",data:{type:"plan",planId:plan.id}});
    await tgSend(token,chatId,"📝 یه اسم برای اشتراکت انتخاب کن (فقط حروف انگلیسی و عدد، حداکثر ۶ کاراکتر) یا از دکمه زیر استفاده کن:",
      {reply_markup:{inline_keyboard:[[{text:"🎲 نام رندوم",callback_data:"random_sub_name",style:"primary"}]]}});
    return;
  }
  if(cdata==="buy_custom"){
    if(!(settings.pricePerGB>0)){await tgSend(token,chatId,"فعلاً خرید سفارشی فعال نیست.");return;}
    await shopPutState(kv,userId,{step:"custom_gb",data:{}});
    await tgSend(token,chatId,"📦 چند گیگابایت میخوای؟ (فقط عدد)");
    return;
  }
  if(cdata.startsWith("confirm_custom:")){
    const [,gbS,daysS,priceS]=cdata.split(":");
    const gb=parseFloat(gbS),days=parseInt(daysS),price=parseInt(priceS);
    if((user.balance||0)<price){
      await tgSend(token,chatId,`❌ موجودی کافی نیست.\n💰 موجودی: ${shopFmtT(user.balance)} تومان`,{reply_markup:{inline_keyboard:[[{text:"➕ شارژ کیف پول",callback_data:"charge_wallet",style:"success"}]]}});
      return;
    }
    if(!settings.ownerUsername){await tgSend(token,chatId,"⚠️ فروشگاه هنوز کامل تنظیم نشده.");return;}
    await shopPutState(kv,userId,{step:"await_sub_name",data:{type:"custom",gb,days,price}});
    await tgSend(token,chatId,"📝 یه اسم برای اشتراکت انتخاب کن (فقط حروف انگلیسی و عدد، حداکثر ۶ کاراکتر) یا از دکمه زیر استفاده کن:",
      {reply_markup:{inline_keyboard:[[{text:"🎲 نام رندوم",callback_data:"random_sub_name",style:"primary"}]]}});
    return;
  }
  if(cdata==="get_test"){
    if(!settings.testEnabled){await tgSend(token,chatId,"اکانت تست فعلاً غیرفعاله.");return;}
    if(user.usedTest){await tgSend(token,chatId,"❌ شما قبلاً از اکانت تست استفاده کردی.");return;}
    if(!settings.ownerUsername){await tgSend(token,chatId,"⚠️ فروشگاه هنوز کامل تنظیم نشده.");return;}
    await shopPutState(kv,userId,{step:"await_sub_name",data:{type:"test"}});
    await tgSend(token,chatId,"📝 یه اسم برای اشتراک تستت انتخاب کن (فقط حروف انگلیسی و عدد، حداکثر ۶ کاراکتر) یا از دکمه زیر استفاده کن:",
      {reply_markup:{inline_keyboard:[[{text:"🎲 نام رندوم",callback_data:"random_sub_name",style:"primary"}]]}});
    return;
  }
  if(cdata==="random_sub_name"){
    const state=await shopGetState(kv,userId);
    if(!state||state.step!=="await_sub_name"){await tgSend(token,chatId,"⚠️ درخواستی برای ساخت اشتراک فعال نیست.");return;}
    let name=shopRandSubName();
    for(let i=0;i<5;i++){
      const ex=await kvGet(kv,settings.ownerUsername,name);
      if(!ex)break;
      name=shopRandSubName();
    }
    await shopFinalizeSubPurchase(kv,base,settings,token,chatId,userId,user,name,state.data);
    return;
  }
  if(cdata==="balance"){
    await tgSend(token,chatId,`💰 <b>کیف پول شما</b>\n\n💵 موجودی: <b>${shopFmtT(user.balance)} تومان</b>`,
      {reply_markup:{inline_keyboard:[[{text:"➕ شارژ کیف پول",callback_data:"charge_wallet",style:"success"}],[{text:"🔙 بازگشت",callback_data:"back_main"}]]}});
    return;
  }
  if(cdata==="charge_wallet"){
    await shopPutState(kv,userId,{step:"charge_amount",data:{}});
    await tgSend(token,chatId,"💵 مبلغ شارژ رو به تومان وارد کن (فقط عدد):");
    return;
  }
  if(cdata==="history"){
    const purchases=await shopGetPurchases(kv,userId);
    if(purchases.length===0){await tgSend(token,chatId,"📋 شما هنوز خریدی نداشتید.",{reply_markup:shopMainKeyboard(settings,userId)});return;}
    let m="📋 <b>سوابق خرید</b>\n\n";
    purchases.slice(0,10).forEach((h,i)=>{m+=`${i+1}. <b>${h.productName}</b> — ${shopFmtT(h.price)} تومان\n📅 ${new Date(h.date).toLocaleDateString("fa-IR")}\n🔗 ${h.config}\n\n`;});
    await tgSend(token,chatId,m,{reply_markup:shopMainKeyboard(settings,userId)});
    return;
  }
  if(cdata==="account"){
    await tgSend(token,chatId,
      `👤 <b>حساب کاربری</b>\n\n🆔 <code>${user.id}</code>\n📛 ${user.name}${user.username?`\n📎 @${user.username}`:""}\n💰 موجودی: ${shopFmtT(user.balance)} تومان\n🛒 تعداد خرید: ${user.purchases||0}\n🎁 تست: ${user.usedTest?"استفاده شده":"استفاده نشده"}`,
      {reply_markup:shopMainKeyboard(settings,userId)});
    return;
  }
  if(cdata==="support"){
    await shopPutState(kv,userId,{step:"support_message",data:{}});
    await tgSend(token,chatId,"💬 پیامت رو بنویس، برای ادمین ارسال میشه:\n\n/cancel برای لغو");
    return;
  }

  // ── مدیریت (فقط ادمین‌های تعریف‌شده) ──
  if(cdata==="admin_panel"){if(!isAdm)return;await tgSend(token,chatId,"⚙️ <b>پنل مدیریت فروشگاه</b>",{reply_markup:shopAdminKeyboard()});return;}
  if(cdata==="admin_add_plan"){if(!isAdm)return;await shopPutState(kv,userId,{step:"admin_add_plan_name",data:{}});await tgSend(token,chatId,"📝 نام پلن رو وارد کن:");return;}
  if(cdata==="admin_edit_plan_price"){
    if(!isAdm)return;
    const plans=await shopGetPlans(kv);
    if(plans.length===0){await tgSend(token,chatId,"❌ پلنی وجود نداره.",{reply_markup:shopAdminKeyboard()});return;}
    let m="✏️ شماره پلنی که میخوای قیمتشو تغییر بدی رو وارد کن:\n\n";
    plans.forEach((p,i)=>{m+=`${i+1}. ${p.name} — ${p.gb}GB/${p.days}روز — ${shopFmtT(p.price)}ت\n`;});
    await shopPutState(kv,userId,{step:"admin_edit_plan_price_idx",data:{}});
    await tgSend(token,chatId,m);
    return;
  }
  if(cdata==="admin_del_plan"){
    if(!isAdm)return;
    const plans=await shopGetPlans(kv);
    if(plans.length===0){await tgSend(token,chatId,"❌ پلنی وجود نداره.",{reply_markup:shopAdminKeyboard()});return;}
    let m="🗑 شماره پلن مورد نظر برای حذف رو وارد کن:\n\n";
    plans.forEach((p,i)=>{m+=`${i+1}. ${p.name} — ${p.gb}GB/${p.days}روز — ${shopFmtT(p.price)}ت\n`;});
    await shopPutState(kv,userId,{step:"admin_del_plan",data:{}});
    await tgSend(token,chatId,m);
    return;
  }
  if(cdata==="admin_set_gb_price"){if(!isAdm)return;await shopPutState(kv,userId,{step:"admin_set_gb_price",data:{}});await tgSend(token,chatId,`💲 نرخ فعلی هر گیگ: ${shopFmtT(settings.pricePerGB)} تومان\n\nنرخ جدید رو وارد کن:`);return;}
  if(cdata==="admin_set_test"){
    if(!isAdm)return;
    await tgSend(token,chatId,`🎁 وضعیت فعلی تست: ${settings.testEnabled?"فعال":"غیرفعال"} — ${settings.testGB||1}GB / ${settings.testDays||1} روز`,
      {reply_markup:{inline_keyboard:[
        [{text:settings.testEnabled?"غیرفعال کردن":"فعال کردن",callback_data:"admin_test_toggle"}],
        [{text:"تنظیم حجم/مدت",callback_data:"admin_test_setval"}],
        [{text:"🔙 بازگشت",callback_data:"admin_panel"}]
      ]}});
    return;
  }
  if(cdata==="admin_test_toggle"){
    if(!isAdm)return;
    settings.testEnabled=!settings.testEnabled;
    await shopPutSettings(kv,settings);
    await tgSend(token,chatId,`✅ اکانت تست ${settings.testEnabled?"فعال":"غیرفعال"} شد.`,{reply_markup:shopAdminKeyboard()});
    return;
  }
  if(cdata==="admin_test_setval"){if(!isAdm)return;await shopPutState(kv,userId,{step:"admin_set_test_gb",data:{}});await tgSend(token,chatId,"📦 حجم اکانت تست رو به GB وارد کن:");return;}
  if(cdata==="admin_set_card"){if(!isAdm)return;await shopPutState(kv,userId,{step:"admin_set_card_number",data:{}});await tgSend(token,chatId,"💳 شماره کارت رو وارد کن (بدون فاصله):");return;}
  if(cdata==="admin_users"){
    if(!isAdm)return;
    const users=await shopListUsers(kv);
    await tgSend(token,chatId,`👥 تعداد کل کاربران: ${users.length}`,{reply_markup:shopAdminKeyboard()});
    return;
  }
  if(cdata==="admin_user_info"){if(!isAdm)return;await shopPutState(kv,userId,{step:"admin_user_info",data:{}});await tgSend(token,chatId,"🔍 آیدی عددی کاربر رو وارد کن:");return;}
  if(cdata==="admin_block"){if(!isAdm)return;await shopPutState(kv,userId,{step:"admin_block",data:{}});await tgSend(token,chatId,"🚫 آیدی عددی کاربر برای بلاک رو وارد کن:");return;}
  if(cdata==="admin_unblock"){if(!isAdm)return;await shopPutState(kv,userId,{step:"admin_unblock",data:{}});await tgSend(token,chatId,"✅ آیدی عددی کاربر برای آنبلاک رو وارد کن:");return;}
  if(cdata==="admin_broadcast"){if(!isAdm)return;await shopPutState(kv,userId,{step:"admin_broadcast",data:{}});await tgSend(token,chatId,"📢 متن پیام همگانی رو وارد کن:");return;}
  if(cdata==="admin_confirm_charge"){
    if(!isAdm)return;
    const charges=(await shopGetCharges(kv)).filter(c=>c.status==="pending");
    if(charges.length===0){await tgSend(token,chatId,"❌ درخواست شارژی در صف نیست.",{reply_markup:shopAdminKeyboard()});return;}
    for(const c of charges.slice(0,10)){
      const cap=`👤 <code>${c.userId}</code>\n💰 ${shopFmtT(c.amount)} تومان\n📅 ${new Date(c.date).toLocaleDateString("fa-IR")}`;
      const kb={reply_markup:{inline_keyboard:[[{text:"✅ تایید",callback_data:`confirm_charge:${c.id}`,style:"success"},{text:"❌ رد",callback_data:`reject_charge:${c.id}`,style:"danger"}]]}};
      if(c.receiptFileId)await tgSendPhoto(token,chatId,c.receiptFileId,cap,kb);
      else await tgSend(token,chatId,cap,kb);
    }
    return;
  }
  if(cdata.startsWith("confirm_charge:")){
    if(!isAdm)return;
    const id=cdata.split(":")[1];
    const charges=await shopGetCharges(kv);
    const c=charges.find(x=>x.id===id);
    if(!c||c.status!=="pending"){await tgSend(token,chatId,"❌ این درخواست دیگه معتبر نیست.");return;}
    c.status="confirmed";
    await shopPutCharges(kv,charges);
    const u=await shopGetUser(kv,c.userId);
    if(u){u.balance=(u.balance||0)+c.amount;await shopPutUser(kv,c.userId,u);}
    await tgSend(token,chatId,"✅ شارژ تایید شد.");
    await tgSend(token,c.userId,`✅ کیف پول شما به مبلغ <b>${shopFmtT(c.amount)} تومان</b> شارژ شد.`,{reply_markup:shopMainKeyboard(settings,c.userId)});
    return;
  }
  if(cdata.startsWith("reject_charge:")){
    if(!isAdm)return;
    const id=cdata.split(":")[1];
    const charges=await shopGetCharges(kv);
    const c=charges.find(x=>x.id===id);
    if(!c||c.status!=="pending"){await tgSend(token,chatId,"❌ این درخواست دیگه معتبر نیست.");return;}
    c.status="rejected";
    await shopPutCharges(kv,charges);
    await tgSend(token,chatId,"❌ درخواست رد شد.");
    await tgSend(token,c.userId,"❌ درخواست شارژ کیف پول شما رد شد. با پشتیبانی تماس بگیرید.");
    return;
  }
}

// ═══════════════════════════════════════════════════════════════════════════
//  PAGE: V2RAY SETTINGS — کلین آی‌پی و پورت (سوپرادمین)
// ═══════════════════════════════════════════════════════════════════════════
function v2SettingsPage(v2s,admin,ok="",err=""){
  const activePorts=v2s.ports||[443];
  const ips=(v2s.ips&&v2s.ips.length)?v2s.ips:[{cleanIp:"",proxyIp:"",name:""}];
  const chk=(p)=>`<label style="display:flex;align-items:center;gap:6px;padding:8px 10px;border:1px solid var(--bd,rgba(255,255,255,.08));border-radius:8px;cursor:pointer;font-size:13px">
    <input type="checkbox" name="ports" value="${p}" ${activePorts.includes(p)?"checked":""}/> ${p}
  </label>`;
  const initIps=JSON.stringify(ips);
  const allPorts=[...HTTPS_PORTS,...HTTP_PORTS];
  return `<!DOCTYPE html><html lang="fa" dir="rtl"><head>
<meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>تنظیمات وی‌توری — ${PANEL_NAME}</title><link rel="icon" type="image/svg+xml" href="/logo.svg"><style>${CSS}${CSSX}${DOCKCSS}${LAYOUTCSS}${NATURALCSS}
.ip-card{border:1px solid var(--bd,rgba(255,255,255,.1));border-radius:12px;padding:14px;margin-bottom:10px;display:flex;flex-direction:column;gap:10px}
.ip-card-row2{display:flex;gap:16px;flex-wrap:wrap}
.chk-lbl{display:flex;align-items:center;gap:6px;font-size:12px;color:var(--t2);cursor:pointer}
.chk-lbl input[type=checkbox]{width:15px;height:15px;accent-color:var(--in);cursor:pointer}
.chk-lbl small{color:var(--t3);font-size:10px}
.ip-card-head{display:flex;align-items:center;gap:8px;flex-wrap:wrap}
.ip-card-head .del-btn{margin-inline-start:auto}
.ip-card-title{font-size:12px;font-weight:600;color:var(--t3)}
.ip-card-field{display:flex;flex-direction:column;gap:4px}
.ip-card-field label{font-size:11px;color:var(--t3)}
.ip-card-field input,.ip-card-field select{font-size:13px;padding:8px 10px;width:100%;box-sizing:border-box}
.ip-card-row{display:grid;grid-template-columns:1fr 110px;gap:10px}
.del-btn{padding:5px 12px;border:1px solid rgba(255,80,80,.35);border-radius:8px;background:rgba(255,80,80,.1);color:#ff5555;cursor:pointer;font-size:12px}
.del-btn:hover{background:rgba(255,80,80,.25)}
.add-btn{display:flex;align-items:center;justify-content:center;gap:6px;padding:12px;border:1px dashed var(--bd,rgba(255,255,255,.15));border-radius:10px;background:transparent;color:var(--t2);cursor:pointer;font-size:13px;margin-top:4px;width:100%;box-sizing:border-box}
.add-btn:hover{background:var(--s2)}
</style></head>
<body>
${nav('تنظیمات وی‌توری',admin)}
<div class="pg wrap" style="max-width:720px">
${ok?`<div class="alr alr-ok fu">${I.check} ${ok}</div>`:''}
${err?`<div class="alr alr-er fu">${err}</div>`:''}

<div class="card fu">
  <div class="card-hd"><div class="card-t">${I.zap} کلین آی‌پی‌ها و پورت‌ها (VLESS / Trojan)</div></div>
  <div class="card-b">
    <form method="POST" action="/panel/v2-settings" style="display:flex;flex-direction:column;gap:18px">

      <div class="fl">
        <label style="margin-bottom:8px">کلین آی‌پی‌ها</label>
        <div style="font-size:11px;color:var(--t3);margin-bottom:10px;line-height:1.6">
          برای هر ردیف میتونی اسم (مثلاً 🇩🇪 آلمان)، کلین IP کلادفلر، و یه پروکسی IP اختیاری وارد کنی.<br>
          اسم هر ردیف توی رمارک اشتراک کاربر نشون داده میشه (مثلاً: <code>myuser-🇩🇪 آلمان-443</code>).<br>
          اگه پروکسی IP وارد کنی، ترافیک از طریق اون سرور روت میشه.
        </div>
        <div id="ips-wrap"></div>
        <datalist id="grp-list">${v2Groups(v2s).map(g=>`<option value="${_he(g)}">`).join("")}</datalist>
        <button type="button" id="add-ip-btn" class="add-btn">+ افزودن کانفیگ جدید</button>
      </div>

      <button class="btn bp" style="justify-content:center">${I.check} ذخیره تنظیمات</button>
    </form>
  </div>
</div>
</div>
${TOAST}<script>${JSG}
(function(){
  var INIT=${initIps};
  var PORT_OPTS=${JSON.stringify([...HTTPS_PORTS,...HTTP_PORTS])};
  function makePortSel(sel){
    return PORT_OPTS.map(function(p){return '<option value="'+p+'"'+(p===sel?' selected':'')+'>'+p+'</option>';}).join('');
  }
  // هشدار قبل از ذخیره اگه کلین IP خالی مونده
  var _sf=document.querySelector('form[action="/panel/v2-settings"]');
  if(_sf)_sf.addEventListener('submit',function(e){
    var empty=0;document.querySelectorAll('#ips-wrap input[name="cleanIp[]"]').forEach(function(i){if(!i.value.trim())empty++;});
    if(empty&&!confirm(empty+' کانفیگ کلین IP ندارن و مستقیم روی دامنه‌ی ورکر میرن (روی workers.dev توی ایران معمولاً کار نمی‌کنه). همین‌طوری ذخیره بشه؟'))e.preventDefault();
  });
  var _rowNum=0;
  function addRow(n,c,p,pt,frag,ins,g){
    _rowNum++;
    var num=_rowNum;
    var card=document.createElement('div');
    card.className='ip-card';
    var grpVal=(g||'').replace(/"/g,'&quot;');
    var nameVal=(n||'').replace(/"/g,'&quot;');
    var cipVal=(c||'').replace(/"/g,'&quot;');
    var pipVal=(p||'').replace(/"/g,'&quot;');
    card.innerHTML=
      '<div class="ip-card-head">'
        +'<span class="ip-card-title">کانفیگ '+num+'</span>'
        +'<button type="button" class="del-btn"><svg width="11" height="11" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><line x1="18" y1="6" x2="6" y2="18"/><line x1="6" y1="6" x2="18" y2="18"/></svg> حذف</button>'
      +'</div>'
      +'<div class="ip-card-field"><label>اسم / پرچم (رمارک کلاینت)</label>'
        +'<input name="ipName[]" placeholder="مثلاً: 🇩🇪 آلمان" value="'+nameVal+'" style="direction:rtl"/>'
      +'</div>'
      +'<div class="ip-card-field"><label>گروه (موقع ساخت اشتراک انتخاب میشه)</label>'
        +'<input name="ipGroup[]" list="grp-list" placeholder="مثلاً: 🇩🇪 آلمان" value="'+grpVal+'" style="direction:rtl"/>'
      +'</div>'
      +'<div class="ip-card-field"><label>کلین IP (خالی = دامنه ورکر)</label>'
        +'<input name="cleanIp[]" placeholder="104.16.x.x" value="'+cipVal+'" style="direction:ltr;font-family:monospace"/>'
      +'</div>'
      +'<div class="ip-card-row">'
        +'<div class="ip-card-field"><label>پروکسی IP (اختیاری)</label>'
          +'<input name="proxyIp[]" placeholder="1.2.3.4" value="'+pipVal+'" style="direction:ltr;font-family:monospace"/>'
        +'</div>'
        +'<div class="ip-card-field"><label>پورت</label>'
          +'<select name="ipPort[]" style="direction:ltr">'+makePortSel(pt||443)+'</select>'
        +'</div>'
      +'</div>'
      +'<div class="ip-card-row2">'
        +'<label class="chk-lbl"><input type="checkbox"'+(frag?' checked':'')+' onchange="this.nextElementSibling.value=this.checked?1:0"/> '
          +'<input type="hidden" name="fragment[]" value="'+(frag?1:0)+'"/>'
          +'<span>Fragment <small>(ضد فیلتر/DPI — تکه‌تکه کردن TLS)</small></span></label>'
        +'<label class="chk-lbl"><input type="checkbox"'+(ins?' checked':'')+' onchange="this.nextElementSibling.value=this.checked?1:0"/> '
          +'<input type="hidden" name="allowInsecure[]" value="'+(ins?1:0)+'"/>'
          +'<span>Allow Insecure <small>(گواهی TLS معتبرسنجی نشه)</small></span></label>'
      +'</div>';
    document.getElementById('ips-wrap').appendChild(card);
    (function(){var gI=card.querySelector('input[name="ipGroup[]"]'),nI=card.querySelector('input[name="ipName[]"]');if(gI&&nI&&gI.value&&gI.value!==nI.value.trim())gI.dataset.touched='1';})();
  }
  // event delegation برای دکمه حذف — مطمئن‌تر از onclick inline
  document.getElementById('ips-wrap').addEventListener('click',function(e){
    var btn=e.target.closest('.del-btn');
    if(btn){btn.closest('.ip-card').remove();return;}
  });
  // دکمه افزودن
  var addBtn=document.getElementById('add-ip-btn');
  if(addBtn)addBtn.addEventListener('click',function(){addRow('','','',443,false,false,'');});
  var _w=document.getElementById('ips-wrap');
  _w.addEventListener('input',function(e){
    var c=e.target.closest('.ip-card');if(!c)return;
    var nI=c.querySelector('input[name="ipName[]"]'),gI=c.querySelector('input[name="ipGroup[]"]');
    if(e.target===gI){gI.dataset.touched='1';return;}
    if(e.target===nI&&gI.dataset.touched!=='1')gI.value=nI.value.trim();
  });
  // مقداردهی اولیه
  try{
    if(INIT&&INIT.length){INIT.forEach(function(e){addRow(e.name||'',e.cleanIp||'',e.proxyIp||'',e.port||443,!!e.fragment,!!e.allowInsecure,e.group||'');});}
    else{addRow();}
  }catch(ex){addRow();}
})()</script></body></html>`;
}

// ═══════════════════════════════════════════════════════════════════════════
//  PAGE: SHOP SETTINGS (سوپرادمین)
// ═══════════════════════════════════════════════════════════════════════════
function shopSettingsPage(settings,admin,base,ok="",err=""){
  const hookUrl=settings.botToken?`${base}/shop/webhook/${settings.botToken}`:"";
  return `<!DOCTYPE html><html lang="fa" dir="rtl"><head>
<meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>بات فروش — ${PANEL_NAME}</title><link rel="icon" type="image/svg+xml" href="/logo.svg"><style>${CSS}${CSSX}${DOCKCSS}${LAYOUTCSS}${NATURALCSS}</style></head>
<body>
${nav('بات فروش تلگرام',admin)}
<div class="pg wrap" style="max-width:680px">
${ok?`<div class="alr alr-ok fu">${I.check} ${ok}</div>`:''}
${err?`<div class="alr alr-er fu">${err}</div>`:''}

<div class="card fu">
  <div class="card-hd"><div class="card-t">${I.zap} اتصال بات</div></div>
  <div class="card-b">
    <form method="POST" action="/panel/shop-settings" style="display:flex;flex-direction:column;gap:12px">
      <div class="fl"><label>توکن بات (از BotFather)</label>
        <input type="text" name="botToken" value="${settings.botToken||''}" placeholder="123456:AA..." style="direction:ltr;font-family:monospace"/>
      </div>
      <div class="fl"><label>آیدی عددی ادمین(ها)</label>
        <input type="text" name="adminIds" value="${(settings.adminIds||[]).join(', ')}" placeholder="مثلا: 111111, 222222" style="direction:ltr"/>
        <div style="font-size:11px;color:var(--t3);margin-top:4px">آیدی عددی تلگرام (نه یوزرنیم). با کاما جدا کن. آیدی خودت رو از بات‌هایی مثل @userinfobot بگیر.</div>
      </div>
      <div class="fl"><label>یوزرنیم پنل مقصد</label>
        <input type="text" name="ownerUsername" value="${settings.ownerUsername||''}" placeholder="مثلا Use9" style="direction:ltr"/>
        <div style="font-size:11px;color:var(--t3);margin-top:4px">باید نام کاربری یکی از پنل‌های ادمین که از قبل ساختی باشه (از «مدیریت پنل‌ها»). اشتراک‌های بات زیر همون پنل ساخته میشن و از داشبوردش هم قابل مدیریتن.</div>
      </div>
      <div class="fg" style="grid-template-columns:1fr 1fr">
        <div class="fl"><label>شماره کارت</label>
          <input type="text" name="cardNumber" value="${settings.cardNumber||''}" placeholder="6037-xxxx-xxxx-xxxx" style="direction:ltr"/>
        </div>
        <div class="fl"><label>نام صاحب کارت</label>
          <input type="text" name="cardHolder" value="${settings.cardHolder||''}" placeholder="نام و نام‌خانوادگی"/>
        </div>
      </div>
      <div class="fl"><label>نرخ هر گیگ برای سفارش سفارشی (تومان) — صفر یعنی غیرفعال</label>
        <input type="number" name="pricePerGB" value="${settings.pricePerGB||0}" min="0"/>
      </div>
      <div class="fg" style="grid-template-columns:auto 1fr 1fr">
        <label style="display:flex;align-items:center;gap:6px;font-size:13px">
          <input type="checkbox" name="testEnabled" ${settings.testEnabled?'checked':''} style="width:16px;height:16px"/> اکانت تست رایگان فعال باشه
        </label>
        <div class="fl"><label>حجم تست (GB)</label><input type="number" step="0.1" name="testGB" value="${settings.testGB||1}" min="0.1"/></div>
        <div class="fl"><label>مدت تست (روز)</label><input type="number" name="testDays" value="${settings.testDays||1}" min="1"/></div>
      </div>
      <button class="btn bp" style="justify-content:center">${I.check} ذخیره تنظیمات</button>
    </form>
  </div>
</div>

<div class="card fu f1">
  <div class="card-hd"><div class="card-t">${I.link} اتصال Webhook</div></div>
  <div class="card-b">
    <div class="si-d" style="font-size:12px;margin-bottom:10px">
      بعد از ذخیره‌ی توکن، دکمه‌ی زیر رو بزن تا تلگرام درخواست‌ها رو مستقیم به همین ورکر بفرسته.
    </div>
    ${hookUrl?`<div style="font-family:monospace;font-size:11px;color:var(--t3);word-break:break-all;margin-bottom:10px">${hookUrl}</div>`:''}
    <form method="POST" action="/panel/shop-settings/set-webhook">
      <button class="btn bam" style="justify-content:center;width:100%">${I.sync} تنظیم / بروزرسانی Webhook</button>
    </form>
  </div>
</div>

<div class="card fu f2">
  <div class="card-hd"><div class="card-t">${I.panel} مدیریت پلن‌ها و کاربران</div></div>
  <div class="card-b">
    <div class="si-d" style="font-size:12px">افزودن/حذف پلن، تنظیم قیمت هر گیگ، بلاک/آنبلاک کاربر، تایید شارژ کیف پول و پیام همگانی همه از داخل خودِ بات و با دکمه‌ی «⚙️ پنل مدیریت» انجام میشه (فقط برای آیدی‌های ادمین بالا).</div>
  </div>
</div>

</div>
${TOAST}<script>${JSG}</script></body></html>`;
}

// ═══════════════════════════════════════════════════════════════════════════
//  MAIN
// ═══════════════════════════════════════════════════════════════════════════
const _W = {
  async fetch(req,env,ctx){
    const url=new URL(req.url),path=url.pathname,method=req.method;
    const base=`${url.protocol}//${url.host}`,kv=makeStore(env);

    if(!kv){
      return new Response(`<!DOCTYPE html><html lang="fa" dir="rtl"><head><meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>${PANEL_NAME}</title></head><body style="font-family:sans-serif;background:#0f0f14;color:#eee;padding:24px;line-height:2"><h2>دیتابیس وصل نیست</h2><p>ورکر نه <b>DB</b> (D1) و نه <b>KV</b> رو پیدا نکرد.</p><p>توی داشبورد کلادفلر: Workers &amp; Pages ← ورکر ← Settings ← Bindings، یه D1 با اسم دقیق <b dir="ltr">DB</b> وصل کن (حروف بزرگ). اگه قبلاً وصل کردی، بعد از ذخیره‌ی Binding دوباره Deploy بزن.</p></body></html>`,{status:503,headers:{"Content-Type":"text/html; charset=utf-8"}});
    }
    if(!_migFinished&&kv.migrateStep&&req.headers.get("Upgrade")!=="websocket"){
      try{ _migFinished=(await kv.migrateStep())===true; }catch(e){}
    }
    await ensureSuperAdmin(kv);

    // ── V2RAY (VLESS/Trojan) WEBSOCKET ENTRYPOINT — عمومی، بدون نیاز به لاگین ──
    // مثل نسخه‌ی قبلاً کارکرده، هر درخواست WebSocket (صرف‌نظر از مسیرش) به موتور
    // وی‌توری میره؛ خودِ موتور از رو UUID/پسورد داخل پروتکل تشخیص میده مال کیه
    if(req.headers.get("Upgrade")==="websocket"){
      if(!wsAllow(req.headers.get("CF-Connecting-IP")||"?"))
        return new Response("busy",{status:429,headers:{"Retry-After":"30"}});
      let bytesAdded=0,matchedOwner=null,matchedName=null;
      const {response,done,kill}=handleV2Socket(req,kv,
        (n)=>{bytesAdded+=n;},
        (owner,name)=>{matchedOwner=owner;matchedName=name;});
      _openWs++;
      done.then(()=>{_openWs=Math.max(0,_openWs-1);},()=>{_openWs=Math.max(0,_openWs-1);});
      // هر ۱ ثانیه (نه فقط موقع قطع اتصال) مصرف رو توی KV ذخیره میکنیم تا حجم
      // برای اتصال‌های طولانی وی‌توری/تروجان لحظه‌ای کم بشه؛ همینجا وضعیت
      // دیسیبل/حذف/انقضا رو هم چک میکنیم تا اگه ادمین وسط اتصال قطعش کرد،
      // همون لحظه اتصالِ درحال‌اجرا هم بسته بشه (نه اینکه تا قطع طبیعی زنده بمونه)
      // ── بهینه‌سازی KV: bytes رو در حافظه جمع کن، هر ۶۰ ث یه بار بنویس ──
      const flushUsage=async(force=false)=>{
        if(!matchedOwner||!matchedName)return true;
        // وضعیت رو از کش بخون (نه هر بار KV) — فقط هر ۳۰ ث یه‌بار KV واقعی
        const fresh=await kvGetSub(kv,matchedOwner,matchedName);
        if(!fresh||fresh.disabled||isExp(fresh)){kill();return false;}
        // bytes جمع‌شده رو به کش اضافه کن
        if(bytesAdded>0){kvSubAddBytes(matchedOwner,matchedName,bytesAdded);bytesAdded=0;}
        // فقط اگه force (قطع اتصال) یا dirty باشه flush کن
        if(force)await kvFlushSub(kv,matchedOwner,matchedName);
        return true;
      };
      if(ctx&&ctx.waitUntil)ctx.waitUntil((async()=>{
        // هر ۶۰ ثانیه flush — به‌جای هر ۱ ثانیه (۶۰ برابر کمتر KV write)
        const timer=setInterval(()=>{flushUsage(true).catch(()=>{});},_FLUSH_MS);
        try{await done;}catch(e){}
        clearInterval(timer);
        // موقع قطع: حتماً flush نهایی بزن
        try{
          if(bytesAdded>0&&matchedOwner&&matchedName)kvSubAddBytes(matchedOwner,matchedName,bytesAdded);
          if(matchedOwner&&matchedName)await kvFlushSub(kv,matchedOwner,matchedName);
        }catch(e){}
      })());
      return response;
    }

    if(path==="/logo.svg"||path==="/favicon.ico")return new Response(LOGO_SVG,{headers:{"Content-Type":"image/svg+xml","Cache-Control":"public, max-age=604800"}});

    const admin=await currentAdmin(req,kv);
    const auth=!!admin;
    const isSuper=admin?.is_super===true;
    const panelExpired = auth && isAdminExpired(admin);

    if(path==="/"||path==="")return redir("/panel/admin");

    // ── LOGIN ──────────────────────────────────────────────────
    if(path==="/panel/admin"){
      if(method==="GET"){
        if(auth&&!panelExpired)return redir("/panel/dashboard");
        if(auth&&panelExpired)return page(loginPage("پنل شما منقضی شده است — برای تمدید با مدیر تماس بگیرید"));
        return page(loginPage());
      }
      if(method==="POST"){
        // حداکثر ۸ تلاش ناموفق در ۱۰ دقیقه برای هر IP — بدون این، هر کسی می‌تونه
        // با ریکوئست الکی سهمیه‌ی روزانه رو تموم کنه یا رمز حدس بزنه
        const ip=req.headers.get("CF-Connecting-IP")||"?";
        const tr=_loginTries.get(ip);
        if(tr&&Date.now()-tr.t<600000&&tr.n>=8)
          return page(loginPage("تلاش‌های ناموفق زیاد بود — چند دقیقه بعد دوباره امتحان کن"),429);
        const f=await req.formData();
        const u=(f.get("username")||"").trim(),p=f.get("password")||"";
        const rec=await kvGetAdmin(kv,u);
        if(!rec||rec.password!==p){
          const t=_loginTries.get(ip);
          _loginTries.set(ip,(t&&Date.now()-t.t<600000)?{n:t.n+1,t:t.t}:{n:1,t:Date.now()});
          if(_loginTries.size>2000)_loginTries.clear();
          return page(loginPage("نام کاربری یا رمز عبور اشتباه است"),401);
        }
        _loginTries.delete(ip);
        if(isAdminExpired(rec))
          return page(loginPage("پنل شما منقضی شده است — برای تمدید با مدیر تماس بگیرید"),403);
        return redir("/panel/dashboard",{"Set-Cookie":mkAuthCookie(u,p,7)});
      }
    }

    if(path==="/panel/logout")
      return redir("/panel/admin",{"Set-Cookie":clrAuthCookie()});

    if(!auth && path.startsWith("/panel/") && path!=="/panel/admin")
      return redir("/panel/admin");

    // اگه پنل منقضی شده، به هیچ صفحه‌ی دیگه‌ای اجازه نده — بندازش بیرون
    if(panelExpired && path.startsWith("/panel/") && path!=="/panel/admin")
      return redir("/panel/admin",{"Set-Cookie":clrAuthCookie()});

    // ── SUPERADMIN: PANEL MANAGEMENT ────────────────────────────
    if(path==="/panel/admins"){
      if(!isSuper)return redir("/panel/dashboard");
      if(method==="GET")return page(adminsPage(await kvListAdmins(kv)));
    }
    if(path==="/panel/admins/create"&&method==="POST"){
      if(!isSuper)return redir("/panel/dashboard");
      const f=await req.formData();
      const u=(f.get("username")||"").trim(),p=f.get("password")||"";
      const ed=parseInt(f.get("expire_days")||"0");
      if(!u||!/^[a-zA-Z0-9_-]+$/.test(u)||p.length<4||isNaN(ed)||ed<1)
        return page(adminsPage(await kvListAdmins(kv),"","اطلاعات نامعتبر — یوزرنیم، رمز حداقل ۴ کاراکتر، و مدت روز الزامیه"));
      if(await kvGetAdmin(kv,u))
        return page(adminsPage(await kvListAdmins(kv),"","این نام کاربری قبلاً وجود دارد"));
      await kvPutAdmin(kv,u,{username:u,password:p,created_at:Date.now(),is_super:false,expire_days:ed});
      return redir("/panel/admins");
    }
    if(path.startsWith("/panel/admins/renew/")&&method==="POST"){
      if(!isSuper)return redir("/panel/dashboard");
      const u=decodeURIComponent(path.replace("/panel/admins/renew/",""));
      const f=await req.formData(),d=parseInt(f.get("days")||"0");
      const rec=await kvGetAdmin(kv,u);
      if(rec&&!rec.is_super&&d>0){
        // اگه از قبل منقضی شده، تمدید از همین الان حساب میشه
        if(isAdminExpired(rec)){
          rec.created_at=Date.now();
          rec.expire_days=d;
        } else {
          rec.expire_days=(rec.expire_days||0)+d;
        }
        await kvPutAdmin(kv,u,rec);
      }
      return redir("/panel/admins");
    }
    if(path.startsWith("/panel/admins/delete/")&&method==="POST"){
      if(!isSuper)return redir("/panel/dashboard");
      const u=decodeURIComponent(path.replace("/panel/admins/delete/",""));
      if(u!==SUPERADMIN_USER){
        // حذف همه اشتراک‌های این پنل هم
        const subs=await kvList(kv,u);
        for(const s of subs){
          await v2IndexDel(kv,s);
          await kvDel(kv,u,s.name);
        }
        await kvDelAdmin(kv,u);
      }
      return redir("/panel/admins");
    }

    // ── DASHBOARD (پنل شخصی) ────────────────────────────────────
    if(path==="/panel/dashboard"){
      const subs=await ensureAllSubToks(kv,admin.username,await kvList(kv,admin.username));
      return page((_V2GROUPS=v2Groups(await v2GetSettings(kv)),dashPage(subs,base,admin,false)));
    }

    if(auth&&!panelExpired&&(path==="/panel/stats"||/^\/panel\/proto-(vless|trojan)$/.test(path))){
      const all=await ensureAllSubToks(kv,admin.username,await kvList(kv,admin.username));
      if(path==="/panel/stats")return page(statsPage(all,base,admin));
      const pk=path.split("proto-")[1];
      _V2GROUPS=v2Groups(await v2GetSettings(kv));
      return page(dashPage(protoSubs(all,pk),base,admin,false,pk));
    }

    if(path==="/panel/diag"&&isSuper){
      const rows=[];const ok=(b,m)=>rows.push(`<tr><td>${b?"✓":"✕"}</td><td>${m}</td></tr>`);
      const esc=s=>String(s).replace(/&/g,"&amp;").replace(/</g,"&lt;");
      ok(true,"دامنه‌ی ورکر: <b>"+esc(url.host)+"</b>"+(url.host.endsWith(".workers.dev")?" — دامنه‌ی workers.dev توی ایران خیلی وقت‌ها فیلتره؛ پنل‌های دیگه‌ات احتمالاً دامنه‌ی شخصی دارن":""));
      try{await kv.get("__diag__");ok(true,"خوندن KV کار می‌کنه");}catch(e){ok(false,"خوندن KV: "+esc(e.message));}
      let canWrite=true;
      try{await kv.put("__diag__",String(Date.now()));await kv.delete("__diag__");ok(true,"نوشتن KV کار می‌کنه");}
      catch(e){canWrite=false;ok(false,"نوشتن KV خطا داد: <b>"+esc(e.message)+"</b> — اشتراک تازه‌ساخته‌شده کامل ذخیره نمیشه و کانفیگش کار نمی‌کنه تا ریست سقف (۰۳:۳۰ تهران) یا خرید پلن Paid");}
      try{
        const subs=await kvList(kv,admin.username);
        for(const s of subs){
          const w=[];
          if(s.v2uuid){const idx=await kv.get(v2IdxUuidKey(s.v2uuid),"json");ok(!!idx,`<b>${esc(s.name)}</b> (VLESS): ایندکس UUID ${idx?"هست":"<b>نیست</b> → این کانفیگ هیچوقت وصل نمیشه؛ حذفش کن و دوباره بساز"}`);}
          if(s.v2passHash){const idx=await kv.get(v2IdxPassKey(s.v2passHash),"json");ok(!!idx,`<b>${esc(s.name)}</b> (Trojan): ایندکس پسورد ${idx?"هست":"<b>نیست</b> → حذفش کن و دوباره بساز"}`);}
          if(s.disabled)w.push("غیرفعاله");if(isExp(s))w.push("منقضیه");if(isDailyCapped(s))w.push("سقف روزانه پر شده");
          if(w.length)ok(false,`<b>${esc(s.name)}</b>: ${w.join("، ")}`);
        }
        if(!subs.length)ok(true,"هنوز اشتراکی نداری");
        const v2s=await v2GetSettings(kv);
        v2s.ips.forEach((e,i)=>{const n=esc(e.name||("کانفیگ "+(i+1)));
          ok(!!e.cleanIp,e.cleanIp?`«${n}»: کلین IP = <b>${esc(e.cleanIp)}</b>`:`«${n}»: <b>کلین IP خالیه</b> → مستقیم روی دامنه‌ی ورکر میره و توی ایران معمولاً وصل نمیشه. توی تنظیمات وی‌توری «اسکن و پر کردن» رو بزن`);});
      }catch(e){ok(false,"لیست اشتراک‌ها: "+esc(e.message));}
      return page(`<!DOCTYPE html><html lang="fa" dir="rtl"><head><meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>عیب‌یابی</title></head><body style="margin:0;background:#05060f;color:#f8f9ff;font-family:Tahoma,sans-serif;padding:16px"><h2>عیب‌یابی کانفیگ‌ها</h2><table style="width:100%;border-collapse:collapse;font-size:14px;line-height:1.9">${rows.join("")}</table><p><a href="/panel/dashboard" style="color:#22e0ff">← برگشت به پنل</a></p></body></html>`);
    }

    // ── SUPERADMIN: مشاهده/مدیریت هر پنل خاص ────────────────────
    // /panel/<owner>          → داشبورد اون پنل (فقط سوپرادمین)
    // /panel/<owner>/<action>/<name> → عملیات روی اشتراک اون پنل (سوپرادمین یا خودِ owner)
    const panelMatch = path.match(/^\/panel\/([a-zA-Z0-9_-]+)(?:\/([a-z]+)\/(.+))?$/);
    if(panelMatch){
      const owner=decodeURIComponent(panelMatch[1]);
      const action=panelMatch[2];
      const nameRaw=panelMatch[3];
      const isReserved=["dashboard","settings","create","admins","admin","logout","shop-settings","v2-settings","stats","diag","proto-vless","proto-trojan"].includes(owner);

      if(!isReserved){
        const canAccess = isSuper || admin.username===owner;
        if(!canAccess) return redir("/panel/dashboard");
        const ownerAdmin = await kvGetAdmin(kv,owner);
        if(!ownerAdmin) return redir(isSuper?"/panel/admins":"/panel/dashboard");

        if(!action){
          if(method==="GET"){
            const subs=await ensureAllSubToks(kv,owner,await kvList(kv,owner));
            return page((_V2GROUPS=v2Groups(await v2GetSettings(kv)),dashPage(subs,base,isSuper?admin:ownerAdmin,false)));
          }
        }

        const n=decodeURIComponent(nameRaw||"");
        const backTo=isSuper?`/panel/${encodeURIComponent(owner)}`:"/panel/dashboard";

        if(action==="reset"&&method==="POST"){
          const s=await kvGet(kv,owner,n);
          if(s){s.used_bytes=0;await kvPut(kv,owner,n,s);}
          return redir(backTo);
        }
        if(action==="setusage"&&method==="POST"){
          const s=await kvGet(kv,owner,n);
          const f=await req.formData(),mb=parseFloat(f.get("mb")||"0");
          if(s&&!isNaN(mb)){s.used_bytes=Math.round(mb*1048576);await kvPut(kv,owner,n,s);}
          return redir(backTo);
        }
        if(action==="renew"&&method==="POST"){
          const s=await kvGet(kv,owner,n);
          const f=await req.formData(),d=parseInt(f.get("days")||"0");
          if(s&&d>0){s.expire_days+=d;await kvPut(kv,owner,n,s);}
          return redir(backTo);
        }
        if(action==="addquota"&&method==="POST"){
          const s=await kvGet(kv,owner,n);
          const f=await req.formData(),gb=parseFloat(f.get("gb")||"0");
          if(s&&gb>0){s.quota_gb=Math.round((s.quota_gb+gb)*100)/100;await kvPut(kv,owner,n,s);}
          return redir(backTo);
        }
        if(action==="toggle"&&method==="POST"){
          const s=await kvGet(kv,owner,n);
          if(s){s.disabled=!s.disabled;await kvPut(kv,owner,n,s);}
          return redir(backTo);
        }
        if(action==="delete"&&method==="POST"){
          const s=await kvGet(kv,owner,n);
          if(s)await v2IndexDel(kv,s);
          await kvDel(kv,owner,n);
          return redir(backTo);
        }
      }
    }

    // ── SETTINGS ───────────────────────────────────────────────
    if(path==="/panel/settings"){
      if(method==="GET")return page(settingsPage(admin));
      if(method==="POST"){
        const f=await req.formData(),np=f.get("newPassword")||"",cp=f.get("confirmPassword")||"";
        if(np.length<4)return page(settingsPage(admin,"","رمز عبور باید حداقل ۴ کاراکتر باشد"));
        if(np!==cp)return page(settingsPage(admin,"","رمز عبور و تکرار مطابقت ندارند"));
        const rec=await kvGetAdmin(kv,admin.username);
        rec.password=np;
        await kvPutAdmin(kv,admin.username,rec);
        return redir("/panel/admin",{"Set-Cookie":clrAuthCookie()});
      }
    }

    // ── SUPERADMIN: تنظیمات بات فروش تلگرام ─────────────────────
    if(path==="/panel/shop-settings"){
      if(!isSuper)return redir("/panel/dashboard");
      const settings=await shopGetSettings(kv);
      if(method==="GET")return page(shopSettingsPage(settings,admin,base));
      if(method==="POST"){
        const f=await req.formData();
        settings.botToken=(f.get("botToken")||"").trim();
        settings.adminIds=(f.get("adminIds")||"").split(",").map(s=>s.trim()).filter(Boolean);
        settings.ownerUsername=(f.get("ownerUsername")||"").trim();
        settings.cardNumber=(f.get("cardNumber")||"").trim();
        settings.cardHolder=(f.get("cardHolder")||"").trim();
        settings.pricePerGB=parseFloat(f.get("pricePerGB")||"0")||0;
        settings.testEnabled=f.get("testEnabled")==="on";
        settings.testGB=parseFloat(f.get("testGB")||"1")||1;
        settings.testDays=parseInt(f.get("testDays")||"1")||1;
        await shopPutSettings(kv,settings);
        return page(shopSettingsPage(settings,admin,base,"تنظیمات ذخیره شد"));
      }
    }
    if(path==="/panel/shop-settings/set-webhook"&&method==="POST"){
      if(!isSuper)return redir("/panel/dashboard");
      const settings=await shopGetSettings(kv);
      if(!settings.botToken)return page(shopSettingsPage(settings,admin,base,"","ابتدا توکن بات رو وارد و ذخیره کن"));
      const hookUrl=`${base}/shop/webhook/${encodeURIComponent(settings.botToken)}`;
      const r=await tgSetWebhook(settings.botToken,hookUrl);
      if(r&&r.ok){
        settings.panelUrl=`${base}/panel/admin`;
        await shopPutSettings(kv,settings);
        try{await tg(settings.botToken,"setChatMenuButton",{menu_button:{type:"web_app",text:"پنل",web_app:{url:settings.panelUrl}}});}catch(e){}
      }
      if(r&&r.ok)return page(shopSettingsPage(settings,admin,base,"وبهوک با موفقیت تنظیم شد ✅"));
      return page(shopSettingsPage(settings,admin,base,"",`خطا در تنظیم وبهوک: ${r?.description||"نامشخص"}`));
    }

    // ── SUPERADMIN: تست سرعت واقعی یه IP/پورت (TCP connect timing) ──
    // این تنها راه واقعی برای «سریع‌تر کردن» کانفیگ‌هاست: مشکل سرعت تقریباً
    // همیشه از کیفیت خودِ کلین‌آی‌پی/پروکسی‌آی‌پی‌ست، نه از منطق پایپ کردن
    // دیتا توی Worker (که همینجا استریم خامه و اورهد اضافه‌ای نداره).

    // ── SUPERADMIN: تنظیمات وی‌توری (کلین آی‌پی + پورت) ──────────
    if(path==="/panel/v2-settings"){
      if(!isSuper)return redir("/panel/dashboard");
      const v2s=await v2GetSettings(kv);
      if(method==="GET")return page(v2SettingsPage(v2s,admin));
      if(method==="POST"){
        const f=await req.formData();
        const allP=[...HTTPS_PORTS,...HTTP_PORTS];
        const rawCips=f.getAll("cleanIp[]");
        const rawPips=f.getAll("proxyIp[]");
        const rawNames=f.getAll("ipName[]");
        const rawPorts=f.getAll("ipPort[]");
        const rawFrag=f.getAll("fragment[]");
        const rawIns=f.getAll("allowInsecure[]");
        const rawGrp=f.getAll("ipGroup[]");
        let ips=rawCips.map((c,i)=>({
          cleanIp:c.trim(),
          proxyIp:(rawPips[i]||"").trim(),
          name:(rawNames[i]||"").trim(),
          group:((rawGrp[i]||"").trim()||(rawNames[i]||"").trim()),
          port:allP.includes(parseInt(rawPorts[i]))?parseInt(rawPorts[i]):443,
          fragment:rawFrag[i]==="1",
          allowInsecure:rawIns[i]==="1"
        })).filter(e=>e.cleanIp||e.name);
        if(ips.length===0)ips=[{cleanIp:"",proxyIp:"",name:"",port:443}];
        await v2PutSettings(kv,{ips});
        return page(v2SettingsPage({ips},admin,"✅ تنظیمات ذخیره شد"));
      }
    }

    // ── SUBSCRIPTION برای کلاینت‌ها (v2rayNG/NekoBox/Streisand و...) ──
    // این آدرس رو داخل بخش «Subscribe / Add subscription» کلاینت وارد کن،
    // همه‌ی کانفیگ‌های پورت‌های فعال + حجم/روز باقی‌مونده رو با هم میاره
    if(path.startsWith("/v2sub/")||path.startsWith("/s/")){
      let owner,name;
      if(path.startsWith("/s/")){
        // لینک کوتاه توکنی — فقط ASCII، بدون یوزرنیم/اسم توش
        const tok=decodeURIComponent(path.split("/")[2]||"");
        const idx=tok?await kv.get(v2IdxSubTokKey(tok),"json"):null;
        if(!idx)return new Response("Not found",{status:404});
        owner=idx.owner;name=idx.name;
      }else{
        const parts=path.split("/");
        owner=decodeURIComponent(parts[2]||"");name=decodeURIComponent(parts[3]||"");
      }
      const row=await kvGet(kv,owner,name);
      if(!row||!row.v2uuid)
        return new Response("Not found",{status:404});
      await v2IndexPut(kv,row,owner,name); // بک‌فیل ایندیکس معکوس برای اشتراک‌های قدیمی
      if(row.disabled||isExp(row))
        return new Response("",{status:200,headers:{"content-type":"text/plain;charset=utf-8"}});
      const v2s=await v2GetSettings(kv);
      const links=buildV2AllLinks(base,owner,name,row,v2s);
      const body=btoa(unescape(encodeURIComponent(links.join("\n"))));
      const qb=row.quota_gb*1073741824, ub=row.used_bytes||0;
      const expireTs=Math.floor((row.created_at+row.expire_days*86400000)/1000);
      return new Response(body,{status:200,headers:{
        "content-type":"text/plain;charset=utf-8",
        "profile-title":`Base64:${btoa(unescape(encodeURIComponent(name)))}`,
        "subscription-userinfo":`upload=0; download=${Math.round(ub)}; total=${Math.round(qb)}; expire=${expireTs}`,
        "profile-update-interval":"6",
      }});
    }

    // ── CREATE (اشتراک جدید در پنل خودِ کاربر لاگین‌شده) ────────
    if(path==="/panel/create"&&method==="POST"){
      const f=await req.formData();
      const name=(f.get("name")||"").trim();
      const protocol=(f.get("protocol")||"all");
      const qtyUnit=(f.get("qtyUnit")||"gb").trim();
      let qg=parseFloat(f.get("quota_gb"));
      if(qtyUnit==="req"&&!isNaN(qg))qg=qg/REQ_PER_GB; // تبدیل تعداد ریکوئست به گیگابایت
      const ed=parseInt(f.get("expire_days"));
      if(!name||!/^[a-zA-Z0-9]{1,6}$/.test(name))
        return redir(`/panel/dashboard?err=${encodeURIComponent("❌ نام نامعتبر — فقط حروف انگلیسی و عدد، حداکثر ۶ کاراکتر")}`);
      if(isNaN(qg)||qg<=0||isNaN(ed)||ed<=0)
        return redir(`/panel/dashboard?err=${encodeURIComponent("❌ حجم و روز باید عدد معتبر باشند")}`);
      if(!["vless","trojan","all"].includes(protocol))
        return redir(`/panel/dashboard?err=${encodeURIComponent("❌ نوع پروتکل نامعتبر")}`);
      if(await kvGet(kv,admin.username,name))
        return redir(`/panel/dashboard?err=${encodeURIComponent("❌ این نام قبلاً استفاده شده، یه نام دیگه انتخاب کن")}`);
      const country=(f.get("country")||"").trim();
      const proxyIp=(f.get("proxyIp")||"").trim();
      const dailyLimitGb=parseFloat(f.get("dailyLimitGb"));
      const row={name,owner:admin.username,quota_gb:qg,expire_days:ed,
        created_at:Date.now(),used_bytes:0,disabled:false,protocol};
      if(country)row.country=country;
      const _vg=v2Groups(await v2GetSettings(kv));
      const _sel=f.getAll("groups").map(String).filter(g=>_vg.includes(g));
      if(_sel.length&&_sel.length<_vg.length)row.v2groups=_sel;
      if(proxyIp)row.proxyIp=proxyIp;
      if(!isNaN(dailyLimitGb)&&dailyLimitGb>0){
        row.dailyLimitGb=dailyLimitGb;row.dailyUsed=0;row.dailyResetAt=Date.now()+86400000;
      }
      if(protocol==="vless"||protocol==="trojan"||protocol==="all"){
        const creds=v2NewCreds();
        row.v2uuid=creds.v2uuid; row.v2pass=creds.v2pass; row.v2passHash=sha224Hex(creds.v2pass);
      }
      await kvPut(kv,admin.username,name,row);
      if(row.v2uuid)await v2IndexPut(kv,row,admin.username,name);
      return redir(`/panel/dashboard?ok=${encodeURIComponent("✅ اشتراک ساخته شد")}`);
    }

    // ── SHOP BOT WEBHOOK (عمومی — بدون نیاز به لاگین پنل) ───────
    if(path.startsWith("/shop/webhook/")){
      const tok=decodeURIComponent(path.replace("/shop/webhook/",""));
      const settings=await shopGetSettings(kv);
      if(!settings.botToken||tok!==settings.botToken)return new Response("Not found",{status:404});
      let update;
      try{update=await req.json();}catch(e){return new Response("ok");}
      try{
        if(update.callback_query)await handleShopCallback(kv,base,settings,update.callback_query);
        else if(update.message)await handleShopMessage(kv,base,settings,update.message);
      }catch(e){/* از رتری بی‌پایان تلگرام جلوگیری کن */}
      return new Response("ok");
    }

    // ── SUBLINK: صفحه‌ی کاربر ────
    if(path.startsWith("/sublink/")){
      const parts=path.split("/");
      const owner=decodeURIComponent(parts[2]||""),name=decodeURIComponent(parts[3]||""),sub_path=parts[4]||"";
      const row=await kvGet(kv,owner,name);
      if(!row)return new Response("اشتراک یافت نشد",{status:404});

      if(!sub_path){
        let v2Links=null,subTok=null;
        if(!row.disabled&&!isExp(row)){
          if(row.v2uuid){
            const v2s=await v2GetSettings(kv);
            await v2IndexPut(kv,row,owner,name); // بک‌فیل ایندیکس معکوس برای اشتراک‌های قدیمی
            v2Links=buildV2AllLinks(base,owner,name,row,v2s);
            subTok=await v2EnsureSubTok(kv,row,owner,name); // لینک ساب کوتاه (فقط ASCII)
          }
        }
        return page(subPage(row,base,owner,v2Links,subTok));
      }
      return new Response("Not found",{status:404});
    }

    return new Response("Not found",{status:404});
  }
};

// ─── پوشش خطا: به‌جای صفحه‌ی خشک «Error 1101» دلیل واقعی رو نشون بده ───────
export default {
  async fetch(req,env,ctx){
    const isWs=(()=>{try{return req.headers.get("Upgrade")==="websocket";}catch(_){return false;}})();
    try{
      const u=new URL(req.url);
      if(u.pathname==="/__health"&&req.method==="GET"){
        const info={
          db:!!(env&&env.DB), kv:!!(env&&env.KV),
          lastError:_lastErr, time:new Date().toISOString()
        };
        if(env&&env.DB){
          try{ const r=await env.DB.prepare("SELECT COUNT(*) AS n FROM kv").first(); info.rows=r&&r.n; }
          catch(e){ info.dbError=String((e&&e.message)||e); }
        }
        return new Response(JSON.stringify(info,null,2),{headers:{"Content-Type":"application/json","Cache-Control":"no-store"}});
      }
      return await _W.fetch(req,env,ctx);
    }catch(e){
      let msg="unknown";
      try{ msg=String((e&&e.message)||e); _lastErr={at:new Date().toISOString(),msg:msg.slice(0,500),stack:String((e&&e.stack)||"").slice(0,800)}; }catch(_){}
      try{ console.error("PANEL_ERROR",msg); }catch(_){}
      if(isWs)return new Response("error",{status:500});
      try{
        const quota=/limit exceeded/i.test(msg);
        const hint=quota
          ?"سقف روزانه‌ی کلادفلر پر شده. ساعت ۰۳:۳۰ تهران ریست میشه."
          :"یه خطای داخلی رخ داد. چند ثانیه صبر کن و دوباره امتحان کن.";
        const safe=msg.replace(/&/g,"&amp;").replace(/</g,"&lt;").slice(0,400);
        const body='<!DOCTYPE html><html lang="fa" dir="rtl"><head><meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>خطا</title></head>'
          +'<body style="margin:0;min-height:100vh;display:flex;align-items:center;justify-content:center;background:#0d0d12;color:#eee;font-family:Tahoma,sans-serif;padding:20px">'
          +'<div style="max-width:460px;background:#14141b;border:1px solid rgba(255,255,255,.1);border-radius:14px;padding:24px;text-align:center">'
          +'<h2 style="margin:6px 0">مشکلی پیش اومد</h2><p style="color:#9aa3c4;line-height:1.9;font-size:14px">'+hint+'</p>'
          +'<pre style="direction:ltr;text-align:left;white-space:pre-wrap;word-break:break-word;background:#0b0b10;border-radius:8px;padding:10px;font-size:11px;color:#f87171">'+safe+'</pre>'
          +'<a href="javascript:location.reload()" style="color:#a78bfa;font-size:13px">تلاش دوباره</a></div></body></html>';
        return new Response(body,{status:500,headers:{"Content-Type":"text/html;charset=utf-8","Cache-Control":"no-store"}});
      }catch(_){
        return new Response("Internal error",{status:500,headers:{"Content-Type":"text/plain"}});
      }
    }
  }
};
