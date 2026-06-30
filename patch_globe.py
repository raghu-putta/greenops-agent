import re

with open('app.py', 'r', encoding='utf-8') as f:
    src = f.read()

# ── 1. Verify we can find the welcome screen section ──────────────────────────
assert 'welcomeScreen' in src or 'welcome-screen' in src or 'canvas' in src.lower(), \
    "ERROR: Could not locate welcome screen in app.py"

# ── 2. Build the globe JS (self-contained, no external image) ─────────────────
GLOBE_JS = r"""
// ═══════════════════════════════════════════════════════
// REVOLVING GLOBE  –  Aurora / Orbit / Overdrive (default)
// Self-contained canvas animation, no external dependencies
// ═══════════════════════════════════════════════════════
(function initGlobe(canvasId, opts) {
  var cv = document.getElementById(canvasId);
  if (!cv) return;
  var ctx = cv.getContext('2d');
  opts = opts || {};
  var theme  = opts.theme  || 'Aurora';
  var spin   = opts.spin   != null ? opts.spin : 0.16;
  var energy = opts.energy || 'Overdrive';

  var SIZE = 480, C = SIZE/2, rs = 193;
  var THEMES = {
    'Cyber Blue':{ ob:[46,140,238], or_:[80,140,110], lb:[20,54,118], lr:[46,88,98],  glow:'70,160,255',  ring0:'120,200,255', ring1:'90,170,255',  rimArc:'150,220,255', shadow:'rgba(45,140,255,0.6)' },
    'Aurora':    { ob:[24,150,138], or_:[70,200,150], lb:[12,70,66],  lr:[44,120,92],  glow:'50,215,170',  ring0:'120,255,205', ring1:'90,230,180',  rimArc:'160,255,212', shadow:'rgba(40,210,160,0.6)' },
    'Magma':     { ob:[150,52,176], or_:[255,120,90], lb:[70,22,58],  lr:[150,64,82],  glow:'255,120,140', ring0:'255,165,120', ring1:'235,120,205', rimArc:'255,200,165', shadow:'rgba(255,110,120,0.6)' },
    'Ghost':     { ob:[92,140,172], or_:[200,230,255],lb:[30,52,72],  lr:[120,172,205],glow:'180,210,255', ring0:'225,240,255', ring1:'185,212,255', rimArc:'232,246,255', shadow:'rgba(190,215,255,0.55)' }
  };
  var ENERGY = {
    'Calm':      { glow:0.55, grid:0.4,  rings:1, dots:0.6, shadowBlur:14, rimAlpha:0.30 },
    'Charged':   { glow:1.0,  grid:1.0,  rings:2, dots:1.0, shadowBlur:26, rimAlpha:0.50 },
    'Overdrive': { glow:1.7,  grid:1.6,  rings:3, dots:1.5, shadowBlur:42, rimAlpha:0.72 }
  };
  var T  = THEMES[theme]  || THEMES['Aurora'];
  var en = ENERGY[energy] || ENERGY['Overdrive'];
  cv.style.filter = 'drop-shadow(0 0 '+en.shadowBlur+'px '+T.shadow+')';

  // ── Build land mask ──────────────────────────────────
  var MW=720, MH=360;
  var mc=document.createElement('canvas'); mc.width=MW; mc.height=MH;
  var mctx=mc.getContext('2d');
  mctx.fillStyle='#000'; mctx.fillRect(0,0,MW,MH);
  mctx.fillStyle='#fff';
  var polys=[
    [[-17,21],[-16,32],[-5,36],[10,37],[24,32],[34,31],[43,12],[51,12],[44,-2],[40,-16],[33,-29],[20,-35],[13,-30],[9,-17],[9,0],[3,5],[-8,5],[-12,8],[-17,15]],
    [[-10,36],[-9,43],[-2,48],[3,51],[8,54],[12,54],[10,58],[5,60],[10,64],[20,70],[34,71],[42,66],[30,55],[30,46],[18,40],[6,38],[-1,36]],
    [[30,46],[45,42],[48,30],[57,25],[66,25],[70,24],[77,8],[80,13],[90,22],[97,8],[105,1],[120,5],[122,23],[135,35],[143,45],[150,59],[162,61],[180,66],[180,73],[120,78],[70,73],[48,66],[40,60],[33,55]],
    [[-168,60],[-155,71],[-120,71],[-95,72],[-62,82],[-55,52],[-66,45],[-70,42],[-75,35],[-81,25],[-97,18],[-105,23],[-110,30],[-117,33],[-124,40],[-124,48],[-130,55],[-150,59]],
    [[-80,8],[-60,11],[-50,5],[-35,-5],[-35,-12],[-40,-22],[-48,-25],[-53,-34],[-58,-39],[-66,-46],[-73,-52],[-75,-45],[-71,-30],[-70,-18],[-78,-5],[-81,2]],
    [[114,-22],[122,-18],[130,-12],[137,-12],[142,-11],[146,-18],[150,-25],[153,-28],[150,-38],[140,-39],[130,-32],[120,-34],[114,-30]],
    [[-45,60],[-30,60],[-18,70],[-25,80],[-45,82],[-55,78],[-50,68]],
    [[100,2],[118,2],[131,-1],[140,-3],[150,-6],[140,-9],[120,-9],[105,-7],[100,-2]],
    [[44,-13],[50,-16],[50,-22],[45,-25],[43,-20],[43,-16]],
    [[131,32],[140,36],[142,42],[145,44],[140,38],[136,34],[132,31]],
    [[-10,51],[-6,55],[-6,58],[-2,58],[-1,52],[-5,50]],
    [[167,-44],[170,-46],[174,-41],[178,-38],[173,-35],[168,-40]]
  ];
  var lon2x=function(lo){return((lo+180)/360)*MW;};
  var lat2y=function(la){return((90-la)/180)*MH;};
  for(var pi=0;pi<polys.length;pi++){
    var p=polys[pi];
    mctx.beginPath();
    mctx.moveTo(lon2x(p[0][0]),lat2y(p[0][1]));
    for(var i=1;i<p.length;i++) mctx.lineTo(lon2x(p[i][0]),lat2y(p[i][1]));
    mctx.closePath(); mctx.fill();
  }
  var mask=mctx.getImageData(0,0,MW,MH).data;

  // ── Pre-compute pixel arrays ─────────────────────────
  var img=ctx.createImageData(SIZE,SIZE); var out=img.data;
  var Lx=0.55,Ly=-0.42,Lz=0.72;
  var Ll=Math.sqrt(Lx*Lx+Ly*Ly+Lz*Lz); Lx/=Ll; Ly/=Ll; Lz/=Ll;
  var DEG=180/Math.PI;
  var ob=T.ob, or_=T.or_, lb=T.lb, lr=T.lr;
  var cl=function(v){return v>255?255:(v<0?0:v);};
  var pixArr=[],vRow=[],baseLon=[];
  var ocR=[],ocG=[],ocB=[],laR=[],laG=[],laB=[],alpha=[];
  var n=0;
  for(var y=0;y<SIZE;y++){
    for(var x=0;x<SIZE;x++){
      var nx=(x-C)/rs, ny=(y-C)/rs;
      var r2=nx*nx+ny*ny;
      if(r2>1) continue;
      var rr=Math.sqrt(r2);
      var nz=Math.sqrt(1-r2);
      var latDeg=Math.asin(Math.max(-1,Math.min(1,-ny)))*DEG;
      var lonDeg=Math.atan2(nx,nz)*DEG;
      var dif=nx*Lx+ny*Ly+nz*Lz; if(dif<0) dif=0;
      var shade=0.55+0.45*dif;
      var rim=Math.pow(1-nz,2.0);
      var v=Math.min(MH-1,Math.max(0,((((90-latDeg)/180)*MH)|0)));
      var latGrid=(Math.abs(((latDeg%15)+15)%15)<1.1)?14*en.grid:0;
      ocR.push(cl(ob[0]*shade+or_[0]*rim+latGrid));
      ocG.push(cl(ob[1]*shade+or_[1]*rim+latGrid));
      ocB.push(cl(ob[2]*shade+or_[2]*rim+latGrid));
      laR.push(cl(lb[0]*shade+lr[0]*rim));
      laG.push(cl(lb[1]*shade+lr[1]*rim));
      laB.push(cl(lb[2]*shade+lr[2]*rim));
      var a=255;
      if(rr>0.965) a=Math.max(0,(1-rr)/0.035)*255;
      alpha.push(a);
      vRow.push(v);
      baseLon.push(lonDeg);
      pixArr.push((y*SIZE+x)*4);
      n++;
    }
  }

  var gridAmt=14*en.grid;
  var ringDefs=[
    {rx:rs*1.20,ry:rs*0.40,tilt:-0.28,speed:0.34,col:T.ring0,w:1.4,dots:5},
    {rx:rs*1.30,ry:rs*0.20,tilt:0.18, speed:-0.22,col:T.ring1,w:1.2,dots:4},
    {rx:rs*1.12,ry:rs*0.56,tilt:0.52, speed:0.27, col:T.ring0,w:1.1,dots:6}
  ];
  var rings=ringDefs.slice(0,en.rings);

  function drawRing(g,r,ang){
    g.save(); g.translate(C,C); g.rotate(r.tilt);
    g.beginPath();
    g.ellipse(0,0,r.rx,r.ry,0,0,Math.PI*2);
    g.strokeStyle='rgba('+r.col+',0.55)';
    g.lineWidth=r.w;
    g.shadowColor='rgba('+r.col+',0.9)';
    g.shadowBlur=8; g.stroke();
    var nd=Math.max(2,Math.round(r.dots*en.dots));
    for(var i=0;i<nd;i++){
      var t=ang+(i/nd)*Math.PI*2;
      var dx=Math.cos(t)*r.rx, dy=Math.sin(t)*r.ry;
      g.beginPath(); g.arc(dx,dy,2.6,0,Math.PI*2);
      g.fillStyle='rgba(235,245,255,0.95)';
      g.shadowColor='rgba(200,225,255,1)';
      g.shadowBlur=10; g.fill();
    }
    g.restore();
  }

  var haloA=Math.min(0.5,0.22*en.glow);
  var glowCol=T.glow, rimArc=T.rimArc, rimAlpha=en.rimAlpha;
  var rot=0;
  var rafId=null;

  function frame(){
    try{
      rot=(rot+spin+360)%360;
      for(var i=0;i<n;i++){
        var lon=baseLon[i]+rot;
        lon=((lon%360)+360)%360;
        var uu=(lon/360*MW)|0;
        var land=mask[(vRow[i]*MW+uu)*4]>127;
        var r,gg,b;
        if(land){r=laR[i];gg=laG[i];b=laB[i];}
        else{r=ocR[i];gg=ocG[i];b=ocB[i];}
        var lm=lon%15;
        if(lm<0.9||lm>14.1){r+=gridAmt;gg+=gridAmt;b+=gridAmt;}
        var pp=pixArr[i];
        out[pp]=r>255?255:r;
        out[pp+1]=gg>255?255:gg;
        out[pp+2]=b>255?255:b;
        out[pp+3]=alpha[i];
      }
      ctx.clearRect(0,0,SIZE,SIZE);
      ctx.putImageData(img,0,0);
      ctx.globalCompositeOperation='lighter';
      var halo=ctx.createRadialGradient(C,C,rs*0.85,C,C,rs*1.35);
      halo.addColorStop(0,'rgba('+glowCol+',0)');
      halo.addColorStop(0.5,'rgba('+glowCol+','+haloA+')');
      halo.addColorStop(1,'rgba('+glowCol+',0)');
      ctx.fillStyle=halo;
      ctx.beginPath(); ctx.arc(C,C,rs*1.35,0,Math.PI*2); ctx.fill();
      ctx.lineWidth=2.2;
      ctx.strokeStyle='rgba('+rimArc+','+rimAlpha+')';
      ctx.shadowColor='rgba('+rimArc+',0.9)';
      ctx.shadowBlur=10;
      ctx.beginPath(); ctx.arc(C,C,rs*1.005,-1.15,0.95); ctx.stroke();
      ctx.shadowBlur=0;
      var tnow=performance.now()/1000;
      for(var ri=0;ri<rings.length;ri++) drawRing(ctx,rings[ri],tnow*rings[ri].speed);
      ctx.globalCompositeOperation='source-over';
    }catch(e){console.error('globe frame',e);return;}
    rafId=requestAnimationFrame(frame);
  }
  frame();
  // expose stop handle on canvas element for cleanup
  cv._globeStop=function(){if(rafId) cancelAnimationFrame(rafId); rafId=null;};
})
"""

# ── 3. Build the welcome screen HTML block ────────────────────────────────────
WELCOME_HTML = '''
<div id="welcomeScreen" style="
  position:fixed; inset:0; z-index:9999;
  background:#04060f;
  display:flex; flex-direction:column;
  align-items:center; justify-content:center;
  overflow:hidden; cursor:pointer;
" onclick="document.getElementById('welcomeScreen').style.display='none';document.getElementById('mainApp').style.display='block';">

  <!-- Starfield background -->
  <canvas id="starsCanvas" style="
    position:absolute; inset:0; width:100%; height:100%; z-index:0;
  "></canvas>

  <!-- Globe container -->
  <div style="position:relative;z-index:1;display:flex;flex-direction:column;align-items:center;gap:32px;">

    <!-- Globe canvas -->
    <canvas id="globeCanvas" width="480" height="480"
      style="width:380px;height:380px;display:block;
             filter:drop-shadow(0 0 42px rgba(40,210,160,0.6));"></canvas>

    <!-- Title -->
    <div style="text-align:center;line-height:1.2;">
      <div style="
        font-family:'Segoe UI',system-ui,sans-serif;
        font-size:3rem; font-weight:700; letter-spacing:0.04em;
        background:linear-gradient(135deg,#32f5b0 0%,#00d4a8 40%,#2af0c8 100%);
        -webkit-background-clip:text; -webkit-text-fill-color:transparent;
        background-clip:text;
        text-shadow:none;
      ">🌱 GreenOps AI</div>
      <div style="
        font-family:'Segoe UI',system-ui,sans-serif;
        font-size:1.1rem; color:#50e3c2; margin-top:6px; opacity:0.85;
        letter-spacing:0.12em; text-transform:uppercase;
      ">Cloud Cost &amp; Carbon Intelligence</div>
    </div>

    <!-- Click prompt -->
    <div style="
      font-family:'Segoe UI',system-ui,sans-serif;
      font-size:0.85rem; color:#32f5b0; opacity:0.6;
      letter-spacing:0.2em; text-transform:uppercase;
      animation:pulse-text 2s ease-in-out infinite;
    ">Click anywhere to enter</div>
  </div>

  <style>
    @keyframes pulse-text {
      0%,100%{opacity:0.4;} 50%{opacity:0.9;}
    }
  </style>
</div>

<script>
// ── Stars ──────────────────────────────────────────────
(function(){
  var sc=document.getElementById('starsCanvas');
  if(!sc) return;
  var ctx=sc.getContext('2d');
  function resize(){sc.width=window.innerWidth;sc.height=window.innerHeight;}
  resize();
  window.addEventListener('resize',resize);
  var stars=[];
  for(var i=0;i<200;i++){
    stars.push({
      x:Math.random(),y:Math.random(),
      r:Math.random()*1.2+0.2,
      a:Math.random()*0.7+0.3,
      speed:Math.random()*0.0003+0.0001
    });
  }
  var t=0;
  function drawStars(){
    ctx.clearRect(0,0,sc.width,sc.height);
    t+=0.01;
    for(var i=0;i<stars.length;i++){
      var s=stars[i];
      var alpha=s.a*(0.5+0.5*Math.sin(t*s.speed*1000+i));
      ctx.beginPath();
      ctx.arc(s.x*sc.width,s.y*sc.height,s.r,0,Math.PI*2);
      ctx.fillStyle='rgba(255,255,255,'+alpha+')';
      ctx.fill();
    }
    requestAnimationFrame(drawStars);
  }
  drawStars();
})();

// ── Revolving Globe ────────────────────────────────────
''' + GLOBE_JS + '''('globeCanvas', {theme:'Aurora', spin:0.16, energy:'Overdrive'});
</script>
'''

# ── 4. Find and replace the existing welcome screen ───────────────────────────
# Look for the welcomeScreen div (the entire block from opening to closing div)
# Strategy: find the id="welcomeScreen" div and replace through its closing tag

# Pattern 1: id="welcomeScreen" with double-quotes
pat1 = r'(<div[^>]*id=["\']welcomeScreen["\'][^>]*>)(.*?)(</div>\s*<!--\s*end welcome|(?=<div[^>]*id=["\']mainApp["\']))'

# Pattern 2: simpler — find the outermost welcome div and replace it
# We'll use a line-based approach: find `id="welcomeScreen"` or `id='welcomeScreen'`
# and track div depth until the matching close

lines = src.split('\n')
start_line = None
for idx, line in enumerate(lines):
    if 'welcomeScreen' in line and ('<div' in line or 'id=' in line):
        start_line = idx
        break

if start_line is None:
    # Fallback: maybe it's a canvas-only welcome, find the canvas block
    for idx, line in enumerate(lines):
        if 'id="welcomeCanvas"' in line or "id='welcomeCanvas'" in line or \
           ('canvas' in line.lower() and ('welcome' in src[max(0,src.find(line)-200):src.find(line)+200].lower())):
            start_line = idx
            break

if start_line is None:
    raise ValueError("Cannot find welcome screen in app.py — searched for welcomeScreen div and welcome canvas")

# Now find the end of the welcomeScreen block by tracking div depth
depth = 0
end_line = None
in_block = False
for idx in range(start_line, len(lines)):
    line = lines[idx]
    opens  = line.count('<div')
    closes = line.count('</div')
    if idx == start_line and opens == 0:
        # The id might be on the line BEFORE <div, handle edge case
        if depth == 0 and closes == 0:
            continue
    depth += opens
    depth -= closes
    if depth > 0:
        in_block = True
    if in_block and depth <= 0:
        end_line = idx
        break

if end_line is None:
    # Fallback: grab 150 lines from start
    end_line = start_line + 150

# Replace the block
new_lines = lines[:start_line] + [WELCOME_HTML] + lines[end_line+1:]
new_src = '\n'.join(new_lines)

print(f"Found welcome screen at lines {start_line}-{end_line}")
print(f"Original length: {len(src)} chars")
print(f"New length: {len(new_src)} chars")
print("Replacement done. Writing app.py...")

with open('app.py', 'w', encoding='utf-8') as f:
    f.write(new_src)

print("SUCCESS: app.py updated with globe welcome screen")
