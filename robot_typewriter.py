with open("app.py", "r", encoding="utf-8") as f:
    c = f.read()

# Remove old bot selection UI and replace with single Alex + typewriter
old_select = '''      <div style="background:#161b22;border-radius:10px;padding:10px;border:1px solid #21262d;">
        <p style="color:#6e7681;font-size:0.7rem;margin:0 0 8px;text-transform:uppercase;letter-spacing:1px;">Choose Your AI Assistant</p>
        <div style="display:flex;gap:6px;">
          <button onclick="selectBot('alex')" id="bot-alex" style="flex:1;padding:8px 4px;border-radius:8px;cursor:pointer;background:#1a3a2a;border:2px solid #34d399;color:#34d399;font-size:0.75rem;font-weight:600;">&#129302;<br/>Alex</button>
          <button onclick="selectBot('aria')" id="bot-aria" style="flex:1;padding:8px 4px;border-radius:8px;cursor:pointer;background:#1a1a2e;border:2px solid #30363d;color:#8b949e;font-size:0.75rem;font-weight:600;">&#128105;&#8205;&#128187;<br/>Aria</button>
          <button onclick="selectBot('max')" id="bot-max" style="flex:1;padding:8px 4px;border-radius:8px;cursor:pointer;background:#1a1a2e;border:2px solid #30363d;color:#8b949e;font-size:0.75rem;font-weight:600;">&#128104;&#8205;&#128187;<br/>Max</button>
          <button onclick="selectBot('nova')" id="bot-nova" style="flex:1;padding:8px 4px;border-radius:8px;cursor:pointer;background:#1a1a2e;border:2px solid #30363d;color:#8b949e;font-size:0.75rem;font-weight:600;">&#129504;<br/>Nova</button>
          <button onclick="selectBot('eco')" id="bot-eco" style="flex:1;padding:8px 4px;border-radius:8px;cursor:pointer;background:#1a1a2e;border:2px solid #30363d;color:#8b949e;font-size:0.75rem;font-weight:600;">&#127807;<br/>Eco</button>
        </div>
      </div>'''

new_select = '''      <div style="background:#161b22;border-radius:10px;padding:10px;border:1px solid #21262d;text-align:center;">
        <p style="color:#34d399;font-size:0.7rem;margin:0;text-transform:uppercase;letter-spacing:2px;font-weight:600;">&#9733; Your GreenOps AI Assistant &#9733;</p>
      </div>'''

if old_select in c:
    c = c.replace(old_select, new_select)
    print("FIX 1 OK - Bot selector removed")
else:
    print("FIX 1 FAILED - selector not found")

# Replace robot bubble with typewriter version
old_bubble = '''    <div class="robot-bubble" style="background:#161b22;border:1px solid #21262d;border-radius:16px 16px 16px 4px;padding:14px 16px;margin-bottom:4px;">
        <div style="display:flex;align-items:flex-start;gap:12px;">
          <div class="robot-avatar-wrap" id="bot-avatar" style="font-size:2rem;flex-shrink:0;">&#129302;</div>
          <div>
            <div id="bot-name" style="color:#34d399;font-size:0.72rem;font-weight:700;margin-bottom:4px;">ALEX</div>
            <div id="bot-msg" style="color:#c9d1d9;font-size:0.85rem;line-height:1.5;">Welcome! The cloud awaits your command. Let us scan for waste and save the planet together!</div>
          </div>
        </div>
      </div>'''

new_bubble = '''    <div style="background:#161b22;border:1px solid #21262d;border-radius:16px 16px 16px 4px;padding:16px;margin-bottom:4px;">
        <div style="display:flex;align-items:flex-start;gap:14px;">
          <div style="flex-shrink:0;position:relative;width:56px;height:56px;">
            <div id="bot-glow" style="position:absolute;inset:-4px;border-radius:50%;background:radial-gradient(circle,rgba(52,211,153,0.4),transparent);animation:eyeGlow 2s ease-in-out infinite;"></div>
            <div style="width:56px;height:56px;border-radius:50%;border:2px solid #34d399;background:#0d1117;display:flex;align-items:center;justify-content:center;font-size:2rem;position:relative;z-index:1;">&#129302;</div>
          </div>
          <div style="flex:1;min-width:0;">
            <div style="display:flex;align-items:center;gap:6px;margin-bottom:6px;">
              <span style="color:#34d399;font-size:0.72rem;font-weight:700;letter-spacing:1px;">ALEX</span>
              <span style="background:#34d399;color:#0a1a0f;font-size:0.6rem;padding:1px 6px;border-radius:4px;font-weight:700;">AI</span>
            </div>
            <div id="bot-msg" style="color:#c9d1d9;font-size:0.85rem;line-height:1.6;min-height:48px;"></div>
            <span id="bot-cursor" style="display:inline-block;width:2px;height:14px;background:#34d399;margin-left:2px;animation:blink 0.7s step-end infinite;vertical-align:middle;"></span>
          </div>
        </div>
      </div>'''

if old_bubble in c:
    c = c.replace(old_bubble, new_bubble)
    print("FIX 2 OK - Robot bubble with typewriter added")
else:
    print("FIX 2 FAILED - bubble not found")

# Replace the JS section
old_js = '''<script>
var bots={
  alex:{avatar:"&#129302;",name:"ALEX",color:"#34d399",msg:"Welcome! The cloud awaits your command. Let us scan for waste and save the planet together!"},
  aria:{avatar:"&#128105;&#8205;&#128187;",name:"ARIA",color:"#f472b6",msg:"Hi there! I am Aria, your cloud optimization partner. Fill in your details and let us make your infrastructure greener!"},
  max:{avatar:"&#128104;&#8205;&#128187;",name:"MAX",color:"#60a5fa",msg:"Yo! Max here. Ready to crush some cloud waste? Drop your GCP creds and let us roll!"},
  nova:{avatar:"&#129504;",name:"NOVA",color:"#a78bfa",msg:"Greetings. I am Nova. Precision is my protocol. Enter your credentials and I shall optimize with surgical accuracy."},
  eco:{avatar:"&#127807;",name:"ECO",color:"#34d399",msg:"Hey eco-warrior! Every idle VM we stop plants a virtual tree. Let us make your cloud carbon-neutral today!"}
};
var currentBot="alex";
function openPanel(){
  document.getElementById("gcp-panel").style.display="flex";
  loadPanelSettings();
  setTimeout(function(){selectBot(currentBot);},200);
}
function closePanel(){document.getElementById("gcp-panel").style.display="none";}
function selectBot(id){
  currentBot=id;
  ["alex","aria","max","nova","eco"].forEach(function(r){
    var b=document.getElementById("bot-"+r);
    b.style.background="#1a1a2e";b.style.borderColor="#30363d";b.style.color="#8b949e";
  });
  var rb=document.getElementById("bot-"+id);
  var bot=bots[id];
  rb.style.background="#1a2a1a";rb.style.borderColor=bot.color;rb.style.color=bot.color;
  var av=document.getElementById("bot-avatar");
  var nm=document.getElementById("bot-name");
  var mg=document.getElementById("bot-msg");
  av.style.opacity="0";
  setTimeout(function(){
    av.innerHTML=bot.avatar;av.style.opacity="1";av.style.transition="opacity 0.3s";
    nm.style.color=bot.color;nm.textContent=bot.name;
    mg.style.opacity="0";
    setTimeout(function(){mg.textContent=bot.msg;mg.style.opacity="1";mg.style.transition="opacity 0.3s";},200);
  },150);
  sessionStorage.setItem("gops-bot",id);
}
function savePanelCfg(){
  var key=document.getElementById("cfg-api-key").value.trim();
  var proj=document.getElementById("cfg-project-id").value.trim();
  var region=document.getElementById("cfg-region").value;
  var zone=document.getElementById("cfg-zone").value.trim()||region+"-a";
  if(!key||!proj){
    document.getElementById("bot-msg").textContent="Oops! API Key and Project ID are required. Please fill them in!";
    return;
  }
  sessionStorage.setItem("gops-cfg",JSON.stringify({apiKey:key,projectId:proj,region:region,zone:zone}));
  document.getElementById("bot-msg").textContent="All set! Your GCP credentials are saved. Ready to scan!";
  setTimeout(function(){closePanel();},1500);
}
function testPanelConn(){
  document.getElementById("bot-msg").textContent="Testing your connection... hang tight!";
  fetch("/test-connection",{method:"POST",headers:{"Content-Type":"application/json"},
    body:JSON.stringify({
      apiKey:document.getElementById("cfg-api-key").value.trim(),
      projectId:document.getElementById("cfg-project-id").value.trim(),
      region:document.getElementById("cfg-region").value
    })
  }).then(function(r){return r.json();})
  .then(function(d){
    document.getElementById("bot-msg").textContent=d.success?"Connection successful! All systems GO!":"Connection failed. Check your credentials.";
  }).catch(function(){
    document.getElementById("bot-msg").textContent="Could not reach server. Check your network.";
  });
}
function loadPanelSettings(){
  try{
    var s=JSON.parse(sessionStorage.getItem("gops-cfg")||"{}");
    if(s.apiKey)document.getElementById("cfg-api-key").value=s.apiKey;
    if(s.projectId)document.getElementById("cfg-project-id").value=s.projectId;
    if(s.region)document.getElementById("cfg-region").value=s.region;
    if(s.zone)document.getElementById("cfg-zone").value=s.zone;
    currentBot=sessionStorage.getItem("gops-bot")||"alex";
  }catch(e){}
}
window.addEventListener("load",function(){loadPanelSettings();});
</script>'''

new_js = '''<script>
var alexQuotes = [
  "Welcome! The cloud awaits your command. Let us scan for waste and save the planet together!",
  "Hi there! I am your cloud optimization partner. Fill in your details and let us make your infrastructure greener!",
  "Yo! Ready to crush some cloud waste? Drop your GCP creds and let us roll!",
  "Precision is my protocol. Enter your credentials and I shall optimize with surgical accuracy.",
  "Every idle VM we stop plants a virtual tree. Let us make your cloud carbon-neutral today!",
  "Your GCP project is waiting to be optimized. Together we can cut costs and carbon emissions!",
  "The best time to optimize your cloud was yesterday. The second best time is right now!"
];
var quoteIdx = 0;
var typeTimer = null;
var cycleTimer = null;
var isTyping = false;

function typeText(text, el, cb) {
  isTyping = true;
  el.textContent = "";
  var i = 0;
  if (typeTimer) clearInterval(typeTimer);
  typeTimer = setInterval(function() {
    if (i < text.length) {
      el.textContent += text[i];
      i++;
    } else {
      clearInterval(typeTimer);
      isTyping = false;
      if (cb) setTimeout(cb, 3000);
    }
  }, 28);
}

function cycleQuote() {
  var el = document.getElementById("bot-msg");
  if (!el) return;
  el.style.opacity = "0";
  el.style.transition = "opacity 0.4s";
  setTimeout(function() {
    quoteIdx = (quoteIdx + 1) % alexQuotes.length;
    el.style.opacity = "1";
    typeText(alexQuotes[quoteIdx], el, cycleQuote);
  }, 400);
}

function startTypewriter() {
  var el = document.getElementById("bot-msg");
  if (!el) return;
  if (typeTimer) clearInterval(typeTimer);
  if (cycleTimer) clearTimeout(cycleTimer);
  quoteIdx = 0;
  typeText(alexQuotes[0], el, cycleQuote);
}

function openPanel() {
  document.getElementById("gcp-panel").style.display = "flex";
  loadPanelSettings();
  setTimeout(startTypewriter, 300);
}

function closePanel() {
  document.getElementById("gcp-panel").style.display = "none";
  if (typeTimer) clearInterval(typeTimer);
}

function savePanelCfg() {
  var key = document.getElementById("cfg-api-key").value.trim();
  var proj = document.getElementById("cfg-project-id").value.trim();
  var region = document.getElementById("cfg-region").value;
  var zone = document.getElementById("cfg-zone").value.trim() || region + "-a";
  if (!key || !proj) {
    if (typeTimer) clearInterval(typeTimer);
    var el = document.getElementById("bot-msg");
    el.style.opacity = "0";
    setTimeout(function() {
      el.style.opacity = "1";
      typeText("Oops! API Key and Project ID are both required. Please fill them in!", el, null);
    }, 300);
    if (!key) document.getElementById("cfg-api-key").style.borderColor = "#f85149";
    if (!proj) document.getElementById("cfg-project-id").style.borderColor = "#f85149";
    return;
  }
  sessionStorage.setItem("gops-cfg", JSON.stringify({apiKey:key, projectId:proj, region:region, zone:zone}));
  if (typeTimer) clearInterval(typeTimer);
  var el = document.getElementById("bot-msg");
  el.style.opacity = "0";
  setTimeout(function() {
    el.style.opacity = "1";
    typeText("All set! Your GCP credentials are saved. Ready to scan the cloud!", el, null);
  }, 300);
  setTimeout(function() { closePanel(); }, 2500);
}

function testPanelConn() {
  if (typeTimer) clearInterval(typeTimer);
  var el = document.getElementById("bot-msg");
  el.style.opacity = "0";
  setTimeout(function() {
    el.style.opacity = "1";
    typeText("Testing your connection... hang tight while I check!", el, null);
  }, 300);
  fetch("/test-connection", {
    method: "POST",
    headers: {"Content-Type": "application/json"},
    body: JSON.stringify({
      apiKey: document.getElementById("cfg-api-key").value.trim(),
      projectId: document.getElementById("cfg-project-id").value.trim(),
      region: document.getElementById("cfg-region").value
    })
  }).then(function(r) { return r.json(); })
  .then(function(d) {
    if (typeTimer) clearInterval(typeTimer);
    var el = document.getElementById("bot-msg");
    el.style.opacity = "0";
    setTimeout(function() {
      el.style.opacity = "1";
      typeText(d.success ? "Connection successful! All systems GO! Ready to scan!" : "Connection failed. Please check your API key and Project ID.", el, null);
    }, 300);
  }).catch(function() {
    if (typeTimer) clearInterval(typeTimer);
    var el = document.getElementById("bot-msg");
    el.textContent = "Could not reach server. Check your network connection.";
  });
}

function loadPanelSettings() {
  try {
    var s = JSON.parse(sessionStorage.getItem("gops-cfg") || "{}");
    if (s.apiKey) document.getElementById("cfg-api-key").value = s.apiKey;
    if (s.projectId) document.getElementById("cfg-project-id").value = s.projectId;
    if (s.region) document.getElementById("cfg-region").value = s.region;
    if (s.zone) document.getElementById("cfg-zone").value = s.zone;
  } catch(e) {}
}

window.addEventListener("load", function() { loadPanelSettings(); });
</script>'''

if old_js in c:
    c = c.replace(old_js, new_js)
    print("FIX 3 OK - Typewriter JS added!")
else:
    print("FIX 3 FAILED - JS not found")

# Add eyeGlow and blink CSS animations
eye_css = """
  @keyframes eyeGlow{0%,100%{opacity:0.4;transform:scale(1)}50%{opacity:1;transform:scale(1.15)}}
  @keyframes blink{0%,100%{opacity:1}50%{opacity:0}}
"""
if "eyeGlow" not in c:
    c = c.replace("</style>", eye_css + "\n  </style>", 1)
    print("FIX 4 OK - Eye glow CSS added")

with open("app.py", "w", encoding="utf-8") as f:
    f.write(c)
print("ALL DONE!")
