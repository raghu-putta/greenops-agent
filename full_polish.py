import re

with open("app.py", "r", encoding="utf-8") as f:
    c = f.read()

# FIX 1: Agent colored names CSS
color_css = """
  .acard:nth-child(1) .acard-num{color:#34d399;}
  .acard:nth-child(1) .acard-name{color:#34d399;font-weight:600;}
  .acard:nth-child(2) .acard-num{color:#60a5fa;}
  .acard:nth-child(2) .acard-name{color:#60a5fa;font-weight:600;}
  .acard:nth-child(3) .acard-num{color:#f97316;}
  .acard:nth-child(3) .acard-name{color:#f97316;font-weight:600;}
  .acard:nth-child(4) .acard-num{color:#a78bfa;}
  .acard:nth-child(4) .acard-name{color:#a78bfa;font-weight:600;}
  .acard:nth-child(1) .acard-icon{border-color:#34d399;box-shadow:0 0 10px rgba(52,211,153,0.5);}
  .acard:nth-child(2) .acard-icon{border-color:#60a5fa;box-shadow:0 0 10px rgba(96,165,250,0.5);}
  .acard:nth-child(3) .acard-icon{border-color:#f97316;box-shadow:0 0 10px rgba(249,115,22,0.5);}
  .acard:nth-child(4) .acard-icon{border-color:#a78bfa;box-shadow:0 0 10px rgba(167,139,250,0.5);}
  @keyframes fadeInUp{from{opacity:0;transform:translateY(20px)}to{opacity:1;transform:translateY(0)}}
  @keyframes pulse-glow{0%,100%{box-shadow:0 0 10px rgba(52,211,153,0.4)}50%{box-shadow:0 0 25px rgba(52,211,153,0.8)}}
  .robot-bubble{animation:fadeInUp 0.4s ease;}
  .robot-avatar-wrap{animation:pulse-glow 2s ease-in-out infinite;}
"""
if "acard:nth-child(1)" not in c:
    c = c.replace("</style>", color_css + "\n  </style>", 1)
    print("FIX 1 OK - Agent colors added")
else:
    print("FIX 1 - Colors already exist")

# FIX 2: Remove old gcp-panel and replace with full panel
if 'id="gcp-panel"' in c:
    # Remove old panel
    start = c.find('<div id="gcp-panel"')
    if start >= 0:
        # Find the closing script tag after panel
        end = c.find('</div></div>', start) + 12
        c = c[:start] + c[end:]
        print("FIX 2 - Old panel removed")

# Full GCP panel with robot personalities
full_panel = '''
<div id="gcp-panel" style="display:none;position:fixed;top:0;left:0;width:100%;height:100%;z-index:9999;background:rgba(0,0,0,0.85);backdrop-filter:blur(8px);justify-content:center;align-items:center;">
  <div style="background:#0d1117;border:1px solid #34d399;border-radius:16px;padding:0;width:480px;max-width:95%;max-height:90vh;overflow-y:auto;box-shadow:0 0 40px rgba(52,211,153,0.2);">
    <div style="background:linear-gradient(135deg,#0a1628,#0d2818);border-bottom:1px solid #1e3a2a;padding:20px 24px;border-radius:16px 16px 0 0;position:sticky;top:0;">
      <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:14px;">
        <h2 style="color:#34d399;margin:0;font-size:1.1rem;">&#9881; Configure GCP</h2>
        <button onclick="closePanel()" style="background:#1a1f2e;border:1px solid #30363d;color:#8b949e;width:32px;height:32px;border-radius:8px;cursor:pointer;font-size:1rem;">&#x2715;</button>
      </div>
      <div style="background:#161b22;border-radius:10px;padding:10px;border:1px solid #21262d;">
        <p style="color:#6e7681;font-size:0.7rem;margin:0 0 8px;text-transform:uppercase;letter-spacing:1px;">Choose Your AI Assistant</p>
        <div style="display:flex;gap:6px;">
          <button onclick="selectBot('alex')" id="bot-alex" style="flex:1;padding:8px 4px;border-radius:8px;cursor:pointer;background:#1a3a2a;border:2px solid #34d399;color:#34d399;font-size:0.75rem;font-weight:600;">&#129302;<br/>Alex</button>
          <button onclick="selectBot('aria')" id="bot-aria" style="flex:1;padding:8px 4px;border-radius:8px;cursor:pointer;background:#1a1a2e;border:2px solid #30363d;color:#8b949e;font-size:0.75rem;font-weight:600;">&#128105;&#8205;&#128187;<br/>Aria</button>
          <button onclick="selectBot('max')" id="bot-max" style="flex:1;padding:8px 4px;border-radius:8px;cursor:pointer;background:#1a1a2e;border:2px solid #30363d;color:#8b949e;font-size:0.75rem;font-weight:600;">&#128104;&#8205;&#128187;<br/>Max</button>
          <button onclick="selectBot('nova')" id="bot-nova" style="flex:1;padding:8px 4px;border-radius:8px;cursor:pointer;background:#1a1a2e;border:2px solid #30363d;color:#8b949e;font-size:0.75rem;font-weight:600;">&#129504;<br/>Nova</button>
          <button onclick="selectBot('eco')" id="bot-eco" style="flex:1;padding:8px 4px;border-radius:8px;cursor:pointer;background:#1a1a2e;border:2px solid #30363d;color:#8b949e;font-size:0.75rem;font-weight:600;">&#127807;<br/>Eco</button>
        </div>
      </div>
    </div>
    <div style="padding:16px 24px 0;">
      <div class="robot-bubble" style="background:#161b22;border:1px solid #21262d;border-radius:16px 16px 16px 4px;padding:14px 16px;margin-bottom:4px;">
        <div style="display:flex;align-items:flex-start;gap:12px;">
          <div class="robot-avatar-wrap" id="bot-avatar" style="font-size:2rem;flex-shrink:0;">&#129302;</div>
          <div>
            <div id="bot-name" style="color:#34d399;font-size:0.72rem;font-weight:700;margin-bottom:4px;">ALEX</div>
            <div id="bot-msg" style="color:#c9d1d9;font-size:0.85rem;line-height:1.5;">Welcome! The cloud awaits your command. Let us scan for waste and save the planet together!</div>
          </div>
        </div>
      </div>
    </div>
    <div style="padding:20px 24px;">
      <div style="margin-bottom:14px;">
        <label style="display:block;color:#34d399;font-size:0.82rem;font-weight:600;margin-bottom:6px;">&#10024; Gemini API Key *</label>
        <div style="position:relative;">
          <input type="password" id="cfg-api-key" placeholder="AIzaSy..." style="width:100%;background:#161b22;border:1.5px solid #30363d;color:#e6edf3;padding:11px 44px 11px 14px;border-radius:10px;font-size:0.88rem;box-sizing:border-box;outline:none;" onfocus="this.style.borderColor='#34d399'" onblur="this.style.borderColor='#30363d'"/>
          <span onclick="var i=document.getElementById('cfg-api-key');i.type=i.type==='password'?'text':'password'" style="position:absolute;right:12px;top:50%;transform:translateY(-50%);cursor:pointer;color:#6e7681;">&#128065;</span>
        </div>
        <small style="color:#6e7681;font-size:0.72rem;">Free at <a href="https://aistudio.google.com/apikey" target="_blank" style="color:#58a6ff;">aistudio.google.com/apikey</a></small>
      </div>
      <div style="margin-bottom:14px;">
        <label style="display:block;color:#34d399;font-size:0.82rem;font-weight:600;margin-bottom:6px;">&#9729; GCP Project ID *</label>
        <input type="text" id="cfg-project-id" placeholder="my-project-123" style="width:100%;background:#161b22;border:1.5px solid #30363d;color:#e6edf3;padding:11px 14px;border-radius:10px;font-size:0.88rem;box-sizing:border-box;outline:none;" onfocus="this.style.borderColor='#34d399'" onblur="this.style.borderColor='#30363d'"/>
        <small style="color:#6e7681;font-size:0.72rem;">Find at <a href="https://console.cloud.google.com" target="_blank" style="color:#58a6ff;">console.cloud.google.com</a></small>
      </div>
      <div style="margin-bottom:14px;">
        <label style="display:block;color:#34d399;font-size:0.82rem;font-weight:600;margin-bottom:6px;">&#127758; GCP Region</label>
        <select id="cfg-region" style="width:100%;background:#161b22;border:1.5px solid #30363d;color:#e6edf3;padding:11px 14px;border-radius:10px;font-size:0.88rem;box-sizing:border-box;outline:none;cursor:pointer;">
          <option value="us-central1">us-central1 - Iowa, USA</option>
          <option value="us-east1">us-east1 - South Carolina, USA</option>
          <option value="us-west1">us-west1 - Oregon, USA</option>
          <option value="europe-west1">europe-west1 - Belgium</option>
          <option value="europe-west2">europe-west2 - London, UK</option>
          <option value="asia-east1">asia-east1 - Taiwan</option>
          <option value="asia-south1">asia-south1 - Mumbai, India</option>
          <option value="australia-southeast1">australia-southeast1 - Sydney</option>
        </select>
      </div>
      <div style="margin-bottom:20px;">
        <label style="display:block;color:#34d399;font-size:0.82rem;font-weight:600;margin-bottom:6px;">&#128205; GCP Zone</label>
        <input type="text" id="cfg-zone" placeholder="us-central1-a" style="width:100%;background:#161b22;border:1.5px solid #30363d;color:#e6edf3;padding:11px 14px;border-radius:10px;font-size:0.88rem;box-sizing:border-box;outline:none;" onfocus="this.style.borderColor='#34d399'" onblur="this.style.borderColor='#30363d'"/>
        <small style="color:#6e7681;font-size:0.72rem;">Usually region + -a e.g. us-central1-a</small>
      </div>
      <div style="display:flex;gap:10px;margin-bottom:12px;">
        <button onclick="savePanelCfg()" style="flex:1;background:linear-gradient(135deg,#34d399,#10b981);color:#0a1a0f;border:none;padding:13px;border-radius:10px;font-weight:700;cursor:pointer;font-size:0.9rem;">Save and Close</button>
        <button onclick="testPanelConn()" style="flex:1;background:transparent;color:#34d399;border:1.5px solid #34d399;padding:13px;border-radius:10px;font-weight:600;cursor:pointer;font-size:0.9rem;">Test Connection</button>
      </div>
      <p style="color:#484f58;font-size:0.72rem;text-align:center;margin:0;">Stored in browser session only. Never sent to our servers.</p>
    </div>
  </div>
</div>
<script>
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
</script>
'''

# Fix the button to use openPanel()
c = c.replace(
    'onclick="document.getElementById(\'gcp-panel\').style.display=\'flex\'"',
    'onclick="openPanel()"'
)

c = c.replace("</body>", full_panel + "\n</body>", 1)
print("FIX 2 OK - Full GCP panel with robots added")

with open("app.py", "w", encoding="utf-8") as f:
    f.write(c)
print("ALL DONE!")
