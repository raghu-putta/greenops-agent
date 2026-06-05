with open("app.py", "r", encoding="utf-8") as f:
    content = f.read()

# 1. Add CSS for settings button
settings_css = """
  .btn-settings{background:#1a3a2a;color:#34d399;border:1px solid #34d399;padding:10px 16px;border-radius:8px;cursor:pointer;font-size:0.82rem;font-weight:600;margin-top:10px;width:100%;transition:all 0.2s;}
  .btn-settings:hover{background:#34d399;color:#0a1a0f;}
  @keyframes robotBob{0%,100%{transform:translateY(0)}50%{transform:translateY(-5px)}}
  @keyframes robotShake{0%,100%{transform:rotate(0)}25%{transform:rotate(-10deg)}75%{transform:rotate(10deg)}}
  @keyframes robotSpin{0%{transform:rotate(0deg)}100%{transform:rotate(360deg)}}
"""
content = content.replace("</style>", settings_css + "\n  </style>", 1)
print("OK - CSS added")

# 2. Add Configure GCP button after Run Real GCP button
old_btn = """      <button class="btn btn-real" id="btn-real" onclick="run('real')">"""
# Find end of this button
idx = content.find(old_btn)
if idx >= 0:
    end = content.find("</button>", idx) + 9
    content = content[:end] + '\n      <button class="btn-settings" onclick="openSettings()">&#9881; Configure GCP</button>' + content[end:]
    print("OK - Configure GCP button added")
else:
    print("WARNING - Run Real GCP button not found!")

# 3. Add robot panel HTML + JS before </body>
robot_panel = """
  <div id="gcp-settings-overlay" onclick="if(event.target===this)closeSettings()" style="position:fixed;top:0;left:0;width:100%;height:100%;z-index:99999;display:none;background:rgba(0,0,0,0.85);backdrop-filter:blur(8px);">
    <div style="position:absolute;right:0;top:0;width:460px;height:100%;background:#0d1117;border-left:2px solid #34d399;overflow-y:auto;">
      <div style="background:linear-gradient(135deg,#0a1628,#0d2818);border-bottom:1px solid #1e3a2a;padding:20px 24px;position:sticky;top:0;z-index:10;">
        <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:14px;">
          <h2 style="color:#34d399;margin:0;font-size:1.1rem;">&#9881; Configure GCP</h2>
          <button onclick="closeSettings()" style="background:#1a1f2e;border:1px solid #30363d;color:#8b949e;width:34px;height:34px;border-radius:8px;cursor:pointer;">X</button>
        </div>
        <div style="background:#161b22;border-radius:10px;padding:10px;border:1px solid #21262d;">
          <p style="color:#6e7681;font-size:0.7rem;margin:0 0 8px;text-transform:uppercase;letter-spacing:1px;">Choose Your AI Assistant</p>
          <div style="display:flex;gap:6px;">
            <button onclick="selectRobot('alex')" id="robot-alex" style="flex:1;padding:8px 4px;border-radius:8px;cursor:pointer;background:#1a3a2a;border:2px solid #34d399;color:#34d399;font-size:0.75rem;font-weight:600;text-align:center;">&#129302;<br/>Alex</button>
            <button onclick="selectRobot('aria')" id="robot-aria" style="flex:1;padding:8px 4px;border-radius:8px;cursor:pointer;background:#1a1a2e;border:2px solid #30363d;color:#8b949e;font-size:0.75rem;font-weight:600;text-align:center;">&#128105;&#8205;&#128187;<br/>Aria</button>
            <button onclick="selectRobot('max')"  id="robot-max"  style="flex:1;padding:8px 4px;border-radius:8px;cursor:pointer;background:#1a1a2e;border:2px solid #30363d;color:#8b949e;font-size:0.75rem;font-weight:600;text-align:center;">&#128104;&#8205;&#128187;<br/>Max</button>
            <button onclick="selectRobot('nova')" id="robot-nova" style="flex:1;padding:8px 4px;border-radius:8px;cursor:pointer;background:#1a1a2e;border:2px solid #30363d;color:#8b949e;font-size:0.75rem;font-weight:600;text-align:center;">&#129504;<br/>Nova</button>
            <button onclick="selectRobot('eco')"  id="robot-eco"  style="flex:1;padding:8px 4px;border-radius:8px;cursor:pointer;background:#1a1a2e;border:2px solid #30363d;color:#8b949e;font-size:0.75rem;font-weight:600;text-align:center;">&#127807;<br/>Eco</button>
          </div>
        </div>
      </div>
      <div style="padding:16px 24px 0;">
        <div style="background:#161b22;border:1px solid #21262d;border-radius:16px 16px 16px 4px;padding:14px 16px;">
          <div style="display:flex;align-items:flex-start;gap:12px;">
            <div id="robot-avatar" style="font-size:2.2rem;animation:robotBob 2s ease-in-out infinite;flex-shrink:0;">&#129302;</div>
            <div>
              <div id="robot-name" style="color:#34d399;font-size:0.72rem;font-weight:700;margin-bottom:4px;letter-spacing:0.5px;">ALEX</div>
              <div id="robot-message" style="color:#c9d1d9;font-size:0.88rem;line-height:1.5;">Hey Techie! I need your GCP details to get started!</div>
            </div>
          </div>
        </div>
      </div>
      <div style="padding:20px 24px;">
        <div style="margin-bottom:16px;">
          <label style="display:block;color:#34d399;font-size:0.82rem;font-weight:600;margin-bottom:6px;">&#10024; Gemini API Key *</label>
          <div style="position:relative;">
            <input type="password" id="cfg-api-key" placeholder="AIzaSy..." autocomplete="off" oninput="onFieldInput()" style="width:100%;background:#161b22;border:1.5px solid #30363d;color:#e6edf3;padding:11px 44px 11px 14px;border-radius:10px;font-size:0.88rem;box-sizing:border-box;outline:none;" onfocus="this.style.borderColor='#34d399'" onblur="this.style.borderColor='#30363d'"/>
            <span onclick="togglePwd()" style="position:absolute;right:12px;top:50%;transform:translateY(-50%);cursor:pointer;color:#6e7681;">&#128065;</span>
          </div>
          <small style="color:#6e7681;font-size:0.72rem;">Free at <a href="https://aistudio.google.com/apikey" target="_blank" style="color:#58a6ff;">aistudio.google.com/apikey</a></small>
        </div>
        <div style="margin-bottom:16px;">
          <label style="display:block;color:#34d399;font-size:0.82rem;font-weight:600;margin-bottom:6px;">&#9729; GCP Project ID *</label>
          <input type="text" id="cfg-project-id" placeholder="my-project-123" oninput="onFieldInput()" style="width:100%;background:#161b22;border:1.5px solid #30363d;color:#e6edf3;padding:11px 14px;border-radius:10px;font-size:0.88rem;box-sizing:border-box;outline:none;" onfocus="this.style.borderColor='#34d399'" onblur="this.style.borderColor='#30363d'"/>
          <small style="color:#6e7681;font-size:0.72rem;">Find at <a href="https://console.cloud.google.com" target="_blank" style="color:#58a6ff;">console.cloud.google.com</a></small>
        </div>
        <div style="margin-bottom:16px;">
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
        <div style="display:flex;gap:10px;margin-bottom:14px;">
          <button onclick="saveCfg()" style="flex:1;background:linear-gradient(135deg,#34d399,#10b981);color:#0a1a0f;border:none;padding:13px;border-radius:10px;font-weight:700;cursor:pointer;font-size:0.9rem;">&#128190; Save and Close</button>
          <button onclick="testCfgConn()" style="flex:1;background:transparent;color:#34d399;border:1.5px solid #34d399;padding:13px;border-radius:10px;font-weight:600;cursor:pointer;font-size:0.9rem;">&#128269; Test</button>
        </div>
        <div style="padding:12px;background:#161b22;border-radius:10px;border:1px solid #21262d;text-align:center;">
          <p style="color:#484f58;font-size:0.72rem;margin:0;">&#128274; Stored in browser session only. Never sent to our servers.</p>
        </div>
      </div>
    </div>
  </div>

  <script>
    var robots={
      alex:{avatar:"&#129302;",name:"ALEX",color:"#34d399",greet:"Hey Techie! I need your GCP details to get started!",missing:"Oops! Some fields are empty. Fill them all in!",missingKey:"Hey! Do not forget your Gemini API Key!",missingProject:"Which GCP project should I scan? Add the Project ID!",testing:"Hold tight! Testing your connection...",success:"All systems GO! Ready to scan your cloud!",fail:"Something is not right. Double-check your credentials!"},
      aria:{avatar:"&#128105;&#8205;&#128187;",name:"ARIA",color:"#f472b6",greet:"Hi there! Fill in your details and I will find cloud waste!",missing:"Hey friend! Some fields are still empty. Complete them!",missingKey:"Your Gemini API Key is missing - I cannot work without it!",missingProject:"I need your GCP Project ID to get started!",testing:"Checking your connection... fingers crossed!",success:"Woohoo! Connected! Let us find that cloud waste together!",fail:"The connection did not work. Check your credentials again!"},
      max:{avatar:"&#128104;&#8205;&#128187;",name:"MAX",color:"#60a5fa",greet:"Yo! Let us GO! Drop your GCP credentials and let us roll!",missing:"Bro! You are missing some fields! Fill them ALL in!",missingKey:"API Key is MISSING! Cannot run without fuel!",missingProject:"Project ID needed! Which GCP are we scanning?",testing:"TESTING CONNECTION... come on come on!",success:"BOOM! Connected and READY TO ROLL! Let us go!",fail:"Connection FAILED! Check credentials and retry!"},
      nova:{avatar:"&#129504;",name:"NOVA",color:"#a78bfa",greet:"Greetings Engineer. Awaiting credentials to begin analysis.",missing:"Incomplete parameters. All fields are required.",missingKey:"Gemini API token not found. Credentials required.",missingProject:"Target project identifier missing. Specify Project ID.",testing:"Establishing secure connection to Google Cloud...",success:"Authentication verified. Cloud scanning ready.",fail:"Connection anomaly detected. Verify credentials."},
      eco:{avatar:"&#127807;",name:"ECO",color:"#34d399",greet:"Hey! Let us save the planet! Fill in your GCP details!",missing:"Almost there! A few fields need your attention!",missingKey:"Your API Key is missing - need it for carbon calculations!",missingProject:"Which GCP project should I analyze for carbon waste?",testing:"Connecting... checking emissions data...",success:"Connected! Ready to calculate carbon footprint!",fail:"Connection failed. Let us fix credentials and try again!"}
    };
    var currentRobot="alex";

    function openSettings(){
      document.getElementById("gcp-settings-overlay").style.display="block";
      loadSettings();
      setTimeout(function(){robotSay("greet");},400);
    }
    function closeSettings(){document.getElementById("gcp-settings-overlay").style.display="none";}
    function selectRobot(id){
      currentRobot=id;
      ["alex","aria","max","nova","eco"].forEach(function(r){
        var b=document.getElementById("robot-"+r);
        b.style.background="#1a1a2e";b.style.borderColor="#30363d";b.style.color="#8b949e";
      });
      var rb=document.getElementById("robot-"+id);
      var r=robots[id];
      rb.style.background="#1a2a1a";rb.style.borderColor=r.color;rb.style.color=r.color;
      document.getElementById("robot-avatar").innerHTML=r.avatar;
      document.getElementById("robot-name").style.color=r.color;
      document.getElementById("robot-name").textContent=r.name;
      robotSay("greet");
      sessionStorage.setItem("gops-robot",id);
    }
    function robotSay(type,custom){
      var r=robots[currentRobot];
      var msg=custom||r[type]||r.greet;
      var el=document.getElementById("robot-message");
      el.style.opacity="0";
      setTimeout(function(){el.textContent=msg;el.style.opacity="1";el.style.transition="opacity 0.3s";},400);
      var av=document.getElementById("robot-avatar");
      if(type==="missing"||type==="missingKey"||type==="missingProject"||type==="fail"){
        av.style.animation="robotShake 0.5s ease";
        setTimeout(function(){av.style.animation="robotBob 2s ease-in-out infinite";},600);
      } else if(type==="success"){
        av.style.animation="robotSpin 0.6s ease";
        setTimeout(function(){av.style.animation="robotBob 2s ease-in-out infinite";},700);
      } else {
        av.style.animation="robotBob 2s ease-in-out infinite";
      }
    }
    function onFieldInput(){
      var key=document.getElementById("cfg-api-key").value;
      var proj=document.getElementById("cfg-project-id").value;
      if(!key&&!proj){robotSay("greet");return;}
      if(!key){robotSay("missingKey");return;}
      if(!proj){robotSay("missingProject");return;}
      robotSay(null,"Looking good! Hit Test or Save when ready!");
    }
    function saveCfg(){
      var key=document.getElementById("cfg-api-key").value.trim();
      var proj=document.getElementById("cfg-project-id").value.trim();
      var region=document.getElementById("cfg-region").value;
      var zone=document.getElementById("cfg-zone").value.trim()||region+"-a";
      if(!key||!proj){
        robotSay("missing");
        if(!key)document.getElementById("cfg-api-key").style.borderColor="#f85149";
        if(!proj)document.getElementById("cfg-project-id").style.borderColor="#f85149";
        return;
      }
      sessionStorage.setItem("gops-cfg",JSON.stringify({apiKey:key,projectId:proj,region:region,zone:zone}));
      robotSay("success");
      setTimeout(function(){closeSettings();},1800);
    }
    function testCfgConn(){
      var key=document.getElementById("cfg-api-key").value.trim();
      var proj=document.getElementById("cfg-project-id").value.trim();
      if(!key||!proj){
        robotSay("missing");
        if(!key)document.getElementById("cfg-api-key").style.borderColor="#f85149";
        if(!proj)document.getElementById("cfg-project-id").style.borderColor="#f85149";
        return;
      }
      robotSay("testing");
      fetch("/test-connection",{method:"POST",headers:{"Content-Type":"application/json"},
        body:JSON.stringify({apiKey:key,projectId:proj,region:document.getElementById("cfg-region").value,zone:document.getElementById("cfg-zone").value})
      }).then(function(r){return r.json();}).then(function(d){
        robotSay(d.success?"success":"fail");
      }).catch(function(){robotSay("fail");});
    }
    function togglePwd(){var i=document.getElementById("cfg-api-key");i.type=i.type==="password"?"text":"password";}
    function loadSettings(){
      try{
        var s=JSON.parse(sessionStorage.getItem("gops-cfg")||"{}");
        if(s.apiKey)document.getElementById("cfg-api-key").value=s.apiKey;
        if(s.projectId)document.getElementById("cfg-project-id").value=s.projectId;
        if(s.region)document.getElementById("cfg-region").value=s.region;
        if(s.zone)document.getElementById("cfg-zone").value=s.zone;
        selectRobot(sessionStorage.getItem("gops-robot")||"alex");
      }catch(e){}
    }
    function getSettings(){try{return JSON.parse(sessionStorage.getItem("gops-cfg")||"{}");}catch(e){return{};}}
    function saveSettings(){saveCfg();}
    function toggleSettings(){openSettings();}
    function testConn(){testCfgConn();}
    window.addEventListener("load",loadSettings);
  </script>
"""

content = content.replace("</body>", robot_panel + "\n</body>", 1)
print("OK - Robot panel added!")

# 4. Add FastAPI test-connection endpoint
endpoint = """

@app.post("/test-connection")
async def test_connection(request: Request):
    try:
        body = await request.json()
        api_key = body.get("apiKey", "")
        project_id = body.get("projectId", "")
        if not api_key or not project_id:
            return JSONResponse({"success": False, "error": "Missing API key or Project ID"})
        import google.generativeai as genai
        genai.configure(api_key=api_key)
        model = genai.GenerativeModel("gemini-2.5-pro")
        model.generate_content("ping", generation_config={"max_output_tokens": 5})
        return JSONResponse({"success": True})
    except Exception as e:
        err = str(e)
        if "API_KEY_INVALID" in err or "invalid" in err.lower():
            return JSONResponse({"success": False, "error": "Invalid Gemini API key"})
        return JSONResponse({"success": False, "error": err[:100]})
"""

content = content + endpoint
print("OK - FastAPI endpoint added!")

# 5. Fix imports - add Request if missing
if "from fastapi import" in content and "Request" not in content.split("from fastapi import")[1].split("\n")[0]:
    content = content.replace("from fastapi import FastAPI,", "from fastapi import FastAPI, Request,")
    content = content.replace("from fastapi import FastAPI", "from fastapi import FastAPI, Request")
    print("OK - Request import added!")

with open("app.py", "w", encoding="utf-8") as f:
    f.write(content)
print("ALL DONE - app.py saved cleanly!")