(function(){
  const GDS_API_BASE = 'http://localhost:8080';
  const NS = "gds.telemetry";

  function plugin(){
    return function install(openmct){

      openmct.objects.addRoot({ namespace: NS, key: "TERALINK-1" });

      openmct.objects.addProvider(NS, {
        get(identifier){
          if(identifier.key==="TERALINK-1")
            return Promise.resolve({ identifier, name:"TERALINK-1", type:"folder", location:"ROOT" });
          return Promise.resolve({
            identifier, name: identifier.key, type:"gds.telemetry",
            location:`${NS}:TERALINK-1`,
            telemetry:{values:[
              {key:"value", name:"Value", hints:{range:1}},
              {key:"timestamp", name:"Time", hints:{domain:1}}
            ]}
          });
        },
        listChildren(id){
          if(id.key==="TERALINK-1")
            return Promise.resolve([{namespace:NS,key:"PWR_BUS_V"}]);
          return Promise.resolve([]);
        }
      });

      openmct.types.addType("gds.telemetry",{name:"Telemetry Point",creatable:false});

      openmct.telemetry.addProvider({
        supportsRequest:o=>o.type==="gds.telemetry",
        request(obj){
          return fetch(`${GDS_API_BASE}/telemetry/history/${obj.identifier.key}`)
            .then(r=>r.json());
        },
        supportsSubscribe:o=>o.type==="gds.telemetry",
        subscribe(obj,cb){
          if(!window.rt){
            window.rt = new WebSocket(GDS_API_BASE.replace("http","ws")+"/realtime");
            window.rt.onmessage=e=>cb(JSON.parse(e.data));
          }
          return ()=>{};
        }
      });
    };
  }

  window.GdsPlugin = plugin;
})();
