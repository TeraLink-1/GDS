const express = require("express");
const path = require("path");

const app = express();
const port = process.env.PORT || 8081;

const dist = path.dirname(require.resolve("openmct/dist/openmct.js"));

app.use("/openmct", express.static(dist));
app.use("/", express.static(__dirname));

app.listen(port, () => console.log("OpenMCT running on", port));
