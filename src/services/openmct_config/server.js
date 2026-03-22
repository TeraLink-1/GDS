const express = require("express");
const path = require("path");

const app = express();
const port = process.env.PORT || 8081;

// Point directly at the dist folder
const dist = path.join(__dirname, 'node_modules/openmct/dist');

app.use("/openmct", express.static(dist));
app.use("/", express.static(__dirname));

app.listen(port, () => console.log("OpenMCT running on", port));