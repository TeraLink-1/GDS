const openmct = window.openmct;
openmct.install(openmct.plugins.Browse());
openmct.install(openmct.plugins.Plot());
openmct.install(openmct.plugins.Table());
openmct.install(GdsPlugin());
openmct.start();
