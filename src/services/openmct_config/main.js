openmct.setAssetPath('/openmct');
openmct.install(openmct.plugins.LocalStorage());
openmct.install(openmct.plugins.MyItems());
openmct.install(openmct.plugins.UTCTimeSystem());
openmct.install(openmct.plugins.Conductor({
  menuOptions: [
    {
      name: "Real Time",
      timeSystem: "utc",
      clock: "local",
      clockOffsets: { start: -15 * 60 * 1000, end: 0 }
    },
    {
      name: "Fixed",
      timeSystem: "utc",
      bounds: {
        start: Date.now() - 15 * 60 * 1000,
        end: Date.now()
      }
    }
  ]
}));
// Light mode theme for openmct
// openmct.install(openmct.plugins.Snow());
// Dark mode themes for openmct
// openmct.install(openmct.plugins.Espresso());
openmct.install(openmct.plugins.DarkmatterTheme());

openmct.install(GdsPlugin());

document.addEventListener('DOMContentLoaded', () => {
  openmct.start();
});