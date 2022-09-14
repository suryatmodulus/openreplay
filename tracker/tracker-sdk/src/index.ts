
interface VitalOptions {
  projectKey: string,
  [x: string]: any, // how to make not necessary
}

export default class OpenReplaySdk {
  constructor(private readonly options: VitalOptions, private readonly sdkServer: string) {}

  loadTracker(doc: Document) {
    return new Promise((resolve, reject) => {

      let tracker;
      const scriptElement = doc.createElement("script")
      scriptElement.setAttribute("src", this.sdkServer)
      scriptElement.setAttribute("type", "text/javascript");
      scriptElement.setAttribute("async", 'async');

      scriptElement.addEventListener('load', () => {
        // @ts-ignore
        const tracker = new window.__OPENREPLAY_API__
        // create tracker instance

        resolve(tracker)
      })

      scriptElement.addEventListener('error', () => {
        reject('OpenReplay: Failed to inject tracker instance')
      })

      doc.head.appendChild(scriptElement);
  })
  }
}
