// The cartesian-only Plotly bundle ships no typings. It is only ever handed to
// react-plotly.js's factory, which accepts it as an opaque value.
declare module 'plotly.js-cartesian-dist-min' {
  const plotly: { default?: unknown; [key: string]: unknown }
  export default plotly
}
