const result = await Bun.build({
  entrypoints: ['ui/chart-app.ts'],
  target: 'browser',
  minify: true,
});

const html = await Bun.file('chart-app.html').text();
let js = await result.outputs[0].text();

// Escape </script> in the bundled JS to prevent the browser's HTML parser
// from prematurely closing the inline <script> tag.
js = js.replaceAll('</script>', '<' + String.fromCharCode(92) + '/script>');

// Use a function replacement to avoid $& and $` patterns in the JS being
// interpreted as special substitution sequences by String.prototype.replace.
await Bun.write(
  'dist/chart-app.html',
  html.replace(
    '<script type="module" src="/ui/chart-app.ts"></script>',
    () => `<script type="module">${js}</script>`,
  ),
);
