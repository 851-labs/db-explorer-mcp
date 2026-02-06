const result = await Bun.build({
  entrypoints: ['ui/chart-app.ts'],
  target: 'browser',
  minify: true,
});

const html = await Bun.file('chart-app.html').text();
const js = await result.outputs[0].text();

await Bun.write(
  'dist/chart-app.html',
  html.replace(
    '<script type="module" src="/ui/chart-app.ts"></script>',
    `<script type="module">${js}</script>`,
  ),
);
