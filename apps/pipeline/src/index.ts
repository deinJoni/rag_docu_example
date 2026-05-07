const layers = ["bronze", "silver", "gold"] as const;
type Layer = (typeof layers)[number];

function isLayer(v: string | undefined): v is Layer {
  return !!v && (layers as readonly string[]).includes(v);
}

async function main() {
  const cmd = process.argv[2];
  if (!isLayer(cmd)) {
    console.error(`Usage: pipeline <${layers.join("|")}>`);
    process.exit(1);
  }
  const mod = await import(`./${cmd}/index.js`);
  await mod.run();
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
