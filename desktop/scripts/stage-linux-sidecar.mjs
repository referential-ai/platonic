import { execFileSync } from 'node:child_process';
import { copyFileSync, mkdirSync } from 'node:fs';
import { dirname, join, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';

if (process.platform !== 'linux') {
	throw new Error('the Linux sidecar must be staged on Linux');
}

const desktop = resolve(dirname(fileURLToPath(import.meta.url)), '..');
const root = resolve(desktop, '..');
const target = 'x86_64-unknown-linux-gnu';
const cargoTarget = process.env.CARGO_TARGET_DIR
	? resolve(root, process.env.CARGO_TARGET_DIR)
	: join(root, 'target');

execFileSync(
	'cargo',
	['build', '--locked', '--release', '--target', target, '--bin', 'platonic'],
	{ cwd: root, stdio: 'inherit' }
);

const source = join(cargoTarget, target, 'release', 'platonic');
const destination = join(
	desktop,
	'src-tauri',
	'binaries',
	`platonic-${target}`
);
mkdirSync(dirname(destination), { recursive: true });
copyFileSync(source, destination);
