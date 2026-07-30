# Quiescent

## Version numbers

The version is tracked in four places. `build.clj` is the source of truth; the other three must be updated to match
whenever it changes:

| File                                           | Form                                           |
|------------------------------------------------|------------------------------------------------|
| `build.clj`                                    | `(def version "0.6.1")` — source of truth      |
| `README.md`                                    | `co.multiply/quiescent {:mvn/version "0.6.1"}` |
| `plugins/quiescent/.claude-plugin/plugin.json` | `"version": "0.6.1"`                           |
| `plugins/quiescent/skills/quiescent/SKILL.md`  | `co.multiply/quiescent {:mvn/version "0.6.1"}` |

`bb version:check` verifies the latter three against `build.clj` and exits non-zero if any is stale. It runs as a
dependency of `bb deploy`, so a mismatch blocks a release rather than shipping one. Run it after any bump.

A bump also wants a `CHANGELOG.md` entry — newest first, `## <version> - <YYYY-MM-DD>`.

The companion skill `plugins/quiescent/skills/quiescent-cljs/SKILL.md` carries no version coord and is deliberately
absent from the check.
