# Working on signalr_native

Flutter plugin wrapping the official SignalR clients: Java on Android, Swift on iOS. Sagar owns the project and every
merge. These rules apply to every agent (coding threads, the Mac build runner, the GitHub reviewer) and to human
contributors.

Read [`docs/CONTEXT.md`](docs/CONTEXT.md) before changing code: it holds the architecture, the decisions (D1, D2, …),
the server contract, and the client facts already verified. Build and test commands are in the README's Development
section.

## Show, then tell

Every change, proposal, plan and report leads with a visual. Text only explains what the visual cannot.

- Flow, lifecycle, threading or reconnect: a Mermaid `sequenceDiagram` or `stateDiagram-v2`, before and after.
- Structure or public API: a before/after diagram, or a before/after code block of the API surface.
- Parity, options, numbers: a table.

GitHub renders Mermaid in PRs, issues and Markdown files, so diagrams go inline.

## Decision rights

| Who decides | What |
|---|---|
| The agent, noting it in the PR | Implementation inside the native or Dart layers, tests, behaviour-preserving refactors, docs, CI fixes |
| Sagar, from the agent's proposal | Public Dart API; the server contract (CONTEXT §3); adding or changing a D-decision; Android and iOS behaving differently; new dependencies or version bumps; performance trade-offs; scope, naming, licensing, release timing |
| Sagar only | Merging, pushing to `main`, force-pushing a shared branch, repo settings and secrets, publishing to pub.dev |

A **proposal** is a visual, two or three options with their trade-offs, and one recommendation. The agent carries on
with work the decision does not block, and records the answer as the next D-number in CONTEXT §2 with its reason.

## The loop

Each step ends on its completion criterion. A step is done when its criterion holds, not before.

1. **Understand.** Read the request, CONTEXT.md and the code it touches. Check every assumption about client behaviour
   against the client's own source or official docs; an API that cannot be found there does not exist.
   _Done when_ you can state the goal in one sentence and name each platform and file it touches.
2. **Check for overlap.** List open PRs and remote branches.
   _Done when_ no other branch covers this work, or you are building on the one that does.
3. **Plan.** Goal, approach (as a diagram), risks, test plan. Post it for Sagar's approval when the change touches
   connection lifecycle, reconnect, threading or the public API, or runs past about 200 lines; otherwise it opens the
   PR description.
   _Done when_ Sagar approved it, or it is in the PR.
4. **Implement** on a feature branch, one concern per PR. Commit granularity is your call. Each behaviour change lands
   on Android and iOS together (**parity**), with tests on each covering the failure and race cases as well as the
   happy path. A gap that cannot close in this PR is written up in CONTEXT.md.
   _Done when_ the code and its tests exist on every platform the change applies to.
5. **Verify.** Run `bash tool/ci.sh dart`, `android` and `ios` for each layer touched (iOS needs macOS: the Mac runner
   or CI). Changes to the **hot path** (data channel, codecs, frame layout, reconnect) carry before/after latency or
   throughput numbers from the example bench app. Once the M4 test hub exists, lifecycle and reconnect changes also
   pass against it and on a real device.
   _Done when_ every check is green and the output or numbers are in hand as **proof**.
6. **Self-review, adversarially.** Read your diff as a reviewer hunting for wrong assumptions about the clients, races,
   thread hops, leaks, parity gaps and unhandled failures.
   _Done when_ each assumption traces to client source, a test, or a measured number.
7. **Independent review.** Open the PR (template in `.github/pull_request_template.md`). CI and the Claude review run
   on it. Fix each finding, or answer its thread with why not.
   _Done when_ CI is green and every review thread is fixed or answered.
8. **Record.** Update CONTEXT.md: status, new D-numbers, known gaps, verified client facts. Update the README when
   public behaviour changed.
   _Done when_ a fresh agent could pick up the work from the repo alone.
9. **Report** to Sagar: the visual, what changed, the proof, what is still open or risky, and what Sagar needs to
   decide. A claim without proof is reported as unverified.
10. **Sagar merges**, squashing the PR.

## Several agents at once

- One owner per branch. Other agents verify and comment; they edit only when the owner or Sagar asks.
- Verifier and reviewer agents report findings with evidence: the command run, its output, the source line.
