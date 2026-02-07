This project is a t2lang project.
https://github.com/raould/t2lang
Code written for this project is in t2lang sexpr format.
.t2 code files must be (1) compiled to .ts using t2tc, (2) compiled to .js using tsc, (3) smoke-checked by running with node.

Read node_modules/t2lang/phaseB/GRAMMAR.md.
This is the specific sexpr syntax to be generating.
The semantics are that of typescript (and thus javascript).

Use clear, descriptive, hyphenated identifiers (e.g., compute-hash, update-state).
Avoid abbreviations unless canonical in Clojure tradition.
Ensure function names reflect transformations, not actions.
Prefer loops, avoid recursion when possible.

Prefer immutable data structures.
Return new values instead of mutating inputs.
Make data flow explicit in every function.
Write all functions as async even if they do not use await, in order to avoid large refactors later.

Document each function with a short docstring explaining:
- purpose
- inputs
- outputs
- invariants or assumptions

Use explicit error signaling forms.
Never rely on exceptions or hidden control flow.
Validate inputs at function boundaries.

Provide at least two example calls for each function.
Examples must be valid s-expressions.

Read all the .md files in order to learn specs, design, todo, etc.
