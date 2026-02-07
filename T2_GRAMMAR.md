# Phase B Grammar

Phase B accepts all Phase A canonical forms, plus additional sugar and macros. This document merges
the Phase A canonical grammar with Phase B surface syntax and notes which canonical forms are
discouraged in Phase B source.

## Preferred bindings

Phase B accepts both parallel bindings (`let` / `const`) and sequential bindings (`let*` / `const*`).
Prefer `let*` and `const*` because they lower more directly to TypeScript and avoid the temporary
bindings introduced by parallel binding sugar.

## Canonical Phase A grammar (accepted in Phase B)

Tokens:
```
<identifier> ::= <identifier-char> <identifier-char>*
<identifier-char> ::= /* any character except whitespace, "(" or ")" */
<number>     ::= /[0-9]+(\.[0-9]+)?/  ; integers and floats
<string>     ::= '"' <chars> '"' | "'" <chars> "'"
<boolean>    ::= "true" | "false"
<null>       ::= "null"
<undefined>  ::= "undefined"
<literal>    ::= <number> | <string> | <boolean> | <null> | <undefined>
```

### Program

All code in a compilation unit (e.g. a single file) must be enclosed in a top-level program expression.

```
<program> ::= "(" "program" <statement>* ")"
```

### Statements
```
<statement> ::= <block>
							| <if>
							| <scope-loop>
							| <for-classic>
							| <for-of>
							| <for-await>
							| <return>
							| <break>
							| <continue>
							| <switch>
							| <let-star>
							| <const-star>
							| <assign>
							| <expression>
							| <fn>
							| <lambda>
							| <method>
							| <getter>
							| <setter>
							| <class>
							| <import>
							| <export>
							| <enum>
							| <namespace>
							| <type-alias>
							| <type-interface>
							| <index-signature>
							| <static-block>

<binding> ::= "(" <binding-target> <expression>? ")"
<param> ::= "(" <param-modifier>* <identifier> <type>? ("default" <expression>)? ")"
					| "(" "this" <type> ")"
<param-modifier> ::= "public" | "protected" | "private" | "readonly"
<fn-signature> ::= "(" <param> (","? <param>)* ")" <type>?

<init> ::= <expression>
<condition> ::= <expression>
<update> ::= <expression>
<class-body> ::= "(" "class-body" <statement>* ")"
<import-spec> ::= "(" "import-spec" <expression>* ")"
<export-spec> ::= "(" "export-spec" <expression>* ")"
<interface-body> ::= "(" "interface-body" <expression>* ")"
<index-signature> ::= "(" "index-signature" "(" <identifier> <type> ")" <type> ("readonly")? ")"

<block> ::= "(" "block" <statement>* ")"
<static-block> ::= "(" "static-block" <statement>* ")"
<if> ::= "(" "if" <expression> <statement> (<statement>)? ")"
<scope-loop> ::= "(" "while" <expression> <statement> ")"
<for-classic> ::= "(" "for" "classic" <init>? <condition>? <update>? <statement> ")"
<for-of> ::= "(" "for" "of" "(" <binding> <expression> ")" <statement> ")"
<for-await> ::= "(" "for" "await" "(" <binding> <expression> ")" <statement> ")"
<let-star> ::= "(" "let*" <binding>* <statement>* ")"
<const-star> ::= "(" "const*" <binding>* <statement>* ")"
<assign> ::= "(" "assign" <expression> <expression> ")"
<callable-flag> ::= "async" | "generator" | "abstract" | "overload"
<fn> ::= "(" "fn" <callable-flag>* <identifier>? <fn-signature> <statement>* ")"
<lambda> ::= "(" "lambda" <callable-flag>* <fn-signature> <statement>* ")"
<method> ::= "(" "method" <callable-flag>* <identifier> <fn-signature> <statement>* ")"
<getter> ::= "(" "getter" <callable-flag>* <identifier> <fn-signature> <statement>* ")"
<setter> ::= "(" "setter" <callable-flag>* <identifier> <fn-signature> <statement>* ")"
<class> ::= "(" "class" <identifier> <type-params>? <class-heritage>* <class-body> ")"
<class-heritage> ::= <class-extends>
									| <class-implements>
									| <class-abstract>
									| <class-decorators>
<class-extends> ::= "(" "extends" <expression> ")"
<class-implements> ::= "(" "implements" <expression>+ ")"
<class-abstract> ::= "(" "abstract" ")"
<class-decorators> ::= "(" "decorators" <expression>+ ")"
<import> ::= "(" "import" <import-spec> ")"
<export> ::= "(" "export" <export-spec> ")"
<enum> ::= "(" "enum" <identifier> <enum-body> ")"
<enum-body> ::= "(" "enum-body" <enum-member>* ")"
<enum-member> ::= "(" <string> <expression>? ")"
<namespace> ::= "(" "namespace" <identifier> <namespace-body> ")"
<namespace-body> ::= "(" "namespace-body" <statement>* ")"
<type-alias> ::= "(" "type-alias" <identifier> <type-params>? <type> ")"
<type-interface> ::= "(" "type-interface" <identifier> <interface-body> ")"
<switch> ::= "(" "switch" <expression> <case>* <default>? ")"
<case> ::= "(" "case" <expression> <statement>* ")"
<default> ::= "(" "default" <statement>* ")"
<return> ::= "(" "return" <expression>? ")"
<break> ::= "(" "break" <identifier>? ")"
<continue> ::= "(" "continue" <identifier>? ")"

<binding-target> ::= <identifier>
										 | <array-pattern>
										 | <object-pattern>
										 | <rest-target>
										 | <default-target>

<array-pattern> ::= "(" "array-pattern" <binding-target>* <rest-target>? ")"
<object-pattern> ::= "(" "object-pattern" <object-pattern-field>* <rest-target>? ")"
<object-pattern-field> ::= "(" <string> <binding-target> ")"
<rest-target> ::= "(" "rest" <binding-target> ")"
<default-target> ::= "(" "default" <binding-target> <expression> ")"
```

### Expressions
```
<expression> ::= <literal>
							 | <identifier>
							 | <call>
							 | <call-with-this>
							 | <prop>
							 | <index>
							 | <new>
							 | <throw>
							 | <try>
							 | <fn>
							 | <class>
							 | <type-assert>
							 | <type-object>
							 | <type-function>
							 | <type-literal>
							 | <template>
							 | <non-null>
							 | <type-union>
							 | <type-intersection>
							 | <type-ref>
							 | <array>
							 | <object>
							 | <spread>
							 | <await>
							 | <ternary>

<call> ::= "(" "call" <expression> <expression>* ")"
<call-with-this> ::= "(" "call-with-this" <expression> <expression> <expression>* ")"
<prop> ::= "(" "prop" <expression> <string> ")"
<index> ::= "(" "index" <expression> <expression> ")"
<new> ::= "(" "new" <expression> <expression>* ")"
<throw> ::= "(" "throw" <expression> ")"
<try> ::= "(" "try" <statement> <catch>? <finally>? ")"
<catch> ::= "(" "catch" <binding>? <statement>* ")"
<finally> ::= "(" "finally" <statement>* ")"
<array> ::= "(" "array" <expression>* ")"
<object> ::= "(" "object" <object-field>* ")"
<object-field> ::= "(" <string> <expression> ")"
							 | "(" "spread" "object" <expression> ")"
<spread> ::= "(" "spread" <spread-kind> <expression> ")"
<spread-kind> ::= "array" | "object" | "rest"
<await> ::= "(" "await" <expression> ")"
<ternary> ::= "(" "ternary" <expression> <expression> <expression> ")"
<template> ::= "(" "template" <expression>* ")"
<non-null> ::= "(" "non-null" <expression> ")"
```

### Type expressions
```
<type> ::= <type-primitive>
				 | <type-ref>
				 | <type-function>
				 | <type-object>
				 | <type-union>
				 | <type-intersection>
				 | <type-this>
				 | <type-mapped>
				 | <type-template>
				 | <type-literal>

<type-primitive> ::= "type-string" | "type-number" | "type-boolean" | "type-null" | "type-undefined"

; Phase B type sugar also emits the `t:`-prefixed forms:
; (t:primitive "number"), (t:ref "Foo"), (t:union ...), etc.
<type-ref> ::= "(" "type-ref" <identifier> <type-args>? ")"
<type-function> ::= "(" "type-function" <type-params>? <type>* <type> ")"
<type-object> ::= "(" "type-object" <type-field>* ")"
<type-field> ::= "(" <string> <type> ")"
<type-union> ::= "(" "type-union" <type>+ ")"
<type-intersection> ::= "(" "type-intersection" <type>+ ")"
<type-literal> ::= "(" "type-literal" <literal>* ")"
<type-this> ::= "(" "type-this" ")"
<type-template> ::= "(" "type-template" (<string> | <type>)* ")"
<type-mapped> ::= "(" "type-mapped" <type-param> <value-type> <name-remap>? <readonly-modifier>? <optional-modifier>? <via>? ")"
<value-type> ::= <type>
<name-remap> ::= <type>
<readonly-modifier> ::= "readonly" | "-readonly"
<optional-modifier> ::= "optional" | "-optional"
<via> ::= <type>

<type-params> ::= "(" "typeparams" <type-param>* ")"
<type-param> ::= "(" <identifier> <variance>? <type>? <type>? ")"
<variance> ::= "in" | "out"

<type-app> ::= "(" "type-app" <expression> <type>* ")"
```

### Canonical forms to avoid in Phase B source

- `(method ...)` uses a string name in the canonical grammar. Prefer surface `obj.method` and
	method forms that use identifier names rather than string literals.
- `(call ...)`, `(prop ...)`, `(index ...)` are canonical but generally unnecessary in Phase B
	source because dot/computed access sugar exists.

## Phase B sugar and macro surface forms

### Equality operator sugar

| Operator | Meaning | JS Equivalent |
|----------|---------|----------------|
| `:=` | Assignment | `=` |
| `==` | Loose equality (coercive) | `==` |
| `!=` | Loose inequality (coercive) | `!=` |
| `===` | Strict equality | `===` |
| `!==` | Strict inequality | `!==` |
| `?=` | Deep structural equality | `lodash.isEqual` |
| `?!=` | Deep structural inequality | `!lodash.isEqual` |

### Reader macros

```
'x        ; (quote x)
`x        ; (quasiquote x)
~x        ; (unquote x)
~@x       ; (unquote-splicing x)
:(...)    ; (infix (...))
```

Custom reader macros (per file):

```
(defreadermacro "#" quote)
#(foo bar)  ; => (quote (foo bar))
```

### Infix

```
(infix (<expr> <op> <expr> ...))
:(<expr> <op> <expr> ...)
```

Rewrites to nested `(call <op> ...)` following JavaScript precedence.

### Logical operators

```
(&& a b)
(|| a b)
(! a)

(and a b)
(or a b)
(not a)
```

Both are supported; prefer `&&`/`||` when matching TypeScript familiarity, and use aliases when
favoring Lisp-style readability.

### Assignment aliases

```
(:= target value)  ; (assign target value)
(set! target value)
```

### Parallel bindings

```
(let ((a b) (b a)) body...)
(const ((a b) (b a)) body...)
```

Rewrites to `let*`/`const*` with temporary bindings to preserve semantics.

### Dotted access and calls

```
obj.prop
(obj.method arg)
```

Rewrites to `(prop obj prop)` and `(call (prop obj method) arg)`.

### Computed access

```
obj.[key]
```

Rewrites to `(index obj key)`.

### Optional chaining

```
obj?.prop
obj?.[key]
(obj?.method arg)
```

Rewrites to guarded `let*` + `if` expressions and `call-with-this` when needed.

### Object and array literals

```
[a b c]             ; (array a b c)
{k v}               ; (object ("k" v))
{k}                 ; punning
{...obj k v}        ; spreads
{k? v}              ; optional keys
```

### Type annotation sugar

```
(fn (x: number, y: string) : boolean ...)
```

Rewrites to structured `type-*` nodes.

### Template interpolation sugar

```
(template-with "Hello ${name}" (name "Ada"))
```

Rewrites to `(template ...)` plus an IIFE.
