tests:
	clojure -A:test -J-Dguardrails.config=guardrails-test.edn -J-Dguardrails.enabled

dev:
	clojure -A:dev:test:clj-tests:provided -J-Dguardrails.config=guardrails-test.edn -J-Dguardrails.enabled --watch --fail-fast --no-capture-output
