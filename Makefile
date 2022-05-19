tests:
	@echo "This will fail if you have not installed dev-local manually. See deps.edn"
	clojure -A:dev:test:clj-tests:cloud:provided -J-Dguardrails.config=guardrails-test.edn -J-Dguardrails.enabled

dev:
	clojure -A:dev:test:clj-tests:provided -J-Dguardrails.config=guardrails-test.edn -J-Dguardrails.enabled --watch --fail-fast --no-capture-output
