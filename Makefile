tests:
	@echo "This will fail if you have not installed Cloud dev-local and on-prem creds in your :mvn/repos and maven settings.xml manually. See Datomic Documentation."
	clojure -A:test -J-Dguardrails.config=guardrails-test.edn -J-Dguardrails.enabled

dev:
	clojure -A:dev:test:clj-tests:provided -J-Dguardrails.config=guardrails-test.edn -J-Dguardrails.enabled --watch --fail-fast --no-capture-output
