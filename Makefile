line_length = 95
package = doltcli

.PHONY: fmt
fmt: ## Format code with black and isort
				black . --check -t py37 --line-length=${line_length} || ( black . -t py37 --line-length=${line_length} && false )
				isort .

.PHONY: lint
lint: ## Run linters
				mypy ${package}
				flake8 ${package}  \
					--max-line-length=${line_length} \
					--ignore=F401,E501

.PHONY: lint
test: ## Run tests
				pytest tests --cov=${package} --cov-report=term --cov-report xml
