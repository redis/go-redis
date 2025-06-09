test:
	@if [ -z "$(REDIS_VERSION)" ]; then \
		echo "REDIS_VERSION not set, running all tests"; \
		go test -v ./...; \
	else \
		MAJOR_VERSION=$$(echo "$(REDIS_VERSION)" | cut -d. -f1); \
		if [ "$$MAJOR_VERSION" -ge 8 ]; then \
			echo "REDIS_VERSION $(REDIS_VERSION) >= 8, running all tests"; \
			go test -v ./...; \
		else \
			echo "REDIS_VERSION $(REDIS_VERSION) < 8, skipping vector_sets tests"; \
			go test -v ./... -run '^(?!.*(?:vectorset|ExampleClient_vectorset)).*$$'; \
		fi; \
	fi

.PHONY: test