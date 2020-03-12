.PHONY: all
all: aio starter worker

.PHONY: build-aio
build-aio:
	docker build --target all-in-one -t "gcr.io/zeebe-io/all-in-one:zeebe" .

.PHONY: push-aio
push-aio:
	docker push "gcr.io/zeebe-io/all-in-one:zeebe"

.PHONY: aio
aio: build-aio push-aio

.PHONY: build-starter
build-starter:
	docker build --target starter -t gcr.io/zeebe-io/starter:zell-rocks .

.PHONY: push-starter
push-starter:
	docker push gcr.io/zeebe-io/starter:zell-rocks

.PHONY: starter
starter: build-starter push-starter

.PHONY: build-worker
build-worker:
	docker build --target worker -t "gcr.io/zeebe-io/worker:zeebe" .

.PHONY: push-worker
push-worker:
	docker push "gcr.io/zeebe-io/worker:zeebe"

.PHONY: worker
worker: build-worker push-worker
