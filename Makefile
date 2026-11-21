IMAGE ?= shopping-sessions:latest
CPUS  ?= 8
MEM   ?= 24g
# The scorer's platform. PLATFORM=linux/arm64 builds and runs natively on an Apple-silicon Mac, for fast local loops.
PLATFORM ?= linux/amd64
DEV   ?= dev
FIXTURES ?= fixtures
ANSWER ?= dev-answer
export IMAGE CPUS MEM PLATFORM

.PHONY: build fixtures dev run-dev check-dev setup validate

setup:
	python3 -m venv .venv
	.venv/bin/pip install -r requirements-dev.txt

## Build the image for the scorer's platform, linux/amd64 (emulated on Apple silicon)
build:
	docker buildx build --platform $(PLATFORM) --load -t $(IMAGE) .

## Run the image on all 36 edge-case fixtures and diff against the expected tables
fixtures: build
	.venv/bin/python scripts/run_fixtures.py $(FIXTURES) -- scripts/docker_run.sh

## Build the published generator and make the 1M-event dev set
dev:
	cd generator && cargo build --release
	./generator/target/release/generate --seed 1 --events 1000000 --clicks 250000 --output $(DEV)

## Run on the dev set with scorer flags and time it
run-dev: build
	rm -rf out && time scripts/docker_run.sh $(DEV) out

## Run on the dev set, then compare with dev-answer/
check-dev:
	.venv/bin/python scripts/compare.py out $(ANSWER)

validate: fixtures dev run-dev check-dev
