# The server on this machine. Every target is one step of running or updating it.
#
#   make test        run the tests (the standard library only: no venv)
#   make run         serve over HTTP in the foreground, against $NOMAD_ADDR
#   make image       build the arm64 image and push it to the registry
#   make deploy      push the image, then roll the Nomad job onto it

PORT      ?= 8111
PYTHON    ?= python3

IMAGE     ?= ghcr.io/ckopsa/nomad-mcp
IMAGE_TAG ?= $(shell git rev-parse --short HEAD)$(shell git diff --quiet HEAD 2>/dev/null || echo -dirty)
PLATFORM  ?= linux/arm64

.PHONY: test run image deploy

test:
	$(PYTHON) -m unittest -v

# NOMAD_ADDR and NOMAD_TOKEN come from the environment, as for the nomad
# CLI. A read-only token is enough; see README.md for the policy.
run:
	$(PYTHON) -m nomadmcp --http $(PORT)

# The image, as CI builds it (.github/workflows/image.yml): the same
# tag from the same command, so a laptop push and a CI push name one
# image. A cross-build under qemu needs binfmt installed once per boot.
image:
	docker buildx build --platform $(PLATFORM) -t $(IMAGE):$(IMAGE_TAG) --push .
	@echo "pushed $(IMAGE):$(IMAGE_TAG)"

# The deploy is one Nomad variable; the job template reads it and
# restarts the task on the new tag. NOMAD_ADDR and NOMAD_TOKEN come
# from the environment, and this token needs to write the variable -
# unlike the server's own, which only reads.
deploy: image
	nomad var put -force nomad/jobs/nomad-mcp/deploy image_tag=$(IMAGE_TAG) >/dev/null
	@echo "deploying $(IMAGE):$(IMAGE_TAG)"
