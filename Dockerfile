FROM quay.io/projectquay/golang:1.26 AS builder
ARG TARGETOS=linux
ARG TARGETARCH
ARG VERSION=dev
ARG ASYNC_PROCESSOR_IMG=ghcr.io/llm-d-incubation/llm-d-async:v0.7.0-RC3

WORKDIR /workspace

COPY go.mod go.sum ./
RUN go mod download

COPY cmd/ cmd/
COPY api/ api/
COPY internal/ internal/

RUN CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} \
    go build -a \
    -ldflags "-X main.version=${VERSION} -X main.defaultAsyncProcessorImage=${ASYNC_PROCESSOR_IMG}" \
    -o bin/manager ./cmd/

FROM gcr.io/distroless/static:nonroot
WORKDIR /
COPY --from=builder /workspace/bin/manager /manager
COPY batch-gateway/charts/batch-gateway/ /charts/batch-gateway/
USER 65532:65532
ENTRYPOINT ["/manager"]
