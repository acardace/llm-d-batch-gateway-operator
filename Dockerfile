FROM quay.io/projectquay/golang:1.26 AS builder
ARG TARGETOS=linux
ARG TARGETARCH

WORKDIR /workspace

COPY go.mod go.sum ./
RUN go mod download

COPY cmd/ cmd/
COPY api/ api/
COPY internal/ internal/

RUN CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} \
    go build -a -o bin/manager ./cmd/

FROM gcr.io/distroless/static:nonroot
WORKDIR /
COPY --from=builder /workspace/bin/manager /manager
COPY batch-gateway/charts/batch-gateway/ /charts/batch-gateway/
COPY llm-d-async/charts/llm-d-async/ /charts/llm-d-async/
USER 65532:65532
ENTRYPOINT ["/manager"]
