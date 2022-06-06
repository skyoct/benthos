FROM golang:1.16-alpine as builder

LABEL maintainer="zhangshaoqian <shaoqian.zhang@appshahe.com>"

WORKDIR /build

ENV GOPROXY https://goproxy.cn,direct

RUN sed -i 's/dl-cdn.alpinelinux.org/mirrors.aliyun.com/g' /etc/apk/repositories && \
  apk add --no-cache upx ca-certificates tzdata

COPY . .

RUN go mod download

#go构建可执行文件
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -ldflags "-s -w"  -o ./app ./cmd/benthos/main.go


FROM alpine as runner

COPY --from=builder /build/app /app

WORKDIR /

RUN apk add tzdata && cp /usr/share/zoneinfo/Asia/Shanghai /etc/localtime \
    && echo "Asia/Shanghai" > /etc/timezone \
    && apk del tzdata

RUN chmod a+x app
EXPOSE 4195

ENTRYPOINT ["/app"]

CMD ["-c", "/config.yaml"]
