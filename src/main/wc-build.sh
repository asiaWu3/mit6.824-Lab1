# 用于构建插件
CGO_ENABLED=1 go build -race  -buildmode=plugin ../mrapps/wc.go