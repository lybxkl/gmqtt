package cli

import (
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"strconv"
	"sync"

	"golang.org/x/net/websocket"

	"github.com/lybxkl/gmqtt/broker/core/service/v1"
	"github.com/lybxkl/gmqtt/broker/gcfg"
	_ "github.com/lybxkl/gmqtt/broker/impl"
	. "github.com/lybxkl/gmqtt/common/log"
	"github.com/lybxkl/gmqtt/util"
)

var once sync.Once

func Start() {
	once.Do(func() {
		// 日志初始化
		NewGLog(gcfg.GetGCfg().Log.GetLevel())

		svr, err := brokerInitAndRun()
		util.MustPanic(err)

		svr.Wait()
	})
}

// broker 初始化
func brokerInitAndRun() (*service.Server, error) {
	cfg := gcfg.GetGCfg()
	var (
		mqttAddr    = cfg.Broker.MqttAddr
		wsAddr      = cfg.Broker.WsAddr
		wsPath      = cfg.Broker.WsPath
		wssAddr     = cfg.Broker.WssAddr
		wssKeyPath  = cfg.Broker.WssKeyPath
		wssCertPath = cfg.Broker.WssCertPath
	)

	svr := service.NewServer(mqttAddr)

	exitSignal(svr)
	pprof(cfg.PProf.Open, cfg.PProf.Port)

	// 启动 ws
	if err := wsRun(wsPath, wsAddr, wssAddr, mqttAddr, wssCertPath, wssKeyPath); err != nil {
		return nil, err
	}

	return svr, svr.ListenAndServe()
}

func wsRun(wsPath string, wsAddr string, wssAddr string, mqttAddr string, wssCertPath string, wssKeyPath string) error {
	if len(wsPath) > 0 && (len(wsAddr) > 0 || len(wssAddr) > 0) {
		err := AddWebsocketHandler(wsPath, mqttAddr) // 将wsAddr的ws连接数据发到 mqttAddr 上
		if err != nil {
			return err
		}
		/* start a plain websocket listener */
		if len(wsAddr) > 0 {
			go ListenAndServeWebsocket(wsAddr)
		}
		/* start a secure websocket listener */
		if len(wssAddr) > 0 && len(wssCertPath) > 0 && len(wssKeyPath) > 0 {
			go ListenAndServeWebsocketSecure(wssAddr, wssCertPath, wssKeyPath)
		}
	}
	return nil
}

// exitSignal 监听信号并关闭
func exitSignal(server *service.Server) {
	signChan := make(chan os.Signal, 1)
	//signal.Notify(sigchan, os.Interrupt, os.Kill)
	signal.Notify(signChan)
	go func() {
		defer func() {
			if err := recover(); err != nil {
				panic(err)
			}
		}()
		sig := <-signChan
		Log.Infof("服务停止：Existing due to trapped signal; %v", sig)

		err := server.Shutdown()
		if err != nil {
			Log.Errorf("server close err: %v", err)
		}
		os.Exit(0)
	}()
}

func pprof(open bool, port int) {
	// 性能分析
	if open {
		go func() {
			// https://pdf.us/2019/02/18/2772.html
			// go tool pprof -http=:8000 http://localhost:8080/debug/pprof/heap    查看内存使用
			// go tool pprof -http=:8000 http://localhost:8080/debug/pprof/profile 查看cpu占用
			// 注意，需要提前安装 Graphviz 用于画图
			Log.Info(http.ListenAndServe(":"+strconv.Itoa(port), nil).Error())
		}()
	}
}

// 转发websocket的数据到tcp处理中去
func AddWebsocketHandler(urlPattern string, uri string) error {
	Log.Infof("AddWebsocketHandler urlPattern=%s, uri=%s", urlPattern, uri)
	u, err := url.Parse(uri)
	if err != nil {
		Log.Errorf("simq/main: %v", err)
		return err
	}

	h := func(ws *websocket.Conn) {
		WebsocketTcpProxy(ws, u.Scheme, u.Host)
	}
	http.Handle(urlPattern, websocket.Handler(h))
	return nil
}

/* handler that proxies websocket <-> unix domain socket */
func WebsocketTcpProxy(ws *websocket.Conn, nettype string, host string) error {
	client, err := net.Dial(nettype, host)
	if err != nil {
		return err
	}
	defer client.Close()
	defer ws.Close()
	chDone := make(chan bool)

	go func() {
		ioWsCopy(client, ws)
		chDone <- true
	}()
	go func() {
		ioCopyWs(ws, client)
		chDone <- true
	}()
	<-chDone
	return nil
}

/* start a listener that proxies websocket <-> tcp */
func ListenAndServeWebsocket(addr string) error {
	return http.ListenAndServe(addr, nil)
}

/* starts an HTTPS listener */
func ListenAndServeWebsocketSecure(addr string, cert string, key string) error {
	return http.ListenAndServeTLS(addr, cert, key, nil)
}

/* copy from websocket to writer, this copies the binary frames as is */
func ioCopyWs(src *websocket.Conn, dst io.Writer) (int, error) {
	var buffer []byte
	count := 0
	for {
		err := websocket.Message.Receive(src, &buffer)
		if err != nil {
			return count, err
		}
		n := len(buffer)
		count += n
		i, err := dst.Write(buffer)
		if err != nil || i < 1 {
			return count, err
		}
	}
}

/* copy from reader to websocket, this copies the binary frames as is */
func ioWsCopy(src io.Reader, dst *websocket.Conn) (int, error) {
	buffer := make([]byte, 2048)
	count := 0
	for {
		n, err := src.Read(buffer)
		if err != nil || n < 1 {
			return count, err
		}
		count += n
		err = websocket.Message.Send(dst, buffer[0:n])
		if err != nil {
			return count, err
		}
	}
}
