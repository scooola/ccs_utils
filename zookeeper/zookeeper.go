//Auther: scola
//Date: 2021/08/05 20:27
//Description:
//Σ(っ °Д °;)っ

package zookeeper

import (
	"fmt"
	"github.com/pochard/zkutils"
	"github.com/samuel/go-zookeeper/zk"
	"time"
)

type ZkClient struct {
	//zk host
	Host []string
	//listen zk path
	WatchPath []string
	//zk conn
	Conn *zk.Conn
	//watcher
	KeepWatcher *zkutils.KeepWatcher
	//TODO:权限认证,后序加入
	Auth string
	// zk event
	ZkEvent <-chan zk.Event
	//config data channel
	MsgChan chan map[string][]byte
}

func NewZkClient(host, watchPath []string, auth string) *ZkClient {
	ch := make(chan map[string][]byte, 5)
	return &ZkClient{
		Host:      host,
		WatchPath: watchPath,
		Auth:      auth,
		MsgChan:   ch,
	}
}

//连接zk并创建监听者
func (z *ZkClient) Connect() (err error) {
	z.Conn, z.ZkEvent, err = zk.Connect(z.Host, time.Second*5)
	if err != nil {
		fmt.Println("zk connect error!")
		return err
	}
	z.KeepWatcher = zkutils.NewKeepWatcher(z.Conn)
	return nil
}

//关闭连接
func (z *ZkClient) Close() {
	z.Conn.Close()
}

func (z *ZkClient) setWatch() {
	for _, path := range z.WatchPath {
		var tmpPath = path
		go z.KeepWatcher.WatchData(tmpPath, func(data []byte, err error) {
			if err != nil {
				fmt.Println("watch error:", err)
			}
			fmt.Printf("path: %s, value: %s\n", tmpPath, string(data))

			msg := make(map[string][]byte)
			msg[tmpPath] = data
			z.MsgChan <- msg
		})
	}
}

func (z *ZkClient) watchHandler() {
	for {
		select {
		case event := <-z.ZkEvent:
			switch event.Type {
			// node data change
			case zk.EventNodeDataChanged:
				//todo something
				fmt.Print("Zk data changed!\n")
			case zk.EventNodeDeleted:
				// todo something
				fmt.Print("Zk node deleted!\n")
			case zk.EventSession:
				// todo something
				fmt.Print("Got session event!\n")
			}
		}
	}
}

//get node data
func (z *ZkClient) GetNodeData(path string) ([]byte, error) {
	data, _, err := z.Conn.Get(path)
	return data, err
}

//listen zk connection chan
func (z *ZkClient) Watch() {
	z.setWatch()
}
