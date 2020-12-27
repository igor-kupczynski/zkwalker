package zkwalker

import "github.com/go-zookeeper/zk"

// zkClient encapsulates zookeeper operations so they can be mocked in tests
type zkClient interface {
	AddAuth(scheme string, auth []byte) error
	Get(path string) ([]byte, *zk.Stat, error)
	Children(path string) ([]string, *zk.Stat, error)
	Close()
}
