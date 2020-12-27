package zkwalker

import (
	"fmt"
	"github.com/go-zookeeper/zk"
	"log"
	"path"
	"time"
)

// ZkWalker encapsulates the internals of the zookeeper connection
type ZkWalker struct {
	conn zkClient
	auth string

	// TODO: custom output printer
	// TODO: custom logger
}

// Option represents a ZkWalker option
type Option func(z *ZkWalker)

// WithAuth adds digest ACL
func WithAuth(auth string) Option {
	return func(z *ZkWalker) {
		z.auth = auth
	}
}

// Connect instantiates ZkWalker and establishes the connection to the target zookeeper
func Connect(servers []string, options ...Option) (*ZkWalker, error) {
	walker := &ZkWalker{}
	for _, option := range options {
		option(walker)
	}

	// TODO: allow timeout customization
	conn, _, err := zk.Connect(servers, time.Second*10, zk.WithLogInfo(false))
	if err != nil {
		return nil, err
	}
	walker.conn = conn

	if len(walker.auth) > 0 {
		if err := walker.conn.AddAuth("digest", []byte(walker.auth)); err != nil {
			walker.conn.Close()
			return nil, err
		}
	}

	return walker, nil
}

// Close closes the connection with zookeeper
//
// ZkWalker is no longer usable after calling Close.
func (z *ZkWalker) Close() {
	z.conn.Close()
}

// Walk recursively walks the znode tree rooted at path
//
// Use znodeFn to process each znode, and childrenFn to decide which children to process.
func (z *ZkWalker) Walk(zpath string, znodeFn ZnodeProcessor, childrenFn ChildrenProcessor) error {
	skip, err := znodeFn(zpath, func() ([]byte, *zk.Stat, error) {
		return z.conn.Get(zpath)
	})
	if err != nil {
		return err
	}
	if skip == true {
		return nil
	}

	lsChildren, lsStat, lsErr := z.conn.Children(zpath)
	children, err := childrenFn(zpath, lsChildren, lsStat, lsErr)
	if err != nil {
		return err
	}

	for _, c := range children {
		if err := z.Walk(path.Join(zpath, c), znodeFn, childrenFn); err != nil {
			return err
		}
	}
	return nil
}

// ZnodeGetter is a func which reads a znode when called
type ZnodeGetter func() ([]byte, *zk.Stat, error)

// ZnodeProcessor is a function called for each znode in the tree.
//
// It can return skipChildren == true to indicate that the subtree shouldn't be processed further. Error != nil
// stops the processing and is bubbled up the call chain.
//
// znode is a lazy ZnodeGetter. It is lazy, so that potentially expensive reads are only performed when needed.
type ZnodeProcessor func(path string, znode ZnodeGetter) (skipChildren bool, err error)

// ChildrenProcessor takes a list of children of a znode and returns a list of children to recursively walk into.
//
//
// It decides which of the children, if any should be processed further. Error != nil stops the processing and is
// bubbled up the call chain.
type ChildrenProcessor func(path string, lsChildren []string, lsStat *zk.Stat, lsErr error) (children []string, err error)

// PrintZnodePath prints only the znode path, it doesn't get its content
func PrintZnodePath(path string, _ ZnodeGetter) (skipChildren bool, err error) {
	fmt.Printf("%s\n", path)
	return false, nil
}

// PrintZnodePathAndContent prints only znode path and its content
func PrintZnodePathAndContent(path string, znode ZnodeGetter) (skipChildren bool, err error) {
	fmt.Printf("%s\n", path)
	buf, _, err := znode()
	if err != nil {
		log.Printf("Can't get %s: %s\n", path, err)
		return true, nil
	}
	if len(buf) > 0 {
		fmt.Printf("\t%s\n", string(buf))
	}
	return false, nil
}

// AllChildren lists all children of the given znode, ignoring any errors
func AllChildren(path string, lsChildren []string, _ *zk.Stat, lsErr error) (children []string, err error) {
	if lsErr != nil {
		log.Printf("Can't list children of %s: %s\n", path, lsErr)
		return []string{}, nil
	}
	return lsChildren, nil
}
