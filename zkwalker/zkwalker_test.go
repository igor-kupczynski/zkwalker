package zkwalker

import (
	"fmt"
	"github.com/go-zookeeper/zk"
	"reflect"
	"testing"
)

func TestZkWalker_Walk(t *testing.T) {
	type args struct {
		zpath      string
		znodeFn    ZnodeProcessor
		childrenFn ChildrenProcessor
	}
	tests := []struct {
		name    string
		conn    zkClient
		args    args
		wantGet []string
		wantLs  []string
		wantErr bool
	}{
		{
			name: "should walk the tree depth first",
			conn: newMemZkClient().
				node("/", "", "foo").
				node("/foo", "", "bar", "baz").
				node("/foo/bar", "", "aaa", "bbb").
				node("/foo/bar/aaa", "").
				node("/foo/bar/bbb", "").
				node("/foo/baz", "", "xxx", "yyy").
				node("/foo/baz/xxx", "").
				node("/foo/baz/yyy", ""),
			args: args{
				zpath:      "/",
				znodeFn:    noopZnodeProcessor,
				childrenFn: AllChildren,
			},
			wantGet: nil, // znode-get is lazy
			wantLs: []string{
				"/",
				"/foo",
				"/foo/bar", "/foo/bar/aaa", "/foo/bar/bbb",
				"/foo/baz", "/foo/baz/xxx", "/foo/baz/yyy",
			},
			wantErr: false,
		},
		{
			name: "should get the znode details if requested the tree depth first",
			conn: newMemZkClient().
				node("/", "", "foo").
				node("/foo", "", "bar").
				node("/foo/bar", "", "aaa").
				node("/foo/bar/aaa", ""),
			args: args{
				zpath:      "/",
				znodeFn:    getZnodeProcessor,
				childrenFn: AllChildren,
			},
			wantGet: []string{"/", "/foo", "/foo/bar", "/foo/bar/aaa"},
			wantLs:  []string{"/", "/foo", "/foo/bar", "/foo/bar/aaa"},
			wantErr: false,
		},
		{
			name: "should walk only requested children",
			conn: newMemZkClient().
				node("/", "", "foo").
				node("/foo", "", "bar").
				node("/foo/bar", "", "aaa").
				node("/foo/bar/aaa", ""),
			args: args{
				zpath:      "/",
				znodeFn:    getZnodeProcessor,
				childrenFn: noChildren,
			},
			wantGet: []string{"/"},
			wantLs:  []string{"/"},
			wantErr: false,
		},
		{
			name: "should walk children of not-skipped nodes only",
			conn: newMemZkClient().
				node("/", "", "foo").
				node("/foo", "", "bar").
				node("/foo/bar", "", "aaa").
				node("/foo/bar/aaa", ""),
			args: args{
				zpath:      "/",
				znodeFn:    skipNonRootZnodeProcessor,
				childrenFn: AllChildren,
			},
			wantGet: []string{"/", "/foo"},
			wantLs:  []string{"/"},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			z := &ZkWalker{
				conn: tt.conn,
			}
			if err := z.Walk(tt.args.zpath, tt.args.znodeFn, tt.args.childrenFn); (err != nil) != tt.wantErr {
				t.Errorf("Walk() error = %v, wantErr %v", err, tt.wantErr)
			}
			mem, _ := z.conn.(*memZkClient)
			if !reflect.DeepEqual(mem.visitedGet, tt.wantGet) {
				t.Errorf("Walk() visitedGet = %v, wantGet %v", mem.visitedGet, tt.wantGet)
			}
			if !reflect.DeepEqual(mem.visitedLs, tt.wantLs) {
				t.Errorf("Walk() visitedLs = %v, wantLs %v", mem.visitedLs, tt.wantLs)
			}
		})
	}
}

type memZkClient struct {
	znodes     map[string][]byte
	children   map[string][]string
	visitedGet []string
	visitedLs  []string
}

func newMemZkClient() *memZkClient {
	return &memZkClient{
		znodes:   make(map[string][]byte),
		children: make(map[string][]string),
	}
}

func (m *memZkClient) node(path string, content string, children ...string) *memZkClient {
	m.znodes[path] = []byte(content)
	var xs []string
	for _, x := range children {
		xs = append(xs, x)
	}
	m.children[path] = xs
	return m
}

func (m *memZkClient) Get(path string) ([]byte, *zk.Stat, error) {
	m.visitedGet = append(m.visitedGet, path)
	content, ok := m.znodes[path]
	if !ok {
		return nil, nil, fmt.Errorf("mem: non-existing znode %s", path)
	}
	return content, nil, nil
}

func (m *memZkClient) Children(path string) ([]string, *zk.Stat, error) {
	m.visitedLs = append(m.visitedLs, path)
	children, ok := m.children[path]
	if !ok {
		return nil, nil, fmt.Errorf("mem: non-existing znode %s", path)
	}
	return children, nil, nil
}

func (m *memZkClient) AddAuth(_ string, _ []byte) error {
	panic("not implemented")
}

func (m *memZkClient) Close() {
	panic("not implemented")
}

func noopZnodeProcessor(_ string, _ ZnodeGetter) (skipChildren bool, err error) {
	return false, nil
}

func getZnodeProcessor(_ string, node ZnodeGetter) (skipChildren bool, err error) {
	_, _, _ = node()
	return false, nil
}

func skipNonRootZnodeProcessor(path string, node ZnodeGetter) (skipChildren bool, err error) {
	_, _, _ = node()
	if path != "/" {
		return true, nil
	}
	return false, nil
}

func noChildren(_ string, _ []string, _ *zk.Stat, _ error) (children []string, err error) {
	return []string{}, nil
}
