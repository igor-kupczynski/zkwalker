# zkwalker

ZkWalker is a recursive zookeeper znode tree walker.

It can be used as a command, or as a library from a go project.

## Use as a command

Install the command:
```sh
$ go get github.com/igor-kupczynski/zkwalker/cmd/zkwalker
```

Help:
```sh
$ zkwalker -help
Usage of zkwalker:
  -auth string
    	<username:password> to use as a digest ACL
  -print
    	print the znode content as string
  -root string
    	znode from which to start the walk (default "/")
  connection-string
	comma separated list of zookeeper servers to connect to: host1:port1,...,hostN:portN
```

Print znode tree with the content of each of the znodes:
```sh
$ zkwalker -auth root:password -print -root /v1 1.2.3.4:2191
/v1
	{}
/v1/foo
	{}
/v1/foo/xyz
	{"xyz": true}
/v1/bar
	{}
/v1/bar/abc
	{"abc": true}
```

## Use as a library

The command [zkwalker](./cmd/zkwalker/main.go) offers a usage example.

Import the package:
```go
import "github.com/igor-kupczynski/zkwalker/zkwalker"
```

Connect to your zookeeper ensemble:
```go
walker, err := zkwalker.Connect(servers, ..)
```

Call `Walk(...)`. `ZnodeProcessor` is a function called on each visited znode, while `ChildrenProcessor` allows customising which children to visit.

```go
err := walker.Walk(root, nodeFn, zkwalker.AllChildren)
```
