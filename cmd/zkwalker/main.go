package main

import (
	"flag"
	"fmt"
	"github.com/igor-kupczynski/zkwalker/zkwalker"
	"log"
	"os"
	"strings"
)

var authFlag = flag.String("auth", "", "<username:password> to use as a digest ACL")
var rootFlag = flag.String("root", "/", "znode from which to start the walk")
var printContentFlag = flag.Bool("print", false, "print the znode content as string")

func main() {
	flag.Usage = func() {
		fmt.Fprintf(flag.CommandLine.Output(), "Usage of %s:\n", os.Args[0])
		flag.PrintDefaults()
		fmt.Fprintf(flag.CommandLine.Output(), "  connection-string")
		fmt.Fprintf(flag.CommandLine.Output(), "  \n\tcomma separated list of zookeeper servers to connect to: host1:port1,...,hostN:portN\n")
	}

	flag.Parse() // This os.Exit(2) in case of flag errors
	// We need to handle required param errors manually
	if flag.NArg() < 1 {
		fmt.Fprintf(flag.CommandLine.Output(), "missing required argument: connection-string\n")
		flag.Usage()
		os.Exit(2)
	}

	servers := strings.Split(flag.Arg(0), ",")

	if err := run(servers, *authFlag, *rootFlag, *printContentFlag); err != nil {
		log.Fatalf("Error: %v\n", err)
	}
}

func run(servers []string, auth string, root string, printContent bool) error {
	var opts []zkwalker.Option
	if len(auth) > 0 {
		opts = append(opts, zkwalker.WithAuth(auth))
	}

	walker, err := zkwalker.Connect(servers, opts...)
	if err != nil {
		return err
	}
	defer walker.Close()

	var nodeFn = zkwalker.PrintZnodePath
	if printContent {
		nodeFn = zkwalker.PrintZnodePathAndContent
	}

	if err := walker.Walk(root, nodeFn, zkwalker.AllChildren); err != nil {
		return err
	}

	return nil
}
