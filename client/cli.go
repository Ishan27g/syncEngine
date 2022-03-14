// wget https://raw.githubusercontent.com/urfave/cli/master/autocomplete/zsh_autocomplete ; PROG=client source zsh_autocomplete; rm zsh_autocomplete
package main

import (
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"

	"github.com/urfave/cli/v2"
)

var Peers = map[string]string{
	"Leader-1": "3101",
	"Peer-A_1": "3102",
	"Peer-B_1": "3103",
	"Leader-2": "3106",
	//"Peer-A_2": "3202",
	//"Peer-B_2": "3203",
	"Leader_3": "3301",
	//"Peer-A_3": "3302",
	//"Peer-B_3": "3303",
}
var base = "http://localhost:"
var engine = "/engine"
var sendGossip = "/gossip/"
var details = "/whoAmI"
var snapshot = "/packet/data"

var send = func(url string) error {
	method := "GET"

	client := &http.Client{}
	req, err := http.NewRequest(method, url, nil)

	if err != nil {
		fmt.Println(err)
		return err
	}
	res, err := client.Do(req)
	if err != nil {
		fmt.Println(err)
		return err
	}
	defer res.Body.Close()

	body, err := ioutil.ReadAll(res.Body)
	if err != nil {
		fmt.Println(err)
		return err
	}
	fmt.Println(string(body))
	return err
}
var printPeers = func() {
	for name := range Peers {
		println(name)
	}
}
var getPeer = func(c *cli.Context) (string, bool) {
	var port string
	for name, p := range Peers {
		if c.Args().First() == name {
			port = p
		}
	}
	return port, port != ""
}

var detailsCmd = cli.Command{
	Name:            "details",
	Aliases:         []string{"d"},
	Usage:           "details",
	ArgsUsage:       "client details [port]",
	HideHelp:        false,
	HideHelpCommand: false,
	Action: func(c *cli.Context) error {

		if c.Args().Len() == 0 {
			printPeers()
			return cli.Exit("port not provided", 1)
		}

		port, valid := getPeer(c)
		if !valid {
			printPeers()
			return cli.Exit("invalid Peer", 1)
		}

		url := base + port + engine + details
		return send(url)
	},
}
var snapshotCmd = cli.Command{
	Name:            "snapshot",
	Aliases:         []string{"s"},
	Usage:           "snapshot",
	ArgsUsage:       "client snapshot [port]",
	HideHelp:        false,
	HideHelpCommand: false,
	Action: func(c *cli.Context) error {

		if c.Args().Len() == 0 {
			printPeers()
			return cli.Exit("port not provided", 1)
		}

		port, valid := getPeer(c)
		if !valid {
			printPeers()
			return cli.Exit("invalid Peer", 1)
		}

		url := base + port + engine + snapshot
		return send(url)
	},
}
var gossipCmd = cli.Command{
	Name:            "gossip",
	Aliases:         []string{"g"},
	Usage:           "gossip",
	ArgsUsage:       "client gossip [port] [data]",
	HideHelp:        false,
	HideHelpCommand: false,
	Action: func(c *cli.Context) error {

		if c.Args().Len() == 0 {
			printPeers()
			return cli.Exit("port not provided", 1)
		}

		port, valid := getPeer(c)
		if !valid {
			printPeers()
			return cli.Exit("invalid Peer", 1)
		}

		url := base + port + engine + sendGossip + c.Args().Get(1)
		return send(url)
	},
}

func main() {
	app := &cli.App{
		HideHelp:             true,
		EnableBashCompletion: true,
		Commands: []*cli.Command{
			&detailsCmd, &snapshotCmd, &gossipCmd,
		},
	}

	err := app.Run(os.Args)
	if err != nil {
		log.Fatal(err)
	}
}
