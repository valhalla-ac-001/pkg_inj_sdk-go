package main

import (
	"context"
	"fmt"

	"github.com/InjectiveLabs/sdk-go/client/common"
	tmclient "github.com/InjectiveLabs/sdk-go/client/tm"
	"github.com/sirupsen/logrus"
)

func main() {
	network := common.LoadNetwork("testnet", "lb")
	tmClient := tmclient.NewRPCClient(network.TmEndpoint, logrus.New())
	clientCtx, cancelFn := context.WithCancel(context.Background())
	defer cancelFn()

	res, err := tmClient.GetBlock(clientCtx, 15478013)
	if err != nil {
		fmt.Println(err)
	}

	fmt.Println(res.Block.Txs)

}
